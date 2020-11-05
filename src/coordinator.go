package main

import (
	"context"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"log"
	pb "my2pc/coordinatorrpc"
	"net"
	"time"
)

type ValidateFunc = func(TxID string) bool
type ActionFunc = func(TxID string, CoordinatorID string)
type Decision = int32

type TxCoordinator struct {
	Cluster  map[string]string
	ServerID string

	TxID          string
	TxPhase       string
	CoordinatorID string

	ValidatePred ValidateFunc
	ReadyFunc    ActionFunc
	CommitFunc   ActionFunc
	AbortFunc    ActionFunc

	logger *log.Logger
}

func (tc *TxCoordinator) nofityTxInfo(TxID string, coorID string) {
	tc.TxID = TxID
	tc.CoordinatorID = coorID
}

func (tc *TxCoordinator) startPrepare(TxID string, prepareChan chan<- string) {
	for _, ap := range tc.Cluster {
		go func(addrPort string) {
			PrepareRequest := pb.PrepareRequest{
				TxID:   TxID,
				FromID: tc.ServerID,
				ToID:   addrPort,
			}
			conn, err := grpc.Dial(addrPort, grpc.WithInsecure())
			if err != nil {
				tc.logger.Printf("Prepare phase, failed to connect %v\n. err = %s", addrPort, err.Error())
				return
			}
			defer conn.Close()
			client := pb.NewCoordinatorClient(conn)
			rep, err := client.Prepare(context.Background(), &PrepareRequest)
			if err != nil {
				tc.logger.Printf("Prepare phase, failed to get reply %v\n, err = %s", addrPort, err.Error())
				return
			} else {
				tc.logger.Printf("got reply %v from %s\n", rep, addrPort)
			}
			if rep.WillToCommit {
				prepareChan <- addrPort
			}
		}(ap)
	}
}

func (tc *TxCoordinator) startDecide(TxID string, decision int32) {
	for _, ap := range tc.Cluster {
		go func(addrPort string) {
			DecideRequest := pb.DecideRequest{
				TxID:     TxID,
				FromID:   tc.ServerID,
				ToID:     addrPort,
				Decision: decision,
			}
			conn, err := grpc.Dial(addrPort, grpc.WithInsecure())
			if err != nil {
				tc.logger.Printf("Decide phase, failed to connect %v\n. err = %s", addrPort, err.Error())
				return
			}
			defer conn.Close()
			client := pb.NewCoordinatorClient(conn)
			_, err = client.Decide(context.Background(), &DecideRequest)
			if err != nil {
				tc.logger.Printf("Decide phase, failed to get reply %v\n. err = %s", addrPort, err.Error())
				return
			}
		}(ap)
	}
}

func (tc *TxCoordinator) waitForReply(TxID string, prepareChan <-chan string, timer *time.Timer) Decision {
	cnt := 0
	quit := false
	for {
		select {
		case <-timer.C:
			tc.logger.Print("Prepare phase timeout")
			quit = true
		case _ = <-prepareChan:
			cnt += 1
			if cnt >= len(tc.Cluster) { // receive all reply
				quit = true
			}
		}
		if quit {
			break
		}
	}

	decision := TxCommit
	if cnt != len(tc.Cluster) {
		decision = TxAbort
	}
	// TODO: coordinator should stabilize this decision

	return decision
}

func (tc *TxCoordinator) CommitTx(TxID string) Decision{
	if tc.CoordinatorID != tc.ServerID {
		tc.logger.Panic()
	}

	// 1. TxCoordinator broadcast PrepareT msg
	// 2. TxCoordinator on a backup use SafePredicate() to decide whether accepting or aborting.
	// 	  when it make decision, it log a <Ready T> and then tells TxManager on the same backup.
	//    The latter will lock the Tx.
	//	  TxCoordinator reply
	// 3. Leader receive all ack and then log <Commit T>. Otherwise the leader will log <Abort T>.
	//	  Leader broadcast Decide Msg with commit/abort T decision.
	// 4. Backups receive the MSG, log <Commit T>/<Abort T>  execute it.
	// 5. TxManager release lock on the Tx.

	prepareChan := make(chan string, len(tc.Cluster))

	// TODO: filtering duplicate replies
	go tc.startPrepare(TxID, prepareChan)

	timer := time.NewTimer(time.Duration(2000) * time.Millisecond)
	decision := tc.waitForReply(TxID, prepareChan, timer)
	tc.logger.Printf("leader made decision: %v", decision)
	tc.startDecide(TxID, decision)
	return decision
}

// Prepare Process prepare phase rpc request
func (tc *TxCoordinator) Prepare(ctx context.Context, in *pb.PrepareRequest) (*pb.PrepareReply, error) {
	if in.TxID != tc.TxID {
		return nil, errors.New("incompatible TxID")
	}

	reply := pb.PrepareReply{
		TxID:         tc.TxID,
		FromID:       tc.ServerID,
		ToID:         in.FromID,
		WillToCommit: true,
	}
	// duplicate request
	if tc.TxPhase == TxPhaseReady {
		return &reply, nil
	}

	reply.WillToCommit = tc.ValidatePred(in.TxID)
	tc.ReadyFunc(tc.TxID, tc.CoordinatorID)
	tc.TxPhase = TxPhaseReady
	return &reply, nil
}

// Decide Process decide phase rpc request.
func (tc *TxCoordinator) Decide(ctx context.Context, in *pb.DecideRequest) (*pb.DecideReply, error) {

	if in.TxID != tc.TxID {
		return nil, errors.New(fmt.Sprintf("incompatible TxID %s ", in.TxID))
	}

	reply := pb.DecideReply{
		TxID:   tc.TxID,
		FromID: tc.ServerID,
		ToID:   in.FromID,
	}
	switch tc.TxPhase {
	case TxPhaseCommit, TxPhaseAbort:
		// duplicate
		return &reply, nil
	case TxPhasePrepare:
		// haven't receive Prepare request -> leader must abort the tx.
		tc.AbortFunc(tc.TxID, tc.CoordinatorID)
		tc.TxPhase = TxPhaseAbort
		return nil, nil // TODO: New filed indicating the state ?
	default:
		switch in.Decision {
		case TxCommit:
			tc.CommitFunc(tc.TxID, tc.CoordinatorID)
			tc.TxPhase = TxPhaseCommit
		case TxAbort:
			tc.AbortFunc(tc.TxID, tc.CoordinatorID)
			tc.TxPhase = TxPhaseAbort
		}
	}
	return &reply, nil
}

func (tc *TxCoordinator) WithLogger(l *log.Logger) *TxCoordinator {
	tc.logger = l
	return tc
}

// runCoordinator Create and run grpc server.
func (tc *TxCoordinator) runCoordinator(ctx context.Context) {
	fmt.Printf("coordinator %s running\n", tc.ServerID)
	addrPort, _ := tc.Cluster[tc.ServerID]
	lis, err := net.Listen("tcp", addrPort)
	if err != nil {
		tc.logger.Panic()
	}
	gs := grpc.NewServer(grpc.ConnectionTimeout(500 * time.Millisecond))
	pb.RegisterCoordinatorServer(gs, tc)
	go func() {
		if err := gs.Serve(lis); err != nil {
			tc.logger.Print(err)
		}
	}()

	select {
	case <-ctx.Done():
		fmt.Printf("stop sm %s\n", tc.ServerID)
		gs.GracefulStop()
	}
}

func NewTxCoordinator(clusterInfo map[string]string, selfID string, infoChan chan<- TxMsg) *TxCoordinator {
	validP := func(TxID string) bool {
		return true
	}

	funcFatory := func(Phase string) ActionFunc {
		return func(TxID string, CoordinatorID string) {
			infoChan <- TxMsg{
				TxID:          TxID,
				CoordinatorID: CoordinatorID,
				Phase:         Phase,
			}
		}
	}

	txc := TxCoordinator{
		TxID:          "",
		TxPhase:       TxPhasePrepare,
		Cluster:       clusterInfo,
		ServerID:      selfID,
		CoordinatorID: "",
		ValidatePred:  validP,
		ReadyFunc:     funcFatory(TxPhaseReady),
		CommitFunc:    funcFatory(TxPhaseCommit),
		AbortFunc:     funcFatory(TxPhaseAbort),
	}

	return &txc
}

func RecoverTxCoordinator(clusterInfo map[string]string, selfID string, infoChan chan<- TxMsg, TxInfo TxMsg) *TxCoordinator {
	tc := NewTxCoordinator(clusterInfo, selfID, infoChan)
	tc.TxID = TxInfo.TxID
	tc.TxPhase = TxInfo.Phase
	tc.CoordinatorID = TxInfo.CoordinatorID
	return tc
}
