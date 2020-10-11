package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	pb "my2pc/coordinatorrpc"
	"net"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	TxPhasePrepare = "Prepare"
	TxPhaseReady   = "Ready"
	TxPhaseCommit  = "Commit"
	TxPhaseAbort   = "Abort"
	// TxPhaseUnknown = "Known"  // crash recover

	TxAbort  = int32(0)
	TxCommit = int32(1)
)

type ValidateFunc = func(TxID string) bool
type ActionFunc = func(TxID string, CoordinatorID string)
type Decision = int32

type SingleTxCoordinator struct {
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

func (tc *SingleTxCoordinator) nofityTxInfo(TxID string, coorID string) {
	tc.TxID = TxID
	tc.CoordinatorID = coorID
}

func (tc *SingleTxCoordinator) startPrepare(TxID string, prepareChan chan<- string) {
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

func (tc *SingleTxCoordinator) startDecide(TxID string, decision int32) {
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

func (tc *SingleTxCoordinator) waitForReply(TxID string, prepareChan <-chan string, timer *time.Timer) Decision {
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

func (tc *SingleTxCoordinator) CommitTx(TxID string) {
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

}

func (tc *SingleTxCoordinator) Prepare(ctx context.Context, in *pb.PrepareRequest) (*pb.PrepareReply, error) {
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

func (tc *SingleTxCoordinator) Decide(ctx context.Context, in *pb.DecideRequest) (*pb.DecideReply, error) {

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

func (tc *SingleTxCoordinator) WithLogger(l *log.Logger) *SingleTxCoordinator {
	tc.logger = l
	return tc
}

// runCoordinator Create and run grpc server.
func (tc *SingleTxCoordinator) runCoordinator(ctx context.Context) {
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
		tc.logger.Print("stop grpc server")
		gs.GracefulStop()
	}
}

func NewSingleTxCoordinator(clusterInfo map[string]string, selfID string, infoChan chan<- TxMsg) *SingleTxCoordinator {
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

	txc := SingleTxCoordinator{
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

func RecoverTxCoordinator(clusterInfo map[string]string, selfID string, infoChan chan<- TxMsg, TxInfo TxMsg) *SingleTxCoordinator {
	tc := NewSingleTxCoordinator(clusterInfo, selfID, infoChan)
	tc.TxID = TxInfo.TxID
	tc.TxPhase = TxInfo.Phase
	tc.CoordinatorID = TxInfo.CoordinatorID
	return tc
}

type TxMsg struct {
	TxID          string
	CoordinatorID string
	Phase         string
}

type TxStablizer interface {
	Stablize(txlog TxMsg) error
}

// SingleTxManager WAL logger
type SingleTxManager struct {
	stabilizer []TxMsg
	sm         map[string]*TxMsg // TxID -> StateSeq

	tc       *SingleTxCoordinator
	listener *net.Listener

	logger     *log.Logger
	TxProgress <-chan TxMsg
	quitC      chan struct{}
}

func (tm *SingleTxManager) WithCoordinator(tc *SingleTxCoordinator) *SingleTxManager {
	tm.tc = tc
	return tm
}

func (tm *SingleTxManager) WithLogger(l *log.Logger) *SingleTxManager {
	tm.logger = l
	return tm
}

func (tm *SingleTxManager) record(msg TxMsg) *SingleTxManager {
	tm.stabilizer = append(tm.stabilizer, msg)
	tm.logger.Printf("log: %v \n", msg)
	return tm
}

func (tm *SingleTxManager) run() {
	//tm.logger.Printf("running\n")
	quit := false
	ctx, cancelF := context.WithCancel(context.Background())
	go tm.tc.runCoordinator(ctx)
	for {
		select {
		case msg := <-tm.TxProgress:
			tm.record(msg)
			if msg.Phase == TxPhaseAbort || msg.Phase == TxPhaseCommit {
				tm.sm[tm.tc.TxID] = &TxMsg{
					TxID: tm.tc.TxID,
					Phase: msg.Phase,
					CoordinatorID: msg.CoordinatorID,
				}
				quit = true
			}
		case _ = <-tm.quitC:
			quit = true
		}
		if quit {
			break
		}
	}
	// stop runCoordinator and other
	cancelF()
	//tm.logger.Printf("done. statemachine = %v", tm.sm[tm.tc.ServerID])
}

func NewSingleTxManager(infoChan <-chan TxMsg, quitC chan struct{}) *SingleTxManager {
	tm := SingleTxManager{
		TxProgress: infoChan,
		stabilizer: make([]TxMsg, 0, 128),
		sm:         make(map[string]*TxMsg),
		quitC:      quitC,
		tc:         nil,
		listener:   nil,
	}
	return &tm
}

type Cluster struct {
	nodes         map[string]*SingleTxManager
	coordinatorID string

	wg *sync.WaitGroup
}

func NewAddrs(MemberID []string) map[string]string {
	clusterMapper := map[string]string{}
	startPort := 8880
	for i, id := range MemberID {
		clusterMapper[id] = "localhost:" + strconv.Itoa(startPort+i)
	}
	return clusterMapper
}

func NewCluster(MemberID []string) Cluster {
	n := len(MemberID)
	clusterMapper := NewAddrs(MemberID)
	for k, v := range clusterMapper {
		println(k, v)
	}

	var wg sync.WaitGroup
	cluster := make(map[string]*SingleTxManager, n)

	globalLogger := zap.NewExample()

	for _, selfID := range MemberID {
		infoChan := make(chan TxMsg, 64)
		quitChan := make(chan struct{}, 0) // sync

		localLogger := globalLogger.With(
			zap.Namespace(fmt.Sprintf("Node-%s", selfID)))
		fakeLogger := zap.NewStdLog(localLogger)

		tc := NewSingleTxCoordinator(clusterMapper, selfID, infoChan).
			WithLogger(fakeLogger)

		tm := NewSingleTxManager(infoChan, quitChan).
			WithCoordinator(tc).
			WithLogger(fakeLogger)

		cluster[selfID] = tm
	}

	c := Cluster{
		nodes:         cluster,
		coordinatorID: "",
		wg:            &wg,
	}
	return c
}

func (c *Cluster) WithNewTx(TxID string, coordinatorID string) *Cluster {
	c.coordinatorID = coordinatorID
	for _, tm := range c.nodes {
		tm.tc.nofityTxInfo(TxID, coordinatorID)
	}
	return c
}

func (c *Cluster) TwoPhaseCommit(TxID string) {
	for _, tm := range c.nodes {
		c.wg.Add(1)
		go func(wGroup *sync.WaitGroup, manager *SingleTxManager) {
			manager.run()
			wGroup.Done()
		}(c.wg, tm)
	}
	fmt.Println("warm up ...")
	time.Sleep(2 * time.Second)
	go c.nodes[c.coordinatorID].tc.CommitTx(TxID)
	c.wg.Wait()
}

func main() {
}
