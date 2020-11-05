package main

import (
	"context"
	"fmt"
	"log"
	"net"
)

// TxManager WAL logger
type TxManager struct {
	id string
	stabilizer []TxMsg
	sm         map[string]*TxMsg // TxID -> StateSeq
	sto Storage

	tc       *TxCoordinator
	listener *net.Listener

	logger     *log.Logger
	TxProgress <-chan TxMsg
	quitC      chan TxManagerAction
}

func (tm *TxManager) WithCoordinator(tc *TxCoordinator) *TxManager {
	tm.tc = tc
	return tm
}

func (tm *TxManager) WithStableStorage(sto Storage) *TxManager{
	tm.sto = sto
	return tm
}

func (tm *TxManager) WithLogger(l *log.Logger) *TxManager {
	tm.logger = l
	return tm
}

func (tm *TxManager) WithName(name string) *TxManager{
	tm.id = name
	return tm
}

func (tm *TxManager) record(msg TxMsg) *TxManager {
	tm.stabilizer = append(tm.stabilizer, msg)
	tm.logger.Printf("log: %v \n", msg)
	return tm
}

func (tm *TxManager) run(ctx context.Context) {
	//tm.logger.Printf("running\n")
	quit := false
	coorCtx, coorCal := context.WithCancel(ctx)
	go tm.tc.runCoordinator(coorCtx)
	defer coorCal()

	tm.sm[tm.tc.TxID] = &TxMsg{
		TxID: tm.tc.TxID,
		Phase: TxPhasePrepare,
		CoordinatorID: tm.tc.CoordinatorID,
	}

	for {
		select {
		case msg := <-tm.TxProgress:
			tm.record(msg)
			if msg.Phase == TxPhaseAbort || msg.Phase == TxPhaseCommit {
				tm.sm[tm.tc.TxID].Phase = msg.Phase
				quit = true
			}
		case <-ctx.Done():
			quit = true
		}
		if quit {
			break
		}
	}

	fmt.Printf("manager %s done\n", tm.id)
}

func NewTxManager(infoChan <-chan TxMsg, quitC chan TxManagerAction) *TxManager {
	tm := TxManager{
		TxProgress: infoChan,
		stabilizer: make([]TxMsg, 0, 128),
		sm:         make(map[string]*TxMsg),
		quitC:      quitC,
		tc:         nil,
		listener:   nil,
	}
	return &tm
}

type TxManagerAction = string

const (
	ActionDown = "down"
)