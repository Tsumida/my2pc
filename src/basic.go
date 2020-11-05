package main

const (
	TxPhasePrepare = "Prepare"
	TxPhaseReady   = "Ready"
	TxPhaseCommit  = "Commit"
	TxPhaseAbort   = "Abort"
	// TxPhaseUnknown = "Known"  // crash recover
)

// Decision
const (
	TxAbort  = int32(0)
	TxCommit = int32(1)
)

type TxMsg struct {
	TxID          string `json:"txid"`
	CoordinatorID string `json:"coordinatorID"`
	Phase         string `json:"phase"`
}