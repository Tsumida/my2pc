package main

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func CheckTxWithExpectedState(c *Cluster, txid string, expectedState string) error{
	// all node should commit
	for _, node := range c.nodes{
		st, ok := node.sm[txid]
		if !ok{
			return errors.New(fmt.Sprintf("node %s didn't record txid = %s", node.tc.ServerID, txid))
		}
		if st == nil{
			return errors.New(fmt.Sprintf("node %s, empty log", node.tc.ServerID))
		}

		if st.TxID != txid || st.Phase != expectedState{
			return errors.New(fmt.Sprintf("incompatible tx (%s, %s)", st.TxID, st.Phase))
		}
	}
	return nil
}

func TestNonFaulty2pc(t *testing.T){
	c := NewCluster([]string{"0", "1", "2", "3"})
	c.WithNewTx("0123", "1").
		TwoPhaseCommit("0123")

	if err := CheckTxWithExpectedState(&c, "0123", TxPhaseCommit); err != nil{
		t.Fatal(err)
	}
}

func TestFollowerCrash(t *testing.T){
	c := NewCluster([]string{"0", "1", "2", "3"})

	crashNode := "2"

	go func(){
		c.nodes[crashNode].quitC <- struct{}{}
	}()

	done := make(chan struct{}, 1)
	go func(){
		c.WithNewTx("0123", "1").
			TwoPhaseCommit("0123")
		done <- struct{}{}
	}()

	select {
		case <- time.After(10 * time.Second):
			t.Fatal("timeout")
		case <- done:
			if err := CheckTxWithExpectedState(&c, "0123", TxPhaseAbort); err != nil{
				t.Fatal(err)
			}
	}

}