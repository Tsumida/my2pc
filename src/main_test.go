package main

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func CheckSubSetWithExpectedState(c *Cluster, nodes []string, txid string, expectedState string) error{
	// all node should commit
	for _, id := range nodes{
		node := c.nodes[id]
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

func CheckAllWithExpectedState(c *Cluster, txid string, expectedState string) error{
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
		RunCluster()
	// wait for grpc sever
	time.Sleep(2 * time.Second)

	if d := c.StartCommit("0123"); d != TxCommit{
			t.Fatalf("unexpected decision %v", d)
	}
	time.Sleep(1 * time.Second)
	c.DropAll()
	if err := CheckAllWithExpectedState(&c, "0123", TxPhaseCommit); err != nil{
		t.Fatal(err)
	}
}

func TestFollowerCrashInPrepare(t *testing.T){
	fmt.Printf("test: follower crashed in prepare phase")
	coorID := "1"
	member := []string{"0", "1", "2", "3"}
	crashID := []string{"0", "2"}
	c := NewCluster(member)

	done := make(chan Decision, 1)
	go func(){
		time.Sleep(100 * time.Millisecond)
		c.WithNewTx("0123", coorID).RunCluster()
		time.Sleep(2 * time.Second)
		d := c.StartCommit("0123")
		time.Sleep(2 * time.Second)
		done <- d
	}()

	// node start first.
	// node crash
	time.Sleep(500 * time.Millisecond)
	for _, crashNode := range crashID{
		c.Drop(crashNode)
	}

	select {
		case <- time.After(10 * time.Second):
			t.Fatal("timeout")
		case d := <- done:
			if d != TxAbort{
				t.Fatal()
			}
			if err := CheckSubSetWithExpectedState(&c, crashID, "0123", TxPhasePrepare); err != nil{
				t.Fatal(err)
			}
			if err := CheckSubSetWithExpectedState(&c, []string{coorID}, "0123", TxPhaseAbort); err != nil{
				t.Fatal(err)
			}
	}
}
