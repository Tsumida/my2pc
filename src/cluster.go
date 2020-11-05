package main

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"strconv"
	"sync"
)

type NodeContext struct{
	Ctx context.Context
	CalFunc context.CancelFunc
}

type Cluster struct {
	ctx map[string]*NodeContext

	mx sync.Mutex

	nodes         map[string]*TxManager
	coordinatorID string
	mapper map[string]string

	wg *sync.WaitGroup
	log *zap.Logger
}

func (c *Cluster) DropAll(){
	c.mx.Lock()
	defer c.mx.Unlock()
	for _, c := range c.ctx{
		if c != nil{
			c.CalFunc()
		}
	}
}

func (c *Cluster) Drop(id string){
	c.mx.Lock()
	defer c.mx.Unlock()
	node, ok := c.ctx[id]
	if ok && node != nil{
		node.CalFunc()
	}
	c.ctx[id] = nil
}

func (c *Cluster) Up(NewNodesID []string){
	for _, crashNode := range NewNodesID{
		infoChan := make(chan TxMsg, 64)
		quitChan := make(chan TxManagerAction, 1) // sync

		c.mx.Lock()
		localLogger := c.log.With(
			zap.Namespace(fmt.Sprintf("Node-%s", crashNode)))
		fakeLogger := zap.NewStdLog(localLogger)

		tc := NewTxCoordinator(c.mapper,  crashNode, infoChan).
			WithLogger(fakeLogger)

		tm := NewTxManager(infoChan, quitChan).
			WithCoordinator(tc).
			WithLogger(fakeLogger).
			WithStableStorage(&LocalSto{filename:  crashNode, file: nil})

		c.nodes[crashNode] = tm
		c.mx.Unlock()
	}
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
	cluster := make(map[string]*TxManager, n)
	globalLogger := zap.NewExample()

	for _, selfID := range MemberID {
		infoChan := make(chan TxMsg, 64)
		quitChan := make(chan TxManagerAction, 1) // sync

		localLogger := globalLogger.With(
			zap.Namespace(fmt.Sprintf("Node-%s", selfID)))
		fakeLogger := zap.NewStdLog(localLogger)

		tc := NewTxCoordinator(clusterMapper, selfID, infoChan).
			WithLogger(fakeLogger)

		tm := NewTxManager(infoChan, quitChan).
			WithName(selfID).
			WithCoordinator(tc).
			WithLogger(fakeLogger).
			WithStableStorage(&LocalSto{filename: selfID, file: nil})

		cluster[selfID] = tm
	}

	c := Cluster{
		nodes:         cluster,
		coordinatorID: "",
		wg:            &wg,
		log: 			globalLogger,
		mapper: clusterMapper,
		ctx: make(map[string]*NodeContext),
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

func (c *Cluster) WithContext() *Cluster{
	for k, _ := range c.nodes{
		ctx, cal := context.WithCancel(context.Background())
		c.ctx[k] = &NodeContext{
			Ctx: ctx,
			CalFunc: cal,
		}
	}
	return c
}

func (c *Cluster) RunCluster(){
	c.mx.Lock()
	defer c.mx.Unlock()
	c.WithContext()
	for id, tm := range c.nodes {
		go tm.run(c.ctx[id].Ctx)
	}
	fmt.Println("cluster up ...")
}

func (c *Cluster) StartCommit(TxID string) Decision{
	return c.nodes[c.coordinatorID].tc.CommitTx(TxID)
}