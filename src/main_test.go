package main

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
)

const (
	ETCD_SERVER_CLIENT_ADDR = "localhost:12379"
)

func NewClient() *clientv3.Client{
	c, err := clientv3.New(
		clientv3.Config{
			Endpoints: []string{
				ETCD_SERVER_CLIENT_ADDR,
			},
			DialTimeout: 5 * time.Second,
		})
	if err != nil{
		panic(err)
	}
	return c
}

func TestEtcdPutAndGet(t *testing.T) {
	t.Log("test: etcd connection")

	client := NewClient()

	defer client.Close()

	kv := clientv3.NewKV(client)
	_, err := kv.Put(context.TODO(), "hello/world", "miao")
	if err != nil {
		t.Fatal(err)
	}

	resp, err := kv.Get(context.TODO(), "hello/world")
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range resp.Kvs {
		t.Log(string(ev.Key), string(ev.Value))
	}

}


func TestEtcdWatch(t *testing.T){
	var wg sync.WaitGroup
	wg.Add(2)
	key := "hello/world"
	quitChan := make(chan struct{}, 1)
	go func(w *sync.WaitGroup){
		client:= NewClient()
		defer client.Close()
		kv := clientv3.NewKV(client)

		for _, v := range[]string{"a", "b", "c", "d"}{
			kv.Put(context.TODO(), key, v)
			time.Sleep(1 * time.Second)
		}

		quitChan <- struct{}{}
		w.Done()
	}(&wg)

	go func(w *sync.WaitGroup){
		client := NewClient()
		defer client.Close()
		watcher := clientv3.NewWatcher(client)
		ch := watcher.Watch(context.TODO(), key)
		breakFlag := false
		for {
			select {
			case <-quitChan:
				breakFlag = true
			case events := <-ch:
				for _, e := range events.Events{
					t.Logf("event type: %s, value = %v", e.Type, string(e.Kv.Value))
				}
			}

			if breakFlag{
				break
			}
		}

		w.Done()
	}(&wg)

	wg.Wait()

}