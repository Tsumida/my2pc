package main

import (
	"encoding/json"
	"os"
)

type Storage interface {
	Stablize(msg *TxMsg) error

	// Recover Transaction state from persistent storage.
	// The channel is closed after all data are loaded.
	Recover(chan TxMsg)
}

type LocalSto struct{
	filename string
	file *os.File
}

func (ls *LocalSto) Open(){
	f, err := os.Open(ls.filename)
	if err != nil{
		panic(err)
	}
	ls.file = f
}

func (ls *LocalSto) Stablize(msg *TxMsg) error{
	if msg == nil{
		return nil
	}
	if ls.file == nil{
		ls.Open()
	}
	enc := json.NewEncoder(ls.file)
	return enc.Encode(msg)
}

func (ls *LocalSto) Recover(ch chan TxMsg) {
	// no log.
	if _, err := os.Stat(ls.filename); os.IsNotExist(err){
		return
	}

	f, err := os.Open(ls.filename)
	if err != nil{
		panic(err)
	}
	defer f.Close()

	msg := TxMsg{}
	dec := json.NewDecoder(f)
	if err := dec.Decode(&msg); err != nil{
		panic(err)
	}
	ch <- msg
	close(ch)
}
