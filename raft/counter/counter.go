/**
该文件主要涉及应用层状态机，主要是：
1、指令的序列化与反序列化（raft底层根本不知道什么是应用层指令，只是在各个节点原封不动的传输应用层指令序列化后的数据）
2、应用层状态机的序列化与发序列化（主要用户快照的保存及恢复）
3、接收raft底层的提交信息，并应用到状态机
 */

package main

import (
	"sync"
	"encoding/json"
	"encoding/gob"
	"bytes"
	"errors"
	"github.com/coreos/etcd/etcdserver/api/snap"
	"fmt"
)

const (
	CmdIncrease = "increase"
	CmdDecrease = "decrease"
)

//指令
type Cmd struct{
	CmdType string 	`form:"cmd_type" json:"cmd_type"`
	Key string		`form:"key" json:"key"`
	Num int64		`form:"num" json:"num"`
}

//校验指令的类型是否正确
func (cmd *Cmd) TypeValid() bool {
	if len(cmd.Key) == 0 {
		return false
	}

	validTypes := []string{CmdIncrease, CmdDecrease}
	for _, t := range validTypes {
		if t == cmd.CmdType {
			return true
		}
	}
	return false
}

//指令序列化
func SerializeCmd(cmd *Cmd)([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(*cmd); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

//指令反序列化
func UnSerializeCmd(data []byte)(*Cmd, error){
	cmd := &Cmd{}
	dec := gob.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(cmd); err != nil {
		return nil, err
	}
	return cmd, nil
}

//计数器
type CounterStore struct {
	proposeC 	chan<-  string
	commitC 	<-chan string
	snapshot    *snap.Snapshotter
	mu 			sync.RWMutex
	store 		map[string]int64
}

func NewCounterStore() *CounterStore{
	return &CounterStore{
		store: make(map[string]int64),
	}
}

func (s *CounterStore) Get(key string)(int64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.store[key]
	return v, ok
}

//从快照中恢复应用层状态机数据，序列化方式应该与getSnapshot相同
func (s *CounterStore) RecoverFromSnapshot() error{
	snap, err := s.snapshot.Load()
	if err != nil {
		return nil
	}

	var store map[string]int64
	if err := json.Unmarshal(snap.Data, &store); err != nil {
		return err
	}
	s.mu.Lock()
	s.store = store
	s.mu.Unlock()
	return nil
}

//将应用层状态机序列化
func (s *CounterStore) GetSnapshot()([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.Marshal(s.store)
}

//指令分发
func (s *CounterStore) Dispatch(data []byte) error {
	cmd, err := UnSerializeCmd(data)
	if err != nil {
		return err
	}
	if !cmd.TypeValid() {
		return errors.New("cmd is not valid")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	switch cmd.CmdType {
	case CmdIncrease:
		if _, ok := s.store[cmd.Key]; ok {
			s.store[cmd.Key] += cmd.Num
		}else{
			s.store[cmd.Key] = cmd.Num
		}
	case CmdDecrease:
		if _, ok := s.store[cmd.Key]; ok {
			s.store[cmd.Key] -= cmd.Num
		}else{
			s.store[cmd.Key] = cmd.Num
		}
	}
	return nil
}

func (s *CounterStore) Run() {
	for {
		select {
		//将raft底层的提交应用到状态机
		case data := <- s.commitC:
			if data == RecoverSnapshot {
				//从快照中恢复应用层状态机数据
				s.RecoverFromSnapshot()
			}else{
				//分发应用层指令
				s.Dispatch([]byte(data))
			}
		}
	}
}




