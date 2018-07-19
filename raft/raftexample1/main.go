package main

import (
	"github.com/coreos/etcd/raft"
	"fmt"
	"time"
	//"context"
	"github.com/coreos/etcd/raft/raftpb"
	"context"
)

func main() {

	//用于日志保存
	storage := raft.NewMemoryStorage()
	c := &raft.Config{
		ID:              0x01,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   4096,
		MaxInflightMsgs: 256,
	}
	peers := []raft.Peer{{ID: 0x01}}
	n := raft.StartNode(c, peers)

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	go func(){
		for {
			select {
			case <- ticker.C:
				n.Tick()
			case readyc := <- n.Ready():
				//必须触发保存，底层会检测日志信息，不append会出现panic问题
				storage.Append(readyc.Entries)

				//TODO 多节点需要实现发送消息的逻辑

				//处理已提交的日志
				for _, ent := range readyc.CommittedEntries {
					fmt.Println("commit entry:", string(ent.Data))
					switch ent.Type {
					//应用层数据变更
					case raftpb.EntryNormal:
						//应用到状态机
						fmt.Println("ent:", ent)

					//raft节点变更
					case raftpb.EntryConfChange:
						var cc raftpb.ConfChange
						cc.Unmarshal(ent.Data)
						n.ApplyConfChange(cc)
					}
				}
				n.Advance()
			}
		}
	}()
	n.Propose(context.TODO(), []byte("hello world"))

	time.Sleep(10 * time.Second)
}
