/**
参考 https://github.com/coreos/etcd/tree/master/contrib/raftexample，实现counter计数器
TODO：
1、当某个Follower节点收到Client请求时，把消息发送给Leader统一处理
2、. . .

 */

package main

import (
	"flag"
)

func main() {
	cluster := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	id := flag.Int("id", 1, "node ID")
	addr := flag.String("addr", "", "counter server addr")
	flag.Parse()

	//chan 用于应用层数据传递给raft底层数据
	proposeC := make(chan string)
	// chan 用户raft底层传递给应用层数据
	commitC := make(chan string)

	raftNode := NewRaftNode(uint64(*id), *cluster, proposeC, commitC)

	store.proposeC = proposeC
	store.commitC = commitC
	store.snapshot = raftNode.snapshotter

	// 数据交互 （raft -> 应用层状态机）
	go store.Run()

	CounterServe(*addr)
}




