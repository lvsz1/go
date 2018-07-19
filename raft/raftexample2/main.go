package main

import (
	"github.com/coreos/etcd/raft"
	"fmt"
	"time"
	"github.com/coreos/etcd/raft/raftpb"
	"context"
	"github.com/coreos/etcd/etcdserver/api/rafthttp"
	"go.uber.org/zap"
	"strconv"
	"github.com/coreos/etcd/pkg/types"
	stats "github.com/coreos/etcd/etcdserver/api/v2stats"
	"flag"
	"strings"
	"net/url"
	"log"
	"net/http"
	"net"
)

type raftNode struct {
	node raft.Node
	id int
}
func (r *raftNode) Process(ctx context.Context, m raftpb.Message) error {
	return r.node.Step(ctx, m)
}
func (r *raftNode) IsIDRemoved(id uint64) bool {
	return false
}
func (r *raftNode)  ReportUnreachable(id uint64){
}
func (r *raftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
}

func main() {
	cluster := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	id := flag.Int("id", 1, "node ID")
	flag.Parse()

	storage := raft.NewMemoryStorage()
	c := &raft.Config{
		ID:              uint64(*id),
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   4096,
		MaxInflightMsgs: 256,
	}

	peers := strings.Split(*cluster, ",")
	var rpeers []raft.Peer
	for i := range peers {
		rpeers = append(rpeers, raft.Peer{ID:uint64(i+1)})
	}
	n := raft.StartNode(c, rpeers)

	transport := &rafthttp.Transport{
		Logger:      zap.NewExample(),
		ID:          types.ID(*id),
		ClusterID:   0x1000,
		Raft:        &raftNode{node: n, id: *id},
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(strconv.Itoa(*id)),
		ErrorC:      make(chan error),
	}
	transport.Start()
	go raftServe(transport, peers[*id - 1])

	for i, v := range peers {
		if i+1 != *id {
			transport.AddPeer(types.ID(i+1), []string{v})
		}
	}

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

				fmt.Println("id:", *id, "write entrieds", readyc.Entries)
				transport.Send(readyc.Messages)

				//处理已提交的日志
				for _, ent := range readyc.CommittedEntries {
					fmt.Println("id:", *id, "commit entry:", string(ent.Data))
					switch ent.Type {
					//应用层数据变更
					case raftpb.EntryNormal:
						//应用到状态机
						fmt.Println("id:", *id, "ent:", ent)

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

func raftServe(transport *rafthttp.Transport, peer string) {
	raftUrl, err := url.Parse(peer)
	if err != nil {
		log.Fatalf("raftexample: Failed parsing URL (%v)", err)
	}
	ln, err := net.Listen("tcp", raftUrl.Host)
	if err != nil {
		log.Fatalf("raftexample: Failed to listen rafthttp (%v)", err)
	}
	err = (&http.Server{Handler: transport.Handler()}).Serve(ln)
	if err != nil {
		log.Fatalf("raftexample: Failed to serve rafthttp (%v)", err)
	}
}
