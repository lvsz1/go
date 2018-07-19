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
	"github.com/coreos/etcd/wal"
	"os"
	"github.com/coreos/etcd/wal/walpb"
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

	//初始化wal
	wal, err := getWal(*id)
	if err != nil {
		fmt.Println("getWal error:", err)
	}
	//加载wal日志
	err = loadWal(wal, storage)
	if err != nil {
		fmt.Println("loadWal error:", err)
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	go func(){
		for {
			select {
			case <- ticker.C:
				n.Tick()
			case readyc := <- n.Ready():
				//保存wal日志
				wal.Save(readyc.HardState, readyc.Entries)

				//必须触发保存，底层会检测日志信息，不append会出现panic问题
				storage.Append(readyc.Entries)
				//处理消息传输
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
				//这里必须调用Advance，告知raft底层上次的readyc已经处理完，不调用不会重新构建新的readyc
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

//先不添加快照
func getWal(id int )(*wal.WAL, error){
	waldir := fmt.Sprintf("raftexample-%d", id)
	if !wal.Exist(waldir) {
		if err := os.Mkdir(waldir, 0750); err != nil {
			log.Fatalf("raftexample: cannot create dir for wal (%v)", err)
		}

		w, err := wal.Create(zap.NewExample(), waldir, nil)
		if err != nil {
			log.Fatalf("raftexample: create wal error (%v)", err)
		}
		return w, err
	}
	//每次打开index大于等于0的wal日志文件
	return wal.Open(zap.NewExample(), waldir, walpb.Snapshot{})
}

//加载wal日志
func loadWal(wal *wal.WAL, storage *raft.MemoryStorage) error {
	//读取文件中的日志记录
	_, st, ents, err := wal.ReadAll()
	if err != nil {
		log.Fatalf("raftexample: failed to read WAL (%v)", err)
		return err
	}
	//将服务器状态加载到storage
	err = storage.SetHardState(st)
	if err != nil {
		log.Fatalf("raftexample: failed to storage.SetHardState (%v)", err)
		return err
	}
	//将先前保存的日志加载到storage中，依次处理
	err = storage.Append(ents)
	if err != nil {
		log.Fatalf("raftexample: failed to storage.Append (%v)", err)
		return err
	}
	return err
}

