/**
使用者需要处理：
1、心跳处理；
2、raft节点的网络传输
3、wal日志
4、snapshot快照
5、应用层状态机与raft底层交互
 */

package main

import (
	"github.com/coreos/etcd/raft/raftpb"
	"context"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/etcdserver/api/rafthttp"
	"github.com/coreos/etcd/etcdserver/api/snap"
	"github.com/coreos/etcd/wal"
	"fmt"
	"os"
	"log"
	"go.uber.org/zap"
	"github.com/coreos/etcd/wal/walpb"
	"github.com/coreos/etcd/pkg/fileutil"
	"strings"
	"strconv"
	"github.com/coreos/etcd/pkg/types"
	stats "github.com/coreos/etcd/etcdserver/api/v2stats"
	"net/url"
	"net"
	"net/http"
	"time"
)

const SnapshotCount = 10000 //每隔SnapshotCount日志保存一次快照
const snapshotCatchUpEntriesN = 10000 //snapshot只保留最近的 snapshotCatchUpEntriesN 个日志，更小的日志要干掉

const RecoverSnapshot = "recover_snapshot_unique_string"  //告诉应用层从快照中恢复数据

//该结构体需要实现 rafthttp.Raft 接口
type RaftNode struct {
	id uint64 //节点id
	peers []string //raft 节点地址信息

	proposeC    <-chan string            // 用于客户端与raft底层交互
	confChangeC <-chan raftpb.ConfChange // raft节点变更时使用
	commitC     chan<- string           //raft底层提交信息

	node raft.Node //raft节点
	transport *rafthttp.Transport // raft网络传输，逻辑需要自己实现

	confState  raftpb.ConfState  //raft集群的节点信息

	storage *raft.MemoryStorage  //raft存储，逻辑需要自己处理
	snapshotter *snap.Snapshotter //raft 快照，逻辑需要自己实现
	wal *wal.WAL  // raft wal日志

	appliedIndex uint64 //已提交日志的index，主要用于判断提交信息以及是否保存快照等
	snapshotIndex uint64  //已经保存快照的最大snapshotIndex
}

//创建应用层raft节点
func NewRaftNode(id uint64, cluster string, proposeC <-chan string, commitC chan<- string) *RaftNode {
	peers := strings.Split(cluster, ",")
	r := &RaftNode{
		proposeC:    proposeC,
		confChangeC: make(chan raftpb.ConfChange),
		commitC:     commitC,

		id:          id,
		peers:       peers,
	}

	r.storage = raft.NewMemoryStorage()
	//获取快照，如果不存在快照目录，则创建
	r.getSnapshot()
	//根据快照保存最近的term/index，选择最近的wal日志，并加载快照
	r.getWal()
	//根据快照中的term/index加载日志，并提交
	r.loadWal()

	c := &raft.Config{
		ID:              id,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         r.storage,
		MaxSizePerMsg:   4096,
		MaxInflightMsgs: 256,
	}

	//启动raft节点
	var rpeers []raft.Peer
	for i := range peers {
		rpeers = append(rpeers, raft.Peer{ID:uint64(i+1)})
	}
	r.node = raft.StartNode(c, rpeers)

	//初始化网络配置
	r.transport = &rafthttp.Transport{
		Logger:      zap.NewExample(),
		ID:          types.ID(id),
		ClusterID:   0x1000,
		Raft:        r,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(strconv.FormatUint(id, 10)),
		ErrorC:      make(chan error),
	}
	r.transport.Start()
	// 添加其它节点的配置，尝试建立连接
	for i, v := range peers {
		if uint64(i+1) != id {
			r.transport.AddPeer(types.ID(i+1), []string{v})
		}
	}

	// raft底层节点监听
	go r.raftServe()
	//处理各种事件
	go r.work()

	return r
}

//RaftNode 定时器及监听数据变化进行处理等
func (r *RaftNode) work() {

	go func() {
		//处理客户端发送的数据，通过proposeC管道进行传输数据; 暂不支持客户端节点变更的请求
		for r.proposeC != nil {
			select {
			case data :=<- r.proposeC:
				r.node.Propose(context.Background(), []byte(data))
			}
		}
	}()

	ticker := time.NewTicker(100 * time.Millisecond)
	go func(){
		defer ticker.Stop()
		for {
			select {
			case <- ticker.C:
				//心跳处理，不同的角色有不同的动作： Follower、Candidate、Leader
				r.node.Tick()
			case readyc := <- r.node.Ready():
				//保存wal日志
				r.wal.Save(readyc.HardState, readyc.Entries)

				//接收到snapshot，需要做保存处理
				if !raft.IsEmptySnap(readyc.Snapshot) {
					r.saveSnap(readyc.Snapshot)
					r.storage.ApplySnapshot(readyc.Snapshot)
					r.publishSnapshot(readyc.Snapshot)
				}

				//必须触发保存，底层会检测日志信息，不append会出现panic问题
				r.storage.Append(readyc.Entries)
				//处理消息传输
				r.transport.Send(readyc.Messages)

				//处理已提交的日志
				for _, ent := range readyc.CommittedEntries {
					//过滤掉小于r.appliedIndex 的日志（raft日志的index是递增的）
					if ent.Index <= r.appliedIndex {
						continue
					}

					switch ent.Type {
					//应用层数据变更
					case raftpb.EntryNormal:
						//应用到状态机
						r.commitC <- string(ent.Data)
						r.appliedIndex = ent.Index

						//raft节点变更
					case raftpb.EntryConfChange:
						var cc raftpb.ConfChange
						cc.Unmarshal(ent.Data)
						r.confState = *r.node.ApplyConfChange(cc)
					}
					//更新r.appliedIndex
					r.appliedIndex = ent.Index

					//判断是否保存快照
					if r.appliedIndex - r.snapshotIndex >= SnapshotCount {
						r.saveCounterSnapshot()
						r.snapshotIndex = r.appliedIndex
						//保存该次快照时的appliedIndex
						r.snapshotIndex = r.appliedIndex
					}
				}
				//这里必须调用Advance，告知raft底层上次的readyc已经处理完，不调用不会重新构建新的readyc
				r.node.Advance()
			}
		}
	}()
}


/*****************  rafthttp.Raft 接口实现 *********************************/
func (r *RaftNode) Process(ctx context.Context, m raftpb.Message) error {
	return r.node.Step(ctx, m)
}
func (r *RaftNode) IsIDRemoved(id uint64) bool {
	return false
}
func (r *RaftNode)  ReportUnreachable(id uint64){
}
func (r *RaftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
}


/****************** 快照相关 *******************************************/
//获取snapshot
func (r * RaftNode)getSnapshot() error {
	snapdir := fmt.Sprintf("counter-%d-snap", r.id)
	if !fileutil.Exist(snapdir) {
		if err := os.Mkdir(snapdir, 0750); err != nil {
			log.Fatalf("counter: cannot create dir for snapshot (%v)", err)
			return err
		}
	}
	r.snapshotter = snap.New(zap.NewExample(), snapdir)
	return nil
}

func (r *RaftNode) saveSnap(snap raftpb.Snapshot) error {
	//保存wal的snapshot index 和 term，因为在启动加载时通过wal的 index 和 term 来确定需要加载那个快照
	walSnap := walpb.Snapshot{
		Index: snap.Metadata.Index,
		Term:  snap.Metadata.Term,
	}
	if err := r.wal.SaveSnapshot(walSnap); err != nil {
		return err
	}

	if err := r.snapshotter.SaveSnap(snap); err != nil {
		return err
	}
	//这个木看懂。。。
	return r.wal.ReleaseLockTo(snap.Metadata.Index)
}

//保存应用层状态机的快照
func (r *RaftNode) saveCounterSnapshot() {
	//获取应用层store的序列化数据; 待优化
	data, err := store.GetSnapshot()
	if err != nil {
		log.Panic(err)
	}
	snap, err := r.storage.CreateSnapshot(r.appliedIndex, &r.confState, data)
	if err != nil {
		panic(err)
	}
	if err := r.saveSnap(snap); err != nil {
		panic(err)
	}

	compactIndex := uint64(1)
	if r.appliedIndex > snapshotCatchUpEntriesN {
		compactIndex = r.appliedIndex - snapshotCatchUpEntriesN
	}
	//只保留最近snapshotCatchUpEntriesN个日志，其余的都干掉，以节省空间
	if err := r.storage.Compact(compactIndex); err != nil {
		panic(err)
	}
}

// 该函数可能执行的时刻：1、raft Leader发送snapshot给节点  2、初始化从硬盘中读取快照时？？？
func (r *RaftNode) publishSnapshot(snapshotToSave raftpb.Snapshot) {
	if raft.IsEmptySnap(snapshotToSave) {
		return
	}
	if snapshotToSave.Metadata.Index <= r.appliedIndex {
		log.Fatalf("snapshot index [%d] should > progress.appliedIndex [%d] + 1", snapshotToSave.Metadata.Index, r.appliedIndex)
	}

	//触发应用层 调用反序列化函数，从快照中恢复应用状态机数据
	r.commitC <- RecoverSnapshot

	//更新相关的状态
	r.confState = snapshotToSave.Metadata.ConfState
	r.snapshotIndex = snapshotToSave.Metadata.Index
	r.appliedIndex = snapshotToSave.Metadata.Index
}

/******************  wal相关 ******************************************/

//初始化wal
func (r * RaftNode) getWal()(error){
	waldir := fmt.Sprintf("counter-%d", r.id)
	if !wal.Exist(waldir) {
		if err := os.Mkdir(waldir, 0750); err != nil {
			log.Fatalf("counter: cannot create dir for wal (%v)", err)
		}

		w, err := wal.Create(zap.NewExample(), waldir, nil)
		if err != nil {
			log.Fatalf("counter: create wal error (%v)", err)
		}
		w.Close()
	}

	//从保存的快照中获取 Term和Index
	var walSnapshot walpb.Snapshot
	if r.snapshotter != nil {
		if raftSnapshot, err := r.snapshotter.Load(); err != nil && err != snap.ErrNoSnapshot{
			log.Fatalf("counter: load snapshot error (%v)", err)
		}else if raftSnapshot != nil {
			walSnapshot.Term, walSnapshot.Index = raftSnapshot.Metadata.Term, raftSnapshot.Metadata.Index
			//用于加载应用层状态机数据
			r.storage.ApplySnapshot(*raftSnapshot)
		}
	}

	//根据快照中Term和Index，读取wal（注意，这里需要读取快照中的data并加载到应用状态机）
	w, err := wal.Open(zap.NewExample(), waldir, walSnapshot)
	r.wal = w
	return err
}

//从wal读取日志信息,并加载到raft的storage
func (r * RaftNode) loadWal() error {
	//读取文件中的日志记录
	_, st, ents, err := r.wal.ReadAll()
	if err != nil {
		log.Fatalf("counter: failed to read WAL (%v)", err)
		return err
	}

	//将服务器状态加载到storage
	err = r.storage.SetHardState(st)
	if err != nil {
		log.Fatalf("counter: failed to storage.SetHardState (%v)", err)
		return err
	}
	//将先前保存的日志加载到storage中，依次处理
	err = r.storage.Append(ents)
	if err != nil {
		log.Fatalf("counter: failed to storage.Append (%v)", err)
		return err
	}
	return err
}

/******************* raft server 监听 *********************************/
func (r *RaftNode)raftServe() error{
	raftUrl, err := url.Parse(r.peers[r.id - 1])
	if err != nil {
		log.Fatalf("counter: Failed parsing URL (%v)", err)
		return err
	}
	ln, err := net.Listen("tcp", raftUrl.Host)
	if err != nil {
		log.Fatalf("counter: Failed to listen rafthttp (%v)", err)
		return err
	}
	err = (&http.Server{Handler: r.transport.Handler()}).Serve(ln)
	if err != nil {
		log.Fatalf("counter: Failed to serve rafthttp (%v)", err)
		return err
	}
	return nil
}
