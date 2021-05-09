package kvdb

import (
	"encoding/json"
	log "github.com/Dganzh/zlog"
	"github.com/Dganzh/zrpc"
	"graft/config"
	"graft/raft"
	"sync"
	"sync/atomic"
	"time"
)

type Op struct {
	CId   string
	Cmd   string // Get/Put/Append
	Key   string
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate      int // snapshot if log grows this big
	opResultCh        map[string]chan OpResult
	opResultCache     sync.Map
	db                map[string]string
	logSize           int
	installSnapshotCh chan bool
	snapshotting      bool // 是否正在执行安装快照中
}

type OpResult struct {
	E Err
	V string
}

func marshall(args map[string]string) []byte {
	r, _ := json.Marshal(args)
	return r
}

func unmarshall(r []byte) map[string]string {
	var args map[string]string
	_ = json.Unmarshal(r, &args)
	return args
}

func (kv *KVServer) Start(cmd interface{}, cid string) bool {
	_, _, ok := kv.rf.Start(cmd)
	return ok
}

func (kv *KVServer) applyOp(op Op) (string, Err) {
	switch op.Cmd {
	case "Get":
		return kv.doGet(op.Key)
	case "Put":
		return kv.doPut(op.Key, op.Value)
	case "Append":
		return kv.doAppend(op.Key, op.Value)
	default:
		return "", ErrUnknownOp
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	if args.Key == "" {
		reply.Err = ErrNoKey
		return
	}
	if v, ok := kv.opResultCache.Load(args.Cid); ok {
		res := v.(OpResult)
		reply.Err = res.E
		reply.Value = res.V
		return
	}

	argsMap := map[string]string{
		"Key": args.Key,
		"Cid": args.Cid,
		"Op":  "Get",
	}
	cmd := marshall(argsMap)
	if ok := kv.Start(cmd, args.Cid); !ok {
		reply.Err = ErrWrongLeader
		log.Error("kvserver Get call raft start failed", args.Cid)
		return
	}
	// 等待raft提交这条log
	kv.mu.Lock()
	if _, existed := kv.opResultCh[args.Cid]; !existed {
		kv.opResultCh[args.Cid] = make(chan OpResult)
	}
	kv.mu.Unlock()
	select {
	case <-time.After(config.GlobalCfg.LogCommitTimeout):
		reply.Err = ErrTimeOut
		log.Warn("get timeout============", args.Cid)
		return
	case opRes := <-kv.opResultCh[args.Cid]:
		reply.Err = opRes.E
		reply.Value = opRes.V
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	if args.Key == "" {
		reply.Err = ErrNoKey
		return
	}
	if v, ok := kv.opResultCache.Load(args.Cid); ok {
		res := v.(OpResult)
		reply.Err = res.E
		return
	}
	argsMap := map[string]string{
		"Key":   args.Key,
		"Value": args.Value,
		"Cid":   args.Cid,
		"Op":    args.Op,
	}
	cmd := marshall(argsMap)
	if ok := kv.Start(cmd, args.Cid); !ok {
		reply.Err = ErrWrongLeader
		log.Error("kvserver PutAppend call raft start failed", args.Cid)
		return
	}
	kv.mu.Lock()
	if _, existed := kv.opResultCh[args.Cid]; !existed {
		kv.opResultCh[args.Cid] = make(chan OpResult, 10)
	}
	kv.mu.Unlock()
	select {
	case <-time.After(config.GlobalCfg.LogCommitTimeout):
		reply.Err = ErrTimeOut
		log.Warn("PutAppend timeout============", args.Cid)
	case opRes := <-kv.opResultCh[args.Cid]:
		reply.Err = opRes.E
	}
}

func (kv *KVServer) sendOpResultNotify(cid string, res OpResult) {
	if ch, ok := kv.opResultCh[cid]; ok && ch != nil {
		select {
		case <-time.After(5 * time.Millisecond):
			log.Info("sendOpResultNotify timeout", cid)
		case kv.opResultCh[cid] <- res:
			log.Info("sendOpResultNotify OK", cid)
		}
	}
}

func (kv *KVServer) ExecOpAndNotify() {
	for {
		select {
		case msg := <-kv.applyCh:
			if msg.Command == "" { 		// 成为leader时发送的empty log
				kv.rf.ApplyLog()
				continue
			}
			if cmd, ok := msg.Command.([]byte); ok && len(cmd) > 0 {
				argsMap := unmarshall(cmd)
				kv.mu.Lock()
				switch msg.CommandType {
				case 1: // snapshot log
					if argsMap["Op"] != "Get" {
						op := Op{
							Cmd:   argsMap["Op"],
							Key:   argsMap["Key"],
							Value: argsMap["Value"],
						}
						kv.applyOp(op)
					}
					kv.mu.Unlock()
					continue
				}

				var opRes OpResult
				isLeader := msg.IsLeader
				if res, ok := kv.opResultCache.Load(argsMap["Cid"]); ok {
					opRes = res.(OpResult)
				} else {
					op := Op{
						Cmd:   argsMap["Op"],
						Key:   argsMap["Key"],
						Value: argsMap["Value"],
					}
					v, e := kv.applyOp(op)
					kv.rf.ApplyLog()
					opRes = OpResult{e, v}
				}
				// 只有leader才需要在执行完命令后发通知，这样才能返回响应给Client
				if isLeader {
					kv.sendOpResultNotify(argsMap["Cid"], opRes)
					kv.opResultCache.Store(argsMap["Cid"], opRes)
				}
				kv.logSize += len(cmd) // 记录log大小
				if kv.maxraftstate > 0 && !kv.snapshotting && kv.logSize > kv.maxraftstate {
					log.Info("start notify install snapshot")
					kv.snapshotting = true
					kv.installSnapshotCh <- true
				}
				kv.mu.Unlock()
			}
		}
		time.Sleep(1 * time.Millisecond)
	}
}


func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) installSnapshot() {
	for {
		select {
		case <-kv.installSnapshotCh:
			kv.mu.Lock()
			lsz := kv.logSize
			kv.mu.Unlock()
			kv.rf.SaveStateAndSnapshot()
			kv.mu.Lock()
			kv.snapshotting = false
			kv.logSize -= lsz
			kv.mu.Unlock()
		}
	}
}


func MakeKVServer(cfg *config.Config) *KVServer {
	kv := new(KVServer)
	kv.me = cfg.Id
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.MakeRaft(cfg, kv.applyCh)
	return kv
}


func (kv *KVServer) StartServer(clients []*zrpc.Client) {
	kv.maxraftstate = config.GlobalCfg.MaxRaftState
	kv.rf.StartRaft(clients)
	kv.opResultCh = make(map[string]chan OpResult)
	//kv.opResultCache = make(map[string]OpResult)
	kv.db = make(map[string]string)
	kv.installSnapshotCh = make(chan bool, 1)

	go kv.installSnapshot()
	kv.RestoreDB()
	log.Info("StartServer Ok")
	kv.ExecOpAndNotify()
	return
}

func (kv *KVServer) RestoreDB() {
	kv.mu.Lock()
	logs, spLogCnt, _ := kv.rf.RestoreLog()
	for i, msg := range logs {
		if cmd, ok := msg.Command.([]byte); ok && len(cmd) > 0 {
			if i >= spLogCnt { // 大于快照部分log需要记录size使得能正确出发下次installSnapshot
				kv.logSize += len(cmd)
			}
			argsMap := unmarshall(cmd)
			switch msg.CommandType {
			case 1: // snapshot log
				if argsMap["Op"] != "Get" {
					op := Op{
						Cmd:   argsMap["Op"],
						Key:   argsMap["Key"],
						Value: argsMap["Value"],
					}
					kv.applyOp(op)
				}
				continue
			}
		}
	}
	kv.mu.Unlock()
}

/*
操作database
*/
func (kv *KVServer) doGet(key string) (string, Err) {
	if v, ok := kv.db[key]; ok {
		return v, OK
	}
	return "", OK
}

func (kv *KVServer) doPut(key string, value string) (string, Err) {
	kv.db[key] = value
	return "", OK
}

func (kv *KVServer) doAppend(key string, value string) (string, Err) {
	if v, ok := kv.db[key]; ok {
		kv.db[key] = v + value
	} else {
		kv.db[key] = value
	}
	return "", OK
}
