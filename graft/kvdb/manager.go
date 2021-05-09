package kvdb

import (
	log "github.com/Dganzh/zlog"
	"github.com/Dganzh/zrpc"
	"graft/config"
	"time"
)

type State int

const (
	StateInit State = iota
	StateStarted
	StateStop
)


// 负责与注册中心交互
type Manager struct {
	client *zrpc.Client
	kvServer *KVServer
	rpcServer *zrpc.Server
	addr string
	me int
	startCh chan int64
	stopCh chan int64
	addrs []string
	state State
}


func NewManager(me int) *Manager {
	cfg := config.GlobalCfg
	cfg.Id = me
	kv := MakeKVServer(config.GlobalCfg)
	m := &Manager{}
	m.state = StateInit
	m.startCh = make(chan int64)
	m.stopCh = make(chan int64)
	m.client = zrpc.NewClient(cfg.RegistryAddr)
	m.kvServer = kv
	m.me = me
	m.addr = cfg.Addrs[m.me]
	rpcServer := zrpc.NewServer(cfg.Addrs[m.me])
	rpcServer.Register(m)
	rpcServer.Register(kv.rf)
	rpcServer.Register(kv)
	m.rpcServer = rpcServer
	return m
}

func (mgr *Manager) Start(startTime int64) {
	if mgr.state == StateStarted {
		return
	}
	// 先连接，但是不立马启动
	rpcClients := make([]*zrpc.Client, len(mgr.addrs))
	for idx, addr := range mgr.addrs {
		rpcClients[idx] = zrpc.NewClient(addr)
	}
	deltaTime := startTime - time.Now().UnixNano()
	time.Sleep(time.Duration(deltaTime))
	log.Info(mgr.me, "now start=============>")
	mgr.state = StateStarted
	go mgr.kvServer.StartServer(rpcClients)
}


func (mgr *Manager) Stop(stopTime int64) {
	if mgr.state == StateStop {
		return
	}
	deltaTime := stopTime - time.Now().UnixNano()
	time.Sleep(time.Duration(deltaTime))
	log.Info(mgr.me, "now stop=============>")
	mgr.kvServer.Kill()
	mgr.state = StateStop
}


type ApplyRegisterArgs struct {
	Addr string
	Idx  int
}

type ApplyRegisterReply struct {
	OK bool
}


func (mgr *Manager) ApplyRegister() {
	args := ApplyRegisterArgs{
		Addr: mgr.addr,
		Idx: mgr.me,
	}
	mgr.client.Call("Registry.RegisterHandler", &args, &ApplyRegisterReply{})
}



type NotifyStartArgs struct {
	Addrs []string
	StartTime int64
}

type NotifyStartReply struct {
	OK bool
}


// 处理注册中心的通知，表示要启动服务了
func (mgr *Manager) NotifyStart(args *NotifyStartArgs, reply *NotifyStartReply) {
	log.Info("receive notify start from registry", args.Addrs)
	mgr.addrs = args.Addrs
	reply.OK = true
	// 这里不能直接调用mgr.Start
	mgr.startCh <- args.StartTime
}


type NotifyStopArgs struct {
	Addrs []string
	StopTime int64
}

type NotifyStopReply struct {
	OK bool
}

// 处理注册中心的通知，表示要启动服务了
func (mgr *Manager) NotifyStop(args *NotifyStopArgs, reply *NotifyStopReply) {
	log.Info("receive notify stop from registry", args.Addrs, time.Now())
	//mgr.addrs = args.Addrs
	reply.OK = true
	// 这里不能直接调用mgr.Stop
	mgr.stopCh <- args.StopTime
}
