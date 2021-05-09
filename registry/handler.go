package main

import (
	log "github.com/Dganzh/zlog"
	"github.com/Dganzh/zrpc"
	"os"
	"sort"
	"time"
)


var globalRegistry *Registry


func SetGlobalRegistry(r *Registry) {
	globalRegistry = r
}


type Registry struct {
	ends map[int]string
}


func NewRegistry() *Registry {
	return &Registry{
		ends: map[int]string{},
	}
}


func (r *Registry) getAddrs() []string {
	keys := make([]int, len(r.ends))
	i := 0
	for k := range r.ends {
		keys[i] = k
		i++
	}
	sort.Ints(keys)
	var addrs []string
	for _, k := range keys {
		addrs = append(addrs, r.ends[k])
	}
	return addrs
}


type RegisterArgs struct {
	Addr string
	Idx int
}


type RegisterReply struct {
	State string
	CanStart bool		// 是否可以启动服务了
}


type NotifyStartArgs struct {
	Addrs []string
	StartTime int64
}

type NotifyStartReply struct {
	OK bool
}


func (r *Registry) NotifyAllStart() {
	addrs := r.getAddrs()
	log.Infof("ends: %+v, addrs: %+v\n", r.ends, addrs)
	args := NotifyStartArgs{
		Addrs: addrs,
		StartTime: time.Now().Add(time.Second).UnixNano(),		// 约定各服务器1s后启动
	}
	reply := NotifyStartReply{}
	for _, addr := range addrs {
		client := zrpc.NewClient(addr)
		client.Call("Manager.NotifyStart", args, reply)
	}
}


func (r *Registry) RegisterHandler(args *RegisterArgs, reply *RegisterReply) {
	log.Infow("register ", "addr", args.Addr)
	r.ends[args.Idx] = args.Addr
	reply.State = "OK"
	time.AfterFunc(10 * time.Millisecond, func() {
		if len(r.ends) >= cfg.TriggerStartNum {
			r.NotifyAllStart()
		}
		log.Info("all server notify finish")
	})
}


type NotifyStopArgs struct {
	Addrs []string
	StopTime int64
}

type NotifyStopReply struct {
	OK bool
}


func (r *Registry) NotifyAllStop() {
	addrs := r.getAddrs()
	log.Infof("ends: %+v, addrs: %+v\n", r.ends, addrs)
	args := NotifyStopArgs{
		Addrs: addrs,
		StopTime: time.Now().Add(time.Second).UnixNano(),		// 约定各服务器1s后启动
	}
	reply := NotifyStopReply{}
	for _, addr := range addrs {
		client := zrpc.NewClient(addr)
		client.Call("Manager.NotifyStop", args, reply)
	}
	log.Info("notify all stop finish")
	time.AfterFunc(3 * time.Second, func() {
		log.Info("registry quiting...")
		os.Exit(0)
	})
}

