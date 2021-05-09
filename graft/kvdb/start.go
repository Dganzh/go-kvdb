package kvdb

import (
	"fmt"
	log "github.com/Dganzh/zlog"
)


func Start(me int) {
	log.InitFileLogger(log.LogLevelInfo, fmt.Sprintf("logs/kvserver%d.log", me))
	log.Info("start kv db server", me)
	mgr := NewManager(me)
	go mgr.rpcServer.Start()
	mgr.ApplyRegister()
	// 这里要阻塞住，等待注册中心通知启动
	for {
		select {
		case startTime := <- mgr.startCh:
			mgr.Start(startTime)
		case stopTime := <- mgr.stopCh:
			mgr.Stop(stopTime)
			return					// 停止就直接退出了,后续可以增加quit操作，stop操作就可以不退出
		}
	}
}

