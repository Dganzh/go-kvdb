package config

import (
	"time"
)


type Config struct {
	Id int
	RegistryAddr string
	LogCommitTimeout time.Duration
	MaxRaftState	int
	Addrs []string
	PingInterval int			// leader周期向follower发心跳的，ms
	CheckLeaderInterval int 	// follower周期检查leader是否存活，ms
	SelectTimeout int			// 选举时间为rand.Int(SelectTimeout) + 150
	LeaderTimeout int 			// 超过这个时间还没收到心跳，认为leader下线了
}

var GlobalCfg *Config
var defaultCfg *Config = &Config{
	RegistryAddr: "localhost:5201",
	LogCommitTimeout: 2 * time.Second,
	MaxRaftState: 100 * 1024 * 1024,
	Addrs: []string{"localhost:5205", "localhost:5206", "localhost:5207"},
	PingInterval: 60,
	CheckLeaderInterval: 100,
	SelectTimeout: 500,
	LeaderTimeout: 1000,
}


func init() {
	GlobalCfg = defaultCfg
}
