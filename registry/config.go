package main


type Config struct {
	TriggerStartNum int		// 触发启动服务的数量
	RpcAddr	string
	HttpAddr string
}

var defaultCfg *Config

func init() {
	defaultCfg = &Config{
		TriggerStartNum: 3,
		RpcAddr:         "localhost:5201",
		HttpAddr:        "localhost:5200",
	}
}

