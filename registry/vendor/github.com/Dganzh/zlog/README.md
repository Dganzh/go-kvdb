### version
v0.0.1

### quick start
write log to file:
```golang
cfg := &Config{
    logLevel: "info",
    logEncoding: "json",
    logFile: "./logs/test/access.log",
}
log := NewLogger(cfg)
log.Debugf("这是一条测试用里: %+v", cfg)
log.Infof("这是一条测试用里 Info级别: %+v", cfg)
log.Infow("试试这样输出: ", "data", cfg, "extra", "what happen?")
```

