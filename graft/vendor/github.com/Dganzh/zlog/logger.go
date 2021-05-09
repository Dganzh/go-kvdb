package zlog

import (
	"fmt"
	"github.com/natefinch/lumberjack"
	"io"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	LogLevelDebug      = "debug"
	LogLevelInfo       = "info"
	LogLevelWarn       = "warn"
	LogLevelError      = "error"
	LogEncodingJson    = "json"
	LogEncodingConsole = "console"
)


type Config struct {
	LogLevel    string
	LogEncoding string
	LogCaller   string
	LogFile     string
}


var defaultConfig = Config{
	LogLevel:    LogLevelDebug,
	LogEncoding: LogEncodingJson,
	LogCaller:   "",
	LogFile:     "",
}

var logConfig = defaultConfig
var logger *zap.SugaredLogger

func init() {
	initDefaultLogger()
}

func NewLogger(cfg *Config) *zap.SugaredLogger {
	if cfg != nil {
		logConfig = *cfg
	}
	initLogger()
	return logger
}


func InitFileLogger(level string, path string) {
	cfg := &Config{
		LogLevel: level,
		LogEncoding: LogEncodingJson,
		LogCaller:   "",
		LogFile:     path,
	}
	NewLogger(cfg)
}


func GetLogLevel() string {
	return logConfig.LogLevel
}

func GetLogEncoding() string {
	return logConfig.LogEncoding
}


func initDefaultLogger() {
	encoderCfg := zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		NameKey:        "logger",
		MessageKey:     "msg",
		CallerKey:      logConfig.LogCaller,
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}
	loggerCfg := zap.Config{
		Level:            getLogLevel(logConfig.LogLevel),
		Development:      false,
		Encoding:         logConfig.LogEncoding,
		EncoderConfig:    encoderCfg,
		OutputPaths:      []string{"stderr"},
		ErrorOutputPaths: []string{"stderr"},
	}
	zapLogger, err := loggerCfg.Build(zap.AddCallerSkip(1), zap.AddStacktrace(zap.ErrorLevel))
	if err != nil {
		panic(fmt.Errorf("Fatal error init logger: %s\n", err))
	}
	logger = zapLogger.Sugar()
}

func initLogger() {
	encoderCfg := zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		NameKey:        "logger",
		MessageKey:     "msg",
		CallerKey:      logConfig.LogCaller,
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderCfg),
		zapcore.AddSync(getWriter(logConfig.LogFile)),
		getLogLevel(logConfig.LogLevel),
	)
	l := zap.New(core, zap.AddCallerSkip(1), zap.AddStacktrace(zap.ErrorLevel))
	logger = l.Sugar()
}


func getWriter(filename string) io.Writer {
	return &lumberjack.Logger{
		Filename:   filename,
		MaxSize:    100,  //最大M数，超过则切割
		MaxBackups: 5,   //最大文件保留数，超过就删除最老的日志文件
		MaxAge:     30,  //保存30天
		Compress:   false,//是否压缩
	}

}


func getLogLevel(level string) zap.AtomicLevel {
	var l zapcore.Level
	switch strings.ToLower(level) {
	case LogLevelDebug:
		l = zap.DebugLevel
	case LogLevelInfo:
		l = zap.InfoLevel
	case LogLevelWarn:
		l = zap.WarnLevel
	case LogLevelError:
		l = zap.ErrorLevel
	default:
		panic(fmt.Errorf("unknown loglevel: %s", level))
	}

	return zap.NewAtomicLevelAt(l)
}

func Debug(args ...interface{}) {
	logger.Debug(args...)
}

func Debugf(format string, args ...interface{}) {
	logger.Debugf(format, args...)
}

func Debugw(msg string, kv ...interface{}) {
	logger.Debugw(msg, kv...)
}

func Info(args ...interface{}) {
	logger.Info(args...)
}

func Infof(format string, args ...interface{}) {
	logger.Infof(format, args...)
}

func Infow(msg string, kv ...interface{}) {
	logger.Infow(msg, kv...)
}

func Warn(args ...interface{}) {
	logger.Warn(args...)
}

func Warnf(format string, args ...interface{}) {
	logger.Warnf(format, args...)
}

func Warnw(msg string, kv ...interface{}) {
	logger.Warnw(msg, kv...)
}

func Error(args ...interface{}) {
	logger.Error(args...)
}

func Errorf(format string, args ...interface{}) {
	logger.Errorf(format, args...)
}

func Errorw(msg string, kv ...interface{}) {
	logger.Errorw(msg, kv...)
}

func Fatal(args ...interface{}) {
	logger.Fatal(args...)
}

func Fatalf(format string, args ...interface{}) {
	logger.Fatalf(format, args...)
}

func Fatalw(msg string, kv ...interface{}) {
	logger.Fatalw(msg, kv...)
}

func Panic(args ...interface{}) {
	logger.Panic(args...)
}

func Panicf(format string, args ...interface{}) {
	logger.Panicf(format, args...)
}

func Panicw(msg string, kv ...interface{}) {
	logger.Panicw(msg, kv...)
}
