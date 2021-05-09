package main

import (
	log "github.com/Dganzh/zlog"
	"graft/kvdb"
	"os"
	"strconv"
)


func main() {
	if len(os.Args) != 2 {
		log.Fatal("invalid run args")
		return
	}
	idx, _ := strconv.Atoi(os.Args[1])
	log.Info("start idx: ", idx)
	kvdb.Start(idx)
}


