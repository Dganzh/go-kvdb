package main

import (
	"github.com/gin-gonic/gin"
)



func Ping(c *gin.Context) {
	c.JSON(200, gin.H{"msg": "pong"})
}


func StopKVServer(c *gin.Context) {
	globalRegistry.NotifyAllStop()
	c.JSON(200, gin.H{"msg": "OK"})
}

