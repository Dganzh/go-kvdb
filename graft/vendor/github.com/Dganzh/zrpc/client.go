package zrpc

import (
	"context"
	log "github.com/Dganzh/zlog"
	pb "github.com/Dganzh/zrpc/core"
	"google.golang.org/grpc"
	"time"
)

const (
	ADDRESS = "localhost:5205"
)


type Client struct {
	client pb.RPCClient
	gob *Gob
}


func NewClient(addr string) *Client {
	if addr == "" {
		addr = ADDRESS
	}
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	return &Client{
		client: pb.NewRPCClient(conn),
		gob: NewGobObject(),
	}
}


func (c *Client) Call(handler string, arg interface{}, reply interface{}) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 3 * time.Second)
	defer cancel()
	data, _ := c.gob.Encode(arg)
	r, err := c.client.Call(ctx, &pb.Request{Handler: handler, Data: data})
	if err != nil {
		log.Fatalf("RPC call handler=%s, failed: %v", handler, err)
		return false
	}
	_ = c.gob.Decode(r.GetData(), reply)
	log.Debugf("Call %s Result: %+v", handler, reply)
	return true
}
