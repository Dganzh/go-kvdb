package kvdb

import (
	"github.com/Dganzh/zrpc"
	"github.com/go-basic/uuid"
	"testing"
)

func TestKV(t *testing.T) {
	addrs := []string{"localhost:5205", "localhost:5206", "localhost:5207"}
	var rpcClients []*zrpc.Client
	for _, addr := range addrs {
		rpcClients = append(rpcClients, zrpc.NewClient(addr))
	}
	ck := MakeClerk(rpcClients)
	key := uuid.New()
	if v := ck.Get(key); v != "" {
		t.Error("Get None key result error", v)
	}
	ck.Put(key, "12345")
	if v := ck.Get(key); v != "12345" {
		t.Error("Get key result error after Put", "expected", "12345", "got", v)
	}
	ck.Append(key, "54321")
	if v := ck.Get(key); v != "1234554321" {
		t.Error("Get key result error after Append", "expected", "1234554321", "got", v)
	}
}

