package kvdb

import (
	"crypto/rand"
	"fmt"
	"github.com/Dganzh/zrpc"
	"math/big"
	"strconv"
	"time"
)


type Clerk struct {
	servers []*zrpc.Client
	Leader int 		// index in Clerk.servers
	Cid   int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*zrpc.Client) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.Leader = -1
	return ck
}

func (ck *Clerk) gen2Cid(clientId int) string {
	ck.Cid++
	cid := fmt.Sprintf("%d:%d", clientId, ck.Cid)
	return cid
}

func (ck *Clerk) genCid() string {
	return strconv.FormatInt(nrand(), 10)
}


func (ck *Clerk) Get(key string) string {
	args := &GetArgs{
		Key: key,
		Cid: ck.genCid(),
	}
	reply := &GetReply{}
	ck.one("Get", args, reply, true)
	return reply.Value
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := &PutAppendArgs{
		Key: key,
		Value: value,
		Op: op,
		Cid: ck.genCid(),
	}
	reply := &PutAppendReply{}
	ck.one("PutAppend", args, reply, true)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}


func (ck *Clerk) one(rpcMethod string, args, reply interface{}, retry bool) bool {
	meth := "KVServer." + rpcMethod
	leader := ck.Leader
	if leader != -1 {
		if ok := ck.servers[leader].Call(meth, args, reply); ok {
			switch reply.(type) {
			case *GetReply:
				if reply.(*GetReply).Err == OK {
					return true
				}
			case *PutAppendReply:
				if reply.(*PutAppendReply).Err == OK {
					return true
				}
			}
		}
	}
	t0 := time.Now()
	for time.Since(t0).Seconds() < 6 {
		for si := 0; si < len(ck.servers); si++ {
			if si == leader {
				continue
			}
			if ok := ck.servers[si].Call(meth, args, reply); ok {
				switch reply.(type) {
				case *GetReply:
					if reply.(*GetReply).Err == OK {
						ck.Leader = si
						return true
					}
				case *PutAppendReply:
					if reply.(*PutAppendReply).Err == OK {
						ck.Leader = si
						return true
					}
				}
			}
			time.Sleep(2 * time.Millisecond)
		}
		// 循环完一圈都没能接收，休息长一点
		time.Sleep(10 * time.Millisecond)
		leader = -1
	}
	return false
}


