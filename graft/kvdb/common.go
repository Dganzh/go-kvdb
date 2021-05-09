package kvdb

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrUnknownOp   = "ErrUnknownOp"
	ErrTimeOut		= "ErrTimeOut"
)

type Err string

type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	Cid string
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	Cid string
}

type GetReply struct {
	Err   Err
	Value string
}
