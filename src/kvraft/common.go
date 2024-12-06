package kvraft

import (
	"log"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	ErrUnknownOp   = "ErrUnknownOp"
)

type OpType int

const (
	GetOp OpType = iota
	PutOp
	AppendOp
	NoOp
)

type RequestType int

const (
	REQ = iota
	ACK
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64
	SerialNum int64
	Req       RequestType
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId  int64
	SerialNum int64
}

type GetReply struct {
	Err        Err
	Value      string
	LeaderHint int64
}

const (
	TIMEOUT = 150 * time.Millisecond
)

type Command struct {
	*Op
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType    OpType
	ClientId  int64
	SerialNum int64
	Key       string
	Value     string
}

type OpReply struct {
	Value string
	Err   Err
}

type OpContext struct {
	SerialNum   int64
	LastOpReply *OpReply
}

type KVStateMachine interface {
	Get(key string) (string, Err)
	Put(key, value string) Err
	Append(key, value string) Err
}

type MemoryKV struct {
	KV map[string]string
}

func newMemoryKV() *MemoryKV {
	return &MemoryKV{
		KV: make(map[string]string),
	}
}

func (mKV *MemoryKV) Get(key string) (string, Err) {
	if value, ok := mKV.KV[key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (mKV *MemoryKV) Put(key, value string) Err {
	mKV.KV[key] = value
	DPrintf("MemoryKV Put key [%s] value [%s] to [%s]", key, value, mKV.KV[key])
	return OK
}

func (mKV *MemoryKV) Append(key, value string) Err {
	mKV.KV[key] += value
	DPrintf("MemoryKV Append key [%s] value [%s] to [%s]", key, value, mKV.KV[key])
	return OK
}
