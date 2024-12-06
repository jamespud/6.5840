package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

const SLEEP_TIME = 50 * time.Millisecond

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId  int64
	leaderId  int64
	serialNum int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := Clerk{
		servers:   servers,
		clientId:  nrand(),
		leaderId:  0,
		serialNum: 0,
	}
	return &ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := &GetArgs{Key: key, ClientId: ck.clientId, SerialNum: ck.serialNum}
	reply := &GetReply{}
	for !ck.servers[ck.leaderId].Call("KVServer.Get", args, reply) || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
		// if target server down or isn't leader, retry with other server
		ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
		time.Sleep(SLEEP_TIME)
	}
	ck.serialNum++
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := &PutAppendArgs{Key: key, Value: value, ClientId: ck.clientId, SerialNum: ck.serialNum, Req: REQ}
	reply := &PutAppendReply{}

	for !ck.servers[ck.leaderId].Call("KVServer."+op, args, reply) || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
		ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
		time.Sleep(SLEEP_TIME)
	}
	ck.serialNum++
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
