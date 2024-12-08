package shardctrler

//
// Shardctrler clerk.
//

import "6.5840/labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	leaderId  int64
	clientId  int64
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

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{num, ck.clientId, ck.serialNum}
	reply := &QueryReply{}
	for !ck.servers[ck.leaderId].Call("ShardCtrler.Query", args, reply) || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
		ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
		time.Sleep(SLEEP_TIME)
	}
	ck.serialNum++
	return reply.Config
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{servers, ck.clientId, ck.serialNum}
	reply := &JoinReply{}
	for !ck.servers[ck.leaderId].Call("ShardCtrler.Join", args, reply) || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
		ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
		time.Sleep(SLEEP_TIME)
	}
	ck.serialNum++
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{gids, ck.clientId, ck.serialNum}
	reply := &LeaveReply{}
	for !ck.servers[ck.leaderId].Call("ShardCtrler.Leave", args, reply) || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
		ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
		time.Sleep(SLEEP_TIME)
	}
	ck.serialNum++
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{shard, gid, ck.clientId, ck.serialNum}
	reply := &MoveReply{}
	for !ck.servers[ck.leaderId].Call("ShardCtrler.Move", args, reply) || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
		ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
		time.Sleep(SLEEP_TIME)
	}
	ck.serialNum++
}
