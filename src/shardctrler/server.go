package shardctrler

import (
	"6.5840/raft"
	"sync/atomic"
	"time"
)
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"

type ShardCtrler struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead int32 // set by Kill()

	// Your definitions here.
	stateMachine ConfigStateMachine    // store KV
	lastOp       map[int64]OpContext   // store the last operation serial number of each client
	notifyChs    map[int]chan *OpReply // store the channel for each raft client to receive operation results
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	opArgs := Op{
		OpType:    JoinOp,
		Servers:   args.Servers,
		ClientId:  args.ClientId,
		SerialNum: args.SerialNum,
	}
	opReply := OpReply{}
	sc.executeCommand(&opArgs, &opReply)
	reply.Err = opReply.Err
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	opArgs := Op{
		OpType:    LeaveOp,
		GIDs:      args.GIDs,
		ClientId:  args.ClientId,
		SerialNum: args.SerialNum,
	}
	opReply := OpReply{}
	sc.executeCommand(&opArgs, &opReply)
	reply.Err = opReply.Err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	opArgs := Op{
		OpType:    MoveOp,
		Shard:     args.Shard,
		GID:       args.GID,
		ClientId:  args.ClientId,
		SerialNum: args.SerialNum,
	}
	opReply := OpReply{}
	sc.executeCommand(&opArgs, &opReply)
	reply.Err = opReply.Err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	opArgs := Op{
		OpType:    QueryOp,
		Num:       args.Num,
		ClientId:  args.ClientId,
		SerialNum: args.SerialNum,
	}
	opReply := OpReply{}
	sc.executeCommand(&opArgs, &opReply)
	reply.Config, reply.Err = opReply.Config, opReply.Err
}

func (sc *ShardCtrler) executeCommand(args *Op, reply *OpReply) {
	sc.mu.RLock()
	if args.OpType != QueryOp && sc.checkDuplicated(args.ClientId, args.SerialNum) {
		lastReply := sc.lastOp[args.ClientId].LastOpReply
		reply.Config, reply.Err = lastReply.Config, lastReply.Err
		sc.mu.RUnlock()
		return
	}
	sc.mu.RUnlock()
	index, _, isLeader := sc.rf.Start(Command{args})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	sc.mu.Lock()
	ch := sc.getNotifyCh(index)
	sc.mu.Unlock()

	select {
	case result := <-ch:
		reply.Config, reply.Err = result.Config, result.Err
	case <-time.After(TIMEOUT):
		reply.Err = ErrTimeout
	}
	go func() {
		sc.mu.Lock()
		sc.deleteNotifyCh(index)
		sc.mu.Unlock()
	}()
}

func (sc *ShardCtrler) getNotifyCh(index int) chan *OpReply {
	if _, ok := sc.notifyChs[index]; !ok {
		sc.notifyChs[index] = make(chan *OpReply, 1)
	}
	return sc.notifyChs[index]
}

func (sc *ShardCtrler) deleteNotifyCh(index int) {
	delete(sc.notifyChs, index)
}

func (sc *ShardCtrler) checkDuplicated(clientId int64, serialNum int64) bool {
	if lastOp, exists := sc.lastOp[clientId]; exists {
		return serialNum <= lastOp.SerialNum
	}
	return false
}

func (sc *ShardCtrler) applyLogToStateMachine(op Op) *OpReply {
	var reply = new(OpReply)

	switch op.OpType {
	case JoinOp:
		reply.Err = sc.stateMachine.Join(op.Servers)
	case LeaveOp:
		reply.Err = sc.stateMachine.Leave(op.GIDs)
	case MoveOp:
		reply.Err = sc.stateMachine.Move(op.Shard, op.GID)
	case QueryOp:
		reply.Config, reply.Err = sc.stateMachine.Query(op.Num)
	default:
		reply.Err = ErrUnknownOp
	}
	return reply
}

func (sc *ShardCtrler) applier() {
	for !sc.killed() {
		select {
		case message := <-sc.applyCh:
			if message.CommandValid {
				sc.mu.Lock()
				reply := new(OpReply)
				command := message.Command.(Command).Op
				if command.OpType != QueryOp && sc.checkDuplicated(command.ClientId, command.SerialNum) {
					reply = sc.lastOp[command.ClientId].LastOpReply
				} else {
					reply = sc.applyLogToStateMachine(*command)
					if command.OpType != QueryOp {
						sc.lastOp[command.ClientId] = OpContext{
							SerialNum:   command.SerialNum,
							LastOpReply: reply,
						}
					}
				}
				if currentTerm, isLeader := sc.rf.GetState(); isLeader && message.CommandTerm == currentTerm {
					ch := sc.getNotifyCh(message.CommandIndex)
					ch <- reply
				}
				sc.mu.Unlock()
			}
		}
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)
}

func (sc *ShardCtrler) killed() bool {
	return atomic.LoadInt32(&sc.dead) == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	labgob.Register(Op{})
	labgob.Register(Command{})

	applyCh := make(chan raft.ApplyMsg)
	sc := &ShardCtrler{
		rf:           raft.Make(servers, me, persister, applyCh),
		applyCh:      applyCh,
		stateMachine: newMemoryConfig(),
		lastOp:       make(map[int64]OpContext),
		notifyChs:    make(map[int]chan *OpReply),
		dead:         0,
	}

	go sc.applier()

	return sc
}
