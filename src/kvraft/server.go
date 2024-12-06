package kvraft

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	lastApplied  int
	persister    *raft.Persister

	// Your definitions here.
	stateMachine KVStateMachine        // store KV
	lastOp       map[int64]OpContext   // store the last operation serial number of each client
	notifyChs    map[int]chan *OpReply // store the channel for each raft client to receive operation results
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("KVServer %d got Get request %v from client %d serialNum %d with args %v", kv.me, args, args.ClientId, args.SerialNum, args)
	opArgs := Op{
		OpType:    GetOp,
		ClientId:  args.ClientId,
		SerialNum: args.SerialNum,
		Key:       args.Key,
	}
	opReply := OpReply{}
	kv.executeCommand(&opArgs, &opReply)
	reply.Value, reply.Err = opReply.Value, opReply.Err
	DPrintf("KVServer %d reply Get request %v to client %d serialNum %d with reply %v", kv.me, reply, args.ClientId, args.SerialNum, reply)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("KVServer %d got Put request %v from client %d serialNum %d with args %v", kv.me, args, args.ClientId, args.SerialNum, args)
	opArgs := Op{
		OpType:    PutOp,
		ClientId:  args.ClientId,
		SerialNum: args.SerialNum,
		Key:       args.Key,
		Value:     args.Value,
	}
	opReply := OpReply{}
	kv.executeCommand(&opArgs, &opReply)
	reply.Err = opReply.Err
	DPrintf("KVServer %d reply Put request %v to client %d serialNum %d with reply %v", kv.me, reply, args.ClientId, args.SerialNum, reply)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("KVServer %d got Append request %v from client %d serialNum %d with args %v", kv.me, args, args.ClientId, args.SerialNum, args)
	opArgs := Op{
		OpType:    AppendOp,
		ClientId:  args.ClientId,
		SerialNum: args.SerialNum,
		Key:       args.Key,
		Value:     args.Value,
	}
	opReply := OpReply{}
	kv.executeCommand(&opArgs, &opReply)
	reply.Err = opReply.Err
	DPrintf("KVServer %d reply Append request %v to client %d serialNum %d with reply %v", kv.me, reply, args.ClientId, args.SerialNum, reply)
}

func (kv *KVServer) executeCommand(args *Op, reply *OpReply) {
	kv.mu.RLock()
	if args.OpType != GetOp && kv.checkDuplicated(args.ClientId, args.SerialNum) {
		lastReply := kv.lastOp[args.ClientId].LastOpReply
		reply.Value, reply.Err = lastReply.Value, lastReply.Err
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()
	index, _, isLeader := kv.rf.Start(Command{args})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	ch := kv.getNotifyCh(index)
	kv.mu.Unlock()

	select {
	case result := <-ch:
		reply.Value, reply.Err = result.Value, result.Err
	case <-time.After(TIMEOUT):
		reply.Err = ErrTimeout
	}
	go func() {
		kv.mu.Lock()
		kv.deleteNotifyCh(index)
		kv.mu.Unlock()
	}()

}

func (kv *KVServer) getNotifyCh(index int) chan *OpReply {
	if _, ok := kv.notifyChs[index]; !ok {
		kv.notifyChs[index] = make(chan *OpReply, 1)
	}
	return kv.notifyChs[index]
}

func (kv *KVServer) deleteNotifyCh(index int) {
	delete(kv.notifyChs, index)
}

func (kv *KVServer) checkDuplicated(clientId int64, serialNum int64) bool {
	if lastOp, exists := kv.lastOp[clientId]; exists {
		return serialNum <= lastOp.SerialNum
	}
	return false
}

func (kv *KVServer) applyLogToStateMachine(op Op) *OpReply {
	var reply = new(OpReply)

	switch op.OpType {
	case GetOp:
		reply.Value, reply.Err = kv.stateMachine.Get(op.Key)
	case PutOp:
		reply.Err = kv.stateMachine.Put(op.Key, op.Value)
	case AppendOp:
		reply.Err = kv.stateMachine.Append(op.Key, op.Value)
	default:
		reply.Err = ErrUnknownOp
	}
	return reply
}

func (kv *KVServer) takeSnapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.stateMachine)
	e.Encode(kv.lastOp)
	data := w.Bytes()
	kv.rf.Snapshot(index, data)
}

func (kv *KVServer) readPersist(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var stateMachine MemoryKV
	var lastOperations map[int64]OpContext
	if d.Decode(&stateMachine) != nil || d.Decode(&lastOperations) != nil {
		panic("Failed to restore state from snapshot")
	}
	kv.stateMachine, kv.lastOp = &stateMachine, lastOperations
}

func (kv *KVServer) applier() {
	for kv.killed() == false {
		select {
		case message := <-kv.applyCh:
			if message.CommandValid {
				kv.mu.Lock()
				if message.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = message.CommandIndex

				reply := new(OpReply)
				command := message.Command.(Command).Op
				if command.OpType != GetOp && kv.checkDuplicated(command.ClientId, command.SerialNum) {
					reply = kv.lastOp[command.ClientId].LastOpReply
				} else {
					reply = kv.applyLogToStateMachine(*command)
					if command.OpType != GetOp {
						kv.lastOp[command.ClientId] = OpContext{
							SerialNum:   command.SerialNum,
							LastOpReply: reply,
						}
					}
				}
				if currentTerm, isLeader := kv.rf.GetState(); isLeader && message.CommandTerm == currentTerm {
					ch := kv.getNotifyCh(message.CommandIndex)
					ch <- reply
				}
				if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
					kv.takeSnapshot(message.CommandIndex)
				}
				kv.mu.Unlock()
			} else if message.SnapshotValid {
				kv.mu.Lock()
				kv.readPersist(message.Snapshot)
				kv.mu.Unlock()
			}
		}
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(Command{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.stateMachine = newMemoryKV()
	kv.lastOp = make(map[int64]OpContext)
	kv.notifyChs = make(map[int]chan *OpReply)
	kv.persister = persister
	kv.readPersist(kv.persister.ReadSnapshot())

	go kv.applier()

	return kv
}
