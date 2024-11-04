package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.RWMutex

	Cache   map[string]string
	history sync.Map
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	value, exists := kv.Cache[args.Key]
	if exists {
		reply.Value = value
	} else {
		reply.Value = ""
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.Req == ACK {
		kv.history.Delete(args.MessageID)
		return
	}
	if res, ok := kv.history.Load(args.MessageID); ok {
		reply.Value = res.(string)
		return
	}

	old := kv.Cache[args.Key]
	kv.Cache[args.Key] = args.Value
	reply.Value = old
	kv.history.Store(args.MessageID, reply.Value)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.Req == ACK {
		kv.history.Delete(args.MessageID)
		return
	}
	if res, ok := kv.history.Load(args.MessageID); ok {
		reply.Value = res.(string)
		return
	}

	old := kv.Cache[args.Key]
	kv.Cache[args.Key] = old + args.Value
	reply.Value = old
	kv.history.Store(args.MessageID, reply.Value)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.Cache = make(map[string]string)

	return kv
}
