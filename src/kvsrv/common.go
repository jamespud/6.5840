package kvsrv

type RequestType int

const (
	REQ = iota
	ACK
)

// Put or Append
type PutAppendArgs struct {
	Key       string
	Value     string
	MessageID int64
	Req       RequestType
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Value string
}
