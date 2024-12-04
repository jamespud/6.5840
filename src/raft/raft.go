package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

type Entry struct {
	Command interface{}
	Term    int // term when the entry was created
	Index   int // index of the entry in rf.logs
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh        chan ApplyMsg // channel to send ApplyMsg to the service
	applyCond      *sync.Cond    // used to wakeup applier goroutine after committing new entries
	replicatorCond []*sync.Cond  // used to signal replicator goroutine to batch replicating entries
	state          NodeState     // current state of the node

	currentTerm int
	votedFor    int     // id of the candidate that this node voted for in current term, -1 if none
	logs        []Entry // log entries maintained by this node

	commitIndex int // index of the highest log entry known to be committed
	lastApplied int
	nextIndex   []int // nextIndex[i] = index of the next log entry to send to server i
	matchIndex  []int // matchIndex[i] = index of the last log entry known to be replicated on server i

	lastIncludedIndex int // index of the last log entry included in the snapshot
	lastIncludedTerm  int // term of the last log entry included in the snapshot
	snapshot          []byte
	applySnapshot     bool

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term = rf.currentTerm
	var isleader = rf.state == Leader
	DPrintf("Node %d at term %d GetState state %v", rf.me, rf.currentTerm, rf.state)
	// Your code here (3A).
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	if rf.snapshot == nil {
		rf.snapshot = rf.persister.ReadSnapshot()
	}
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []Entry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil || d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		DPrintf("Node %d at term %d state %v readPersist error", rf.me, rf.currentTerm, rf.state)
		os.Exit(-1)
	} else {
		rf.currentTerm, rf.votedFor, rf.logs = currentTerm, votedFor, logs
		rf.lastIncludedIndex, rf.lastIncludedTerm, rf.lastApplied, rf.commitIndex = lastIncludedIndex, lastIncludedTerm, lastIncludedIndex, lastIncludedIndex
	}
}

// for follower, discard previous logs
func (rf *Raft) discardLogs(term int, index int) {
	headLog, tailLog := rf.getHeadAndTailLog()
	newLog := make([]Entry, 1)
	if index <= tailLog.Index {
		newLog = append(newLog, rf.logs[index-headLog.Index+1:]...)
	}
	rf.logs = newLog
	rf.logs[0] = Entry{Term: term, Index: index}
	rf.lastApplied = index
	rf.commitIndex = index
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	if index <= rf.lastIncludedIndex {
		return
	}

	headLog, _ := rf.getHeadAndTailLog()
	offset := index - headLog.Index
	rf.lastIncludedIndex, rf.lastIncludedTerm = rf.logs[offset].Index, rf.logs[offset].Term
	rf.logs = rf.logs[offset:]
	rf.snapshot = snapshot
	rf.persist()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term              int
	LeaderId          int
	PrevLogIndex      int
	PrevLogTerm       int
	Entries           []Entry
	LeaderCommitIndex int
}

type AppendEntriesReply struct {
	Term       int
	Success    bool
	FirstIndex int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("Node %d at term %d received vote request from node %d at term %d", rf.me, rf.currentTerm, args.CandidateId, args.Term)

	reply.Term, reply.VoteGranted = rf.currentTerm, false

	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		// If the candidate's term is less than the current term, or the candidate has already voted for another candidate in the current term, reject the vote.
		DPrintf("Node %d at term %d state %v reject vote from node %d at term %d", rf.me, rf.currentTerm, rf.state, args.CandidateId, args.Term)
		return
	}

	if args.Term > rf.currentTerm {
		// If the candidate's term is greater than the current term,update the current term and become a follower.
		rf.currentTerm, rf.votedFor = args.Term, -1
	}

	if !rf.checkLogUpToDate(args.LastLogTerm, args.LastLogIndex) {
		// If the candidate's log is not up to date, reject the vote.
		DPrintf("Node %d at term %d state %v reject Node %d's vote request: curLogTerm: %d argLogTerm %d, curLogIndex: %d argLogIndex %d", rf.me, rf.currentTerm, rf.state, args.CandidateId, rf.logs[len(rf.logs)-1].Term, args.LastLogTerm, rf.logs[len(rf.logs)-1].Index, args.LastLogIndex)
		return
	}

	rf.changeState(Follower)
	rf.votedFor = args.CandidateId
	reply.Term, reply.VoteGranted = rf.currentTerm, true
	DPrintf("Node %d at term %d voted for node %d at term %d state %v", rf.me, rf.currentTerm, rf.votedFor, rf.currentTerm, rf.state)
	rf.persist()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("Node %d at term %d state %d received appendentry request from node %d at term %d", rf.me, rf.currentTerm, rf.state, args.LeaderId, args.Term)
	DPrintf("\tCurLogIndex: %d, CurLogTerm %d, commitIndex: %d, lastApplied: %d", rf.logs[len(rf.logs)-1].Index, rf.logs[len(rf.logs)-1].Term, rf.commitIndex, rf.lastApplied)
	DPrintf("\tPreLogIndex: %d, PreLogTerm %d, LeaderCommitIndex %d, Entry length %d", args.PrevLogIndex, args.Term, args.LeaderCommitIndex, len(args.Entries))

	headLog, tailLog := rf.getHeadAndTailLog()

	if args.Term < rf.currentTerm {
		// If the leader's term is less than the current term, reject the heartbeat.
		reply.Term, reply.Success, reply.FirstIndex = rf.currentTerm, false, tailLog.Index+1
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.persist()
	}

	rf.changeState(Follower)

	if args.PrevLogIndex > tailLog.Index || args.PrevLogIndex < headLog.Index-1 {
		// args.PrevLogIndex isn't in follower's logs
		reply.Term, reply.Success, reply.FirstIndex = rf.currentTerm, false, tailLog.Index+1
		return
	}

	// headLog.Index is an offset here when logs may persist to snapshot
	matchTerm := rf.logs[args.PrevLogIndex-headLog.Index].Term

	if matchTerm != args.PrevLogTerm {
		// find the first mismatch log entry
		for i := args.PrevLogIndex - 1; i >= headLog.Index; i-- {
			if rf.logs[i-headLog.Index].Term != matchTerm {
				reply.Term, reply.Success = rf.currentTerm, false
				reply.Term, reply.Success, reply.FirstIndex = rf.currentTerm, false, i+1
				return
			}
		}
	}

	rf.logs = append(rf.logs[:args.PrevLogIndex-headLog.Index+1], args.Entries...)
	_, tailLog = rf.getHeadAndTailLog()
	reply.Term, reply.Success, reply.FirstIndex = rf.currentTerm, true, args.PrevLogIndex+len(args.Entries)
	if args.LeaderCommitIndex > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommitIndex, tailLog.Index)
		rf.applyCond.Signal()
	}
	rf.persist()
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.changeState(Follower)
	}
	rf.persist()

	rf.discardLogs(args.LastIncludedTerm, args.LastIncludedIndex)
	rf.Snapshot(args.LastIncludedIndex, args.Data)
	rf.applySnapshot = true
	rf.applyCond.Signal()
}

func (rf *Raft) getHeadAndTailLog() (Entry, Entry) {
	return rf.logs[0], rf.logs[len(rf.logs)-1]
}

// should be called with lock held
func (rf *Raft) changeState(newState NodeState) {
	if newState == Leader {
		if rf.state != Candidate {
			DPrintf("Illegal state transition: %v -> %v", rf.state, newState)
			os.Exit(-1)
		}
		rf.state = Leader
		rf.electionTimer.Stop()
		rf.heartbeatTimer.Reset(stableHeartbeatTimeout())
		newIndex := rf.logs[len(rf.logs)-1].Index
		for i := range rf.peers {
			// Initialize nextIndex to leader's lastLogIndex + 1
			rf.nextIndex[i] = newIndex + 1
			rf.matchIndex[i] = 0
		}
		DPrintf("Node %d at term %d state %v become leader", rf.me, rf.currentTerm, rf.state)
		rf.broadcastHeartbeat()
	} else if newState == Candidate {
		if rf.state == Leader {
			DPrintf("Illegal state transition: %v -> %v", rf.state, newState)
			os.Exit(-1)
		}
		rf.state = Candidate
		rf.electionTimer.Reset(randomElectionTimeout())
		rf.heartbeatTimer.Stop()
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.persist()
		DPrintf("Node %d at term %d state %v become candidate", rf.me, rf.currentTerm, rf.state)
	} else {
		rf.state, rf.votedFor = Follower, -1
		rf.heartbeatTimer.Stop()
		rf.electionTimer.Reset(randomElectionTimeout())
		rf.persist()
		DPrintf("Node %d at term %d state %v become follower", rf.me, rf.currentTerm, rf.state)
	}
}

func (rf *Raft) checkLogUpToDate(LastLogTerm int, lastLogIndex int) bool {
	term, index := rf.logs[len(rf.logs)-1].Term, rf.logs[len(rf.logs)-1].Index
	return LastLogTerm > term || (LastLogTerm == term && lastLogIndex >= index)
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader := rf.state == Leader
	term := rf.currentTerm

	if !isLeader {
		return -1, term, isLeader
	}

	DPrintf("Node %d at term %d starts to commit command %v", rf.me, term, command)
	_, tailLog := rf.getHeadAndTailLog()
	index := tailLog.Index + 1
	rf.logs = append(rf.logs, Entry{Command: command, Term: term, Index: index})
	rf.persist()

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			DPrintf("Node %d at term %d state %v election timeout", rf.me, rf.currentTerm, rf.state)
			rf.changeState(Candidate)
			rf.startElection()
			rf.electionTimer.Reset(randomElectionTimeout())
			rf.heartbeatTimer.Stop()
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			DPrintf("Node %d at term %d state %v heartbeat timeout", rf.me, rf.currentTerm, rf.state)
			if rf.state == Leader {
				rf.broadcastHeartbeat()
				rf.heartbeatTimer.Reset(stableHeartbeatTimeout())
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) startElection() {
	DPrintf("node %d at term %d state %v start election", rf.me, rf.currentTerm, rf.state)

	votes := 1
	rf.persist()

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.logs[len(rf.logs)-1].Index,
		LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
	}

	for peer := range rf.peers {
		if peer != rf.me {
			go func(server int) {
				reply := RequestVoteReply{}
				if rf.sendRequestVote(server, &args, &reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.currentTerm == args.Term && rf.state == Candidate {
						if reply.Term > rf.currentTerm {
							DPrintf("Node %d at term %d state %v received a higher term from node %d at term %d", rf.me, rf.currentTerm, rf.state, server, reply.Term)
							rf.currentTerm, rf.votedFor = reply.Term, -1
							rf.changeState(Follower)
							rf.persist()
							// prevent previous request's influence
						} else if reply.VoteGranted && reply.Term == rf.currentTerm {
							votes++
							if votes > len(rf.peers)/2 {
								DPrintf("Node %d at term %d state %v received majority votes from other nodes", rf.me, rf.currentTerm, rf.state)
								rf.currentTerm, rf.votedFor = args.Term, -1
								rf.changeState(Leader)
							}
						}
					}
				}
			}(peer)
		}
	}
}

func (rf *Raft) sendInstallSnapshotRoutine(server int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	reply := &InstallSnapshotReply{}
	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.snapshot,
	}
	rf.mu.Unlock()
	if rf.sendInstallSnapshot(server, args, reply) {
		rf.mu.Lock()
		if rf.state == Leader && args.Term == rf.currentTerm {
			if reply.Term > rf.currentTerm {
				rf.currentTerm, rf.votedFor = reply.Term, -1
				rf.changeState(Follower)
				rf.persist()
			} else if args.LastIncludedIndex > rf.matchIndex[server] {
				rf.matchIndex[server] = max(rf.nextIndex[server], args.LastIncludedIndex)
				rf.nextIndex[server] = rf.matchIndex[server] + 1
				rf.updateCommitIndex()
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) broadcastHeartbeat() {
	for peer := range rf.peers {
		if peer != rf.me {
			go func(server int) {
				success := false
				for !success {
					rf.mu.Lock()
					if rf.state != Leader {
						rf.mu.Unlock()
						return
					}
					// if the leader has already installed snapshot, send snapshot to the follower

					if rf.lastIncludedIndex > 0 && rf.nextIndex[server] <= rf.lastIncludedIndex {
						go rf.sendInstallSnapshotRoutine(server)
						rf.mu.Unlock()
						return
					}
					headLog, _ := rf.getHeadAndTailLog()
					prevLogIndex := rf.nextIndex[server] - 1
					prevLogTerm := rf.logs[prevLogIndex-headLog.Index].Term
					entries := rf.logs[rf.nextIndex[server]-headLog.Index:]
					DPrintf("Node %d at term %d state %v sendAppendEntry to node %d: prevLogIndex: %d, prevLogTerm: %d, LeaderCommitIndex: %d", rf.me, rf.currentTerm, rf.state, server, prevLogIndex, prevLogTerm, rf.commitIndex)
					DPrintf("\tEntry start at %d: , Enetry length %d", rf.nextIndex[server]-headLog.Index, len(entries))

					args := AppendEntriesArgs{
						Term:              rf.currentTerm,
						LeaderId:          rf.me,
						PrevLogIndex:      prevLogIndex,
						PrevLogTerm:       prevLogTerm,
						Entries:           entries,
						LeaderCommitIndex: rf.commitIndex,
					}
					reply := AppendEntriesReply{}

					// release lock to get better performance
					rf.mu.Unlock()

					for !rf.sendAppendEntry(server, &args, &reply) {
						time.Sleep(50 * time.Millisecond)
						// DPrintf("Node %d at term %d state %v sendAppendEntry to node %d failed. retry", rf.me, rf.currentTerm, rf.state, server)
					}

					rf.mu.Lock()
					if rf.state != Leader || rf.currentTerm != args.Term {
						rf.mu.Unlock()
						return
					}

					if reply.Term > rf.currentTerm {
						DPrintf("Node %d at term %d state %d received higher appendentry reply from Node %d term %d", rf.me, rf.currentTerm, rf.state, server, reply.Term)
						rf.currentTerm = reply.Term
						rf.changeState(Follower)
						rf.persist()
						rf.mu.Unlock()
						return
					}

					if reply.Success {
						if len(args.Entries) > 0 {
							rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
							rf.nextIndex[server] = rf.matchIndex[server] + 1
							DPrintf("Node %d at term %d state %v update matchIndex[%d] to %d, nextIndex[%d] to %d", rf.me, rf.currentTerm, rf.state, server, rf.matchIndex[server], server, rf.nextIndex[server])
						}
						rf.updateCommitIndex()
						success = true
						rf.mu.Unlock()
						return
					} else {
						// if failed, reduce nextIndex and retry
						DPrintf("Node %d at term %d state %v sendAppendEntry to node %d failed, reduce nextIndex[%d] from %d to %d", rf.me, rf.currentTerm, rf.state, server, server, rf.nextIndex[server], rf.nextIndex[server]-1)
						rf.nextIndex[server] = min(reply.FirstIndex, rf.logs[len(rf.logs)-1].Index)
						time.Sleep(100 * time.Millisecond)
					}
					rf.mu.Unlock()
				}
			}(peer)
		}
	}
}

func (rf *Raft) updateCommitIndex() {
	// get the last index of current logs
	headLog, tailLog := rf.getHeadAndTailLog()
	// check from the last index to the commitIndex
	for index := tailLog.Index; index > rf.commitIndex && index > headLog.Index && rf.logs[index-headLog.Index].Term == rf.currentTerm; index-- {
		// count the number of nodes that have replicated the log entry, including itself
		count := 1
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= index {
				count++
			}
		}
		// DPrintf("Node %d at term %d state %v log index: %d, commit count: %d", rf.me, rf.currentTerm, rf.state, index, count)
		// if more than half of the nodes have replicated the log entry and the entry is created in the current term
		if count > len(rf.peers)/2 {
			DPrintf("Node %d at term %d state %v update commitIndex to %d", rf.me, rf.currentTerm, rf.state, index)
			rf.commitIndex = index
			rf.applyCond.Signal()
			break
		}
		// else {
		// 	DPrintf("Node %d at term %d state %v log index: %d, commit count: %d", rf.me, rf.currentTerm, rf.state, index, count)
		// }
	}
}

func (rf *Raft) applyLogs() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		if rf.applySnapshot {
			rf.applySnapshot = false
			msg := ApplyMsg{
				CommandValid:  false,
				SnapshotValid: true,
				Snapshot:      rf.snapshot,
				SnapshotTerm:  rf.lastIncludedTerm,
				SnapshotIndex: rf.lastIncludedIndex,
			}
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
		} else if rf.lastApplied < rf.commitIndex {
			//DPrintf("Node %d at term %d %v applyLogs: %v, %v, %v", rf.me, rf.currentTerm, rf.state, rf.lastApplied, rf.commitIndex, rf.logs)
			msgs := make([]ApplyMsg, 0)
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				msgs = append(msgs, ApplyMsg{
					CommandValid: true,
					Command:      rf.logs[i-rf.logs[0].Index].Command,
					CommandIndex: i,
					CommandTerm:  rf.logs[i-rf.logs[0].Index].Term,
				})
			}
			commitIndex := rf.commitIndex
			rf.mu.Unlock()
			for _, msg := range msgs {
				rf.applyCh <- msg
			}
			rf.mu.Lock()
			rf.lastApplied = max(rf.lastApplied, commitIndex)
		}
		rf.mu.Unlock()
	}
}

func randomElectionTimeout() time.Duration {
	return time.Duration(250+rand.Intn(200)) * time.Millisecond
}

func stableHeartbeatTimeout() time.Duration {
	return time.Duration(150) * time.Millisecond
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:             peers,
		persister:         persister,
		me:                me,
		dead:              0,
		applyCh:           applyCh,
		state:             Follower,
		replicatorCond:    make([]*sync.Cond, len(peers)),
		currentTerm:       0,
		votedFor:          -1,
		logs:              make([]Entry, 1),
		commitIndex:       0,
		lastApplied:       0,
		lastIncludedIndex: 0,
		lastIncludedTerm:  0,
		applySnapshot:     false,
		nextIndex:         make([]int, len(peers)),
		matchIndex:        make([]int, len(peers)),
		electionTimer:     time.NewTimer(randomElectionTimeout()),
		heartbeatTimer:    time.NewTimer(stableHeartbeatTimeout()),
	}

	// Your initialization code here (3A, 3B, 3C).
	rf.heartbeatTimer.Stop()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()
	rf.applyCond = sync.NewCond(&rf.mu)

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applyLogs()

	return rf
}
