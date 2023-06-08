package raft

import (
	"bytes"
	"context"
	"log"
	"math/rand"
	"mit6824/labgob"
	"mit6824/labrpc"
	"sync"
	"sync/atomic"
	"time"
)

const (
	follower  string = "follower"
	candidate string = "candidate"
	leader    string = "leader"
)

// ApplyMsg for sending committed logs to server
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
// you'll want to send other kinds of messages (e.g. snapshots) on the applyCh,
// but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	timeout      bool   // check for timeout by 0 or 1
	role         string // the role of this peer
	lastLog      LogEntry
	resetTimerCh chan struct{}

	// Persistent state on all servers
	CurrentTerm   int
	VotedFor      int
	Logs          []LogEntry
	LastSnapTerm  int
	LastSnapIndex int

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int // term in the conflicting entry (if any)
	XIndex  int // index of first entry with that term (if any)
	XLen    int // log length
}

type InstallSnapshotArgs struct {
	LeaderId          int    // so follower can redirect clients
	Term              int    // leader’s term
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Data              []byte // raw bytes of the snapshot chunk, starting at offset
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.CurrentTerm {
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
		return
	} else if args.Term > rf.CurrentTerm {
		rf.role = follower
		rf.VotedFor = -1
		rf.CurrentTerm = args.Term
	}

	lastLog := rf.lastLog
	if (rf.VotedFor == -1 || rf.VotedFor == args.CandidateId) && 
	(args.LastLogTerm > lastLog.Term || (args.LastLogTerm == lastLog.Term && args.LastLogIndex >= lastLog.Index)) {
		reply.VoteGranted = true
		rf.VotedFor = args.CandidateId
		// Restart election timer when grant a vote to another peer.
		rf.timeout = false
		return
	}
	reply.VoteGranted = false
}

//Whether the current logs of raft exceed the log records sent
func (rf *Raft) isOutOfArgsEntries(args *AppendEntriesArgs) bool {
	argsLastLogIndex := args.PrevLogIndex + len(args.Entries)
	if rf.lastLog.Term == args.Term && argsLastLogIndex < rf.lastLog.Index {
		return true
	}
	return false
}

// AppendEntries RPC handler.
// if len(entries) is zero, means heartbeat
//
// In raft, a leader is elected for each term.
// When the cluster enters a new term or the node election fails,
// the role and voteFor need to be reset
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return
	}
	rf.timeout = false

	if args.Term > rf.CurrentTerm {
		rf.role = follower
		rf.VotedFor = -1
		rf.CurrentTerm = args.Term
	}
	if args.Term == rf.CurrentTerm && rf.role == candidate {
		rf.role = follower
	}

	if args.PrevLogIndex < rf.LastSnapIndex {
		// expired logs
		reply.XIndex = rf.LastSnapIndex
		reply.XTerm = rf.LastSnapTerm
		reply.Success = false
		return
	} else if args.PrevLogIndex == rf.LastSnapIndex {
		if rf.isOutOfArgsEntries(args) {
			// insertion will cause out of order
			reply.XIndex = -1
			reply.Success = false
		} else {
			reply.Success = true
			rf.Logs = append([]LogEntry{}, args.Entries...)
			rf.changeLastLog()
			if rf.commitIndex < args.LeaderCommit {
				rf.commitIndex = args.LeaderCommit
			}
			rf.persist()
		}
		return
	} else if args.PrevLogIndex > rf.lastLog.Index {
		// Reply false if log does not contain an entry at prevLogIndex whose term matches prevLogTerm
		reply.XLen = rf.lastLog.Index
		reply.Success = false
		return
	}

	logCom := rf.Logs[args.PrevLogIndex-rf.LastSnapIndex-1]
	if logCom.Term != args.PrevLogTerm {
		reply.XIndex = logCom.Index
		reply.Success = false
		reply.XLen = rf.lastLog.Index
		reply.XTerm = logCom.Term
		for reply.XIndex > rf.commitIndex && reply.XIndex > rf.LastSnapIndex && 
		rf.Logs[reply.XIndex-rf.LastSnapIndex-1].Term == logCom.Term {
			reply.XIndex--
		}
		if (reply.XIndex == rf.LastSnapIndex && rf.LastSnapTerm != logCom.Term) || 
		(reply.XIndex > rf.LastSnapIndex && rf.Logs[reply.XIndex-rf.LastSnapIndex-1].Term != logCom.Term) {
			reply.XIndex++
		}
		return
	}

	if rf.isOutOfArgsEntries(args) {
		// insertion will cause out of order
		reply.XIndex = -1
		reply.Success = false
		return
	}

	reply.Success = true
	rf.Logs = append(rf.Logs[:args.PrevLogIndex-rf.LastSnapIndex], args.Entries...)
	rf.changeLastLog()

	if rf.commitIndex < args.LeaderCommit {
		rf.commitIndex = args.LeaderCommit
	}
	rf.persist()
}

// InstallSnapshot RPC handler.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		rf.mu.Unlock()
		return
	}
	rf.timeout = false

	if args.Term > rf.CurrentTerm {
		rf.role = follower
		rf.VotedFor = -1
		rf.CurrentTerm = args.Term
	}
	if args.Term == rf.CurrentTerm && rf.role == candidate {
		rf.role = follower
	}

	if args.LastIncludedIndex <= rf.LastSnapIndex {
		// expired snapshot
		rf.mu.Unlock()
		return
	}

	if args.LastIncludedIndex >= rf.lastLog.Index {
		rf.Logs = []LogEntry{}
	} else {
		rf.Logs = rf.Logs[args.LastIncludedIndex-rf.LastSnapIndex:]
	}
	rf.LastSnapIndex, rf.LastSnapTerm = args.LastIncludedIndex, args.LastIncludedTerm

	if rf.commitIndex < rf.LastSnapIndex {
		rf.commitIndex = rf.LastSnapIndex
	}
	rf.changeLastLog()
	rf.persister.Save(rf.encodeState(), args.Data)
	rf.mu.Unlock()

	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.CurrentTerm) != nil || e.Encode(rf.VotedFor) != nil || e.Encode(rf.Logs) != nil ||
		e.Encode(rf.LastSnapIndex) != nil || e.Encode(rf.LastSnapTerm) != nil {
		log.Fatalln("failed to restore raft's state")
	}
	return w.Bytes()
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
func (rf *Raft) persist() {
	rf.persister.Save(rf.encodeState(), rf.persister.ReadSnapshot())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	var lastSnapIndex int
	var lastSnapTerm int
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil ||
		d.Decode(&lastSnapIndex) != nil || d.Decode(&lastSnapTerm) != nil {
		log.Fatalln("failed to restore previously persisted state")
	} else {
		rf.mu.Lock()
		rf.CurrentTerm = currentTerm
		rf.VotedFor = votedFor
		rf.Logs = append([]LogEntry{}, logs...)
		rf.LastSnapIndex = lastSnapIndex
		rf.LastSnapTerm = lastSnapTerm
		if rf.LastSnapIndex >= 0 {
			rf.commitIndex = rf.LastSnapIndex
		}
		rf.changeLastLog()
		rf.mu.Unlock()
	}
}

// change rf.lastLog
func (rf *Raft) changeLastLog() {
	if len(rf.Logs) > 0 {
		rf.lastLog = rf.Logs[len(rf.Logs)-1]
	} else {
		if rf.LastSnapIndex >= 0 {
			rf.lastLog = LogEntry{
				Command: nil,
				Term:    rf.LastSnapTerm,
				Index:   rf.LastSnapIndex,
			}
		} else {
			log.Println("log append error")
		}
	}
}

// GetState return CurrentTerm and whether this peer believes it is the leader.
func (rf *Raft) GetState() (currentTerm int, isLeader bool) {
	rf.mu.Lock()
	currentTerm = rf.CurrentTerm
	isLeader = rf.role == leader
	rf.mu.Unlock()
	return
}

// Snapshot for server
// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.LastSnapIndex {
		return
	}

	oldSnapIndex := rf.LastSnapIndex
	term := rf.Logs[index-oldSnapIndex-1].Term
	rf.LastSnapIndex = index
	rf.LastSnapTerm = term
	rf.Logs = rf.Logs[index-oldSnapIndex:]
	rf.changeLastLog()
	rf.persister.Save(rf.encodeState(), snapshot)
}

// Start a command
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
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index, term, isLeader = -1, -1, false
	if rf.killed() || rf.role != leader {
		return
	}
	index = rf.LastSnapIndex + 1
	if len(rf.Logs) != 0 {
		index = rf.lastLog.Index + 1
	}
	term = rf.CurrentTerm
	isLeader = true
	rf.Logs = append(rf.Logs, LogEntry{
		Index:   index,
		Term:    term,
		Command: command,
	})
	rf.changeLastLog()
	select {
	case rf.resetTimerCh <- struct{}{}:
	default:
	}
	return
}

// Kill the peer
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		ctx, cancel := context.WithCancel(context.Background())
		rf.mu.Lock()
		// Check if a leader election should be started.
		if rf.timeout && rf.role != leader {
			rf.role = candidate
			rf.CurrentTerm++
			rf.VotedFor = rf.me
			go rf.election(ctx)
		}
		rf.timeout = true
		rf.mu.Unlock()
		// pause for a random amount of time between 400 and 700 milliseconds.
		ms := 400 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		// leader election timeout, enter next election
		cancel()
	}
}

// leader election function
func (rf *Raft) election(ctx context.Context) {
	rf.mu.Lock()
	var count uint64
	lastLog := rf.lastLog
	curTerm := rf.CurrentTerm
	rf.mu.Unlock()

	go func(ctx context.Context) {
		for func(args uint64) int {
			ans := 0
			for args > 0 {
				if args%2 == 1 {
					ans++
				}
				args >>= 1
			}
			return ans
		}(atomic.LoadUint64(&count)) < len(rf.peers)/2 {
			select {
			case <-ctx.Done():
				return
			default:
			}
		}

		rf.mu.Lock()
		if rf.role != candidate {
			rf.mu.Unlock()
			return
		}
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = rf.lastLog.Index + 1
			rf.matchIndex[i] = 0
		}
		rf.role = leader
		go rf.heartbeat(rf.CurrentTerm)
		rf.mu.Unlock()
	}(ctx)

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int) {
			for {
				args := &RequestVoteArgs{curTerm, rf.me, lastLog.Index, lastLog.Term}
				reply := &RequestVoteReply{}

				if ok := rf.peers[server].Call("Raft.RequestVote", args, reply); !ok {
					return
				}

				if reply.VoteGranted && atomic.LoadUint64(&count)&(1<<server) == 0 {
					atomic.AddUint64(&count, 1<<server)
				} else {
					rf.mu.Lock()
					if reply.Term > rf.CurrentTerm {
						rf.role = follower
						rf.VotedFor = -1
						rf.CurrentTerm = reply.Term
					}
					rf.mu.Unlock()
				}

				rf.mu.Lock()
				if rf.role != candidate {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()

				time.Sleep(time.Millisecond * 100)
			}
		}(i)
	}
}

// Raft never commits log entries from previous terms by counting
// replicas. Only log entries from the leader’s current
// term are committed by counting replicas; once an entry
// from the current term has been committed in this way,
// then all prior entries are committed indirectly because
// of the Log Matching Property.
func (rf *Raft) heartbeat(curTerm int) {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.p2pHeartbeat(i, curTerm)
	}
	go rf.commitIndexPushDown()
}

func (rf *Raft) p2pHeartbeat(server int, curTerm int) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.role != leader {
			rf.mu.Unlock()
			return
		}

		args := AppendEntriesArgs{
			Term:     curTerm,
			LeaderId: rf.me,
		}
		var reply AppendEntriesReply
		if rf.nextIndex[server] < rf.LastSnapIndex+1 {
			rf.nextIndex[server] = rf.LastSnapIndex + 1
			args.PrevLogIndex = rf.LastSnapIndex
			args.PrevLogTerm = rf.LastSnapTerm
			args.Entries = append([]LogEntry{}, rf.Logs...)
			args.LeaderCommit = rf.commitIndex
			data := rf.persister.ReadSnapshot()
			rf.mu.Unlock()
			if ok := rf.sendSnap(server, curTerm, args.PrevLogIndex, args.PrevLogTerm, data); !ok {
				return
			}
		} else if rf.nextIndex[server] == rf.LastSnapIndex+1 {
			args.PrevLogIndex = rf.LastSnapIndex
			args.PrevLogTerm = rf.LastSnapTerm
			args.Entries = append([]LogEntry{}, rf.Logs...)
			args.LeaderCommit = rf.commitIndex
			rf.mu.Unlock()
		} else {
			args.PrevLogIndex = rf.nextIndex[server] - 1
			args.PrevLogTerm = rf.Logs[args.PrevLogIndex-rf.LastSnapIndex-1].Term
			args.Entries = append([]LogEntry{}, rf.Logs[rf.nextIndex[server]-rf.LastSnapIndex-1:]...)
			args.LeaderCommit = rf.commitIndex
			rf.mu.Unlock()
		}

		waitCh := make(chan bool)
		go func(server int) {
			if isReply := rf.peers[server].Call("Raft.AppendEntries", &args, &reply); isReply {
				waitCh <- true
			}
		}(server)

		select {
		case <-time.Tick(time.Millisecond * 200):
			continue
		case <-waitCh:
		}

		rf.mu.Lock()
		if reply.Term > curTerm && reply.Term > rf.CurrentTerm {
			rf.role = follower
			rf.CurrentTerm = reply.Term
			rf.VotedFor = -1
			rf.persist()
			rf.mu.Unlock()
			return
		}
		if !reply.Success {
			if reply.XLen < args.PrevLogIndex {
				rf.nextIndex[server] = reply.XLen + 1
				rf.mu.Unlock()
				continue
			}

			if reply.XIndex == -1 {
				rf.mu.Unlock()
				continue
			} else if reply.XIndex < rf.LastSnapIndex {
				rf.nextIndex[server] = reply.XIndex
			} else if reply.XIndex == rf.LastSnapIndex {
				if reply.XTerm == rf.LastSnapTerm {
					start := rf.LastSnapIndex + 1
					for rf.Logs[start-rf.LastSnapIndex-1].Term == reply.Term {
						start++
					}
					rf.nextIndex[server] = start
				} else {
					rf.nextIndex[server] = rf.LastSnapIndex
				}
			} else {
				if rf.Logs[reply.XIndex-rf.LastSnapIndex-1].Term != reply.XTerm {
					rf.nextIndex[server] = reply.XIndex
				} else {
					start := reply.XIndex
					for rf.Logs[start-rf.LastSnapIndex-1].Term == reply.Term {
						start++
					}
					rf.nextIndex[server] = start
				}
			}
			rf.mu.Unlock()
			continue
		} else {
			if len(args.Entries) > 0 && rf.role == leader {
				rf.matchIndex[server] = args.Entries[len(args.Entries)-1].Index
				rf.nextIndex[server] = rf.matchIndex[server] + 1
			}
			rf.persist()
		}
		rf.mu.Unlock()

		select {
		case <-time.Tick(time.Millisecond * 150):
		case <-rf.resetTimerCh:
			time.Sleep(time.Millisecond * 10)
		}
	}
}

func (rf *Raft) sendSnap(server int, curTerm int, lastSnapIndex int, lastSnapTerm int, data []byte) bool {
	args := &InstallSnapshotArgs{
		LeaderId:          rf.me,
		Term:              curTerm,
		LastIncludedIndex: lastSnapIndex,
		LastIncludedTerm:  lastSnapTerm,
		Data:              data,
	}

	for !rf.killed() {
		rf.mu.Lock()
		if rf.role != leader {
			rf.mu.Unlock()
			return false
		}
		rf.mu.Unlock()

		reply := &InstallSnapshotReply{}
		waitCh := make(chan bool)
		go func(server int) {
			if isReply := rf.peers[server].Call("Raft.InstallSnapshot", args, reply); isReply {
				waitCh <- true
			}
		}(server)

		select {
		case <-time.Tick(time.Millisecond * 200):
			continue
		case <-waitCh:
		}

		rf.mu.Lock()
		if reply.Term > rf.CurrentTerm {
			rf.CurrentTerm = reply.Term
			rf.VotedFor = -1
			rf.role = follower
			rf.persist()
			rf.mu.Unlock()
			return false
		}
		rf.mu.Unlock()

		return true
	}
	return false
}

func (rf *Raft) commitIndexPushDown() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.role != leader {
			rf.mu.Unlock()
			return
		}
		rf.matchIndex[rf.me] = rf.lastLog.Index
		tmp := make([]int, len(rf.matchIndex))
		copy(tmp, rf.matchIndex)
		rf.mu.Unlock()

		med := findMed(tmp)

		rf.mu.Lock()
		if med > rf.commitIndex && rf.Logs[med-rf.LastSnapIndex-1].Term == rf.CurrentTerm {
			rf.commitIndex = med
		}
		rf.mu.Unlock()

		time.Sleep(time.Millisecond * 10)
	}
}

func (rf *Raft) applier() {
	for !rf.killed() {
		var logs []LogEntry
		rf.mu.Lock()
		if rf.lastApplied < rf.LastSnapIndex {
			rf.lastApplied = rf.LastSnapIndex
		}
		logs = append(logs, rf.Logs[rf.lastApplied-rf.LastSnapIndex:rf.commitIndex-rf.LastSnapIndex]...)
		if len(logs) > 0 {
			rf.lastApplied = logs[len(logs)-1].Index
		}
		rf.mu.Unlock()

		for _, logEntry := range logs {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      logEntry.Command,
				CommandIndex: logEntry.Index,
			}
		}

		time.Sleep(10 * time.Millisecond)
	}
}

// Make a peer
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		mu:            sync.Mutex{},
		peers:         peers,
		persister:     persister,
		me:            me,
		dead:          0,
		applyCh:       applyCh,
		role:          follower,
		timeout:       false,
		lastLog:       LogEntry{nil, 0, 0},
		resetTimerCh:  make(chan struct{}, 1),
		CurrentTerm:   0,
		VotedFor:      -1,
		Logs:          []LogEntry{{nil, 0, 0}},
		LastSnapIndex: -1,
		LastSnapTerm:  -1,
		commitIndex:   0,
		lastApplied:   0,
		nextIndex:     make([]int, len(peers)),
		matchIndex:    make([]int, len(peers)),
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start a goroutine to send applyMsg to server
	go rf.applier()

	return rf
}
