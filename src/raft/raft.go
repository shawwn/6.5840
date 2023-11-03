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
	//	"bytes"
	"fmt"
	"golang.org/x/exp/constraints"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

func min[T constraints.Ordered](a, b T) T {
	if a < b {
		return a
	}
	return b
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	RaftFollower  = iota
	RaftCandidate = iota
	RaftLeader    = iota
)

type LogEntry struct {
	Command interface{} // command for state machine
	Term    int         // term when entry was received by leader
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 2A
	currentTerm     int           // latest Term server has seen (initialized to 0, increases monotonically)
	votedFor        int           // CandidateId that received vote in current Term (or -1 if none)
	state           int           // whether we're a follower, candidate, or leader
	electionTimeout time.Duration // time till a new election is started

	// 2B
	currentLeader int
	applyCh       chan ApplyMsg
	logs          map[int]LogEntry // log entries (first index is 1)
	//
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	// reinitialized after each election:
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
}

func (rf *Raft) lastLogIndex() int {
	/*
		index := 0
		for i, _ := range rf.logs {
			if i > index {
				index = i
			}
		}
		return index
	*/
	return len(rf.logs)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == RaftLeader

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
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// invoked by leader to replicate log entries.
type AppendEntriesArgs struct {
	Term     int // leader's Term
	LeaderId int // so follower can redirect clients

	// 2B
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// example AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.debug("AppendEntries %v", args)
	defer func() {
		rf.debug("-- AppendEntries %v %v", args, reply)
	}()

	rf.checkTerm(args.Term, "AppendEntries")

	reply.Term = rf.currentTerm
	reply.Success = false

	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		rf.info("args.Term{%d} < rf.currentTerm{%d}", args.Term, rf.currentTerm)
		return
	}

	if rf.currentLeader < 0 {
		rf.currentLeader = args.LeaderId
		rf.info("currentLeader = %d", args.LeaderId)
	}
	if args.LeaderId == rf.currentLeader {
		rf.resetElectionTimeout()
	} else {
		rf.info("Skipping mismatched leader. args.LeaderId{%d} != rf.currentLeader{%d}", args.LeaderId, rf.currentLeader)
		return
	}

	// 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex > 0 {
		entry, ok := rf.logs[args.PrevLogIndex]
		if !ok {
			rf.info("no rf.logs[args.PrevLogIndex{%d}]", args.PrevLogIndex)
			return
		}
		if entry.Term != args.PrevLogTerm {
			rf.info("entry.Term{%d} != args.PrevLogTerm{%d}", entry.Term, args.PrevLogTerm)
			return
		}
	}

	// 3. If an existing entry conflicts with a new one (same index
	//but different terms), delete the existing entry and all that
	//follow it (§5.3)
	index := args.PrevLogIndex + 1
	for i := 0; i < len(args.Entries); i++ {
		entry, ok := rf.logs[index+i]
		if ok && entry.Term != args.Entries[i].Term {
			rf.info("deleting logs after %d (entry.Term{%d} != args.Entries[i].Term{%d})",
				index+i, entry.Term, args.Entries[i].Term)
			for ok {
				//if index+i > rf.commitIndex
				{
					delete(rf.logs, index+i)
					rf.info("delete logs[%d], commitIndex=%d", index+i, rf.commitIndex)
				}
				i++
				entry, ok = rf.logs[index+i]
			}
			break
		}
	}

	// 4. Append any new entries not already in the log
	lastNewEntryIndex := args.LeaderCommit
	for i := 0; i < len(args.Entries); i++ {
		entry, ok := rf.logs[index+i]
		if ok {
			if entry.Command != args.Entries[i].Command {
				log.Fatalf("Entry exists %d with different command %v", index+i, entry.Command)
			}
		}
		rf.logs[index+i] = args.Entries[i]
		rf.info("logs[%d] = %v", index+i, args.Entries[i])
		lastNewEntryIndex = index + i
	}

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		commitIndex := min(args.LeaderCommit, lastNewEntryIndex)
		rf.info("commitIndex = min(args.LeaderCommit{%d}, lastNewEntryIndex{%d}) = %d",
			args.LeaderCommit, lastNewEntryIndex, commitIndex)
		rf.commitIndex = commitIndex
	}

	reply.Success = true
}

func (rf *Raft) prevLogIndex(index int) int {
	/*
		for i, _ := range rf.logs {
			if i+1 == index {
				return i
			}
		}
		return 0
	*/
	if index <= 0 {
		return 0
	}
	return index - 1
}

func (rf *Raft) sendHeartbeat(server int) bool {
	if rf.state != RaftLeader {
		rf.warn("rf.state{%d} != RaftLeader", rf.state)
		return false
	}
	if rf.currentLeader != rf.me {
		rf.warn("rf.currentLeader{%d} != rf.me{%d}", rf.currentLeader, rf.me)
		return false
	}
	args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.currentLeader
	args.LeaderCommit = rf.commitIndex

	/*
		index := rf.nextIndex[server]
		first := true
		for {
			entry, ok := rf.logs[index]
			if ok {
				if first {
					prev := rf.logs[index-1]
					args.PrevLogIndex = index - 1
					args.PrevLogTerm = prev.Term
					first = false
				}
				rf.info("server=%d appending entry %d: %v", server, index, entry)
				args.Entries = append(args.Entries, entry)
				index++
			} else {
				break
			}
		}
	*/

	// If last log index ≥ nextIndex for a follower:
	// send AppendEntries RPC with log entries starting at nextIndex
	lastIdx := rf.lastLogIndex()
	nextIdx := rf.nextIndex[server]
	prevIdx := rf.prevLogIndex(nextIdx)
	if prevIdx > 0 {
		args.PrevLogIndex = prevIdx
		args.PrevLogTerm = rf.logs[prevIdx].Term
	}
	if lastIdx >= nextIdx {
		for index := nextIdx; index <= lastIdx; index++ {
			entry := rf.logs[index]
			rf.info("server=%d appending entry %d: %v", server, index, entry)
			args.Entries = append(args.Entries, entry)
		}
	}

	reply := AppendEntriesReply{}
	rf.debug("sending heartbeat to %d", server)
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	rf.debug("-- sending heartbeat to %d: %v", server, ok)

	if ok {
		if rf.state == RaftLeader {
			if reply.Term == rf.currentTerm {
				if reply.Success {
					// If successful: update nextIndex and matchIndex for follower (§5.3)
					index := lastIdx + 1
					if rf.nextIndex[server] != index {
						rf.nextIndex[server] = index
						rf.info("rf.nextIndex[server{%d}] = %d", server, index)
					}
					if rf.matchIndex[server] != lastIdx {
						rf.matchIndex[server] = lastIdx
						rf.info("rf.matchIndex[server{%d}] = %d", server, lastIdx)
					}
				} else {
					// If AppendEntries fails because of log inconsistency:
					//decrement nextIndex and retry (§5.3)
					rf.info("decrement nextIndex{%d} and retry", rf.nextIndex[server])
					rf.nextIndex[server]--
					if rf.nextIndex[server] < 1 {
						rf.nextIndex[server] = 1
					}
					return rf.sendHeartbeat(server)
				}
			}
		}
		rf.checkTerm(reply.Term, "heartbeat")
	}

	return ok
}

func (rf *Raft) sendHeartbeats() {
	sent := false
	term := rf.currentTerm
	for ii := 0; ii < len(rf.peers); ii++ {
		if ii != rf.me {
			go func(i int) {
				if rf.sendHeartbeat(i) {
					sent = true
				}
			}(ii)
		}
	}
	go func() {
		time.Sleep(750 * time.Millisecond)
		if rf.state == RaftLeader && rf.currentTerm == term {
			if !sent {
				// no peers, transition to follower
				rf.info("no peers, becoming follower")
				rf.setState(RaftFollower)
			}
		}
	}()
}

func (rf *Raft) updateCommit() {
	if rf.state == RaftLeader {
		// If there exists an N such that N > commitIndex, a majority
		//of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
		for N := 0; N < rf.lastLogIndex()+1; N++ {
			if N > rf.commitIndex {
				if replicatedMajority(rf.matchIndex, N) {
					entry, ok := rf.logs[N]
					if ok && entry.Term == rf.currentTerm {
						rf.info("Setting commitIndex{%d} = N{%d}", rf.commitIndex, N)
						rf.commitIndex = N
					}
				}
			}
		}
	}

	for rf.commitIndex > rf.lastApplied {
		rf.debug("commitIndex{%d} > lastApplied{%d}: %v", rf.commitIndex, rf.lastApplied, rf.commitIndex > rf.lastApplied)
		// increment lastApplied
		rf.lastApplied++
		// apply log[lastApplied] to state machine
		rf.commit(rf.lastApplied)
	}

}

func (rf *Raft) commit(index int) {
	//if rf.commitIndex >= index {
	//	return
	//}
	//for rf.commitIndex < index
	{
		//rf.commitIndex++
		rf.info("Committing %d", index)
		msg := ApplyMsg{}
		msg.CommandIndex = index
		msg.Command = rf.logs[index].Command
		msg.CommandValid = true
		rf.applyCh <- msg
	}
	//rf.info("rf.commitIndex is now %d", rf.commitIndex)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	// 2A
	Term        int // candidate's Term
	CandidateId int // candidate requesting vote

	// 2B
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).

	// 2A
	Term        int // current Term, for candidate to update itself
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.debug("RequestVote %v", args)
	defer func() {
		rf.debug("-- RequestVote %v %v", args, reply)
	}()

	rf.checkTerm(args.Term, "RequestVote")

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// reply false if Term < currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	// if votedFor is null or CandidateId, and candidate's log is at
	// least as up-to-date as receiver's log, grant vote
	if rf.votedFor < 0 || rf.votedFor == args.CandidateId {
		if !rf.ownLogMoreUpToDate(args.LastLogIndex, args.LastLogTerm) {
			rf.votedFor = args.CandidateId
			rf.info("votedFor = %d", args.CandidateId)
			reply.VoteGranted = true
			rf.resetElectionTimeout()
		}
	}
}

func (rf *Raft) ownLogMoreUpToDate(candidateLastLogIndex int, candidateLastLogTerm int) bool {
	// Raft determines which of two logs is more up-to-date by comparing the index and term of the last entries in the
	//logs. If the logs have last entries with different terms, then the log with the later term is more up-to-date.
	//If the logs end with the same term, then whichever log is longer is more up-to-date.
	if len(rf.logs) <= 0 {
		return false
	}
	lastIdx := rf.lastLogIndex()
	lastEntry := rf.logs[lastIdx]
	if lastEntry.Term != candidateLastLogTerm {
		return lastEntry.Term > candidateLastLogTerm
	}
	return lastIdx > candidateLastLogIndex
}

func (rf *Raft) checkTerm(T int, where string) bool {
	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if T > rf.currentTerm {
		rf.info("%s: T=%d > rf.currentTerm=%d; converting to follower", where, T, rf.currentTerm)
		rf.setTerm(T)
		rf.setState(RaftFollower)
		return true
	}
	return false
}

func (rf *Raft) startLeaderElection() {
	// increment current term
	rf.setTerm(rf.currentTerm + 1)
	rf.info("Leader election start")
	// send RequestVote RPCs to all other servers
	votes := 0
	total := len(rf.peers)
	for ii := 0; ii < total; ii++ {
		go func(i int) {
			args := RequestVoteArgs{}
			args.Term = rf.currentTerm
			args.CandidateId = rf.me
			lastIdx := rf.lastLogIndex()
			if lastIdx > 0 {
				args.LastLogIndex = lastIdx
				args.LastLogTerm = rf.logs[lastIdx].Term
			}

			reply := RequestVoteReply{}
			if rf.sendRequestVote(i, &args, &reply) {
				rf.checkTerm(reply.Term, "RequestVoteReply")
				if rf.state == RaftCandidate {
					if reply.Term == rf.currentTerm {
						if reply.VoteGranted {
							votes += 1
							if majority(total, votes) {
								rf.info("got a majority for %v, becoming leader", rf.me)
								rf.setState(RaftLeader)
							}
						}
					}
				}
			}
		}(ii)
	}
}

func majority(total int, n int) bool {
	return n >= (total+1)/2
}

func replicatedMajority(indices []int, index int) bool {
	n := 0
	for i := 0; i < len(indices); i++ {
		if indices[i] >= index {
			n++
		}
	}
	return majority(len(indices), n)
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
	rf.info("Start(%v)", command)
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	isLeader = rf.state == RaftLeader
	if isLeader {
		term = rf.currentTerm

		entry := LogEntry{}
		entry.Term = rf.currentTerm
		entry.Command = command

		index = rf.lastLogIndex() + 1
		rf.logs[index] = entry
		rf.info("logs[%d] = entry{%v}", index, entry)
		rf.nextIndex[rf.me] = index + 1
		rf.info("rf.nextIndex[server{%d}] = %d", rf.me, index+1)
		rf.matchIndex[rf.me] = index
		rf.info("rf.matchIndex[server{%d}] = %d", rf.me, index)
	}

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
	rf.setState(RaftFollower)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) setTerm(term int) {
	rf.currentLeader = -1
	rf.currentTerm = term
	rf.votedFor = -1
	rf.resetElectionTimeout()
}

func (rf *Raft) setState(state int) {
	if state == rf.state {
		return
	}
	rf.state = state
	if state == RaftCandidate {
		// on conversion to candidate, start election
		rf.startLeaderElection()
	} else if state == RaftLeader {
		// Upon election: reinitialize state
		rf.currentLeader = rf.me
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		lastIdx := rf.lastLogIndex()
		rf.info("Upon election: reinitialize state (nextIdx=%d)", lastIdx+1)
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = lastIdx + 1
			rf.matchIndex[i] = 0
		}
		// Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server
		rf.info("Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server")
		rf.sendHeartbeats()
	}
}

func (rf *Raft) resetElectionTimeout() {
	ms := 550 + (rand.Int63() % 150)
	rf.electionTimeout = time.Duration(ms) * time.Millisecond
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.debug("tick")

		// Your code here (2A)
		rf.updateCommit()

		if rf.state == RaftLeader {
			rf.sendHeartbeats()
		}

		// Check if a leader election should be started.
		if rf.electionTimeout < 0 {
			if rf.state == RaftFollower {
				// If election timeout elapses without receiving AppendEntries
				// RPC from current leader or granting vote to candidate: convert to candidate
				rf.info("election timeout elapsed, converting to candidate")
				rf.setState(RaftCandidate)
			} else if rf.state == RaftCandidate {
				// If election timeout elapses: start new election
				rf.info("election timeout elapsed, starting new election")
				rf.startLeaderElection()
			}
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		dt := time.Duration(ms) * time.Millisecond
		time.Sleep(dt)
		if rf.electionTimeout > 0 {
			rf.electionTimeout -= dt
			if rf.electionTimeout == 0 {
				rf.electionTimeout -= 1
			}
		}
	}
	rf.log("killed")
}

func stateName(state int) string {
	if state == RaftFollower {
		return "FOLL"
	} else if state == RaftCandidate {
		return "CAND"
	} else if state == RaftLeader {
		return "LEAD"
	}
	return "UNKN"
}

func (rf *Raft) log(format string, v ...any) {
	msg := fmt.Sprintf(format, v...)
	log.Printf("(%d/%d) %s term=%d: %s", rf.me, len(rf.peers), stateName(rf.state), rf.currentTerm, msg)
}

func (rf *Raft) warn(format string, v ...any) {
	rf.log(format, v...)
}

func (rf *Raft) info(format string, v ...any) {
	rf.log(format, v...)
}

func (rf *Raft) debug(format string, v ...any) {
	rf.log(format, v...)
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// 2A
	rf.setTerm(0)
	rf.setState(RaftFollower) // servers begin as followers

	// 2B
	rf.applyCh = applyCh
	rf.logs = map[int]LogEntry{}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.commitIndex + 1
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
