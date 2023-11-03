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
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

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
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// example AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.log("AppendEntries %v", args)
	defer func() {
		rf.log("-- AppendEntries %v %v", args, reply)
	}()
	reply.Term = rf.currentTerm
	reply.Success = false
	// reset election timeout.
	rf.resetElectionTimeout()
}

func (rf *Raft) sendHeartbeat(server int) bool {
	args := AppendEntriesArgs{}
	args.LeaderId = rf.me
	args.Term = rf.currentTerm

	reply := AppendEntriesReply{}
	rf.log("sending heartbeat to %d", server)
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	rf.log("-- sending heartbeat to %d: %v", server, ok)
	return ok
}

func (rf *Raft) sendHeartbeats() {
	sent := false
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
		time.Sleep(250 * time.Millisecond)
		if !sent && rf.state == RaftLeader {
			// no peers, transition to follower
			rf.log("no peers, becoming follower")
			rf.setState(RaftFollower)
		}
	}()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	// 2A
	Term        int // candidate's Term
	CandidateId int // candidate requesting vote
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
	rf.log("RequestVote %v", args)
	defer func() {
		rf.log("-- RequestVote %v %v", args, reply)
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
		rf.votedFor = args.CandidateId
		rf.log("votedFor = %d", args.CandidateId)
		reply.VoteGranted = true
		rf.resetElectionTimeout()
	}
}

func (rf *Raft) checkTerm(T int, where string) bool {
	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if T > rf.currentTerm {
		rf.log("%s: T=%d > rf.currentTerm=%d; converting to follower", where, T, rf.currentTerm)
		rf.currentTerm = T
		rf.votedFor = -1
		rf.setState(RaftFollower)
		rf.resetElectionTimeout()
		return true
	}
	return false
}

func (rf *Raft) startLeaderElection() {
	rf.resetElectionTimeout()
	// increment current term
	rf.currentTerm += 1
	// vote for self
	//rf.votedFor = rf.me
	rf.votedFor = -1
	rf.log("Leader election start")
	// send RequestVote RPCs to all other servers
	votes := 0
	total := len(rf.peers)
	for ii := 0; ii < total; ii++ {
		go func(i int) {
			args := RequestVoteArgs{}
			args.Term = rf.currentTerm
			args.CandidateId = rf.me

			reply := RequestVoteReply{}
			if rf.sendRequestVote(i, &args, &reply) {
				rf.checkTerm(reply.Term, "RequestVoteReply")
				if rf.state == RaftCandidate {
					if reply.Term == rf.currentTerm {
						if reply.VoteGranted {
							votes += 1
							if majority(total, votes) {
								rf.log("got a majority for %v, becoming leader", rf.me)
								rf.setState(RaftLeader)
								// Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server
								rf.sendHeartbeats()
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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

func (rf *Raft) setState(state int) {
	if state == rf.state {
		return
	}
	rf.state = state
	if state == RaftCandidate {
		// on conversion to candidate, start election
		rf.startLeaderElection()
	}
}

func (rf *Raft) resetElectionTimeout() {
	ms := 150 + (rand.Int63() % 150)
	rf.electionTimeout = time.Duration(ms) * time.Millisecond
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.log("tick")

		// Your code here (2A)

		if rf.state == RaftLeader {
			rf.sendHeartbeats()
		}

		// Check if a leader election should be started.
		if rf.electionTimeout < 0 {
			if rf.state == RaftFollower {
				// If election timeout elapses without receiving AppendEntries
				// RPC from current leader or granting vote to candidate: convert to candidate
				rf.log("election timeout elapsed, converting to candidate")
				rf.setState(RaftCandidate)
			} else if rf.state == RaftCandidate {
				// If election timeout elapses: start new election
				rf.log("election timeout elapsed, starting new election")
				rf.startLeaderElection()
			}
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 5 + (rand.Int63() % 30)
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
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.setState(RaftFollower) // servers begin as followers
	rf.resetElectionTimeout()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
