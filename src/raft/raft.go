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

import "sync"
import "labrpc"

import "bytes"
import "encoding/gob"
import "time"
import "math/rand"
import "log"
import "sort"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type State int
const(
	Follower State = 0+iota
	Candidate
	Leader
)
type Log struct {
	Index int
	Term int
	Command interface{}
}
//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]
	currentTerm int
	votedFor int
	voteCount int
	commitIndex int
	lastApplied int
	nextIndex map[int]int
	matchIndex map[int]int
	state State //follower:0,candidate:1,leader:2
	logs []Log
	//lastLogIndex int
	//lastLogTerm int
	electionTimeout time.Time
	termTimeout time.Time
	//electionTicket int
	//termTicket int
	stopped bool
	applyCh chan ApplyMsg
	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	isleader = false
	if rf.state == Leader{
		isleader = true
	}
	term = rf.currentTerm
	// Your code here.
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.logs)
}



type AppendEntriesArgs struct{
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	LeaderCommit int
	Entries []Log
}

type AppendEntriesReply struct{
	Term int
	Success bool
	NextIndex int
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) buildAppendEntriesArgs(peer int) AppendEntriesArgs{
	args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	lastIndex := rf.getLastIndex()
	args.LeaderCommit = rf.commitIndex
	//log.Printf("rf.me:%+v,rf.nextIndex:%+v",rf.me,rf.nextIndex)
	//log.Printf("buildAppendEntriesArgs rf:%+v",rf)
	if rf.nextIndex[peer] < 1{
		args.PrevLogIndex = -1
		args.PrevLogTerm = -1
	}else{
		prevLog := rf.logs[rf.nextIndex[peer]-1]
		args.PrevLogIndex = prevLog.Index
		args.PrevLogTerm = prevLog.Term
	}
	if rf.nextIndex[peer] > lastIndex && lastIndex >= 0{
		return args
	}
	args.Entries = rf.logs[rf.nextIndex[peer]:]
	return args
}

func min(a, b int) int {
   if a > b {
      return b
   }
   return a
}
func max(a, b int) int {
   if a < b {
      return b
   }
   return a
}

func (rf *Raft) checkAndApply(){
	var indexs []int
	for peer := 0;peer<len(rf.peers);peer++{
		indexs = append(indexs,rf.matchIndex[peer])
	}
	sort.Ints(indexs)
	//log.Printf("indexs:%+v,rf:%+v",indexs,rf)
	index := indexs[len(rf.peers)/2]
	if index > -1 && rf.logs[index].Term == rf.currentTerm{
		rf.commitIndex = indexs[len(rf.peers)/2]
		rf.apply()
	}
}

func (rf *Raft) sendAppendEntries2Peers(){
	if rf.state == Leader{
		//var wg sync.WaitGroup
		for peer := 0;peer<len(rf.peers);peer++{
			if peer == rf.me{
				continue
			}
			//wg.Add(1)
			go func(peer int){
				rf.mu.Lock()
				//defer wg.Done()
				args := rf.buildAppendEntriesArgs(peer)
				reply := &AppendEntriesReply{}
				if rf.state != Leader{
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
				ok := rf.sendAppendEntries(peer,args,reply)
				rf.mu.Lock()
				if ok && args.Term == rf.currentTerm && rf.state == Leader{
					//log.Printf("rf.me:%+v,peer:%+v,ok:%+v,reply:%+v",rf.me,peer,ok,reply)
					if reply.Success && len(args.Entries) > 0{
						lastEntry := args.Entries[len(args.Entries)-1]
						rf.nextIndex[peer] = lastEntry.Index+1
						rf.matchIndex[peer] = lastEntry.Index
						rf.checkAndApply()
					}else if !reply.Success && reply.NextIndex > -1{
						rf.nextIndex[peer] = max(reply.NextIndex,0)
					}
					if !reply.Success && reply.Term > rf.currentTerm{
						rf.currentTerm = reply.Term
						rf.state = Follower
						rf.votedFor = -1
						rf.voteCount = 0
						rf.persist()
						rf.resetElectionTimeout()
					}
					//log.Printf("peer:%+v,args:%+v,reply:%+v,AppendEntries,rf:%+v",peer,args,reply,rf)
				}
				rf.mu.Unlock()
			}(peer)
		}
	}
}
func (rf *Raft) sendAppendEntriesPeriodically(){
	for rf.stopped == false{
		time.Sleep(50*time.Millisecond)
		go rf.sendAppendEntries2Peers()
	}
}
//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term int
	VoteGranted bool
}

func (rf *Raft) apply(){
	//log.Printf("apply rf:%+v",rf)
	for rf.lastApplied < rf.commitIndex{
		rf.lastApplied++
		applyMsg := ApplyMsg{}
		applyMsg.Index = rf.lastApplied+1
		applyMsg.Command = rf.logs[rf.lastApplied].Command
		rf.applyCh<-applyMsg
	}
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args AppendEntriesArgs,reply *AppendEntriesReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false
	reply.Term = rf.currentTerm
	reply.NextIndex = -1
	if args.Term < rf.currentTerm{
		//reply.Term = rf.currentTerm
		return
	}
	defer rf.persist()
	if args.Term > rf.currentTerm{
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.state = Follower
	}
	//log.Printf("AppendEntriesArgs:%+v,rf:%+v",args,rf)
	rf.resetElectionTimeout()
	rf.voteCount = 0
	if args.PrevLogIndex >= len(rf.logs) {
		reply.NextIndex = rf.getLastIndex()+1
		return
	}
	if args.PrevLogIndex >= 0 && rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm{
		reply.NextIndex = max(0,(args.PrevLogIndex-1)/2)
		return
	}
	//log.Printf("AppendEntries,args:%+v,rf:%+v",args,rf)
	if len(args.Entries) > 0{
		//log.Printf("AppendEntries,args:%+v,rf:%+v",args,rf)
		if args.PrevLogIndex < 0 {
			rf.logs = args.Entries
		}else if rf.logs[args.PrevLogIndex].Term == args.PrevLogTerm{
			rf.logs = rf.logs[:args.PrevLogIndex+1]
			rf.logs = append(rf.logs,args.Entries...)
			//log.Printf("return AppendEntriesArgs:%+v,rf:%+v",args,rf)
			//log.Printf("AppendEntries,rf:%+v",rf)
		}
	}
	if args.LeaderCommit > rf.commitIndex{
		rf.commitIndex = min(rf.getLastIndex(),args.LeaderCommit)
		rf.apply()
	}
	reply.Success = true
}

//
// example RequestVote RPC handler.

//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here.
	reply.VoteGranted = false
	if args.Term < rf.currentTerm{
		reply.Term = rf.currentTerm
		return
	}
	if args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId{
		return
	}
	//log.Printf("RequestVote rf:%+v,args:%+v",rf,args)
	rf.state = Follower
	rf.currentTerm = args.Term
	rf.voteCount = 0
	rf.resetElectionTimeout()
	rf.votedFor = -1
	if rf.getLastIndex() > -1 {
		if args.LastLogTerm < rf.logs[rf.getLastIndex()].Term{
			return
		}else if  args.LastLogTerm == rf.logs[rf.getLastIndex()].Term && args.LastLogIndex < rf.getLastIndex(){
			return
		}
	}
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader{
		return index, term, isLeader
	}
	defer rf.persist()
	DPrintf("%+v\n",command)
	isLeader = true
	index = rf.getLastIndex() + 1
	currentTerm := rf.currentTerm
	entry := Log{Index:index,Term:currentTerm,Command:command}
	log.Printf("rf.me:%d,term:%d,index:%d,command:%+v,commitIndex:%+v,nextIndex:%+v",rf.me,currentTerm,index,command,rf.commitIndex,rf.nextIndex)
	rf.logs = append(rf.logs,entry)
	rf.matchIndex[rf.me] = index
	//rf.lastLogIndex = index
	//rf.lastLogTerm = rf.currentTerm
	//go rf.sendAppendEntries2Peers()

	return index+1, currentTerm, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.stopped = true
}

func (rf *Raft) getLastIndex() int{
	return len(rf.logs) -1
}

func (rf *Raft) applyVote(args RequestVoteArgs,reply *RequestVoteReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if reply.Term>rf.currentTerm{
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.voteCount = 0
		rf.votedFor = -1
		rf.resetElectionTimeout()
		return
	}
	if args.Term != rf.currentTerm{
		return
	}
	if rf.state != Candidate{
		return
	}
	if reply.VoteGranted && rf.state == Candidate{
		rf.voteCount++
	}
	//log.Printf("rf.voteCount:%+v,len(rf.peer)/2:%+v,rf.votedFor:%+v",rf.voteCount+1, len(rf.peers)/2,rf.votedFor)
	if rf.voteCount+1 > len(rf.peers)/2 && rf.state == Candidate{
		rf.state = Leader
		rf.initIndex()
		rf.voteCount=0
	}
}

func GetVote(rf *Raft){
	//var wg sync.WaitGroup
	for peer := 0;peer<len(rf.peers);peer++{
		if peer == rf.me{
			continue
		}
		go func(peer int){
			rf.mu.Lock()
			args := RequestVoteArgs{}
			args.Term = rf.currentTerm
			args.CandidateId = rf.me
			lastIndex := rf.getLastIndex()
			if lastIndex >=0 {
				args.LastLogIndex = rf.logs[lastIndex].Index
				args.LastLogTerm = rf.logs[lastIndex].Term
			}else{
				args.LastLogIndex = -1
				args.LastLogTerm = -1
			}
			rf.mu.Unlock()
			if rf.state != Candidate{
				return
			}
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(peer,args,reply)
			if ok{
				rf.applyVote(args,reply)
			}
			//DPrintf("rf.me:%d,%s",rf.me,ok)
		}(peer)
	}
	//log.Printf("voted:%d,half:%d",voted, (len(rf.peers)/2))
}

func (rf *Raft) elect(){
	for rf.stopped == false{
		rf.mu.Lock()
		currentTime := time.Now()
		if (rf.state == Follower && /*rf.electionTicket <=0*/currentTime.After(rf.electionTimeout)) || (rf.state == Candidate && /*rf.termTicket<=0*/currentTime.After(rf.termTimeout)){
			//log.Printf("rf.me:%d,rf.state:%+v,rf.term:%+v,rf.logs:%+v,rf.nextIndex:%+v",rf.me,rf.state,rf.currentTerm,rf.logs,rf.nextIndex)
			rf.currentTerm += 1
			rf.resetTermTimeout()
			rf.state = Candidate
			rf.voteCount = 0
			rf.votedFor = rf.me
			rf.persist()
			rf.mu.Unlock()
			GetVote(rf)
			rf.mu.Lock()
		}
		rf.mu.Unlock()
		//if rf.state == Follower{
		//	rf.electionTicket--
		//}else if rf.state == Candidate{
		//	rf.termTicket--
		//}
		//DPrintf("rf.me:%d,rf.state:%s",rf.me,rf.state)
		time.Sleep(10*time.Millisecond)
	}
}

func (rf *Raft) resetElectionTimeout(){
	rf.electionTimeout = time.Now().Add(time.Duration(450+rand.Intn(330))*time.Millisecond)
}

func (rf *Raft) resetTermTimeout(){
	rf.termTimeout = time.Now().Add(time.Duration(450+rand.Intn(330))*time.Millisecond)
}

func (rf *Raft) initIndex(){
	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)
	for peer := 0;peer<len(rf.peers);peer++{
		//if peer == rf.me{
		//	continue
		//}
		lastIndex := rf.getLastIndex() + 1
		rf.nextIndex[peer] = lastIndex
		rf.matchIndex[peer] = -1
	}
}
//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here.
	rf.voteCount = 0
	rf.state = Follower
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.votedFor = -1
	//rf.logs = make([]Log,100)
	rf.stopped = false
	rf.applyCh = applyCh
	rf.initIndex()

	//elect
	rf.resetElectionTimeout()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.elect()
	go rf.sendAppendEntriesPeriodically()
	return rf
}
