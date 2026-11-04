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
	//"bytes"

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

	// lab 2a用到的
	// 当前任期
	currentTerm int
	// 投票
	votedFor int
	// 简单用string表示状态 "Candidate" "Follower" "Leader"
	status string
	// 超时时间
	start time.Time

	// lab 2a用不到
	// 暂时设为int数组
	log []int

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status == "Leader" {
		term = rf.currentTerm
		isleader = true
	} else {
		term = rf.currentTerm
		isleader = false
	}
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// lab 2a用到的
	// 任期
	Term int
	// 投票id
	CandidateId int

	// lab 2a用不到
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	// 任期
	Term int
	// 投票成功
	VoteGranted bool
}

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	// lab 2a用到的
	// 任期轮数
	Term int
	// 领导id
	LeaderId int

	// lab 2a没用到
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []int // 暂时设为int数组
	LeaderCommit int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type AppendEntriesReply struct {
	// Your data here (2A).
	// 任期
	Term int
	// 正常返回
	Success bool
}

// example RequestVote RPC handler.
// candidate拉票包
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	///log.Printf("Peer %d(%d,%d) : receive RequestVote", rf.me,rf.currentTerm,rf.state)
	rf.start = time.Now()
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 包装一系列返回信息
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// 这里的votedFor参数 实际节点没有清空的必要
	// 只要投出了票 轮数就会有更新
	if args.Term == rf.currentTerm && rf.votedFor == -1 {
		//log.Printf("time=%s rf.me=%d rf.status=%s rf.currentTerm=%d rf.votedFor=%d", time.Now(), rf.me, rf.status, rf.currentTerm, rf.votedFor)
		// 如果是相同任期发出的 要看这一次任期已经投票了没
		// 如果还没投过票 那当然可以投这一票
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.BeFollower(args.Term, args.CandidateId)
	} else if args.Term > rf.currentTerm {
		// 如果投票的任期直接大于我当前任期 那就可以直接投票
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.BeFollower(args.Term, args.CandidateId)
	}
}

// leader心跳包
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//log.Printf("time=%s args.Term=%d args.LeaderId=%d->rf.me=%d rf.currentTerm=%d", time.Now(), args.Term, args.LeaderId, rf.me, rf.currentTerm)
	if args.Term > rf.currentTerm {
		// leader心跳包发给当前的rf rf发现对方的任期比自己大
		// 当前rf就立刻转变为对方的follower （无论当前是什么状态）
		// 但是不参与投票行为 只是心跳行为
		rf.BeFollower(args.Term, -1)
		rf.start = time.Now()
	} else if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = true
		//rf.start = time.Now()
	} else if args.Term == rf.currentTerm && rf.status == "Leader" {
		// todo：会出现同任期领导的情况 原因未知 照理来说在投票环节就不会产生了
		rf.BeFollower(args.Term, -1)
		rf.start = time.Now()
	} else {
		rf.start = time.Now()
	}
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

// 提供一个心跳包的rpc调用
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 提供切换三个状态的方法
// 实际的行为控制托付给ticker
func (rf *Raft) BeFollower(currentTerm int, voteFor int) {
	rf.start = time.Now()
	rf.currentTerm = currentTerm
	rf.status = "Follower"
	rf.votedFor = voteFor
}

func (rf *Raft) BeCanidate() {
	rf.start = time.Now()
	rf.status = "Candidate"
}

func (rf *Raft) BeLeader() {
	rf.start = time.Now()
	rf.status = "Leader"
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		//log.Printf("time=%s rf.me=%d rf.status=%s rf.currentTerm=%d", time.Now(), rf.me, rf.status, rf.currentTerm)
		// 定时判断状态
		if rf.status == "Follower" {
			// 如果是follower状态 判断是否超时
			// 如果超时就转为candidate状态
			// 只有收到leader发的消息才会更新超时时间
			rf.mu.Lock()
			elapsed := time.Since(rf.start)
			// 设置超时时间为1000毫秒后
			if elapsed >= 1000*time.Millisecond {
				rf.BeCanidate()
			}
			rf.mu.Unlock()
		} else if rf.status == "Candidate" {
			// 作为candidate会一直拉票 除非出现一个任期更大的leader 或者自己成为leader
			// 拉票的逻辑就是向所有其他的节点发rpc 判断是否拉到一半的票
			for {
				rf.start = time.Now()
				if rf.killed() {
					break
				}
				rf.mu.Lock()
				if rf.status != "Candidate" {
					rf.mu.Unlock()
					break
				}

				// 正式开始拉票环节
				// 任期数往后推 投票给自己 更新超时时间 记录投票数
				// 同时要防止投票过程中 状态被转换的影响（candidate可能变其他的follower）

				// 拉票初始化
				rf.currentTerm += 1
				rf.votedFor = rf.me
				rf.start = time.Now()
				voteCount := 1
				Term := rf.currentTerm
				rf.mu.Unlock()

				args := &RequestVoteArgs{
					Term:        rf.currentTerm,
					CandidateId: rf.me,
				}
				reply := &RequestVoteReply{
					Term:        -1,
					VoteGranted: false,
				}

				// 拉票
				for i := 0; i < len(rf.peers); i++ {
					// 自己的票不用拉
					if rf.votedFor == i {
						continue
					}
					go func(server int) {
						ok := rf.sendRequestVote(server, args, reply)
						rf.mu.Lock()
						if ok {
							if reply.VoteGranted {
								// 收到拉票
								voteCount++
							} else if reply.Term > rf.currentTerm {
								// 如果发现目标任期更大 说明这个candidate不合适 同时和目标任期同步
								// 这里直接作为i的follower 可能和表达的参数意思不一致：并没有投出这一票 而是后续更进的状态
								rf.BeFollower(reply.Term, -1)
								rf.mu.Unlock()
								return
							}
						}
						rf.mu.Unlock()
					}(i)
					// 一个语法点 要把i作为参数传进去 不然是不稳定的
				}

				// 这个等待时间一定要锁住（基于测试） 防止其他节点对本节点信息进行额外修改
				rf.mu.Lock()
				time.Sleep(500 * time.Millisecond)
				rf.mu.Unlock()

				rf.mu.Lock()

				// 如果有更高任期的leader 就会变成follower
				if rf.status == "Follower" {
					rf.mu.Unlock()
					break
				}

				// 任期不一致 本次拉票信息已经丢失
				if Term != rf.currentTerm {
					rf.mu.Unlock()
					break
				}

				// 获得半数投票 成为leader
				if voteCount*2 > len(rf.peers) {
					rf.BeLeader()
					rf.mu.Unlock()
					break
				}
				rf.mu.Unlock()
			}
		} else if rf.status == "Leader" {
			// 作为leader要一直发心跳包给其他节点 更新其他节点的超时时间 防止其他节点成为leader
			// 基本干啥都重置一下时间 保险起见都写了
			rf.start = time.Now()
			go func() {
				for {
					if rf.status != "Leader" {
						break
					}
					rf.mu.Lock()
					args := &AppendEntriesArgs{
						Term:     rf.currentTerm,
						LeaderId: rf.me,
					}
					reply := &AppendEntriesReply{
						Term:    0,
						Success: false,
					}

					for i := 0; i < len(rf.peers); i++ {
						// 自己就不用发心跳了
						if rf.me == i {
							continue
						}
						go func(server int) {
							rf.sendAppendEntries(server, args, reply)
							// if reply.Success {
							// 	rf.BeFollower(reply.Term, server)
							// }
						}(i)
					}
					// 可能额外需要一个判断
					// 就是回收所有的心跳包返回 看是否过半 决定要不要把leader砍掉
					rf.mu.Unlock()
					time.Sleep(100 * time.Millisecond)
				}
			}()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 100 + (rand.Int63() % 50)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
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
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.status = "Follower"
	rf.start = time.Now()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	return rf
}
