package raft

//
// Raft implementation covering lab 2A (leader election), 2B (log replication),
// 2C (persistence) and 2D (log compaction / snapshots).
//

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

// 角色常量
const (
	Follower  = 0
	Candidate = 1
	Leader    = 2
)

// 计时参数
const (
	heartbeatInterval = 100 * time.Millisecond
	checkInterval     = 50 * time.Millisecond
)

// ApplyMsg 一条提交/快照消息
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// 2D 快照
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// Entry 日志条目
type Entry struct {
	Term    int
	Command interface{}
}

// Raft 单个节点
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int
	dead      int32

	applyCh   chan ApplyMsg
	applyCond *sync.Cond

	// 持久化状态 (2A/2B/2C)
	currentTerm int
	votedFor    int
	role        int
	log         []Entry

	// 提交/应用
	commitIndex int
	lastApplied int

	// Leader 专用
	nextIndex  []int
	matchIndex []int

	// 2D 快照
	lastIncludedIndex int
	lastIncludedTerm  int
	snapshot          []byte

	// 选举计时
	electionTimeout time.Duration
	lastHeartbeat   time.Time
}

// ===== 日志索引辅助 =====

func (rf *Raft) getLastLogIndex() int {
	return rf.lastIncludedIndex + len(rf.log) - 1
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) == 0 {
		return rf.lastIncludedTerm
	}
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) getTerm(index int) int {
	if index < rf.lastIncludedIndex {
		return -1
	}
	if index == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}
	pos := index - rf.lastIncludedIndex
	if pos < 0 || pos >= len(rf.log) {
		return -1
	}
	return rf.log[pos].Term
}

func (rf *Raft) getEntry(index int) Entry {
	pos := index - rf.lastIncludedIndex
	return rf.log[pos]
}

func randomElectionTimeout() time.Duration {
	return time.Duration(300+rand.Int63()%300) * time.Millisecond
}

// ===== 状态切换 =====

func (rf *Raft) becomeFollower(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
	}
	rf.role = Follower
	rf.votedFor = -1
	rf.electionTimeout = randomElectionTimeout()
	rf.lastHeartbeat = time.Now()
	rf.persist()
}

func (rf *Raft) becomeLeader() {
	rf.role = Leader
	rf.lastHeartbeat = time.Now()
	last := rf.getLastLogIndex()
	n := len(rf.peers)
	rf.nextIndex = make([]int, n)
	rf.matchIndex = make([]int, n)
	for i := 0; i < n; i++ {
		rf.nextIndex[i] = last + 1
	}
	rf.persist()
}

func (rf *Raft) startElection() {
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.role = Candidate
	rf.lastHeartbeat = time.Now()
	rf.electionTimeout = randomElectionTimeout()
	term := rf.currentTerm
	me := rf.me
	n := len(rf.peers)
	rf.persist()
	rf.mu.Unlock()

	votes := int32(1)
	for i := 0; i < n; i++ {
		if i == me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			lastIdx := rf.getLastLogIndex()
			lastTerm := rf.getLastLogTerm()
			args := &RequestVoteArgs{
				Term:         term,
				CandidateId:  me,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			}
			rf.mu.Unlock()
			reply := &RequestVoteReply{}
			if rf.sendRequestVote(server, args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					rf.becomeFollower(reply.Term)
					return
				}
				if reply.VoteGranted && term == rf.currentTerm && rf.role == Candidate {
					if atomic.AddInt32(&votes, 1) > int32(n/2) {
						if rf.role == Candidate && rf.currentTerm == term {
							rf.becomeLeader()
						}
					}
				}
			}
		}(i)
	}
}

// ===== 提交推进 =====

func (rf *Raft) setCommit(idx int) {
	if idx > rf.commitIndex {
		rf.commitIndex = idx
		rf.applyCond.Broadcast()
	}
}

func (rf *Raft) updateCommit() {
	n := len(rf.peers)
	last := rf.getLastLogIndex()
	for N := last; N > rf.commitIndex; N-- {
		if N <= rf.lastIncludedIndex {
			break
		}
		if rf.getTerm(N) != rf.currentTerm {
			continue
		}
		count := 1
		for i := 0; i < n; i++ {
			if i != rf.me && rf.matchIndex[i] >= N {
				count++
			}
		}
		if count > n/2 {
			rf.setCommit(N)
			break
		}
	}
}

// ===== 后台循环 =====

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.role == Leader {
			rf.mu.Unlock()
			rf.leaderAppendEntries()
			time.Sleep(heartbeatInterval)
			continue
		}
		elapsed := time.Since(rf.lastHeartbeat)
		if elapsed >= rf.electionTimeout {
			rf.startElection() // 内部会释放锁
		} else {
			rf.mu.Unlock()
		}
		time.Sleep(checkInterval)
	}
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			if rf.killed() {
				rf.mu.Unlock()
				return
			}
			rf.applyCond.Wait()
		}
		msgs := make([]ApplyMsg, 0)
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			if i <= rf.lastIncludedIndex {
				continue
			}
			e := rf.getEntry(i)
			msgs = append(msgs, ApplyMsg{CommandValid: true, Command: e.Command, CommandIndex: i})
		}
		rf.lastApplied = rf.commitIndex
		rf.mu.Unlock()
		for _, m := range msgs {
			rf.applyCh <- m
		}
	}
}

// ===== Leader 复制日志 =====

func (rf *Raft) leaderAppendEntries() {
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}
	term := rf.currentTerm
	me := rf.me
	n := len(rf.peers)
	lastIncluded := rf.lastIncludedIndex
	base := rf.lastIncludedIndex

	type req struct {
		server int
		isSnap bool
		args   interface{}
	}
	var reqs []req
	for i := 0; i < n; i++ {
		if i == me {
			continue
		}
		ni := rf.nextIndex[i]
		if ni <= lastIncluded {
			data := make([]byte, len(rf.snapshot))
			copy(data, rf.snapshot)
			args := &InstallSnapshotArgs{
				Term:              term,
				LeaderId:          me,
				LastIncludedIndex: rf.lastIncludedIndex,
				LastIncludedTerm:  rf.lastIncludedTerm,
				Data:              data,
			}
			reqs = append(reqs, req{server: i, isSnap: true, args: args})
			continue
		}
		prevIdx := ni - 1
		var entries []Entry
		start := ni - base
		if start < 1 {
			start = 1
		}
		if start < len(rf.log) {
			entries = make([]Entry, len(rf.log)-start)
			copy(entries, rf.log[start:])
		}
		args := &AppendEntriesArgs{
			Term:         term,
			LeaderId:     me,
			PrevLogIndex: prevIdx,
			PrevLogTerm:  rf.getTerm(prevIdx),
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		reqs = append(reqs, req{server: i, isSnap: false, args: args})
	}
	rf.mu.Unlock()

	for _, r := range reqs {
		go func(server int, isSnap bool, a interface{}) {
			if isSnap {
				args := a.(*InstallSnapshotArgs)
				reply := &InstallSnapshotReply{}
				if rf.sendInstallSnapshot(server, args, reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.Term > rf.currentTerm {
						rf.becomeFollower(reply.Term)
						return
					}
					if reply.Success {
						rf.matchIndex[server] = args.LastIncludedIndex
						rf.nextIndex[server] = args.LastIncludedIndex + 1
					}
				}
			} else {
				args := a.(*AppendEntriesArgs)
				reply := &AppendEntriesReply{}
				if rf.sendAppendEntries(server, args, reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.Term > rf.currentTerm {
						rf.becomeFollower(reply.Term)
						return
					}
					if reply.Success {
						rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
						rf.nextIndex[server] = rf.matchIndex[server] + 1
						rf.updateCommit()
					} else {
						if rf.nextIndex[server] > 1 {
							rf.nextIndex[server]--
						}
					}
				}
			}
		}(r.server, r.isSnap, r.args)
	}
}

// ===== RPC 结构 =====

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
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
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

// ===== RPC Handler =====

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	rf.lastHeartbeat = time.Now()

	upToDate := false
	if args.LastLogTerm > rf.getLastLogTerm() {
		upToDate = true
	} else if args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex >= rf.getLastLogIndex() {
		upToDate = true
	}
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		rf.role = Follower
		rf.persist()
		reply.VoteGranted = true
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm || (args.Term == rf.currentTerm && rf.role == Candidate) {
		rf.becomeFollower(args.Term)
	}
	rf.lastHeartbeat = time.Now()

	if args.PrevLogIndex > rf.getLastLogIndex() {
		return
	}
	if args.PrevLogIndex < rf.lastIncludedIndex {
		return
	}
	if rf.getTerm(args.PrevLogIndex) != args.PrevLogTerm {
		return
	}

	// 冲突检测并截断
	for i, e := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx <= rf.getLastLogIndex() {
			if rf.getTerm(idx) != e.Term {
				rf.log = rf.log[:idx-rf.lastIncludedIndex]
				break
			}
		}
	}
	// 追加新条目
	for i, e := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx > rf.getLastLogIndex() {
			rf.log = append(rf.log, e)
		}
	}
	rf.persist()

	if args.LeaderCommit > rf.commitIndex {
		newCommit := args.LeaderCommit
		if newCommit > rf.getLastLogIndex() {
			newCommit = rf.getLastLogIndex()
		}
		rf.setCommit(newCommit)
	}
	reply.Success = true
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	rf.lastHeartbeat = time.Now()
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		rf.mu.Unlock()
		return
	}
	rf.installSnapshot(args.LastIncludedIndex, args.LastIncludedTerm, args.Data)
	snap := make([]byte, len(args.Data))
	copy(snap, args.Data)
	idx := rf.lastIncludedIndex
	term := rf.lastIncludedTerm
	rf.mu.Unlock()
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      snap,
		SnapshotIndex: idx,
		SnapshotTerm:  term,
	}
}

func (rf *Raft) installSnapshot(index, term int, data []byte) {
	if index <= rf.lastIncludedIndex {
		return
	}
	if index < rf.getLastLogIndex() {
		pos := index - rf.lastIncludedIndex
		rf.log = rf.log[pos:]
	} else {
		rf.log = []Entry{{Term: term}}
	}
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = term
	rf.snapshot = data
	if rf.commitIndex < index {
		rf.commitIndex = index
		rf.applyCond.Broadcast()
	}
	if rf.lastApplied < index {
		rf.lastApplied = index
	}
	rf.persist()
}

// ===== 发送 RPC =====

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
}

// ===== 对外接口 =====

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader {
		return -1, rf.currentTerm, false
	}
	idx := rf.getLastLogIndex() + 1
	rf.log = append(rf.log, Entry{Term: rf.currentTerm, Command: command})
	rf.matchIndex[rf.me] = idx
	rf.nextIndex[rf.me] = idx + 1
	rf.persist()
	rf.updateCommit()
	return idx, rf.currentTerm, true
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.mu.Lock()
	if rf.applyCond != nil {
		rf.applyCond.Broadcast()
	}
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.lastIncludedIndex {
		return
	}
	if index > rf.getLastLogIndex() {
		return
	}
	pos := index - rf.lastIncludedIndex
	rf.lastIncludedTerm = rf.log[pos].Term
	rf.lastIncludedIndex = index
	rf.log = rf.log[pos:]
	rf.snapshot = snapshot
	rf.persist()
}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if lastIncludedIndex <= rf.commitIndex {
		return false
	}
	rf.installSnapshot(lastIncludedIndex, lastIncludedTerm, snapshot)
	return true
}

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.Save(data, rf.snapshot)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votedFor int
	var log []Entry
	var lii int
	var lit int
	if d.Decode(&term) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil || d.Decode(&lii) != nil || d.Decode(&lit) != nil {
		return
	}
	rf.currentTerm = term
	rf.votedFor = votedFor
	if log != nil && len(log) > 0 {
		rf.log = log
	} else {
		rf.log = []Entry{{Term: 0}}
	}
	rf.lastIncludedIndex = lii
	rf.lastIncludedTerm = lit
}

// Make 创建 Raft 节点
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.role = Follower
	rf.log = []Entry{{Term: 0}}
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.electionTimeout = randomElectionTimeout()
	rf.lastHeartbeat = time.Now()
	rf.applyCond = sync.NewCond(&rf.mu)

	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()
	if rf.lastApplied < rf.lastIncludedIndex {
		rf.lastApplied = rf.lastIncludedIndex
	}
	if rf.commitIndex < rf.lastIncludedIndex {
		rf.commitIndex = rf.lastIncludedIndex
	}

	go rf.ticker()
	go rf.applier()
	return rf
}
