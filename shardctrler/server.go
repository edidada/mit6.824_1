package shardctrler

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Op struct {
	Type     string // Join, Leave, Move, Query
	Servers  map[int][]string
	GIDs     []int
	Shard    int
	GID      int
	Num      int
	ClientId int64
	Seq      int
}

type OpResult struct {
	Config Config
	Err    Err
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	configs []Config // indexed by config num
	dupe    map[int64]int
	pending map[int]chan OpResult

	persister   *raft.Persister
	lastApplied int
}

// Query ops are also logged. Without snapshotting, a long run with many rapid
// reconfigurations grows the Raft log without bound and persist() becomes
// O(n^2), stalling Queries. Keep the log small with a fixed threshold.
const ctrlerSnapshotThreshold = 4096

// copyConfig deep-copies a Config so configs never share a Groups map.
func copyConfig(c Config) Config {
	nc := Config{Num: c.Num, Shards: c.Shards, Groups: make(map[int][]string)}
	for gid, srvs := range c.Groups {
		nc.Groups[gid] = append([]string(nil), srvs...)
	}
	return nc
}

// rebalance assigns every shard to a valid group, balancing the load.
// Shards owned by a group that still exists are left in place where possible.
func (sc *ShardCtrler) rebalance(cfg *Config) {
	if len(cfg.Groups) == 0 {
		for i := range cfg.Shards {
			cfg.Shards[i] = 0
		}
		return
	}
	gids := make([]int, 0, len(cfg.Groups))
	for g := range cfg.Groups {
		gids = append(gids, g)
	}
	sort.Ints(gids)

	cnt := make(map[int]int)
	for _, g := range gids {
		cnt[g] = 0
	}
	free := make([]int, 0)
	for s, g := range cfg.Shards {
		if _, ok := cnt[g]; ok {
			cnt[g]++
		} else {
			free = append(free, s)
		}
	}

	// assign shards with no (or a removed) owner to the least-loaded groups
	for len(free) > 0 {
		minG := gids[0]
		for _, g := range gids {
			if cnt[g] < cnt[minG] {
				minG = g
			}
		}
		s := free[0]
		free = free[1:]
		cfg.Shards[s] = minG
		cnt[minG]++
	}

	// move shards from the most-loaded to the least-loaded group until balanced
	for {
		maxG, minG := gids[0], gids[0]
		for _, g := range gids {
			if cnt[g] > cnt[maxG] {
				maxG = g
			}
			if cnt[g] < cnt[minG] {
				minG = g
			}
		}
		if cnt[maxG]-cnt[minG] <= 1 {
			break
		}
		for s, g := range cfg.Shards {
			if g == maxG {
				cfg.Shards[s] = minG
				cnt[maxG]--
				cnt[minG]++
				break
			}
		}
	}
}

// applyOp applies op. For Query it returns the requested config; for the
// state-changing ops it appends a new config. caller must hold sc.mu.
func (sc *ShardCtrler) applyOp(op Op) OpResult {
	res := OpResult{Err: OK}
	if op.Type == "Query" {
		if op.Num >= 0 && op.Num < len(sc.configs) {
			res.Config = copyConfig(sc.configs[op.Num])
		} else {
			res.Config = copyConfig(sc.configs[len(sc.configs)-1])
		}
		return res
	}
	if last, ok := sc.dupe[op.ClientId]; ok && op.Seq <= last {
		return res
	}
	sc.dupe[op.ClientId] = op.Seq

	prev := sc.configs[len(sc.configs)-1]
	next := copyConfig(prev)
	next.Num = prev.Num + 1
	switch op.Type {
	case "Join":
		for gid, srvs := range op.Servers {
			next.Groups[gid] = append([]string(nil), srvs...)
		}
		sc.rebalance(&next)
	case "Leave":
		for _, gid := range op.GIDs {
			delete(next.Groups, gid)
		}
		sc.rebalance(&next)
	case "Move":
		next.Shards[op.Shard] = op.GID
	}
	sc.configs = append(sc.configs, next)
	return res
}

// makeSnapshot encodes the whole state machine. caller must hold sc.mu.
func (sc *ShardCtrler) makeSnapshot(index int) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(index)
	e.Encode(sc.configs)
	e.Encode(sc.dupe)
	return w.Bytes()
}

// installSnapshot restores state from a snapshot. caller must hold sc.mu.
func (sc *ShardCtrler) installSnapshot(data []byte) {
	if len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var index int
	var configs []Config
	var dupe map[int64]int
	if d.Decode(&index) != nil || d.Decode(&configs) != nil || d.Decode(&dupe) != nil {
		return
	}
	sc.configs = configs
	sc.dupe = dupe
	sc.lastApplied = index
}

func (sc *ShardCtrler) applier() {
	for !sc.killed() {
		msg := <-sc.applyCh
		sc.mu.Lock()
		if msg.CommandValid {
			op := msg.Command.(Op)
			res := sc.applyOp(op)
			sc.lastApplied = msg.CommandIndex
			if ch, ok := sc.pending[msg.CommandIndex]; ok {
				delete(sc.pending, msg.CommandIndex)
				ch <- res
			}
			if sc.lastApplied > 0 && sc.persister.RaftStateSize() > ctrlerSnapshotThreshold {
				sc.rf.Snapshot(sc.lastApplied, sc.makeSnapshot(sc.lastApplied))
			}
		} else if msg.SnapshotValid {
			sc.installSnapshot(msg.Snapshot)
			sc.lastApplied = msg.SnapshotIndex
		}
		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) submit(op Op) (OpResult, bool) {
	if sc.killed() {
		return OpResult{}, false
	}
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		return OpResult{}, false
	}
	index, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		return OpResult{}, false
	}
	ch := make(chan OpResult, 1)
	sc.mu.Lock()
	sc.pending[index] = ch
	sc.mu.Unlock()

	timeout := time.NewTimer(time.Second)
	defer timeout.Stop()
	check := time.NewTicker(20 * time.Millisecond)
	defer check.Stop()
	for {
		select {
		case res := <-ch:
			return res, true
		case <-timeout.C:
			sc.mu.Lock()
			delete(sc.pending, index)
			sc.mu.Unlock()
			return OpResult{}, false
		case <-check.C:
			if sc.killed() {
				return OpResult{}, false
			}
			curTerm, stillLeader := sc.rf.GetState()
			if !stillLeader || curTerm != term {
				sc.mu.Lock()
				delete(sc.pending, index)
				sc.mu.Unlock()
				return OpResult{}, false
			}
		}
	}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	op := Op{Type: "Join", Servers: args.Servers, ClientId: args.ClientId, Seq: args.Seq}
	res, ok := sc.submit(op)
	reply.WrongLeader = !ok
	reply.Err = res.Err
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	op := Op{Type: "Leave", GIDs: args.GIDs, ClientId: args.ClientId, Seq: args.Seq}
	res, ok := sc.submit(op)
	reply.WrongLeader = !ok
	reply.Err = res.Err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	op := Op{Type: "Move", Shard: args.Shard, GID: args.GID, ClientId: args.ClientId, Seq: args.Seq}
	res, ok := sc.submit(op)
	reply.WrongLeader = !ok
	reply.Err = res.Err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	op := Op{Type: "Query", Num: args.Num, ClientId: args.ClientId, Seq: args.Seq}
	res, ok := sc.submit(op)
	reply.WrongLeader = !ok
	reply.Err = res.Err
	reply.Config = res.Config
}

func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
}

func (sc *ShardCtrler) killed() bool {
	return atomic.LoadInt32(&sc.dead) == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me
	sc.persister = persister

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	sc.dupe = make(map[int64]int)
	sc.pending = make(map[int]chan OpResult)

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	snap := persister.ReadSnapshot()
	if len(snap) > 0 {
		sc.mu.Lock()
		sc.installSnapshot(snap)
		sc.mu.Unlock()
	}

	go sc.applier()
	return sc
}
