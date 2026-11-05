package shardkv

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

// Op types replicated through Raft.
const (
	opClient    = "Client"
	opConfig    = "Config"
	opShard     = "Shard"
	opDelete    = "Delete"
	opDelClear  = "DelClear"
)

// per-shard states during reconfiguration.
const (
	stServing = iota
	stPulling
	stPushing
)

type Op struct {
	Type      string
	Key       string
	Value     string
	Op        string // "Get" / "Put" / "Append"
	ClientId  int64
	Seq       int
	Config    shardctrler.Config
	Shard     int
	Data      map[string]string
	Dupe      map[int64]int
	ConfigNum int
}

type OpResult struct {
	Err   Err
	Value string
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int
	dead         int32

	persister *raft.Persister
	mck       *shardctrler.Clerk

	cfg         shardctrler.Config
	prevCfg     shardctrler.Config
	data        [shardctrler.NShards]map[string]string
	dupe        [shardctrler.NShards]map[int64]int
	status      [shardctrler.NShards]int
	// pullFrom[s] holds the server names of the group we must fetch shard s
	// from. It is stored in the replicated state (and snapshots) so that a
	// pending pull survives intervening configuration changes, which would
	// otherwise make prevCfg point at the wrong owner.
	pullFrom    [shardctrler.NShards][]string
	// deleteTo[s] records the group we pulled shard s from, so we can tell it
	// to drop its copy once we have safely installed the shard. Stored in the
	// replicated state so a config change cannot orphan the deletion.
	deleteTo    [shardctrler.NShards][]string
	pending     map[int]chan OpResult
	lastApplied int

	// ends caches ClientEnds built by make_end; only touched by the
	// migrator goroutine, so no extra locking is needed.
	ends map[string]*labrpc.ClientEnd
}

func copyStrMap(m map[string]string) map[string]string {
	r := make(map[string]string)
	for k, v := range m {
		r[k] = v
	}
	return r
}

func copyIntMap(m map[int64]int) map[int64]int {
	r := make(map[int64]int)
	for k, v := range m {
		r[k] = v
	}
	return r
}

// applyConfig installs the next configuration. Shards newly assigned to this
// group must be pulled from their previous owner; shards that left are kept
// (stPushing) until the new owner confirms it has the data.
// caller must hold kv.mu.
func (kv *ShardKV) applyConfig(cfg shardctrler.Config) {
	if cfg.Num != kv.cfg.Num+1 {
		return
	}
	old := kv.cfg
	for s := 0; s < shardctrler.NShards; s++ {
		oldOwner := old.Shards[s]
		newOwner := cfg.Shards[s]
		if newOwner == kv.gid {
			if kv.status[s] == stPulling {
				// A pull from an earlier config is still outstanding. Keep its
				// state and (crucially) its stored pull source intact.
				continue
			}
			if kv.status[s] == stPushing {
				// The shard is coming back to us and we still hold its data
				// (the other group never confirmed receipt via DeleteShard,
				// so our copy is authoritative). Serve it directly instead of
				// re-pulling from a group that may never have received it.
				kv.status[s] = stServing
				kv.pullFrom[s] = nil
				kv.deleteTo[s] = nil
				continue
			}
			kv.pullFrom[s] = nil
			kv.deleteTo[s] = nil
			if oldOwner == kv.gid {
				kv.status[s] = stServing
			} else if oldOwner == 0 {
				// initial assignment: no previous owner holds any data
				kv.data[s] = make(map[string]string)
				kv.dupe[s] = make(map[int64]int)
				kv.status[s] = stServing
			} else {
				// newly gained: remember the old owner's servers to pull from
				kv.status[s] = stPulling
				kv.pullFrom[s] = append([]string(nil), old.Groups[oldOwner]...)
				kv.data[s] = make(map[string]string)
				kv.dupe[s] = make(map[int64]int)
			}
		} else if kv.status[s] == stPulling {
			// The shard is moving on to another group but we have not received
			// it yet. Keep pulling it from the original source; once installed
			// it becomes stServing and we can serve it to the next owner. This
			// avoids becoming an empty pull source when configs change rapidly.
			continue
		} else if oldOwner == kv.gid {
			// we are giving the shard away; keep data for the puller
			kv.status[s] = stPushing
			kv.pullFrom[s] = nil
			kv.deleteTo[s] = nil
		} else if kv.status[s] != stPushing {
			// not owned before or now and not holding data for a pending pull
			kv.data[s] = make(map[string]string)
			kv.dupe[s] = make(map[int64]int)
			kv.status[s] = stServing
			kv.pullFrom[s] = nil
			kv.deleteTo[s] = nil
		}
		// else: stPushing with data — keep it for a slower puller even after
		// we have advanced to a later config.
	}
	kv.prevCfg = old
	kv.cfg = cfg
}

// applyClient applies a client command to the state machine.
// caller must hold kv.mu.
func (kv *ShardKV) applyClient(op Op) OpResult {
	res := OpResult{}
	shard := key2shard(op.Key)
	if kv.cfg.Shards[shard] != kv.gid || kv.status[shard] != stServing {
		res.Err = ErrWrongGroup
		return res
	}
	if op.Op == "Get" {
		res.Err = OK
		if v, ok := kv.data[shard][op.Key]; ok {
			res.Value = v
		} else {
			res.Err = ErrNoKey
		}
		return res
	}
	if last, ok := kv.dupe[shard][op.ClientId]; !ok || op.Seq > last {
		if op.Op == "Append" {
			kv.data[shard][op.Key] += op.Value
		} else {
			kv.data[shard][op.Key] = op.Value
		}
		kv.dupe[shard][op.ClientId] = op.Seq
	}
	res.Err = OK
	return res
}

// applyOp dispatches a replicated operation. caller must hold kv.mu.
func (kv *ShardKV) applyOp(op Op) OpResult {
	switch op.Type {
	case opClient:
		return kv.applyClient(op)
	case opConfig:
		kv.applyConfig(op.Config)
		return OpResult{Err: OK}
        case opShard:
                // The pull may have started under an older config; accept it as long
                // as the shard is still waiting for data.
                if kv.status[op.Shard] == stPulling {
                        kv.data[op.Shard] = copyStrMap(op.Data)
                        kv.dupe[op.Shard] = copyIntMap(op.Dupe)
                        kv.deleteTo[op.Shard] = kv.pullFrom[op.Shard]
                        kv.pullFrom[op.Shard] = nil
                        if kv.cfg.Shards[op.Shard] == kv.gid {
                                kv.status[op.Shard] = stServing
                        } else {
                                // While we were pulling, another config handed this shard
                                // to a third group. Keep the data as stPushing so that
                                // group can pull it from us instead of getting an empty
                                // map (or being refused by GetShard) and losing data.
                                kv.status[op.Shard] = stPushing
                        }
                }
                return OpResult{Err: OK}
	case opDelete:
		if kv.status[op.Shard] == stPushing {
			kv.data[op.Shard] = make(map[string]string)
			kv.dupe[op.Shard] = make(map[int64]int)
			kv.status[op.Shard] = stServing
		}
		return OpResult{Err: OK}
	case opDelClear:
		kv.deleteTo[op.Shard] = nil
		return OpResult{Err: OK}
	}
	return OpResult{Err: OK}
}

// makeSnapshot encodes the whole state machine. caller must hold kv.mu.
func (kv *ShardKV) makeSnapshot(index int) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(index)
	e.Encode(kv.cfg)
	e.Encode(kv.prevCfg)
	e.Encode(kv.data)
	e.Encode(kv.dupe)
	e.Encode(kv.status)
	e.Encode(kv.pullFrom)
	e.Encode(kv.deleteTo)
	return w.Bytes()
}

// installSnapshot restores state from a snapshot. caller must hold kv.mu.
func (kv *ShardKV) installSnapshot(data []byte) {
	if len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var index int
	var cfg, prevCfg shardctrler.Config
	var dmap [shardctrler.NShards]map[string]string
	var dupe [shardctrler.NShards]map[int64]int
	var status [shardctrler.NShards]int
	var pullFrom [shardctrler.NShards][]string
	var deleteTo [shardctrler.NShards][]string
	if d.Decode(&index) != nil || d.Decode(&cfg) != nil || d.Decode(&prevCfg) != nil ||
		d.Decode(&dmap) != nil || d.Decode(&dupe) != nil || d.Decode(&status) != nil ||
		d.Decode(&pullFrom) != nil || d.Decode(&deleteTo) != nil {
		return
	}
	kv.cfg = cfg
	kv.prevCfg = prevCfg
	kv.data = dmap
	kv.dupe = dupe
	kv.status = status
	kv.pullFrom = pullFrom
	kv.deleteTo = deleteTo
	kv.lastApplied = index
	for s := 0; s < shardctrler.NShards; s++ {
		if kv.data[s] == nil {
			kv.data[s] = make(map[string]string)
		}
		if kv.dupe[s] == nil {
			kv.dupe[s] = make(map[int64]int)
		}
	}
}

func (kv *ShardKV) applier() {
	for !kv.killed() {
		msg := <-kv.applyCh
		kv.mu.Lock()
		if msg.CommandValid {
			op := msg.Command.(Op)
			res := kv.applyOp(op)
			kv.lastApplied = msg.CommandIndex
			if ch, ok := kv.pending[msg.CommandIndex]; ok {
				delete(kv.pending, msg.CommandIndex)
				ch <- res
			}
			if kv.maxraftstate > 0 && kv.lastApplied > 0 &&
				kv.persister.RaftStateSize() > kv.maxraftstate {
				kv.rf.Snapshot(kv.lastApplied, kv.makeSnapshot(kv.lastApplied))
			}
		} else if msg.SnapshotValid {
			kv.installSnapshot(msg.Snapshot)
			kv.lastApplied = msg.SnapshotIndex
		}
		kv.mu.Unlock()
	}
}

// submit replicates op through Raft and waits for it to be applied.
func (kv *ShardKV) submit(op Op) OpResult {
	bad := OpResult{Err: ErrWrongLeader}
	if kv.killed() {
		return bad
	}
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		return bad
	}
	index, term, ok := kv.rf.Start(op)
	if !ok {
		return bad
	}
	ch := make(chan OpResult, 1)
	kv.mu.Lock()
	kv.pending[index] = ch
	kv.mu.Unlock()
	defer func() {
		kv.mu.Lock()
		delete(kv.pending, index)
		kv.mu.Unlock()
	}()

	timeout := time.NewTimer(time.Second)
	defer timeout.Stop()
	check := time.NewTicker(20 * time.Millisecond)
	defer check.Stop()
	for {
		select {
		case res := <-ch:
			return res
		case <-timeout.C:
			return bad
		case <-check.C:
			if kv.killed() {
				return bad
			}
			curTerm, stillLeader := kv.rf.GetState()
			if !stillLeader || curTerm != term {
				return bad
			}
		}
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	op := Op{Type: opClient, Key: args.Key, Op: "Get", ClientId: args.ClientId, Seq: args.Seq}
	res := kv.submit(op)
	reply.Err = res.Err
	reply.Value = res.Value
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{Type: opClient, Key: args.Key, Value: args.Value, Op: args.Op,
		ClientId: args.ClientId, Seq: args.Seq}
	res := kv.submit(op)
	reply.Err = res.Err
}

// GetShard serves a shard's data to its new owner. The old owner only answers
// once it has installed a configuration at least as new as the reassignment.
func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// Only a stPushing shard holds authoritative data we are allowed to hand
	// off. stServing/stPulling states may be empty placeholders (a shard we
	// never had, or one whose data was already GC'd), and serving those would
	// install an empty map in the new owner and lose data.
	if kv.status[args.Shard] != stPushing {
		reply.Err = ErrWrongGroup
		return
	}
	// We must have installed the requester's config (so we have stopped
	// accepting client ops for this shard) and must no longer own it.
	if args.ConfigNum > kv.cfg.Num {
		reply.Err = ErrWrongGroup
		return
	}
	if kv.cfg.Shards[args.Shard] == kv.gid {
		reply.Err = ErrWrongGroup
		return
	}
	reply.Data = copyStrMap(kv.data[args.Shard])
	reply.Dupe = copyIntMap(kv.dupe[args.Shard])
	reply.Err = OK
}

// DeleteShard lets the new owner tell the old owner it can drop its copy.
func (kv *ShardKV) DeleteShard(args *DeleteShardArgs, reply *DeleteShardReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	num := kv.cfg.Num
	st := kv.status[args.Shard]
	kv.mu.Unlock()
	if num < args.ConfigNum {
		reply.Err = ErrWrongGroup
		return
	}
	if st != stPushing {
		reply.Err = OK
		return
	}
	res := kv.submit(Op{Type: opDelete, Shard: args.Shard, ConfigNum: args.ConfigNum})
	if res.Err == ErrWrongLeader {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err = OK
}

func (kv *ShardKV) getEnd(name string) *labrpc.ClientEnd {
	if e, ok := kv.ends[name]; ok {
		return e
	}
	e := kv.make_end(name)
	kv.ends[name] = e
	return e
}

// fetchShard pulls one shard from any server of its previous owner group.
func (kv *ShardKV) fetchShard(servers []string, shard, num int) GetShardReply {
	args := GetShardArgs{Shard: shard, ConfigNum: num}
	for _, name := range servers {
		var reply GetShardReply
		if kv.getEnd(name).Call("ShardKV.GetShard", &args, &reply) && reply.Err == OK {
			return reply
		}
	}
	return GetShardReply{Err: ErrWrongGroup}
}

// sendDelete asks the previous owner group to drop one shard.
func (kv *ShardKV) sendDelete(servers []string, shard, num int) bool {
	args := DeleteShardArgs{Shard: shard, ConfigNum: num}
	for _, name := range servers {
		var reply DeleteShardReply
		if kv.getEnd(name).Call("ShardKV.DeleteShard", &args, &reply) && reply.Err == OK {
			return true
		}
	}
	return false
}

// configPoller pulls new configurations from the shardctrler, one at a time.
// A config is installed only once every shard we still need to pull has its
// data (no stPulling shards). Shards we are giving away (stPushing) do not
// block, so an old owner can always catch up and serve pulls.
func (kv *ShardKV) configPoller() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		kv.mu.Lock()
		ready := true
		for s := 0; s < shardctrler.NShards; s++ {
			if kv.status[s] == stPulling {
				ready = false
				break
			}
		}
		curNum := kv.cfg.Num
		kv.mu.Unlock()
		if ready {
			next := kv.mck.Query(curNum + 1)
			if next.Num == curNum+1 {
				kv.submit(Op{Type: opConfig, Config: next})
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// migrator pulls shards this group just gained, replicates the pulled data
// through Raft, and then asks the previous owners to delete their copies.
// Both the pull source and the delete target are read from the replicated
// per-shard state, so intervening config changes cannot change them.
func (kv *ShardKV) migrator() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(80 * time.Millisecond)
			continue
		}
		type task struct {
			shard   int
			servers []string
		}
		var pulls []task
		var deletes []task
		kv.mu.Lock()
		num := kv.cfg.Num
		for s := 0; s < shardctrler.NShards; s++ {
			if kv.status[s] == stPulling && len(kv.pullFrom[s]) > 0 {
				pulls = append(pulls, task{s, kv.pullFrom[s]})
			}
			if kv.status[s] == stServing && kv.cfg.Shards[s] == kv.gid &&
				len(kv.deleteTo[s]) > 0 {
				deletes = append(deletes, task{s, kv.deleteTo[s]})
			}
		}
		kv.mu.Unlock()

		for _, t := range pulls {
			reply := kv.fetchShard(t.servers, t.shard, num)
			if reply.Err == OK {
				kv.submit(Op{Type: opShard, Shard: t.shard, Data: reply.Data,
					Dupe: reply.Dupe, ConfigNum: num})
			}
		}
		for _, t := range deletes {
			if kv.sendDelete(t.servers, t.shard, num) {
				kv.submit(Op{Type: opDelClear, Shard: t.shard})
			}
		}
		time.Sleep(80 * time.Millisecond)
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *ShardKV) killed() bool {
	return atomic.LoadInt32(&kv.dead) == 1
}

// needed by shardkv tester
func (kv *ShardKV) Raft() *raft.Raft {
	return kv.rf
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.persister = persister
	kv.mck = shardctrler.MakeClerk(ctrlers)
	kv.cfg = shardctrler.Config{Num: 0, Groups: make(map[int][]string)}
	kv.prevCfg = shardctrler.Config{Num: 0, Groups: make(map[int][]string)}
	kv.pending = make(map[int]chan OpResult)
	kv.ends = make(map[string]*labrpc.ClientEnd)
	for s := 0; s < shardctrler.NShards; s++ {
		kv.data[s] = make(map[string]string)
		kv.dupe[s] = make(map[int64]int)
		kv.status[s] = stServing
	}

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	snap := persister.ReadSnapshot()
	if len(snap) > 0 {
		kv.mu.Lock()
		kv.installSnapshot(snap)
		kv.mu.Unlock()
	}

	go kv.applier()
	go kv.configPoller()
	go kv.migrator()
	return kv
}
