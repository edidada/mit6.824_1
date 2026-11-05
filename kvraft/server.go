package kvraft

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	return
}

type Op struct {
	Key      string
	Value    string
	Op       string
	ClientId int64
	Seq      int
}

type OpResult struct {
	Err   Err
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	maxraftstate int
	persister    *raft.Persister

	data        map[string]string
	dupe        map[int64]int
	pending     map[int]chan OpResult
	lastApplied int
}

// applyOp applies op to the state machine. caller must hold kv.mu.
func (kv *KVServer) applyOp(op Op) OpResult {
	res := OpResult{}
	if op.Op == "Get" {
		res.Err = OK
		if v, ok := kv.data[op.Key]; ok {
			res.Value = v
		}
		return res
	}
	// Put/Append: dedupe by (ClientId, Seq).
	if lastSeq, ok := kv.dupe[op.ClientId]; !ok || op.Seq > lastSeq {
		if op.Op == "Append" {
			kv.data[op.Key] += op.Value
		} else {
			kv.data[op.Key] = op.Value
		}
		kv.dupe[op.ClientId] = op.Seq
	}
	res.Err = OK
	return res
}

func (kv *KVServer) makeSnapshot(index int) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(index)
	e.Encode(kv.data)
	e.Encode(kv.dupe)
	return w.Bytes()
}

// installSnapshot restores state from a snapshot. caller must hold kv.mu.
func (kv *KVServer) installSnapshot(data []byte) {
	if len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var index int
	var dmap map[string]string
	var dupe map[int64]int
	if d.Decode(&index) != nil || d.Decode(&dmap) != nil || d.Decode(&dupe) != nil {
		return
	}
	kv.data = dmap
	kv.dupe = dupe
	kv.lastApplied = index
}

func (kv *KVServer) applier() {
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
			if kv.maxraftstate > 0 && kv.lastApplied > 0 && kv.persister.RaftStateSize() > kv.maxraftstate {
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
func (kv *KVServer) submit(op Op) (Err, string) {
	if kv.killed() {
		return ErrWrongLeader, ""
	}
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		return ErrWrongLeader, ""
	}
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		return ErrWrongLeader, ""
	}
	ch := make(chan OpResult, 1)
	kv.mu.Lock()
	kv.pending[index] = ch
	kv.mu.Unlock()

	timeout := time.NewTimer(time.Second)
	defer timeout.Stop()
	check := time.NewTicker(20 * time.Millisecond)
	defer check.Stop()
	for {
		select {
		case res := <-ch:
			return res.Err, res.Value
		case <-timeout.C:
			kv.mu.Lock()
			delete(kv.pending, index)
			kv.mu.Unlock()
			return ErrWrongLeader, ""
		case <-check.C:
			if kv.killed() {
				return ErrWrongLeader, ""
			}
			curTerm, stillLeader := kv.rf.GetState()
			if !stillLeader || curTerm != term {
				kv.mu.Lock()
				delete(kv.pending, index)
				kv.mu.Unlock()
				return ErrWrongLeader, ""
			}
		}
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{Key: args.Key, Op: "Get", ClientId: args.ClientId, Seq: args.Seq}
	err, value := kv.submit(op)
	reply.Err = err
	reply.Value = value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{Key: args.Key, Value: args.Value, Op: args.Op, ClientId: args.ClientId, Seq: args.Seq}
	err, _ := kv.submit(op)
	reply.Err = err
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister
	kv.data = make(map[string]string)
	kv.dupe = make(map[int64]int)
	kv.pending = make(map[int]chan OpResult)
	kv.lastApplied = 0

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	snap := persister.ReadSnapshot()
	if len(snap) > 0 {
		kv.mu.Lock()
		kv.installSnapshot(snap)
		kv.mu.Unlock()
	}
	go kv.applier()
	return kv
}
