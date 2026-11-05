package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key      string
	Value    string
	Op       string // "Put" or "Append"
	ClientId int64
	Seq      int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key      string
	ClientId int64
	Seq      int
}

type GetReply struct {
	Err   Err
	Value string
}

// GetShard is used by a group that just gained a shard to pull that shard's
// data from its previous owner. ConfigNum identifies the configuration under
// which the shard was reassigned; the old owner only serves the data once it
// has moved to a configuration at least that new.
type GetShardArgs struct {
	Shard     int
	ConfigNum int
}

type GetShardReply struct {
	Err  Err
	Data map[string]string
	Dupe map[int64]int
}

// DeleteShard is sent by the new owner after it has safely replicated the
// pulled data, so the old owner can drop its copy of the shard.
type DeleteShardArgs struct {
	Shard     int
	ConfigNum int
}

type DeleteShardReply struct {
	Err Err
}
