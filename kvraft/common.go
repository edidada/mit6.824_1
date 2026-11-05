package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string
	// for duplicate detection
	ClientId int64
	Seq      int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// for duplicate detection (not strictly needed for Get, but kept uniform)
	ClientId int64
	Seq      int
}

type GetReply struct {
	Err   Err
	Value string
}
