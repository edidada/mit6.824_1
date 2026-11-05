package kvraft

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"
import "sync"
import "time"

type Clerk struct {
	servers  []*labrpc.ClientEnd
	clientId int64
	seq      int
	leader   int
	mu       sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientId = nrand()
	ck.seq = 0
	ck.leader = 0
	return ck
}

func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	ck.seq++
	seq := ck.seq
	ck.mu.Unlock()
	args := &GetArgs{Key: key, ClientId: ck.clientId, Seq: seq}
	for {
		reply := &GetReply{}
		ok := ck.servers[ck.leader].Call("KVServer.Get", args, reply)
		if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
			return reply.Value
		}
		ck.mu.Lock()
		ck.leader = (ck.leader + 1) % len(ck.servers)
		ck.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	ck.seq++
	seq := ck.seq
	ck.mu.Unlock()
	args := &PutAppendArgs{Key: key, Value: value, Op: op, ClientId: ck.clientId, Seq: seq}
	for {
		reply := &PutAppendReply{}
		ok := ck.servers[ck.leader].Call("KVServer.PutAppend", args, reply)
		if ok && reply.Err == OK {
			return
		}
		ck.mu.Lock()
		ck.leader = (ck.leader + 1) % len(ck.servers)
		ck.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
