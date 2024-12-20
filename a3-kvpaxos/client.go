// package kvpaxos

// import (
// 	"crypto/rand"
// 	"math/big"
// 	"sync"
// 	"time"

// 	"cs651/labrpc"
// )

// type Clerk struct {
// 	servers  []*labrpc.ClientEnd
// 	clientId int64
// 	seqNum   int64
// 	mu       sync.Mutex
// 	leaderId int
// }

// func nrand() int64 {
// 	max := big.NewInt(int64(1) << 62)
// 	bigx, _ := rand.Int(rand.Reader, max)
// 	x := bigx.Int64()
// 	return x
// }

// func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
// 	ck := new(Clerk)
// 	ck.servers = servers
// 	ck.clientId = nrand()
// 	ck.seqNum = 0
// 	ck.leaderId = 0
// 	return ck
// }

// func (ck *Clerk) Get(key string) string {
// 	ck.mu.Lock()
// 	seqNum := ck.seqNum
// 	ck.seqNum++
// 	ck.mu.Unlock()

// 	for i := 0; ; i++ {
// 		args := GetArgs{
// 			Key:      key,
// 			ClientId: ck.clientId,
// 			SeqNum:   seqNum,
// 		}
// 		var reply GetReply
// 		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)

// 		if ok && reply.Err == OK {
// 			return reply.Value
// 		}
// 		if ok && reply.Err == ErrNoKey {
// 			return ""
// 		}

// 		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
// 		time.Sleep(time.Duration(100+i*50) * time.Millisecond)
// 	}
// }

// func (ck *Clerk) PutAppend(key string, value string, op string) {
// 	ck.mu.Lock()
// 	seqNum := ck.seqNum
// 	ck.seqNum++
// 	ck.mu.Unlock()

// 	for i := 0; ; i++ {
// 		args := PutAppendArgs{
// 			Key:      key,
// 			Value:    value,
// 			Op:       op,
// 			ClientId: ck.clientId,
// 			SeqNum:   seqNum,
// 		}
// 		var reply PutAppendReply
// 		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)

// 		if ok && reply.Err == OK {
// 			return
// 		}

// 		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
// 		time.Sleep(time.Duration(100+i*50) * time.Millisecond)
// 	}
// }

// func (ck *Clerk) Put(key string, value string) {
// 	ck.PutAppend(key, value, "Put")
// }

// func (ck *Clerk) Append(key string, value string) {
// 	ck.PutAppend(key, value, "Append")
// }

package kvpaxos

import (
	"crypto/rand"
	"cs651/labrpc"
	"math/big"
	"sync"
	"time"
)

const (
	baseRetryDelay = 100 * time.Millisecond
	retryIncrement = 50 * time.Millisecond
)

type Clerk struct {
	servers  []*labrpc.ClientEnd
	clientId int64
	seqNum   int64
	mu       sync.Mutex
	leaderId int
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
	ck.seqNum = 0
	ck.leaderId = 0
	return ck
}

func (ck *Clerk) Get(key string) string {
	seqNum := ck.getNextSeqNum()
	args := GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		SeqNum:   seqNum,
	}

	for attempt := 0; ; attempt++ {
		reply := &GetReply{}
		if ck.tryGet(&args, reply) {
			switch reply.Err {
			case OK:
				return reply.Value
			case ErrNoKey:
				return ""
			}
		}
		ck.rotateLeader()
		ck.backoff(attempt)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	seqNum := ck.getNextSeqNum()
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		SeqNum:   seqNum,
	}

	for attempt := 0; ; attempt++ {
		reply := &PutAppendReply{}
		if ck.tryPutAppend(&args, reply) {
			if reply.Err == OK {
				return
			}
		}
		ck.rotateLeader()
		ck.backoff(attempt)
	}
}

// Helper methods
func (ck *Clerk) getNextSeqNum() int64 {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	seqNum := ck.seqNum
	ck.seqNum++
	return seqNum
}

func (ck *Clerk) tryGet(args *GetArgs, reply *GetReply) bool {
	return ck.servers[ck.leaderId].Call("KVServer.Get", args, reply)
}

func (ck *Clerk) tryPutAppend(args *PutAppendArgs, reply *PutAppendReply) bool {
	return ck.servers[ck.leaderId].Call("KVServer.PutAppend", args, reply)
}

func (ck *Clerk) rotateLeader() {
	ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
}

func (ck *Clerk) backoff(attempt int) {
	delay := baseRetryDelay + time.Duration(attempt)*retryIncrement
	time.Sleep(delay)
}
