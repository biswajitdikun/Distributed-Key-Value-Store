package kvpaxos

import (
	"errors"
	"fmt"
	"plugin"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"cs651/labgob"
	"cs651/labrpc"

	omnipaxoslib "cs651-gitlab.bu.edu/cs651-fall24/omnipaxos-lib/omnipaxos-lib"
)

const (
	Debug          = false
	RequestTimeout = time.Second
)

type KVServer struct {
	mu            sync.Mutex
	me            int
	rf            omnipaxoslib.IOmnipaxos
	applyCh       chan omnipaxoslib.ApplyMsg
	dead          int32 // set by Kill()
	enableLogging int32

	kvStore        map[string]string
	duplicateTable map[int64]int64 // ClientId -> last applied SeqNum
	notifyCh       map[int]chan Op
}

var (
	ErrTimeout           = errors.New("Timeout")
	ErrOperationMismatch = errors.New("OperationMismatch")
	errNotLeader         = errors.New("not leader")
)

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		Type:     "Get",
		Key:      args.Key,
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
	}

	if err := kv.processRequest(op); err != nil {
		reply.Err = Err(err.Error())
		return
	}

	kv.mu.Lock()
	if value, exists := kv.kvStore[args.Key]; exists {
		reply.Value = value
		reply.Err = OK
	} else {
		reply.Err = ErrNoKey
	}
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		Type:     args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
	}

	if err := kv.processRequest(op); err != nil {
		reply.Err = Err(err.Error())
		return
	}
	reply.Err = OK
}

// processRequest handles the common logic for processing requests
func (kv *KVServer) processRequest(op Op) error {
	index, _, isLeader := kv.rf.Proposal(op)
	if !isLeader {
		return errNotLeader
	}

	ch := make(chan Op, 1)
	kv.mu.Lock()
	kv.notifyCh[index] = ch
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.notifyCh, index)
		kv.mu.Unlock()
	}()

	select {
	case committedOp := <-ch:
		if !kv.isMatchingOperation(committedOp, op) {
			return ErrOperationMismatch
		}
		return nil
	case <-time.After(RequestTimeout):
		return ErrTimeout
	}
}

// Checks if two operations match
func (kv *KVServer) isMatchingOperation(committed, requested Op) bool {
	return committed.Type == requested.Type &&
		committed.ClientId == requested.ClientId &&
		committed.SeqNum == requested.SeqNum
}

func (kv *KVServer) applyOp(op Op) bool {
	lastSeqNum, exists := kv.duplicateTable[op.ClientId]
	if exists && op.SeqNum <= lastSeqNum {
		return false
	}
	switch op.Type {
	case "Get":
		//Continue / Do nothing
	case "Put":
		kv.kvStore[op.Key] = op.Value
	case "Append":
		kv.kvStore[op.Key] += op.Value
	}
	kv.duplicateTable[op.ClientId] = op.SeqNum
	return true

}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister omnipaxoslib.Persistable, maxomnipaxosstate int) *KVServer {
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.enableLogging = 1

	kv.applyCh = make(chan omnipaxoslib.ApplyMsg)

	p, err := plugin.Open(fmt.Sprintf("./omnipaxosmain-%s-%s.so", runtime.GOOS, runtime.GOARCH))
	if err != nil {
		panic(err)
	}
	xrf, err := p.Lookup("MakeOmnipaxos")
	if err != nil {
		panic(err)
	}

	mkrf := xrf.(func([]omnipaxoslib.Callable, int, omnipaxoslib.Persistable, chan omnipaxoslib.ApplyMsg) omnipaxoslib.IOmnipaxos)

	callables := make([]omnipaxoslib.Callable, len(servers))
	for i, s := range servers {
		callables[i] = s
	}

	kv.rf = mkrf(callables, me, persister, kv.applyCh)

	kv.kvStore = make(map[string]string)
	kv.duplicateTable = make(map[int64]int64)
	kv.notifyCh = make(map[int]chan Op)

	go kv.applyLoop()

	return kv
}

type OperationResult struct {
	op      Op
	applied bool
}

func (kv *KVServer) notifyClients(index int, op Op) {
	if ch, exists := kv.notifyCh[index]; exists {
		ch <- op
		close(ch)
		delete(kv.notifyCh, index)
	}
}

func (kv *KVServer) executeOperation(msg omnipaxoslib.ApplyMsg) OperationResult {
	op := msg.Command.(Op)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	applied := kv.applyOp(op)
	kv.notifyClients(msg.CommandIndex, op)

	return OperationResult{op, applied}
}
func (kv *KVServer) handleResult(result OperationResult) {
	if result.applied {
		kv.logSuccess(result.op)
	} else {
		kv.logDuplicate(result.op)
	}
}

// logOperation logs the operation result
func (kv *KVServer) logSuccess(op Op) {
	fmt.Printf("Server %d: Applied %v, Store: %v\n", kv.me, op, kv.kvStore)
}

func (kv *KVServer) logDuplicate(op Op) {
	fmt.Printf("Server %d: Skipped duplicate %v\n", kv.me, op)
}
func (kv *KVServer) processMessage(msg omnipaxoslib.ApplyMsg) {
	result := kv.executeOperation(msg)
	kv.handleResult(result)
}
func (kv *KVServer) applyLoop() {
	// for {
	// 	select {
	// 	case msg := <-kv.applyCh:
	// 		if !msg.CommandValid {
	// 			continue
	// 		}
	// 		kv.processMessage(msg)
	// 	default:
	// 		if kv.killed() {
	// 			return
	// 		}
	// 	}
	// }
	for !kv.killed() {
		msg := <-kv.applyCh
		if !msg.CommandValid {
			continue
		}
		kv.processMessage(msg)
	}
}
