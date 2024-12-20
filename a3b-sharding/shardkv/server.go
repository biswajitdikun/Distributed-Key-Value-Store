package shardkv

import (
	"cs651/a3b-sharding/shardctrler"
	"cs651/labgob"
	"cs651/labrpc"
	"fmt"
	"plugin"
	"runtime"
	"sync"
	"time"

	omnipaxoslib "cs651-gitlab.bu.edu/cs651-fall24/omnipaxos-lib/omnipaxos-lib"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key      string
	Value    string
	ClientID int64
	SeqNum   int64
	Type     string // "Get", "Put", "Append".
	Config   shardctrler.Config
	Shard    int
	State    ShardState
}

type ShardKV struct {
	mu            sync.Mutex
	me            int
	op            omnipaxoslib.IOmnipaxos
	applyCh       chan omnipaxoslib.ApplyMsg
	make_end      func(string) *labrpc.ClientEnd
	gid           int
	ctrlers       []*labrpc.ClientEnd
	maxpaxosstate int // snapshot if log grows this big

	// Your definitions here.
	Config      shardctrler.Config
	shards      map[int]ShardState
	lastApplied int
	mck         *shardctrler.Clerk
}

func (kv *ShardKV) initShard(shard int) {
	if _, ok := kv.shards[shard]; !ok {
		kv.shards[shard] = ShardState{
			KV:        make(map[string]string),
			ClientSeq: make(map[int64]int64),
		}
	}
}

// Common operation handling logic extracted into a separate method
func (kv *ShardKV) handleOperation(op Op, shard int) (string, Err) {
	kv.mu.Lock()
	if kv.Config.Shards[shard] != kv.gid {
		kv.mu.Unlock()
		return "", ErrWrongGroup
	}
	kv.mu.Unlock()

	index, _, isLeader := kv.op.Proposal(op)
	if !isLeader {
		return "", ErrWrongLeader
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if !kv.Wait(index) {
		return "", ErrWrongLeader
	}

	return "", OK
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		Type:     "Get",
		Key:      args.Key,
		ClientID: args.ClientID,
		SeqNum:   args.SeqNum,
	}

	shard := key2shard(args.Key)
	value, err := kv.handleOperation(op, shard)

	if err == OK {
		kv.mu.Lock()
		kv.initShard(shard)
		if val, exists := kv.shards[shard].KV[args.Key]; exists {
			value = val
		} else {
			err = ErrNoKey
		}
		kv.mu.Unlock()
	}

	reply.Value = value
	reply.Err = err
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		Type:     args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientID: args.ClientID,
		SeqNum:   args.SeqNum,
	}

	shard := key2shard(args.Key)
	_, err := kv.handleOperation(op, shard)
	reply.Err = err
}

func (kv *ShardKV) Wait(index int) bool {
	timeout := time.After(500 * time.Millisecond)
	for kv.lastApplied < index {
		kv.mu.Unlock()
		select {
		case <-timeout:
			kv.mu.Lock()
			return false
		case <-time.After(10 * time.Millisecond):
		}
		kv.mu.Lock()
	}
	return true
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.op.Kill()
	// Your code here, if desired.
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying OmniPaxos
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the OmniPaxos state along with the snapshot.
//
// the k/v server should snapshot when OmniPaxos's saved state exceeds
// maxOmniPaxosstate bytes, in order to allow OmniPaxos to garbage-collect its
// log. if maxOmniPaxosstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister omnipaxoslib.Persistable, maxOmniPaxosstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxpaxosstate = maxOmniPaxosstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.applyCh = make(chan omnipaxoslib.ApplyMsg)
	kv.Config = shardctrler.Config{}
	kv.shards = make(map[int]ShardState)
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan omnipaxoslib.ApplyMsg)
	// kv.op = omnipaxos.Make(servers, me, persister, kv.applyCh)

	p, err := plugin.Open(fmt.Sprintf("../omnipaxosmain-%s-%s.so", runtime.GOOS, runtime.GOARCH))
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

	kv.op = mkrf(callables, me, persister, kv.applyCh)

	go kv.applyLoop()  // Process operations from OmniPaxos's apply channel.
	go kv.pollConfig() // Periodically poll the shard controller for configuration changes.

	return kv
}

func (kv *ShardKV) applyOperation(op Op) {
	shard := key2shard(op.Key)
	kv.initShard(shard)

	if seq, exists := kv.shards[shard].ClientSeq[op.ClientID]; !exists || op.SeqNum > seq {
		kv.shards[shard].ClientSeq[op.ClientID] = op.SeqNum
		switch op.Type {
		case "Put":
			kv.shards[shard].KV[op.Key] = op.Value
		case "Append":
			kv.shards[shard].KV[op.Key] += op.Value
		}
	}
}

func (kv *ShardKV) applyConfig(newConfig shardctrler.Config) {
	if newConfig.Num <= kv.Config.Num {
		return
	}

	for shard := 0; shard < shardctrler.NShards; shard++ {
		if newConfig.Shards[shard] == kv.gid && kv.Config.Shards[shard] != kv.gid {
			kv.initShard(shard)
		}
	}
	kv.Config = newConfig
}

func (kv *ShardKV) GetShard(args *ShardOperationArgs, reply *ShardOperationReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.ConfigNum >= kv.Config.Num {
		reply.Err = ErrWrongGroup
		return
	}

	if state, ok := kv.shards[args.Shard]; ok {
		args.State = ShardState{
			KV:        make(map[string]string),
			ClientSeq: make(map[int64]int64),
		}
		for k, v := range state.KV {
			args.State.KV[k] = v
		}
		for k, v := range state.ClientSeq {
			args.State.ClientSeq[k] = v
		}
		reply.Err = OK
	} else {
		reply.Err = ErrWrongGroup
	}
}

func (kv *ShardKV) applyLoop() {
	for msg := range kv.applyCh {
		if !msg.CommandValid {
			continue
		}

		kv.mu.Lock()
		if msg.CommandIndex <= kv.lastApplied {
			kv.mu.Unlock()
			continue
		}

		op, ok := msg.Command.(Op)
		if !ok {
			kv.mu.Unlock()
			continue
		}

		kv.configOperation(op)
		kv.lastApplied = msg.CommandIndex
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) configOperation(op Op) {
	switch op.Type {
	case "Config":
		kv.applyConfig(op.Config)
	case "Put", "Append", "Get":
		if kv.isResponsibleForShard(op.Key) {
			kv.applyOperation(op)
		}
	}
}

func (kv *ShardKV) isResponsibleForShard(key string) bool {
	return kv.Config.Shards[key2shard(key)] == kv.gid
}

func (kv *ShardKV) killed() bool {
	return false
}

func (kv *ShardKV) pollConfig() {
	const pollInterval = 100 * time.Millisecond

	for !kv.killed() {
		nextNum := kv.getNextConfigNum()
		if config := kv.fetchNewConfig(nextNum); config != nil {
			kv.proposeConfig(*config)
		}
		time.Sleep(pollInterval)
	}
}

func (kv *ShardKV) getNextConfigNum() int {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.Config.Num + 1
}

func (kv *ShardKV) fetchNewConfig(num int) *shardctrler.Config {
	newConfig := kv.mck.Query(num)
	if newConfig.Num <= num-1 {
		return nil
	}
	return &newConfig
}

func (kv *ShardKV) proposeConfig(config shardctrler.Config) {
	op := Op{
		Type:   "Config",
		Config: config,
	}
	kv.op.Proposal(op)
}
