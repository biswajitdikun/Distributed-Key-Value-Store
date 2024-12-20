// package shardctrler

// import (
//  "cs651/labgob"
//  "cs651/labrpc"
//  "fmt"
//  "plugin"
//  "runtime"
//  "sync"

//  omnipaxoslib "cs651-gitlab.bu.edu/cs651-fall24/omnipaxos-lib/omnipaxos-lib"
// )

// type ShardCtrler struct {
//  mu      sync.Mutex
//  me      int
//  op      omnipaxoslib.IOmnipaxos
//  applyCh chan omnipaxoslib.ApplyMsg

//  // Your data here.

//  configs []Config // indexed by config num
// }

// type Op struct {
//  // Your data here.
// }

// func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
//  // Your code here.
// }

// func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
//  // Your code here.
// }

// func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
//  // Your code here.
// }

// func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
//  // Your code here.
// }

// // the tester calls Kill() when a ShardCtrler instance won't
// // be needed again. you are not required to do anything
// // in Kill(), but it might be convenient to (for example)
// // turn off debug output from this instance.
// func (sc *ShardCtrler) Kill() {
//  sc.op.Kill()
//  // Your code here, if desired.
// }

// // needed by shardkv tester
// func (sc *ShardCtrler) OmniPaxos() omnipaxoslib.IOmnipaxos {
//  return sc.op
// }

// // servers[] contains the ports of the set of
// // servers that will cooperate via OmniPaxos to
// // form the fault-tolerant shardctrler service.
// // me is the index of the current server in servers[].
// func StartServer(servers []*labrpc.ClientEnd, me int, persister omnipaxoslib.Persistable) *ShardCtrler {
//  sc := new(ShardCtrler)
//  sc.me = me

//  sc.configs = make([]Config, 1)
//  sc.configs[0].Groups = map[int][]string{}

//  labgob.Register(Op{})
//  sc.applyCh = make(chan omnipaxoslib.ApplyMsg)
//  // sc.op = omnipaxos.Make(servers, me, persister, sc.applyCh)

//  p, err := plugin.Open(fmt.Sprintf("../omnipaxosmain-%s-%s.so", runtime.GOOS, runtime.GOARCH))
//  if err != nil {
//      panic(err)
//  }
//  xrf, err := p.Lookup("MakeOmnipaxos")
//  if err != nil {
//      panic(err)
//  }

//  mkrf := xrf.(func([]omnipaxoslib.Callable, int, omnipaxoslib.Persistable, chan omnipaxoslib.ApplyMsg) omnipaxoslib.IOmnipaxos)

//  callables := make([]omnipaxoslib.Callable, len(servers))
//  for i, s := range servers {
//      callables[i] = s
//  }

//  sc.op = mkrf(callables, me, persister, sc.applyCh)

//  // Your code here.

//  return sc
// }

package shardctrler

import (
	"cs651/labgob"
	"cs651/labrpc"
	"fmt"
	"plugin"
	"runtime"
	"sort"
	"sync"
	"time"

	omnipaxoslib "cs651-gitlab.bu.edu/cs651-fall24/omnipaxos-lib/omnipaxos-lib"
)

const (
	opTimeout  = 150 * time.Millisecond
	debugLevel = 3
	retryDelay = 10 * time.Millisecond
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	op      omnipaxoslib.IOmnipaxos
	applyCh chan omnipaxoslib.ApplyMsg

	// Your data here.

	configs     []Config // indexed by config num
	lastApplied int
	clientSeq   map[int64]int64
}

type Op struct {
	// Your data here.
	Type     string
	Servers  map[int][]string
	GIDs     []int
	Shard    int
	GID      int
	Num      int
	ClientId int64
	SeqNum   int64
}

func (sc *ShardCtrler) checkLeader(operation Op) bool {
	index, _, isLeader := sc.op.Proposal(operation)
	if !isLeader {
		return false
	}
	return sc.Wait(index)
}
func (sc *ShardCtrler) handleRequest(operation Op, reply interface{}) {
	if !sc.checkLeader(operation) {
		switch r := reply.(type) {
		case *JoinReply:
			r.WrongLeader = true
		case *LeaveReply:
			r.WrongLeader = true
		case *MoveReply:
			r.WrongLeader = true
		case *QueryReply:
			r.WrongLeader = true
		}
		return
	}
	switch r := reply.(type) {
	case *JoinReply:
		r.Err = OK
	case *LeaveReply:
		r.Err = OK
	case *MoveReply:
		r.Err = OK
	}
}
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	op := Op{
		Type:     "Join",
		Servers:  args.Servers,
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
	}
	sc.handleRequest(op, reply)

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	op := Op{
		Type:     "Leave",
		GIDs:     args.GIDs,
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
	}

	sc.handleRequest(op, reply)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	op := Op{
		Type:     "Move",
		Shard:    args.Shard,
		GID:      args.GID,
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
	}

	sc.handleRequest(op, reply)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	op := Op{
		Type:     "Query",
		Num:      args.Num,
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
	}

	sc.handleRequest(op, reply)
	if !reply.WrongLeader {
		sc.mu.Lock()
		if args.Num < 0 || args.Num >= len(sc.configs) {
			reply.Config = sc.configs[len(sc.configs)-1]
		} else {
			reply.Config = sc.configs[args.Num]
		}
		sc.mu.Unlock()
		reply.Err = OK
	}
}

func (sc *ShardCtrler) Wait(index int) bool {
	timeout := time.After(opTimeout)
	for {
		select {
		case <-timeout:
			println("Got Timeout in Wait")
			return false
		default:
			sc.mu.Lock()
			completed := sc.lastApplied >= index
			sc.mu.Unlock()
			if completed {
				return true
			}
			time.Sleep(retryDelay)
		}
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.op.Kill()
}

// needed by shardkv tester
func (sc *ShardCtrler) OmniPaxos() omnipaxoslib.IOmnipaxos {
	return sc.op
}

// servers[] contains the ports of the set of
// servers that will cooperate via OmniPaxos to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister omnipaxoslib.Persistable) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan omnipaxoslib.ApplyMsg)
	// sc.op = omnipaxos.Make(servers, me, persister, sc.applyCh)

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

	sc.op = mkrf(callables, me, persister, sc.applyCh)

	// Your code here.

	for i := 0; i < NShards; i++ {
		sc.configs[0].Shards[i] = 0
	}

	sc.clientSeq = make(map[int64]int64)

	go sc.applyLoop()

	return sc
}

func (sc *ShardCtrler) applyLoop() {
	for msg := range sc.applyCh {
		if !msg.CommandValid {
			continue
		}
		sc.mu.Lock()
		sc.processCommand(msg)
		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) processCommand(msg omnipaxoslib.ApplyMsg) {
	fmt.Printf("Server %d: Processing command at index %d\n", sc.me, msg.CommandIndex)

	command := msg.Command.(Op)
	if sc.isDuplicate(command.ClientId, command.SeqNum) {
		fmt.Printf("Server %d: Skipping duplicate command from client %d\n",
			sc.me, command.ClientId)
		sc.lastApplied = msg.CommandIndex
		return
	}

	sc.executeCommand(command)
	sc.clientSeq[command.ClientId] = command.SeqNum
	sc.lastApplied = msg.CommandIndex
}

func (sc *ShardCtrler) executeCommand(command Op) {
	fmt.Printf("Server %d: Executing %s operation from client %d\n",
		sc.me, command.Type, command.ClientId)

	switch command.Type {
	case "Join":
		sc.JoinOP(command.Servers)
	case "Leave":
		sc.LeaveOP(command.GIDs)
	case "Move":
		sc.MoveOP(command.Shard, command.GID)
	case "Query":
		// Query operations don't modify state
	default:
		fmt.Printf("Server %d: Unknown operation type: %s\n",
			sc.me, command.Type)
	}
}

func (sc *ShardCtrler) isDuplicate(clientId int64, requestId int64) bool {
	lastSeq, ok := sc.clientSeq[clientId]
	return ok && lastSeq >= requestId
}
func (sc *ShardCtrler) createnewConfig(newConfig Config) Config {
	return Config{
		Num:    len(sc.configs),
		Shards: newConfig.Shards,
		Groups: make(map[int][]string),
	}
}
func (sc *ShardCtrler) appendlastConfigGroups(next Config, groups map[int][]string) {
	for gid, servers := range groups {
		next.Groups[gid] = append([]string(nil), servers...)
	}
}
func (sc *ShardCtrler) JoinOP(servers map[int][]string) {
	println("Inside JoinOP")
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := sc.createnewConfig(lastConfig)

	sc.appendlastConfigGroups(newConfig, lastConfig.Groups)

	for gid, serverList := range servers {
		newConfig.Groups[gid] = append([]string(nil), serverList...)
	}
	sc.modifyShards(&newConfig)
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) LeaveOP(gids []int) {
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := sc.createnewConfig(lastConfig)

	sc.appendlastConfigGroups(newConfig, lastConfig.Groups)

	for _, gid := range gids {
		delete(newConfig.Groups, gid)
		for i := range newConfig.Shards {
			if newConfig.Shards[i] == gid {
				newConfig.Shards[i] = 0
			}
		}
	}
	sc.modifyShards(&newConfig)
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) MoveOP(shard int, gid int) {
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := sc.createnewConfig(lastConfig)

	sc.appendlastConfigGroups(newConfig, lastConfig.Groups)

	newConfig.Shards[shard] = gid
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) modifyShards(config *Config) {
	//If no grop exists skip modification
	if len(config.Groups) == 0 {
		return
	}

	//Initialization per Group
	shardCount := make(map[int]int)
	for gid := range config.Groups {
		shardCount[gid] = 0
	}

	//Current shard count per group
	for _, gid := range config.Shards {
		if gid != 0 {
			shardCount[gid]++
		}
	}

	// sorted list of group IDS
	gids := make([]int, 0, len(config.Groups))
	for gid := range config.Groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	fmt.Printf("Sorted gids%v\n", gids)

	configLength := len(config.Groups)
	targetShards := NShards / configLength
	fmt.Printf("TargetShards:%d\n", targetShards)
	for shard, gid := range config.Shards {
		if gid == 0 || shardCount[gid] > targetShards {
			mnGid := gids[0]
			for _, currentGid := range gids {
				if shardCount[mnGid] > shardCount[currentGid] {
					mnGid = currentGid
				}
			}
			config.Shards[shard] = mnGid
			if gid != 0 {
				shardCount[gid]--
			}
			shardCount[mnGid]++
		}
	}
}
