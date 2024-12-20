package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running OmniPaxos.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
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
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID int64 // Unique ID for the client
	SeqNum   int64 // Sequence number for the request
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientID int64 // Unique ID for the client
	SeqNum   int64 // Sequence number for the request
}

type GetReply struct {
	Err   Err
	Value string
}

type ShardOperationArgs struct {
	ConfigNum int
	Shard     int
	State     ShardState
}

type ShardOperationReply struct {
	Err Err
}

type ShardState struct {
	KV        map[string]string
	ClientSeq map[int64]int64
}

// package shardkv

// const (
//     OK             = "OK"
//     ErrNoKey       = "ErrNoKey"
//     ErrWrongGroup  = "ErrWrongGroup"
//     ErrWrongLeader = "ErrWrongLeader"
//     ErrTimeout     = "ErrTimeout"
//     ErrNotReady    = "ErrNotReady"
// )

// type Err string

// // Put or Append
// type PutAppendArgs struct {
//     Key      string
//     Value    string
//     Op       string // "Put" or "Append"
//     ClientId int64
//     CmdNum   int
// }

// type PutAppendReply struct {
//     Err Err
// }

// type GetArgs struct {
//     Key      string
//     ClientId int64
//     CmdNum   int
// }

// type GetReply struct {
//     Err   Err
//     Value string
// }

// // For shard migration
// type MigrateArgs struct {
//     ConfigNum int
//     ShardNum  int
// }

// type MigrateReply struct {
//     ShardData   map[string]string
//     SessionData map[int64]Session
//     Err         Err
// }

// type AckArgs struct {
//     ConfigNum int
//     ShardNum  int
// }

// type AckReply struct {
//     Receive bool
//     Err     Err
// }

// type Session struct {
//     LastCmdNum int
//     OpType     string
//     Response   Reply
// }

// type Reply struct {
//     Err   Err
//     Value string
// }

// type ShardState int

// const (
//     NoExist  ShardState = iota
//     Exist
//     WaitGet
//     WaitGive
// )
