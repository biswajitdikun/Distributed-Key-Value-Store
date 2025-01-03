// package kvpaxos

// const (
// 	OK             = "OK"
// 	ErrNoKey       = "ErrNoKey"
// 	ErrWrongLeader = "ErrWrongLeader"
// )

// type Err string

// // Put or Append
// type PutAppendArgs struct {
// 	Key   string
// 	Value string
// 	Op    string // "Put" or "Append"
// 	// You'll have to add definitions here.
// 	// Field names must start with capital letters,
// 	// otherwise RPC will break.
// }

// type PutAppendReply struct {
// 	Err Err
// }

// type GetArgs struct {
// 	Key string
// 	// You'll have to add definitions here.
// }

// type GetReply struct {
// 	Err   Err
// 	Value string
// }

// package kvpaxos

// const (
// 	OK             = "OK"
// 	ErrNoKey       = "ErrNoKey"
// 	ErrWrongLeader = "ErrWrongLeader"
// )

// type Err string

// // Put or Append
// type PutAppendArgs struct {
// 	Key   string
// 	Value string
// 	Op    string // "Put" or "Append"
// 	// You'll have to add definitions here.
// 	// Field names must start with capital letters,
// 	// otherwise RPC will break.
// 	ClientId int64
// 	SeqNum   int
// }

// type PutAppendReply struct {
// 	Err Err
// }

// type GetArgs struct {
// 	Key string
// 	// You'll have to add definitions here.
// 	ClientId int64
// 	SeqNum   int
// }

//	type GetReply struct {
//		Err   Err
//		Value string
//	}
package kvpaxos

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
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	SeqNum   int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int64
	SeqNum   int64
}

type GetReply struct {
	Err   Err
	Value string
}

type Op struct {
	Type     string // "Get", "Put", or "Append"
	Key      string
	Value    string
	ClientId int64
	SeqNum   int64
}
