package shardkv

import "mit6824/labgob"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
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
	ErrTimeOut     = "ErrTimeOut"
	ErrServer      = "ErrServer"
)

type Err string

func init() {
	labgob.Register(CleanShardDataArgs{})
	labgob.Register(MergeShardData{})
}

type PutAppendArgs struct {
	Key       string
	Value     string
	Op        string
	ClientId  int64
	CommandId int64
	ConfigNum int
}

type PutAppendReply struct {
	Err Err
}

func (c *PutAppendArgs) copy() PutAppendArgs {
	r := PutAppendArgs{
		Key:       c.Key,
		Value:     c.Value,
		Op:        c.Op,
		ClientId:  c.ClientId,
		CommandId: c.CommandId,
		ConfigNum: c.ConfigNum,
	}
	return r
}

type GetArgs struct {
	Key       string
	ClientId  int64
	CommandId int64
	ConfigNum int
}

type GetReply struct {
	Err   Err
	Value string
}

func (c *GetArgs) copy() GetArgs {
	r := GetArgs{
		Key:       c.Key,
		ClientId:  c.ClientId,
		CommandId: c.CommandId,
		ConfigNum: c.ConfigNum,
	}
	return r
}

type FetchShardDataArgs struct {
	ConfigNum int
	ShardNum  int
}

type FetchShardDataReply struct {
	Success        bool
	CommandIndexes map[int64]int64
	Data           map[string]string
}

func (reply *FetchShardDataReply) Copy() FetchShardDataReply {
	res := FetchShardDataReply{
		Success:        reply.Success,
		Data:           make(map[string]string),
		CommandIndexes: make(map[int64]int64),
	}
	for k, v := range reply.Data {
		res.Data[k] = v
	}
	for k, v := range reply.CommandIndexes {
		res.CommandIndexes[k] = v
	}
	return res
}

type CleanShardDataArgs struct {
	ConfigNum int
	ShardNum  int
}

type CleanShardDataReply struct {
	Success bool
}

type MergeShardData struct {
	ConfigNum      int
	ShardNum       int
	CommandIndexes map[int64]int64
	Data           map[string]string
}
