package shardctrler

import "mit6824/labgob"

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// NShards express The number of shards.
const NShards = 10

// Config :A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK             = "OK"
	ErrWrongLeader = "wrongLeader"
	ErrTimeOut     = "timeout"
)

type Err string

type JoinArgs struct {
	Servers   map[int][]string // new GID -> servers mappings
	ClientId  int64
	CommandId int64
}

type JoinReply struct {
	Err Err
}

type LeaveArgs struct {
	GIDs      []int
	ClientId  int64
	CommandId int64
}

type LeaveReply struct {
	Err Err
}

type MoveArgs struct {
	Shard     int
	GID       int
	ClientId  int64
	CommandId int64
}

type MoveReply struct {
	Err Err
}

type QueryArgs struct {
	Num       int // desired config number
	ClientId  int64
	CommandId int64
}

type QueryReply struct {
	Err    Err
	Config Config
}

func (c *Config) Copy() Config {
	cfg := Config{
		Num:    c.Num,
		Shards: c.Shards,
		Groups: make(map[int][]string),
	}
	for gid, s := range c.Groups {
		cfg.Groups[gid] = append([]string{}, s...)
	}
	return cfg
}

func init() {
	labgob.Register(Config{})
	labgob.Register(QueryArgs{})
	labgob.Register(QueryReply{})
	labgob.Register(JoinArgs{})
	labgob.Register(JoinReply{})
	labgob.Register(LeaveArgs{})
	labgob.Register(LeaveReply{})
	labgob.Register(MoveArgs{})
	labgob.Register(MoveReply{})
}
