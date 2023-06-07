package shardctrler

import (
	"crypto/rand"
	"math/big"
	"mit6824/labrpc"
)

type Clerk struct {
	servers   []*labrpc.ClientEnd
	clientId  int64
	leaderId  int
	commandId int64
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
	ck.leaderId = 0
	ck.commandId = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := QueryArgs{num, ck.clientId, ck.commandId}
	ck.commandId++
	for {
		reply := QueryReply{}
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Query", &args, &reply)
		if !ok {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		if reply.Err == OK {
			return reply.Config
		} else if reply.Err == ErrTimeOut {
			continue
		} else {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := JoinArgs{servers, ck.clientId, ck.commandId}
	ck.commandId++
	for {
		reply := JoinReply{}
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Join", &args, &reply)
		if !ok {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}

		if reply.Err == OK {
			return
		} else if reply.Err == ErrTimeOut {
			continue
		} else {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := LeaveArgs{gids, ck.clientId, ck.commandId}
	ck.commandId++
	for {
		reply := LeaveReply{}
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Leave", &args, &reply)
		if !ok {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}

		if reply.Err == OK {
			return
		} else if reply.Err == ErrTimeOut {
			continue
		} else {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := MoveArgs{shard, gid, ck.clientId, ck.commandId}
	ck.commandId++
	for {
		reply := MoveReply{}
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Move", &args, &reply)
		if !ok {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}

		if reply.Err == OK {
			return
		} else if reply.Err == ErrTimeOut {
			continue
		} else {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
	}
}
