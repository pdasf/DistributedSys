package kvraft

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

// Get fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{key, ck.clientId, ck.commandId}
	ck.commandId++
	for {
		reply := GetReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		if !ok {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		if reply.Err == OK {
			return reply.Value
		} else if reply.Err == ErrNoKey {
			return ""
		} else if reply.Err == ErrTimeOut {
			continue
		} else {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
	}
}

// PutAppend shared by Put and Append.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{key, value, op, ck.clientId, ck.commandId}
	ck.commandId++
	for {
		reply := PutAppendReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
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

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
