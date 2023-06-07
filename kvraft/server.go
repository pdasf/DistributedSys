package kvraft

import (
	"bytes"
	"log"
	"mit6824/labgob"
	"mit6824/labrpc"
	"mit6824/raft"
	"sync"
	"sync/atomic"
	"time"
)

type Op struct {
	Index     int64
	Op        string
	Key       string
	Value     string
	ClientId  int64
	CommandId int64
}

type OpResult struct {
	Err   Err
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	persister    *raft.Persister
	stateMachine map[string]string
	notifyCh     map[int64]chan OpResult // index:OpResult
	lastApplies  map[int64]int64         //ClientId:CommandId
}

func (kv *KVServer) delNotKey(index int64) {
	kv.mu.Lock()
	delete(kv.notifyCh, index)
	kv.mu.Unlock()
}

func (kv *KVServer) waitApplier(op Op) (res OpResult) {
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		res.Err = ErrWrongLeader
		return
	}

	ch := make(chan OpResult, 1)
	kv.mu.Lock()
	kv.notifyCh[op.Index] = ch
	kv.mu.Unlock()

	select {
	case res = <-ch:
		kv.delNotKey(op.Index)
		return
	case <-time.Tick(time.Millisecond * 500):
		kv.delNotKey(op.Index)
		res.Err = ErrTimeOut
		return
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{nrand(), "Get", args.Key, "", args.ClientId, args.CommandId}
	res := kv.waitApplier(op)
	reply.Err = res.Err
	reply.Value = res.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{nrand(), args.Op, args.Key, args.Value, args.ClientId, args.CommandId}
	res := kv.waitApplier(op)
	reply.Err = res.Err
}

// Kill this server.
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) saveSnapshot(logIndex int) {
	if kv.maxraftstate == -1 || kv.persister.RaftStateSize() < kv.maxraftstate {
		return
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.stateMachine) != nil || e.Encode(kv.lastApplies) != nil {
		log.Fatal("encode error")
	}
	data := w.Bytes()
	kv.rf.Snapshot(logIndex, data)
}

func (kv *KVServer) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvData map[string]string
	var lastApplies map[int64]int64

	if d.Decode(&kvData) != nil || d.Decode(&lastApplies) != nil {
		log.Fatal("read persist err")
	} else {
		kv.stateMachine = kvData
		kv.lastApplies = lastApplies
	}
}

func (kv *KVServer) sendNotifyCh(index int64, err Err, value string) {
	if ch, ok := kv.notifyCh[index]; ok {
		ch <- OpResult{
			Err:   err,
			Value: value,
		}
	}
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			cmd := msg.Command.(Op)
			kv.mu.Lock()
			if cmd.Op == "Get" {
				if v, ok := kv.stateMachine[cmd.Key]; !ok {
					kv.sendNotifyCh(cmd.Index, ErrNoKey, v)
				} else {
					kv.sendNotifyCh(cmd.Index, OK, v)
				}
			} else {
				isReply := false
				if v, ok := kv.lastApplies[cmd.ClientId]; ok {
					if v == cmd.CommandId {
						isReply = true
					}
				}

				if !isReply {

					switch cmd.Op {
					case "Put":
						kv.stateMachine[cmd.Key] = cmd.Value
					case "Append":
						if v, ok := kv.stateMachine[cmd.Key]; ok {
							kv.stateMachine[cmd.Key] = v + cmd.Value
						} else {
							kv.stateMachine[cmd.Key] = cmd.Value
						}
					}

				}
				kv.lastApplies[cmd.ClientId] = cmd.CommandId
				kv.sendNotifyCh(cmd.Index, OK, "")
			}

			kv.saveSnapshot(msg.CommandIndex)
			kv.mu.Unlock()
		} else {
			kv.mu.Lock()
			kv.readPersist(msg.Snapshot)
			kv.mu.Unlock()
		}
	}
}

// StartKVServer make a server instance.
// servers[] contains the ports of the set of servers that will
// cooperate via Raft to form the fault-tolerant key/value service.
// me is the Index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.lastApplies = make(map[int64]int64)
	kv.stateMachine = make(map[string]string)
	kv.readPersist(kv.persister.ReadSnapshot())
	kv.notifyCh = make(map[int64]chan OpResult)

	go kv.applier()
	return kv
}
