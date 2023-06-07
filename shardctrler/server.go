package shardctrler

import (
	"mit6824/labgob"
	"mit6824/labrpc"
	"mit6824/raft"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Op struct {
	Index     int64
	Op        string
	Args      interface{}
	ClientId  int64
	CommandId int64
}

type OpResult struct {
	Err    Err
	Config Config
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	notifyCh    map[int64]chan OpResult
	lastApplies map[int64]int64

	configs []Config // indexed by config num
}

func (sc *ShardCtrler) delNotKey(index int64) {
	sc.mu.Lock()
	delete(sc.notifyCh, index)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) waitApplier(op Op) (res OpResult) {
	_, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		res.Err = ErrWrongLeader
		return
	}

	ch := make(chan OpResult, 1)
	sc.mu.Lock()
	sc.notifyCh[op.Index] = ch
	sc.mu.Unlock()

	select {
	case res = <-ch:
		sc.delNotKey(op.Index)
		return
	case <-time.Tick(time.Millisecond * 500):
		sc.delNotKey(op.Index)
		res.Err = ErrTimeOut
		return
	}
}

func (sc *ShardCtrler) getConfig(index int) Config {
	if index == -1 || index >= len(sc.configs) {
		return sc.configs[len(sc.configs)-1].Copy()
	}
	return sc.configs[index].Copy()
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	op := Op{nrand(), "Join", *args, args.ClientId, args.CommandId}
	res := sc.waitApplier(op)
	reply.Err = res.Err
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	op := Op{nrand(), "Leave", *args, args.ClientId, args.CommandId}
	res := sc.waitApplier(op)
	reply.Err = res.Err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	op := Op{nrand(), "Move", *args, args.ClientId, args.CommandId}
	res := sc.waitApplier(op)
	reply.Err = res.Err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	sc.mu.Lock()
	if args.Num >= 0 && args.Num < len(sc.configs) {
		reply.Err = OK
		reply.Config = sc.getConfig(args.Num)
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	op := Op{nrand(), "Query", *args, args.ClientId, args.CommandId}
	res := sc.waitApplier(op)
	reply.Err = res.Err
	reply.Config = res.Config
}

// Raft needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// Kill when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

func (sc *ShardCtrler) adjustConfig(cfg *Config) {
	if len(cfg.Groups) == 0 {
		cfg.Shards = [NShards]int{}
	} else if len(cfg.Groups) == 1 {
		for gid := range cfg.Groups {
			for i := range cfg.Shards {
				cfg.Shards[i] = gid
			}
		}
	} else if len(cfg.Groups) <= NShards {
		avgShardsCount := NShards / len(cfg.Groups)
		otherShardsCount := NShards - avgShardsCount*len(cfg.Groups)
		isTryAgain := true

		for isTryAgain {
			isTryAgain = false

			var gids []int
			for gid := range cfg.Groups {
				gids = append(gids, gid)
			}
			sort.Ints(gids)

			for _, gid := range gids {
				count := 0
				for _, val := range cfg.Shards {
					if val == gid {
						count++
					}
				}

				if count == avgShardsCount {
					continue
				} else if count > avgShardsCount && otherShardsCount == 0 {
					temp := 0
					for k, v := range cfg.Shards {
						if gid == v {
							if temp < avgShardsCount {
								temp += 1
							} else {
								cfg.Shards[k] = 0
							}
						}
					}
				} else if count > avgShardsCount && otherShardsCount > 0 {
					temp := 0
					for k, v := range cfg.Shards {
						if gid == v {
							if temp < avgShardsCount {
								temp += 1
							} else if temp == avgShardsCount && otherShardsCount != 0 {
								otherShardsCount -= 1
							} else {
								cfg.Shards[k] = 0
							}
						}
					}

				} else {
					for k, v := range cfg.Shards {
						if v == 0 && count < avgShardsCount {
							cfg.Shards[k] = gid
							count += 1
						}
						if count == avgShardsCount {
							break
						}
					}
					if count < avgShardsCount {
						isTryAgain = true
						continue
					}
				}
			}

			cur := 0
			for k, v := range cfg.Shards {
				if v == 0 {
					cfg.Shards[k] = gids[cur]
					cur += 1
					cur %= len(cfg.Groups)
				}
			}

		}
	} else {
		gidsFlag := make(map[int]int)
		emptyShards := make([]int, 0, NShards)
		for k, gid := range cfg.Shards {
			if gid == 0 {
				emptyShards = append(emptyShards, k)
				continue
			}
			if _, ok := gidsFlag[gid]; ok {
				cfg.Shards[k] = 0
				emptyShards = append(emptyShards, k)
			} else {
				gidsFlag[gid] = 1
			}
		}
		if len(emptyShards) > 0 {
			var gids []int
			for k := range cfg.Groups {
				gids = append(gids, k)
			}
			sort.Ints(gids)
			temp := 0
			for _, gid := range gids {
				if _, ok := gidsFlag[gid]; !ok {
					cfg.Shards[emptyShards[temp]] = gid
					temp += 1
				}
				if temp >= len(emptyShards) {
					break
				}
			}

		}
	}
}

func (sc *ShardCtrler) handleJoin(args JoinArgs) {
	cfg := sc.getConfig(-1)
	cfg.Num++

	for k, v := range args.Servers {
		cfg.Groups[k] = v
	}

	sc.adjustConfig(&cfg)
	sc.configs = append(sc.configs, cfg)
}

func (sc *ShardCtrler) handleLeave(args LeaveArgs) {
	cfg := sc.getConfig(-1)
	cfg.Num++

	for _, gid := range args.GIDs {
		delete(cfg.Groups, gid)
		for i, v := range cfg.Shards {
			if v == gid {
				cfg.Shards[i] = 0
			}
		}
	}

	sc.adjustConfig(&cfg)
	sc.configs = append(sc.configs, cfg)
}

func (sc *ShardCtrler) handleMove(args MoveArgs) {
	cfg := sc.getConfig(-1)
	cfg.Num++
	cfg.Shards[args.Shard] = args.GID
	sc.configs = append(sc.configs, cfg)
}

func (sc *ShardCtrler) sendNotifyCh(index int64, err Err, cfg Config) {
	if ch, ok := sc.notifyCh[index]; ok {
		ch <- OpResult{
			Err:    err,
			Config: cfg,
		}
	}
}

func (sc *ShardCtrler) applier() {
	for !sc.killed() {
		msg := <-sc.applyCh
		cmd := msg.Command.(Op)
		sc.mu.Lock()
		if cmd.Op == "Query" {
			cfg := sc.getConfig(cmd.Args.(QueryArgs).Num)
			sc.sendNotifyCh(cmd.Index, OK, cfg)
		} else {
			isReply := false
			if v, ok := sc.lastApplies[cmd.ClientId]; ok {
				if v == cmd.CommandId {
					isReply = true
				}
			}

			if !isReply {
				switch cmd.Op {
				case "Join":
					sc.handleJoin(cmd.Args.(JoinArgs))
				case "Leave":
					sc.handleLeave(cmd.Args.(LeaveArgs))
				case "Move":
					sc.handleMove(cmd.Args.(MoveArgs))
				}
			}

			sc.lastApplies[cmd.ClientId] = cmd.CommandId
			sc.sendNotifyCh(cmd.Index, OK, Config{})
		}
		sc.mu.Unlock()
	}
}

// StartServer make a server instance.
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	labgob.Register(Op{})

	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.notifyCh = make(map[int64]chan OpResult)
	sc.lastApplies = make(map[int64]int64)

	go sc.applier()

	return sc
}
