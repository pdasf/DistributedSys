package shardkv

import (
	"bytes"
	"log"
	"mit6824/labgob"
	"mit6824/labrpc"
	"mit6824/raft"
	"mit6824/shardctrler"
	"sync"
	"time"
)

type Op struct {
	Index     int64
	Op        string
	Key       string
	Value     string
	ClientId  int64
	CommandId int64
	ConfigNum int
}

type OpResult struct {
	Err   Err
	Value string
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	makeEnd      func(string) *labrpc.ClientEnd
	gid          int
	maxraftstate int // snapshot if log grows this big
	
	stopCh       chan struct{}
	notifyCh     map[int64]chan OpResult
	lastApplies  [shardctrler.NShards]map[int64]int64 //k-v：ClientId-CommandId
	config       shardctrler.Config
	oldConfig    shardctrler.Config
	meShards     map[int]bool
	stateMachine [shardctrler.NShards]map[string]string

	inputShards  map[int]bool
	outputShards map[int]map[int]MergeShardData
	scc          *shardctrler.Clerk
	persister    *raft.Persister
}

func (kv *ShardKV) delNotKey(index int64) {
	kv.mu.Lock()
	if _, ok := kv.notifyCh[index]; ok {
		delete(kv.notifyCh, index)
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) waitApplier(clientId int64, commandId int64, method, key, value string, configNum int) (res OpResult) {
	op := Op{
		Index:     nrand(),
		ClientId:  clientId,
		CommandId: commandId,
		Op:        method,
		Key:       key,
		ConfigNum: configNum,
		Value:     value,
	}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		res.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	ch := make(chan OpResult, 1)
	kv.notifyCh[op.Index] = ch
	kv.mu.Unlock()

	select {
	case <-time.Tick(time.Millisecond * 500):
		res.Err = ErrTimeOut
	case res = <-ch:
	case <-kv.stopCh:
		res.Err = ErrServer
	}

	kv.delNotKey(op.Index)
	return

}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	res := kv.waitApplier(args.ClientId, args.CommandId, "Get", args.Key, "", args.ConfigNum)
	reply.Err = res.Err
	reply.Value = res.Value
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	res := kv.waitApplier(args.ClientId, args.CommandId, args.Op, args.Key, args.Value, args.ConfigNum)
	reply.Err = res.Err
}

// Kill this server instance
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	close(kv.stopCh)
}

func (kv *ShardKV) saveSnapshot(logIndex int) {
	if kv.maxraftstate == -1 || kv.persister.RaftStateSize() < kv.maxraftstate {
		return
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.stateMachine) != nil ||
		e.Encode(kv.lastApplies) != nil ||
		e.Encode(kv.inputShards) != nil ||
		e.Encode(kv.outputShards) != nil ||
		e.Encode(kv.config) != nil ||
		e.Encode(kv.oldConfig) != nil ||
		e.Encode(kv.meShards) != nil {
		log.Fatal("encode error")
	}
	data := w.Bytes()
	kv.rf.Snapshot(logIndex, data)
}

func (kv *ShardKV) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvData [shardctrler.NShards]map[string]string
	var lastApplies [shardctrler.NShards]map[int64]int64
	var inputShards map[int]bool
	var outputShards map[int]map[int]MergeShardData
	var config shardctrler.Config
	var oldConfig shardctrler.Config
	var meShards map[int]bool

	if d.Decode(&kvData) != nil ||
		d.Decode(&lastApplies) != nil ||
		d.Decode(&inputShards) != nil ||
		d.Decode(&outputShards) != nil ||
		d.Decode(&config) != nil ||
		d.Decode(&oldConfig) != nil ||
		d.Decode(&meShards) != nil {
		log.Fatal("kv read persist err")
	} else {
		kv.stateMachine = kvData
		kv.lastApplies = lastApplies
		kv.inputShards = inputShards
		kv.outputShards = outputShards
		kv.config = config
		kv.oldConfig = oldConfig
		kv.meShards = meShards
	}
}

func (kv *ShardKV) OutputDataExist(configNum int, shardId int) bool {
	if _, ok := kv.outputShards[configNum]; ok {
		if _, ok = kv.outputShards[configNum][shardId]; ok {
			return true
		}
	}
	return false
}

func (kv *ShardKV) FetchShardData(args *FetchShardDataArgs, reply *FetchShardDataReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.ConfigNum >= kv.config.Num {
		return
	}

	reply.Success = false
	if configData, ok := kv.outputShards[args.ConfigNum]; ok {
		if shardData, ok := configData[args.ShardNum]; ok {
			reply.Success = true
			reply.Data = make(map[string]string)
			reply.CommandIndexes = make(map[int64]int64)
			for k, v := range shardData.Data {
				reply.Data[k] = v
			}
			for k, v := range shardData.CommandIndexes {
				reply.CommandIndexes[k] = v
			}
		}
	}
	return

}

func (kv *ShardKV) CleanShardData(args *CleanShardDataArgs, reply *CleanShardDataReply) {
	kv.mu.Lock()

	if args.ConfigNum >= kv.config.Num {
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	_, _, isLeader := kv.rf.Start(*args)
	if !isLeader {
		return
	}

	for i := 0; i < 10; i++ {
		kv.mu.Lock()
		exist := kv.OutputDataExist(args.ConfigNum, args.ShardNum)
		kv.mu.Unlock()
		if !exist {
			reply.Success = true
			return
		}
		time.Sleep(time.Millisecond * 20)
	}
	return

}

func (kv *ShardKV) callPeerCleanShardData(config shardctrler.Config, shardId int) {
	args := CleanShardDataArgs{
		ConfigNum: config.Num,
		ShardNum:  shardId,
	}

	for {
		for _, group := range config.Groups[config.Shards[shardId]] {
			reply := CleanShardDataReply{}
			srv := kv.makeEnd(group)
			done := make(chan bool, 1)

			go func(args *CleanShardDataArgs, reply *CleanShardDataReply) {
				done <- srv.Call("ShardKV.CleanShardData", args, reply)
			}(&args, &reply)

			select {
			case <-kv.stopCh:
				return
			case <-time.Tick(time.Millisecond * 500):
			case isDone := <-done:
				if isDone && reply.Success == true {
					return
				}
			}

		}
		kv.mu.Lock()
		if kv.config.Num != config.Num+1 || len(kv.inputShards) == 0 {
			kv.mu.Unlock()
			break
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) getValueByKey(key string) (err Err, value string) {
	if v, ok := kv.stateMachine[key2shard(key)][key]; ok {
		err = OK
		value = v
	} else {
		err = ErrNoKey
		value = ""
	}
	return
}

func (kv *ShardKV) ProcessKeyReady(configNum int, key string) Err {
	if configNum == 0 || configNum != kv.config.Num {
		return ErrWrongGroup
	}
	shardId := key2shard(key)
	if _, ok := kv.meShards[shardId]; !ok {
		return ErrWrongGroup
	}
	if _, ok := kv.inputShards[shardId]; ok {
		return ErrWrongGroup
	}
	return OK
}

func (kv *ShardKV) sendNotifyCh(index int64, err Err, value string) {
	if ch, ok := kv.notifyCh[index]; ok {
		ch <- OpResult{
			Err:   err,
			Value: value,
		}
	}
}

func (kv *ShardKV) handleOp(cmdIdx int, op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shardId := key2shard(op.Key)
	if err := kv.ProcessKeyReady(op.ConfigNum, op.Key); err != OK {
		kv.sendNotifyCh(op.Index, err, "")
		return
	}
	if op.Op == "Get" {
		e, v := kv.getValueByKey(op.Key)
		kv.sendNotifyCh(op.Index, e, v)
	} else if op.Op == "Put" || op.Op == "Append" {
		isRepeated := false
		if v, ok := kv.lastApplies[shardId][op.ClientId]; ok {
			if v == op.CommandId {
				isRepeated = true
			}
		}

		if !isRepeated {
			switch op.Op {
			case "Put":
				kv.stateMachine[shardId][op.Key] = op.Value
				kv.lastApplies[shardId][op.ClientId] = op.CommandId
			case "Append":
				e, v := kv.getValueByKey(op.Key)
				if e == ErrNoKey {
					kv.stateMachine[shardId][op.Key] = op.Value
					kv.lastApplies[shardId][op.ClientId] = op.CommandId
				} else {
					kv.stateMachine[shardId][op.Key] = v + op.Value
					kv.lastApplies[shardId][op.ClientId] = op.CommandId
				}
			default:
				panic("unknown method " + op.Op)
			}

		}
		kv.sendNotifyCh(op.Index, OK, "")
	} else {
		panic("unknown method " + op.Op)
	}

	kv.saveSnapshot(cmdIdx)
}

func (kv *ShardKV) handleConfig(cmdIdx int, config shardctrler.Config) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if config.Num <= kv.config.Num {
		kv.saveSnapshot(cmdIdx)
		return
	}

	if config.Num != kv.config.Num+1 {
		panic("applyConfig err")
	}

	oldConfig := kv.config.Copy()
	outputShards := make([]int, 0, shardctrler.NShards)
	inputShards := make([]int, 0, shardctrler.NShards)
	meShards := make([]int, 0, shardctrler.NShards)

	for i := 0; i < shardctrler.NShards; i++ {
		if config.Shards[i] == kv.gid {
			meShards = append(meShards, i)
			if oldConfig.Shards[i] != kv.gid {
				inputShards = append(inputShards, i)
			}
		} else {
			if oldConfig.Shards[i] == kv.gid {
				outputShards = append(outputShards, i)
			}
		}
	}

	kv.meShards = make(map[int]bool)
	for _, shardId := range meShards {
		kv.meShards[shardId] = true
	}

	d := make(map[int]MergeShardData)
	for _, shardId := range outputShards {
		mergeShardData := MergeShardData{
			ConfigNum:      oldConfig.Num,
			ShardNum:       shardId,
			Data:           kv.stateMachine[shardId],
			CommandIndexes: kv.lastApplies[shardId],
		}
		d[shardId] = mergeShardData
		kv.stateMachine[shardId] = make(map[string]string)
		kv.lastApplies[shardId] = make(map[int64]int64)
	}
	kv.outputShards[oldConfig.Num] = d
	kv.inputShards = make(map[int]bool)
	if oldConfig.Num != 0 {
		for _, shardId := range inputShards {
			kv.inputShards[shardId] = true
		}
	}

	kv.config = config
	kv.oldConfig = oldConfig
	kv.saveSnapshot(cmdIdx)
}

func (kv *ShardKV) handleMergeShardData(cmdIdx int, data MergeShardData) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.config.Num != data.ConfigNum+1 {
		return
	}

	if _, ok := kv.inputShards[data.ShardNum]; !ok {
		return
	}

	kv.stateMachine[data.ShardNum] = make(map[string]string)
	kv.lastApplies[data.ShardNum] = make(map[int64]int64)

	for k, v := range data.Data {
		kv.stateMachine[data.ShardNum][k] = v
	}
	for k, v := range data.CommandIndexes {
		kv.lastApplies[data.ShardNum][k] = v
	}
	delete(kv.inputShards, data.ShardNum)

	kv.saveSnapshot(cmdIdx)
	go kv.callPeerCleanShardData(kv.oldConfig, data.ShardNum)
}

func (kv *ShardKV) handleCleanShardData(cmdIdx int, data CleanShardDataArgs) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.OutputDataExist(data.ConfigNum, data.ShardNum) {
		delete(kv.outputShards[data.ConfigNum], data.ShardNum)
	}

	kv.saveSnapshot(cmdIdx)
}

func (kv *ShardKV) applier() {
	for {
		select {
		case <-kv.stopCh:
			return
		case msg := <-kv.applyCh:
			if msg.CommandValid{
				index := msg.CommandIndex
				if op, ok := msg.Command.(Op); ok {
					kv.handleOp(index, op)
				} else if config, ok := msg.Command.(shardctrler.Config); ok {
					kv.handleConfig(index, config)
				} else if mergeData, ok := msg.Command.(MergeShardData); ok {
					kv.handleMergeShardData(index, mergeData)
				} else if cleanData, ok := msg.Command.(CleanShardDataArgs); ok {
					kv.handleCleanShardData(index, cleanData)
				}
			}else{
				kv.mu.Lock()
				kv.readPersist(msg.Snapshot)
				kv.mu.Unlock()
			}
		}
	}
}

func (kv *ShardKV) pullConfig() {
	for {
		select {
		case <-kv.stopCh:
			return
		case <-time.Tick(time.Millisecond * 100):
			_, isLeader := kv.rf.GetState()
			if !isLeader {
				break
			}
			kv.mu.Lock()
			lastNum := kv.config.Num
			kv.mu.Unlock()

			config := kv.scc.Query(lastNum + 1)
			if config.Num == lastNum+1 {
				kv.mu.Lock()
				if len(kv.inputShards) == 0 && kv.config.Num+1 == config.Num {
					kv.mu.Unlock()
					kv.rf.Start(config.Copy())
				} else {
					kv.mu.Unlock()
				}
			}
		}
	}
}

func (kv *ShardKV) fetchShards() {
	for {
		select {
		case <-kv.stopCh:
			return
		case <-time.Tick(time.Millisecond * 200):
			_, isLeader := kv.rf.GetState()
			if isLeader {
				kv.mu.Lock()
				for shardId := range kv.inputShards {
					go kv.fetchShard(shardId, kv.oldConfig)
				}
				kv.mu.Unlock()
			}
		}
	}
}

func (kv *ShardKV) fetchShard(shardId int, config shardctrler.Config) {
	args := FetchShardDataArgs{
		ConfigNum: config.Num,
		ShardNum:  shardId,
	}

	for {
		for _, s := range config.Groups[config.Shards[shardId]] {
			reply := FetchShardDataReply{}
			srv := kv.makeEnd(s)
			done := make(chan bool, 1)
			go func(args *FetchShardDataArgs, reply *FetchShardDataReply) {
				done <- srv.Call("ShardKV.FetchShardData", args, reply)
			}(&args, &reply)

			select {
			case <-kv.stopCh:
				return
			case <-time.Tick(time.Millisecond * 500):
			case isDone := <-done:
				if isDone && reply.Success == true {
					kv.mu.Lock()
					if _, ok := kv.inputShards[shardId]; ok && kv.config.Num == config.Num+1 {
						replyCopy := reply.Copy()
						mergeShardData := MergeShardData{
							ConfigNum:      args.ConfigNum,
							ShardNum:       args.ShardNum,
							Data:           replyCopy.Data,
							CommandIndexes: replyCopy.CommandIndexes,
						}
						kv.mu.Unlock()
						kv.rf.Start(mergeShardData)
						return
					} else {
						kv.mu.Unlock()
					}
				}
			}

		}
	}

}

// StartServer make a server instance.
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// makeEnd(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and makeEnd() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, makeEnd func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.makeEnd = makeEnd
	kv.gid = gid
	kv.persister = persister
	kv.scc = shardctrler.MakeClerk(ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.stopCh = make(chan struct{})
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.stateMachine = [shardctrler.NShards]map[string]string{}
	for i := range kv.stateMachine {
		kv.stateMachine[i] = make(map[string]string)
	}
	kv.lastApplies = [shardctrler.NShards]map[int64]int64{}
	for i := range kv.lastApplies {
		kv.lastApplies[i] = make(map[int64]int64)
	}

	kv.inputShards = make(map[int]bool)
	kv.outputShards = make(map[int]map[int]MergeShardData)
	config := shardctrler.Config{
		Num:    0,
		Shards: [shardctrler.NShards]int{},
		Groups: map[int][]string{},
	}
	kv.config = config
	kv.oldConfig = config

	kv.readPersist(kv.persister.ReadSnapshot())

	kv.notifyCh = make(map[int64]chan OpResult)

	go kv.applier()

	go kv.pullConfig()

	go kv.fetchShards()

	return kv
}
