package mr

import (
	"bufio"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

var errEmpty error = errors.New("empty file")

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Register for worker
	var nMap, nReduce int
	reply1 := RegisterReply{}
	call("Coordinator.Register", &RegisterArgs{}, &reply1)
	nMap = reply1.NMap
	nReduce = reply1.NReduce

	// Apply For Map task
	for {
		// define a slice to save intermediate keyvalue
		// Assume sufficient memory space
		mapOutput := make([][]*KeyValue, nReduce)

		reply2 := ApplyForMapReply{}
		if ok := call("Coordinator.ApplyForMap", &ApplyForMapArgs{}, &reply2); !ok {
			break
		}

		mapId := reply2.MapID
		file := reply2.Filename
		content, err := ioutil.ReadFile(file)
		if err != nil {
			log.Fatalf("Read file error: %v\n", err)
		}

		mapOut := mapf(file, string(content))
		for _, ikeyValue := range mapOut {
			outIndex := ihash(ikeyValue.Key) % nReduce
			mapOutput[outIndex] = append(mapOutput[outIndex], &KeyValue{ikeyValue.Key, ikeyValue.Value})
		}

		// sort each slice
		for _, kvSlice := range mapOutput {
			sort.Sort(ByKey(kvSlice))
		}

		// create file to save intermediate keyvalue
		for i := range mapOutput {
			iFile, err := os.Create(fmt.Sprintf("mr-%v-%v", mapId, i))
			if err != nil {
				log.Fatalf("Create intermediate file error: %v\n", err)
			}

			writer := bufio.NewWriter(iFile)
			for _, ikeyValue := range mapOutput[i] {
				_, err := writer.WriteString(fmt.Sprintf("%v %v\n", ikeyValue.Key, ikeyValue.Value))
				if err != nil {
					log.Fatalf("Write intermediate file mr-%v-%v error: %v\n", mapId, i, err)
				}
			}

			if err := writer.Flush(); err != nil {
				log.Fatalf("close file error: %v\n", err)
			}

			if err := iFile.Close(); err != nil {
				log.Fatalf("close file error: %v\n", err)
			}
		}

		// send done message to coordinator
		call("Coordinator.MapDone", &MapDoneArgs{MapId: mapId}, &MapDoneReply{})

		//time.Sleep(time.Second)
	}

	// Apply For Reduce task
	for {
		reply3 := ApplyForReduceReply{}
		if ok := call("Coordinator.ApplyForReduce", &ApplyForReduceArgs{}, &reply3); !ok {
			break
		}
		reduceId := reply3.ReduceId

		// merge all data into tempFile
		tempFile, err := ioutil.TempFile("", fmt.Sprintf("temp-%v.txt", reduceId))
		if err != nil {
			log.Fatal(err)
		}

		var temp []*os.File
		for i := 0; i < nMap; i++ {
			f, err := os.Open(fmt.Sprintf("mr-%v-%v", i, reduceId))
			if err != nil {
				log.Fatalf("open intermediate mr-%v-%v failed: %v\n", i, reduceId, err)
			}
			temp = append(temp, f)
		}

		// sort each reduce files
		errSort := Emsort(tempFile, temp...)
		if errSort != nil && errSort != errEmpty {
			log.Fatalln("sort files failed", errSort)
		}

		// close each files
		for _, f := range temp {
			f.Close()
		}

		if errSort == errEmpty {
			call("Coordinator.ReduceDone", &ReduceDoneArgs{ReduceId: reduceId}, &ReduceDoneReply{})
			os.Remove(tempFile.Name())
			continue
		}
		output, err1 := os.Create(fmt.Sprintf("mr-out-%v", reduceId))
		if err1 != nil {
			log.Fatalf("create mr-out-%v failed: %v\n", reduceId, err1)
		}

		bufWriter := bufio.NewWriter(output)
		var values []string

		tempFile.Seek(0, 0)
		bufScan := bufio.NewScanner(tempFile)
		bufScan.Scan()

		line := strings.Split(bufScan.Text(), " ")
		key := line[0]
		values = append(values, line[1])
		for bufScan.Scan() {
			line := bufScan.Text()
			if len(line) == 0 {
				continue
			}
			kv := strings.Split(line, " ")
			if kv[0] != key {
				bufWriter.WriteString(fmt.Sprintf("%v %v\n", key, reducef(key, values)))
				key = kv[0]
				values = []string{kv[1]}
			} else {
				values = append(values, kv[1])
			}
		}
		bufWriter.WriteString(fmt.Sprintf("%v %v\n", key, reducef(key, values)))
		bufWriter.Flush()

		if err := output.Close(); err != nil {
			log.Fatalf("close file mr-out-%v failed: %v\n", reduceId, err)
		}

		if err := os.Remove(tempFile.Name()); err != nil {
			log.Fatalf("remove tempFile failed: %v\n", err)
		}

		call("Coordinator.ReduceDone", &ReduceDoneArgs{ReduceId: reduceId}, &ReduceDoneReply{})
		time.Sleep(time.Second)
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
