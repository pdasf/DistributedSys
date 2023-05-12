package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync/atomic"
	"time"
)

type Coordinator struct {
	files       []string
	nMap        int
	nReduce     int
	maps        chan int
	reduces     chan int
	mapDoing    []chan struct{}
	reduceDoing []chan struct{}
	mapDone     int32
	reduceDone  int32
}

var mapWait = make(chan struct{}, 1)
var reduceWait = make(chan struct{}, 1)

func (c *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
	*reply = RegisterReply{
		NMap:    c.nMap,
		NReduce: c.nReduce,
	}
	return nil
}

func (c *Coordinator) ApplyForMap(args *ApplyForMapArgs, reply *ApplyForMapReply) error {
	select {
	case tmp := <-c.maps:
		*reply = ApplyForMapReply{
			MapID:    tmp,
			Filename: c.files[tmp],
		}

		// create a goroutine to check the Map task
		go func(tmp int) {
			ticker := time.Tick(10 * time.Second)
			select {
			case <-ticker:
				c.maps <- tmp
			case <-c.mapDoing[tmp]:
				return
			}
		}(tmp)

	case <-mapWait:
		mapWait <- struct{}{}
		return errors.New("all Map tasks are assigned")
	}
	return nil
}

func (c *Coordinator) MapDone(args *MapDoneArgs, reply *MapDoneReply) error {
	c.mapDoing[args.MapId] <- struct{}{}
	atomic.AddInt32(&c.mapDone, 1)
	if atomic.CompareAndSwapInt32(&c.mapDone, int32(c.nMap), int32(c.nMap)) {
		mapWait <- struct{}{}
	}
	return nil
}

func (c *Coordinator) ApplyForReduce(args *ApplyForReduceArgs, reply *ApplyForReduceReply) error {
	// wait for maps task
	select {
	case tmp := <-c.reduces:
		*reply = ApplyForReduceReply{
			ReduceId: tmp,
		}

		// create a goroutine to check the Reduce task
		go func(tmp int) {
			ticker := time.Tick(10 * time.Second)
			select {
			case <-ticker:
				c.reduces <- tmp
			case <-c.reduceDoing[tmp]:
				return
			}
		}(tmp)

	case <-reduceWait:
		reduceWait <- struct{}{}
		return errors.New("the reduce task assignment is completed")
	}
	return nil
}

func (c *Coordinator) ReduceDone(args *ReduceDoneArgs, reply *ReduceDoneReply) error {
	c.reduceDoing[args.ReduceId] <- struct{}{}
	atomic.AddInt32(&c.reduceDone, 1)
	if atomic.CompareAndSwapInt32(&c.reduceDone, int32(c.nReduce), int32(c.nReduce)) {
		reduceWait <- struct{}{}
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := atomic.CompareAndSwapInt32(&c.reduceDone, int32(c.nReduce), int32(c.nReduce))
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	maps, reduces := make(chan int, len(files)), make(chan int, nReduce)
	mapDoing, reduceDoing := make([]chan struct{}, len(files)), make([]chan struct{}, nReduce)
	for i := 0; i < len(files); i++ {
		mapDoing[i] = make(chan struct{})
		maps <- i
	}
	for i := 0; i < nReduce; i++ {
		reduceDoing[i] = make(chan struct{})
		reduces <- i
	}
	c := Coordinator{
		files:       files,
		nMap:        len(files),
		nReduce:     nReduce,
		maps:        maps,
		reduces:     reduces,
		mapDoing:    mapDoing,
		reduceDoing: reduceDoing,
		reduceDone:  0,
		mapDone:     0,
	}
	c.server()
	return &c
}
