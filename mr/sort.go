package mr

import (
	"bufio"
	"container/heap"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)

// for sorting by key.
type ByKey []*KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func (a *ByKey) Push(x interface{}) {
	*a = append(*a, x.(*KeyValue))
}

func (a *ByKey) Pop() interface{} {
	old := *a
	n := len(old)
	x := old[n-1]
	*a = old[0 : n-1]
	return x
}

//
// Emsort function Merge and sort the data in the files of readers
//
func Emsort(writer io.Writer, readers ...*os.File) error {
	kvSlice := &ByKey{}
	heap.Init(kvSlice)
	nReader := len(readers)
	fileMap := make(map[*KeyValue]int, nReader)

	bufW := bufio.NewWriter(writer)
	defer func(bufW *bufio.Writer) {
		err := bufW.Flush()
		if err != nil {
			log.Fatal("store data error")
		}
	}(bufW)

	bufR := []*bufio.Reader{}
	for i := range readers {
		bufR = append(bufR, bufio.NewReader(readers[i]))
		tmp, err := readKV(bufR[len(bufR)-1])
		if err != nil && err != io.EOF {
			return err
		}
		if tmp == nil {
			nReader--
			bufR = bufR[:len(bufR)-1]
			continue
		}
		fileMap[tmp] = len(bufR) - 1
		heap.Push(kvSlice, tmp)
	}

	// if all reader is empty, do return
	if nReader == 0 {
		return errEmpty
	}

	for nReader > 1 {
		tmp := heap.Pop(kvSlice).(*KeyValue)
		if _, err := bufW.WriteString(fmt.Sprintf("%v %v\n", tmp.Key, tmp.Value)); err != nil {
			return err
		}

		if ret, err := readKV(bufR[fileMap[tmp]]); err != nil && err != io.EOF {
			return err
		} else if ret != nil {
			fileMap[ret] = fileMap[tmp]
			heap.Push(kvSlice, ret)
		} else {
			nReader--
		}
		delete(fileMap, tmp)
	}

	tmp := heap.Pop(kvSlice).(*KeyValue)
	if _, err := bufW.WriteString(fmt.Sprintf("%v %v\n", tmp.Key, tmp.Value)); err != nil {
		return err
	}

	leave, err := bufR[fileMap[tmp]].ReadBytes('\n')
	for err == nil {
		_, err1 := bufW.Write(leave)
		if err1 != nil {
			return err1
		}
		leave, err = bufR[fileMap[tmp]].ReadBytes('\n')
	}
	if err != io.EOF {
		return err
	}
	return nil
}

func readKV(r *bufio.Reader) (*KeyValue, error) {
	s, err := r.ReadString('\n')
	s = strings.Trim(strings.Trim(s, "\n"), "\r")
	if err != nil && err != io.EOF {
		return nil, err
	}
	if err == io.EOF && len(s) == 0 {
		return nil, err
	}
	if len(s) == 0 {
		return readKV(r)
	}
	tmp := strings.Split(strings.Trim(s, "\n"), " ")
	ret := &KeyValue{
		Key:   tmp[0],
		Value: tmp[1],
	}
	if err == io.EOF {
		return ret, err
	}
	return ret, nil
}
