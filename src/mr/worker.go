package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"os/exec"
	"sort"
	"sync"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	// ask for a file to process
	workerID := getIDFromCoord()
	colorPurple := "\033[35m"
	log.SetPrefix(colorPurple)
	workerExit := make(chan struct{})
	go heartbeat(workerExit)
	defer func() {
		workerExit <- struct{}{}
	}()
	for {
		askTaskReply, err := AskTask(workerID)
		log.Printf("receive AskTask %+v", askTaskReply)
		if err != nil {
			log.Fatalf("askTask failed %v", err)
			return
		}
		switch askTaskReply.TaskType {
		case TaskTypeMap:
			outputFilenames, err := doMap(askTaskReply, mapf)
			if err != nil {
				log.Fatalf("doMap failed %v", err)
			}
			NoticeMapperTaskDone(workerID, outputFilenames)
		case TaskTypeReduce:
			outputFilename, err := doReduce(askTaskReply, reducef)
			if err != nil {
				log.Fatalf("doReduce failed %v", err)
			}
			NoticeReducerTaskDone(workerID, outputFilename)
		case TaskTypeNothing:
			log.Printf("nothing to do, exiting...")
			return
		case TaskTypeWait:
			waitTime := time.Second
			log.Printf("wait %v", waitTime)
			time.Sleep(waitTime)
		default:
			log.Fatalf("bad task type %v", askTaskReply.TaskType)
		}
	}
}

func AskTask(id string) (*AskTaskReply, error) {
	var askTaskReply AskTaskReply
	if succ := call("Coordinator.AskTask", &AskTaskArgs{BaseTaskArgs{ID: id}}, &askTaskReply); succ {
		return &askTaskReply, nil
	}
	return nil, errors.New("call Coordinator.AskTask failed")
}

func NoticeMapperTaskDone(id string, outputFilenames []string) (*NoticeTaskDoneReply, error) {
	var reply NoticeTaskDoneReply
	if succ := call("Coordinator.NoticeTaskDone", &NoticeTaskDoneArgs{
		BaseTaskArgs:       BaseTaskArgs{ID: id},
		TaskType:           TaskTypeMap,
		MapOutputFilenames: outputFilenames,
	}, &reply); succ {
		return &reply, nil
	}
	return nil, errors.New("call Coordinator.NoticeMapperTaskDone failed")
}

func NoticeReducerTaskDone(id, outputFilename string) (*NoticeTaskDoneReply, error) {
	var reply NoticeTaskDoneReply
	if succ := call("Coordinator.NoticeTaskDone", &NoticeTaskDoneArgs{
		BaseTaskArgs:         BaseTaskArgs{ID: id},
		TaskType:             TaskTypeReduce,
		ReduceOutputFilename: outputFilename,
	}, &reply); succ {
		return &reply, nil
	}
	return nil, errors.New("call Coordinator.NoticeReducerTaskDone failed")
}

func doMap(askTaskReply *AskTaskReply, mapf func(string, string) []KeyValue) ([]string, error) {
	if askTaskReply.NumR <= 0 {
		panic("numR should be greater 0")
	}
	filename := askTaskReply.Filename
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("open file failed, filename: %v, err: %v", filename, err)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("read file failed, filename: %v, err: %v", filename, err)
	}
	file.Close()
	kva := mapf(filename, string(content))
	var wg sync.WaitGroup
	wg.Add(askTaskReply.NumR)
	chs := []chan KeyValue{}
	filenameCh := make(chan string, askTaskReply.NumR)
	for i := 0; i < askTaskReply.NumR; i++ {
		ch := make(chan KeyValue)
		go func(idxR int, ch chan KeyValue) {
			defer wg.Done()
			tmpFile, err := ioutil.TempFile("/tmp", "mr-map-")
			if err != nil {
				log.Fatalf("create tmp file failed %v", err)
			}
			enc := json.NewEncoder(tmpFile)
			for kv := range ch {
				enc.Encode(kv)
			}
			outputFilename := fmt.Sprintf("./mr-map-%d-%d", askTaskReply.Index, idxR)
			err = mv(tmpFile.Name(), outputFilename)
			if err != nil {
				log.Fatalf("rename file failed tmp file: %v err: %v", outputFilename, err)
			}
			log.Printf("finish map output filename: %v", outputFilename)
			filenameCh <- outputFilename
		}(i, ch)
		chs = append(chs, ch)
	}
	for _, kv := range kva {
		n := ihash(kv.Key) % askTaskReply.NumR
		chs[n] <- kv
	}
	log.Printf("staring close chs")
	for _, ch := range chs {
		close(ch)
	}
	wg.Wait()
	log.Printf("starting receive output filename")
	close(filenameCh)
	outputFilenames := make([]string, 0, askTaskReply.NumR)
	for filename := range filenameCh {
		outputFilenames = append(outputFilenames, filename)
	}
	sort.Strings(outputFilenames)
	log.Printf("starting notice mapper task done outputFilenames %v", outputFilenames)
	return outputFilenames, nil
}

func doReduce(askTaskReply *AskTaskReply, reducef func(string, []string) string) (string, error) {
	if askTaskReply.TaskType != TaskTypeReduce {
		return "", fmt.Errorf("wrong type %v", askTaskReply.TaskType)
	}
	kva := []KeyValue{}
	// read file
	for _, filename := range askTaskReply.IntermediateFiles {
		file, err := os.Open(filename)
		if err != nil {
			return "", fmt.Errorf("open intermediate file failed, filename: %v, err: %v", filename, err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	// sort by key
	sort.Sort(ByKey(kva))
	// create tmp file and rename when finish
	tmpFile, err := ioutil.TempFile("/tmp", "mr-map-")
	if err != nil {
		log.Fatalf("create tmp file failed %v", err)
	}
	// same key use redecef
	lastKey := ""
	sameV := []string{}
	for _, kv := range kva {
		if len(sameV) == 0 || kv.Key == lastKey {
			sameV = append(sameV, kv.Value)
			lastKey = kv.Key
		} else {
			line := fmt.Sprintf("%v %v\n", lastKey, reducef(lastKey, sameV))
			_, _ = tmpFile.WriteString(line)
			lastKey = kv.Key
			sameV = []string{kv.Value}
		}
	}
	if len(sameV) > 0 {
		line := fmt.Sprintf("%v %v\n", lastKey, reducef(lastKey, sameV))
		_, _ = tmpFile.WriteString(line)
	}
	// rename tmp file
	outputFilename := fmt.Sprintf("mr-out-%v", askTaskReply.Index)
	mv(tmpFile.Name(), outputFilename)
	return outputFilename, nil
}

func getIDFromCoord() string {
	reply, err := Heartbeat(true, "")
	if err != nil {
		log.Fatalf("init heartbeat failed %v", err)
	}
	return reply.ID
}

func heartbeat(exitCh <-chan struct{}) {
	reply, err := Heartbeat(true, "")
	if err != nil {
		log.Fatalf("init heartbeat failed %v", err)
	}
	tick := time.NewTicker(time.Second)
	for {
		select {
		case <-tick.C:
			Heartbeat(false, reply.ID)
		case <-exitCh:
			log.Printf("receive exit, stoppint beat")
			return
		}
	}
}

func Heartbeat(init bool, id string) (*HeartbeatReply, error) {
	var heartbeatReply HeartbeatReply
	if succ := call("Coordinator.Heartbeat", &HeartbeatArgs{Init: init, BaseTaskArgs: BaseTaskArgs{ID: id}}, &heartbeatReply); succ {
		return &heartbeatReply, nil
	}
	return nil, errors.New("call Coordinator.Heartbeat failed")
}

func mv(oldpath, newpath string) error {
	cmd := exec.Command("mv", oldpath, newpath)
	_, err := cmd.Output()
	return err
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

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}
