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
	"sync"
)

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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// ask for a file to process
	askTaskReply, err := AskTask()
	log.Printf("receive AskTask %+v", askTaskReply)
	if err != nil {
		log.Fatalf("askTask failed %v", err)
		return
	}
	if askTaskReply.TaskType == TaskTypeMap {
		if askTaskReply.NumR <= 0 {
			panic("numR should be greater 0")
		}
		filename := askTaskReply.Filename
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
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
		log.Printf("starting notice mapper task done outputFilenames %v", outputFilenames)
		NoticeMapperTaskDone(outputFilenames)
	} else if askTaskReply.TaskType == TaskTypeReduce {
		NoticeReducerTaskDone()
	} else {
		log.Fatalf("bad task type %v", askTaskReply.TaskType)
	}
}

func AskTask() (*AskTaskReply, error) {
	var askTaskReply AskTaskReply
	if succ := call("Coordinator.AskTask", &AskTaskArgs{}, &askTaskReply); succ {
		return &askTaskReply, nil
	}
	return nil, errors.New("call Coordinator.AskTask failed")
}

func NoticeMapperTaskDone(outputFilenames []string) (*NoticeTaskDoneReply, error) {
	var reply NoticeTaskDoneReply
	if succ := call("Coordinator.NoticeTaskDone", &NoticeTaskDoneArgs{
		TaskType:           TaskTypeMap,
		MapOutputFilenames: outputFilenames,
	}, &reply); succ {
		return &reply, nil
	}
	return nil, errors.New("call Coordinator.NoticeMapperTaskDone failed")
}

func NoticeReducerTaskDone() (*NoticeTaskDoneReply, error) {
	var reply NoticeTaskDoneReply
	if succ := call("Coordinator.NoticeTaskDone", &NoticeTaskDoneArgs{
		TaskType: TaskTypeReduce,
	}, &reply); succ {
		return &reply, nil
	}
	return nil, errors.New("call Coordinator.NoticeReducerTaskDone failed")
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
