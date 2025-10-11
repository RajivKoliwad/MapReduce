package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		task, nreduce, Job_number := Fetchtask()
		//log.Printf("Received task: %v\n", task.Filename)
		if ismap(task) {
			//log.Printf("Starting Map Task \n")
			err := map_func(nreduce, task.Filename, mapf, Job_number)
			if err != nil {
				//log.Printf("Map task failed: %v\n", err)
			} else {
				//log.Printf("Map task completed \n")
				ok := Job_Confirmation(*task)
				if ok {
					//log.Printf("Map task confirmed by Coordinator \n")
				} else {
					//log.Printf("Map task confirmation failed \n")
				}
			}
		} else if isreduce(task) {
			//log.Printf("Starting Reduce Task \n")
			err := red_func(nreduce, reducef, Job_number)
			if err != nil {
				//log.Printf("Reduce task failed: %v\n", err)
			} else {
				//log.Printf("Reduce task completed \n")
				ok := Job_Confirmation(*task)
				if ok {
					//log.Printf("Reduce task confirmed by Coordinator \n")
				} else {
					//log.Printf("Reduce task confirmation failed \n")
				}
			}
		} else if iswait(task) {
			time.Sleep(time.Second)
			continue
		} else if isdone(task) {
			break
		}
	}
	//log.Printf("Finishing")
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		//log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func ismap(job *Cordjob) bool {
	return (job.Msg == "map")
}

func isreduce(job *Cordjob) bool {
	return job.Msg == "reduce"
}

func iswait(job *Cordjob) bool {
	return job.Msg == "wait"
}
func isdone(job *Cordjob) bool {
	return job.Msg == "done"
}

func map_func(nreduce int, filename string, mapf func(string, string) []KeyValue, Job_number int) error {

	//fnames := make([]string, 0, nreduce)
	kv := map_file_to_kv(filename, mapf)
	intermediate := make(map[int][]KeyValue)
	for _, key := range kv {
		//log.Printf("Hashing Key: %v\n", key.Key)
		bucket_number := ihash(key.Key) % nreduce
		//log.Printf("Bucket: %v\n", bucket_number)
		// file := "mr-" + strconv.Itoa(Job_number) + "-" + strconv.Itoa(bucket_number)
		// fnames = append(fnames, file)
		//time.Sleep(time.Second)
		//log.Printf("Writing to file: %v\n", file)

		intermediate[bucket_number] = append(intermediate[bucket_number], key)

		// ofile, err := os.OpenFile(file, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

		// if err != nil {
		// 	return fmt.Errorf("cannot open file %v: %w", file, err)
		// }
		// enc := json.NewEncoder(ofile)
		// err = enc.Encode(key)
		// if err != nil {
		// 	ofile.Close()
		// 	return fmt.Errorf("cannot write to %v: %w", file, err)
		// }
		// ofile.Close(
	}
	for key, _ := range intermediate {
		file := "mr-" + strconv.Itoa(Job_number) + "-" + strconv.Itoa(key)
		ofile, err := os.OpenFile(file, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

		if err != nil {
			return fmt.Errorf("cannot open file %v: %w", file, err)
		}
		enc := json.NewEncoder(ofile)

		for _, item := range intermediate[key] {
			enc.Encode(item)
		}
		// err = enc.Encode(intermediate[key])

		if err != nil {
			ofile.Close()
			return fmt.Errorf("cannot write to %v: %w", file, err)
		}
		ofile.Close()
	}
	//log.Printf("Filenames we wrote to %v", fnames)
	return nil
}

func map_file_to_kv(filename string, mapf func(string, string) []KeyValue) []KeyValue {
	file, err := os.Open(filename)
	if err != nil {
		//log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		//log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kv := mapf(filename, string(content))
	return kv
}

func red_func(nreduce int, reducef func(string, []string) string, jobnumber int) error {
	filenames, outfile := file_names_generator_reduce(jobnumber, nreduce)
	////log.Printf("Reducing task: %v\n", jobnumber)
	//log.Printf("Reduce Task: Reading files: %v\n", filenames)
	//log.Printf("writing to %v\n", outfile)
	// Read all key-value pairs from all intermediate files
	var kva []KeyValue
	for _, file := range filenames {
		kvs := read_file(file)
		kva = append(kva, kvs...)
	}

	// Sort by key
	sort.Sort(ByKey(kva))

	ofile, err := os.Create(outfile)
	if err != nil {
		return fmt.Errorf("cannot create output file %v: %w", outfile, err)
	}
	defer ofile.Close()

	writer := bufio.NewWriter(ofile)
	defer writer.Flush()

	// Process each unique key
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(writer, "%v %v\n", kva[i].Key, output)
		i = j
	}

	return nil
}

func file_names_generator_reduce(jobnumber int, nreduce int) ([]string, string) {
	filenames := make([]string, nreduce)
	for i := 0; i < nreduce; i++ {
		filenames[i] = "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(jobnumber)
	}
	outfile := "mr-out-" + strconv.Itoa(jobnumber)
	return filenames, outfile
}

//readfile needs to return the key value pairs of a file

func read_file(filename string) []KeyValue {
	var kva []KeyValue
	file, err := os.Open(filename)
	if err != nil {
		//log.Printf("cannot open %v: %v", filename, err)
		return kva
	}
	defer file.Close()

	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}

	return kva
}

func Fetchtask() (*Cordjob, int, int) {
	input := FetchTaskArgs{}
	reply := FetchTaskReply{}
	ok := call("Coordinator.FetchTask", &input, &reply)
	if ok {
		//log.Printf("Received task: %v\n", reply.Job.Filename)
		return &reply.Job, reply.Nreduce, reply.Job_number
	} else {
		//log.Printf("call failed!\n")
		return &Cordjob{Msg: "done"}, -1, -1
	}
}

func Job_Confirmation(job Cordjob) bool {
	JR := JobRequest{Job: job}
	JRESP := JobResponse{}
	////log.Printf("Sending job confirmation for task number: %v\n", job.Task_number)
	ok := call("Coordinator.Job_Confirmation", &JR, &JRESP)
	if ok {
		return true
	} else {

		return false
	}
}
