package mr

import (
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	mutex             sync.Mutex
	done_map          bool
	done_reduce       bool
	nreduce           int
	numfiles          int
	transitioned      bool
	jobcounter        int
	num_files_mapped  int
	num_files_reduced int
	Jobs              []Cordjob
	Reduce_jobs       []Cordjob
}

type Cordjob struct {
	Start_time  time.Time
	Task_number int
	Msg         string
	Assigned    string
	Filename    string
}

func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		//log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.check_reduce()
	if c.done_map && c.done_reduce {
		return true
	} else {
		return false
	}

}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	//log.Printf("In MakeCoordinator\n")
	c := Coordinator{}
	c.Jobs = make([]Cordjob, len(files))
	c.Reduce_jobs = make([]Cordjob, nReduce)

	for i, filename := range files {
		job := Cordjob{
			Task_number: i,
			Msg:         "map",
			Assigned:    "unassigned",
			Filename:    filename,
		}
		c.Jobs[i] = job
	}
	c.numfiles = len(files)
	c.num_files_mapped = 0
	c.jobcounter = 0
	c.num_files_reduced = 0
	c.done_map = false
	c.done_reduce = false
	c.transitioned = false
	c.nreduce = nReduce
	c.done_map = false
	c.server()
	return &c
}

func (c *Coordinator) FetchTask(args *FetchTaskArgs, reply *FetchTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	//log.Printf("mapped: %v, reduced: %v\n", c.num_files_mapped, c.num_files_reduced)
	//log.Printf("done_map: %v, done_reduce: %v\n", c.done_map, c.done_reduce)
	if c.done_map && c.done_reduce {
		// All tasks are done; inform the worker
		reply.Job = Cordjob{Msg: "done"}
		return nil
	} else if !c.done_map {
		first := c.firstMap() //Find First Map Task	{
		if first != -1 {
			c.Jobs[first].Assigned = "in-progress"
			//log.Printf("Assigning Map Task %v\n", first)
			c.Jobs[first].Start_time = time.Now()
			c.Jobs[first].Task_number = first
			//log.Printf("Assigned Map Task w/ name: %v\n", c.Jobs[first].Filename)
			reply.Job = c.Jobs[first]
			reply.Job_number = first
			reply.Nreduce = c.nreduce
			c.jobcounter++
			return nil

		} else {
			//log.Printf("No Map Task Available\n")
			c.check_transition()
			if c.done_map {
				//log.Printf("Transitioning to Reduce Phase\n")
				c.transition()
			}
			reply.Job = Cordjob{Msg: "wait"}
			return nil
		}
	} else if c.done_map && !c.transitioned {
		reply.Job = Cordjob{Msg: "wait"}
		return nil
	} else if c.done_map && !c.done_reduce {
		//log.Printf("In Reduce Phase\n")
		first := c.firstReduce()
		//log.Printf("First Reduce Task: %v\n", first)
		if first != -1 {
			//log.Printf("Assigning Reduce Task %v\n", first)
			c.Reduce_jobs[first].Assigned = "in-progress"
			c.Reduce_jobs[first].Start_time = time.Now()
			c.Reduce_jobs[first].Task_number = first
			reply.Job = c.Reduce_jobs[first]
			reply.Job_number = first
			reply.Nreduce = c.nreduce
			c.jobcounter++
			//log.Printf("%v", c.jobcounter)
			return nil

		} else {
			//log.Printf("No Reduce Task Available\n")
			c.check_reduce()
			reply.Job = Cordjob{Msg: "wait"}
			return nil
		}
	}
	//return failed
	return nil
}

func (c *Coordinator) check_transition() {
	//implies that num_files_mappescawnum_files_reduced is only counding finished jobs
	//log.Printf("Checking Transition\n")
	//log.Printf("num files mapped %v", c.num_files_mapped)
	//log.Printf("num files %v", c.numfiles)
	if c.num_files_mapped == c.numfiles {
		c.done_map = true
		c.transition()
	}

}

func (c *Coordinator) check_reduce() {
	//implies that num_files_mappes is only counding finished jobs
	//log.Printf("num files reduced %v", c.num_files_reduced)
	//log.Printf("nreduce %v \n", c.nreduce)
	if c.num_files_reduced == c.nreduce {
		c.done_reduce = true
	}

}

func (c *Coordinator) overdue(job_num int) bool {
	//log.Printf("Checking Overdue")
	if !c.done_map {
		start := c.Jobs[job_num].Start_time
		if time.Since(start) > 10*time.Second {
			//log.Printf("Overdue Map")
			c.jobcounter--
			return true
		}
	} else if !c.done_reduce {
		//log.Printf("checking overdue Reduce")
		start := c.Reduce_jobs[job_num].Start_time
		if time.Since(start) > 10*time.Second {
			c.jobcounter--
			return true
		}
	}
	return false
}

func (c *Coordinator) transition() error {
	//log.Printf("In Transition\n")
	if c.done_map && !c.done_reduce {
		//initiate reduce tasks
		for i := 0; i < c.nreduce; i++ {
			job := Cordjob{
				Task_number: i,
				Msg:         "reduce",
				Assigned:    "unassigned",
				Filename:    "",
			}
			c.Reduce_jobs[i] = job
		}
		c.jobcounter = 0
		c.transitioned = true
	}
	////log.Printf("Jobs %v", c.Reduce_jobs)
	return nil
}

func (c *Coordinator) Job_Confirmation(args *JobRequest, reply *JobResponse) error {
	//if already completed, dont increased mapped
	//log.Printf("Job Done ")
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if args.Job.Msg == "map" {
		c.Jobs[args.Job.Task_number].Assigned = "completed"
		c.num_files_mapped++
		reply.Err = nil
		c.check_transition()

	} else if args.Job.Msg == "reduce" {
		c.Reduce_jobs[args.Job.Task_number].Assigned = "completed"
		reply.Err = nil
		c.num_files_reduced++
		c.check_reduce()
	}
	return nil
}

// below functions return first unassinged m or r task
func (c *Coordinator) firstMap() int {
	for i := range c.Jobs {
		if c.Jobs[i].Assigned == "in-progress" {
			if c.overdue(i) && c.Jobs[i].Msg == "map" {
				c.Jobs[i].Assigned = "unassigned"
				//log.Printf("assigning %v", i)
				return i
			}
		} else if c.Jobs[i].Assigned == "unassigned" && c.Jobs[i].Msg == "map" {
			//log.Printf("assigning %v", i)
			return i
		}
	}
	return -1
}

func (c *Coordinator) firstReduce() int {

	for i := range c.Reduce_jobs {
		//log.Printf("Reduce job %v assigned status: %v\n", i, c.Reduce_jobs[i].Assigned)
		if c.Reduce_jobs[i].Assigned == "in-progress" {
			if c.overdue(i) && c.Reduce_jobs[i].Msg == "reduce" {
				c.Reduce_jobs[i].Assigned = "unassigned"
				return i
			}

		} else if c.Reduce_jobs[i].Assigned == "unassigned" && c.Reduce_jobs[i].Msg == "reduce" {
			return i
		}
	}
	return -1
}

//How do I deal with the response and reduce tasks?
///WWhat happens if there is a late returning map task
