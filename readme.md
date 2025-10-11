# MapReduce in Golang


## About

This my my implementation of the [MapReduce paper](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf) from google. Mapreduce is a technique for robustly and efficiently completing distributed systems workloads.


Map Reduce works by having a **coordinator** (called master in the paper) process create "Map" and "Reduce" tasks. Several **worker** processes can be created, which will get map or reduce tasks from the coordinator.

In this implementation, MapReduce counts words from several books. Map jobs have workers count occurances of words in a single book. Reduce tasks join these word counts from several files into one overall count.


## Tech Specifics

I use **remote procedure call (RPC)** to communicate between processes. 

## How to Run/Test


### Running the given test file:

1. `cd ~/golabs/src/main` - this gets to correct folder

2. `rm mr-out*` - this removes previous temp files created by the map jobs

3. `bash test-mr.sh` - this runs test, printing out the results of map task 


### Spawning a **coordinator** and some **worker** processes


1. `cd ~/golabs/src/main` - this gets to correct folder

2. `rm mr-out*` - this removes previous temp files created by the map jobs

3. `go run mrcoordinator.go pg-*.txt` - this runs coordinatoe

4. Finally, in another terminal run this to spawn a worker: `go run mrworker.go wc.so`

5. You can repeat steps **1**, **2** and **4** in another terminal to make more workers to make more workers
