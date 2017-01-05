package mapreduce

import (
//	"hash/fnv"
	"os"
	"fmt"
	"log"
//	"bytes"
//	"io"
	"encoding/json"
	"sort"
)
// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	intermediate_file_name := reduceName(jobName, nMap, reduceTaskNumber)
	fmt.Println("yanxin"+intermediate_file_name)
	file,err := os.Open(intermediate_file_name)
	if err != nil{
		log.Fatal(err)
	}
	dec := json.NewDecoder(file)
	var kv KeyValue
	var keys []string
	kvs := make(map[string][]string)
	for{
		err := dec.Decode(&kv)
		if err != nil{
			log.Fatal(err)
			break
		}
		kvs[kv.Key] = append(kvs[kv.Key],kv.Value)
	}
	file.Close()
	for k := range kvs {
		keys = append(keys,k)
	}
	sort.Strings(keys)
	merge_name := mergeName(jobName, reduceTaskNumber)
	fmt.Println("yintao"+merge_name)
	merge_file,err := os.OpenFile(merge_name,os.O_WRONLY|os.O_CREATE ,0600)
	if err != nil{
		log.Fatal(err)
	}
	enc := json.NewEncoder(merge_file)
	for _,k := range keys {
		enc.Encode(KeyValue{k, reduceF(k,kvs[k])})
	}
	merge_file.Close()
}
