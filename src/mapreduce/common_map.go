package mapreduce

import (
	"hash/fnv"
	"os"
	//"fmt"
	"log"
	"bytes"
	"io"
	"encoding/json"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	// to change the flags on the default logger
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	// TODO:
	// You will need to write this function.
	// You can find the filename for this map task's input to reduce task number
	// r using reduceName(jobName, mapTaskNumber, r). The ihash function (given
	// below doMap) should be used to decide which file a given key belongs into.
	//
	// The intermediate output of a map task is stored in the file
	// system as multiple files whose name indicates which map task produced
	// them, as well as which reduce task they are for. Coming up with a
	// scheme for how to store the key/value pairs on disk can be tricky,
	// especially when taking into account that both keys and values could
	// contain newlines, quotes, and any other character you can think of.
	//
	// One format often used for serializing data to a byte stream that the
	// other end can correctly reconstruct is JSON. You are not required to
	// use JSON, but as the output of the reduce tasks *must* be JSON,
	// familiarizing yourself with it here may prove useful. You can write
	// out a data structure as a JSON string to a file using the commented
	// code below. The corresponding decoding functions can be found in
	// common_reduce.go.
	//
	//   enc := json.NewEncoder(file)
	//   for _, kv := ... {
	//     err := enc.Encode(&kv)
	//
	// Remember to close the file after you have written all the values!
	buf := bytes.NewBuffer(nil)
	file,err := os.Open(inFile)
	if err != nil{
		log.Fatal(err)
	}
	io.Copy(buf, file)
	file.Close()

	str := string(buf.Bytes())
	keyValue :=mapF(inFile,str)
	//open files
	files := make(map[string] *os.File)
	for i:=0;i<nReduce;i++{
		intermediate_file_name := reduceName(jobName, mapTaskNumber, i)
		log.Println(intermediate_file_name)
		file,err := os.OpenFile(intermediate_file_name,os.O_WRONLY|os.O_CREATE,0600)
		if err != nil{
			log.Fatal(err)
		}
		files[intermediate_file_name] = file
	}
	for key,kv:= range keyValue{
		r := int(ihash(string(key))) %  nReduce
		//go - priority lower than mod and other arth 
		if r < 0{
			r = -r
		}
		intermediate_file_name := reduceName(jobName, mapTaskNumber, r)
		enc := json.NewEncoder(files[intermediate_file_name])
		if err := enc.Encode(&kv); err != nil {
		    log.Fatal(err)
		}
	}
	for _,file:= range files{
		file.Close()
	}

}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
