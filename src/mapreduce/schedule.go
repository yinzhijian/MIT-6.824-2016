package mapreduce

import (
	"fmt"
	"strconv"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}
	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	for ntasks > 0{
		w := <-mr.registerChannel
		go func(w string,ntasks *int){
			*ntasks = *ntasks - 1
			args := new(DoTaskArgs)
			args.JobName = "task"+strconv.Itoa(*ntasks)
			args.Phase = phase
			args.TaskNumber = *ntasks
			args.NumOtherPhase = nios
			args.File = mr.files[*ntasks]
			ok := call(w, "Worker.DoTask", args, new(struct{}))
			if ok {
				mr.registerChannel <- w
			}else{
				*ntasks = *ntasks + 1
			}
		}(w,&ntasks)
		//worker := <-mr.registerChannel
	}
	fmt.Printf("Schedule: %v phase done\n", phase)
}
