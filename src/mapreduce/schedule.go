package mapreduce
import (
	"fmt"
	"sync"
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
	var mutex = &sync.Mutex{}
	var taskList []int
	for i:=0;i<ntasks;i+=1{
		taskList = append(taskList,i)
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
		var task int
		mutex.Lock()
		if len(taskList) > 1{
			task, taskList = taskList[0],taskList[1:]
		}else{
			task = taskList[0]
		}
		mutex.Unlock()
		go func(w string,task int){
			args := new(DoTaskArgs)
			args.JobName = mr.jobName
			args.Phase = phase
			args.TaskNumber = task
			args.NumOtherPhase = nios
			args.File = mr.files[task]
			ok := call(w, "Worker.DoTask", args, new(struct{}))
			if ok {
				mr.registerChannel <- w
				mutex.Lock()
				ntasks -= 1
				mutex.Unlock()
			}else{
				mutex.Lock()
				taskList = append(taskList,task)
				ntasks += 1
				mutex.Unlock()
			}
		}(w,task)
	}
	fmt.Printf("Schedule: %v phase done\n", phase)
}
