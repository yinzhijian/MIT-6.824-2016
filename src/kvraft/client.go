package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "fmt"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	fmt.Printf("Get")
	args := &GetArgs{}
	args.Key = key
	reply := &GetReply{}
	ok := ck.servers[ck.lastLeader].Call("RaftKV.Get", args, reply)
	if ok && reply.WrongLeader == false && reply.Err == ""{
		return reply.Value
	}
	i := (ck.lastLeader+1) %len(ck.servers)
	for {
		ok := ck.servers[i].Call("RaftKV.Get", args, reply)
		if ok && reply.WrongLeader == false && reply.Err == ""{
			ck.lastLeader = i
			return reply.Value
		}
		i = (i+1) %len(ck.servers)
	}
	// You will have to modify this function.
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	fmt.Printf("client PutAppend")
	args := &PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	reply := &PutAppendReply{}
	ok := ck.servers[ck.lastLeader].Call("RaftKV.PutAppend", args, reply)
	if ok && reply.WrongLeader == false && reply.Err == ""{
		return
	}
	i := (ck.lastLeader+1) %len(ck.servers)
	for {
		ok := ck.servers[i].Call("RaftKV.PutAppend", args, reply)
		if ok && reply.WrongLeader == false && reply.Err == ""{
			ck.lastLeader = i
			return
		}
		i = (i+1) %len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
