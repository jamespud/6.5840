package shardctrler

import (
	"log"
	"sort"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

//
// Shard controller: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func DefaultConfig() Config {
	return Config{
		Groups: make(map[int][]string),
	}
}

const (
	TIMEOUT    = 150 * time.Millisecond
	SLEEP_TIME = 50 * time.Millisecond
)

const (
	OK             Err = "OK"
	ErrNoKey       Err = "ErrNoKey"
	ErrWrongLeader Err = "ErrWrongLeader"
	ErrTimeout     Err = "ErrTimeout"
	ErrUnknownOp   Err = "ErrUnknownOp"
)

type Err string

type OpType int

const (
	JoinOp OpType = iota
	LeaveOp
	MoveOp
	QueryOp
	NopOp
)

type JoinArgs struct {
	Servers   map[int][]string // new GID -> servers mappings
	ClientId  int64
	SerialNum int64
}

type JoinReply struct {
	Err Err
}

type LeaveArgs struct {
	GIDs      []int
	ClientId  int64
	SerialNum int64
}

type LeaveReply struct {
	Err Err
}

type MoveArgs struct {
	Shard     int
	GID       int
	ClientId  int64
	SerialNum int64
}

type MoveReply struct {
	Err Err
}

type QueryArgs struct {
	Num       int // desired config number
	ClientId  int64
	SerialNum int64
}

type QueryReply struct {
	Err    Err
	Config Config
}

type Command struct {
	*Op
}

type Op struct {
	OpType    OpType
	Servers   map[int][]string
	GIDs      []int
	Shard     int
	GID       int
	Num       int
	ClientId  int64
	SerialNum int64
}

type OpReply struct {
	Config Config
	Err    Err
}

type OpContext struct {
	SerialNum   int64
	LastOpReply *OpReply
}

type ConfigStateMachine interface {
	Join(groups map[int][]string) Err
	Leave(gids []int) Err
	Move(shard, gid int) Err
	Query(num int) (Config, Err)
}

type MemoryConfig struct {
	Configs []Config
}

func newMemoryConfig() *MemoryConfig {
	cf := &MemoryConfig{make([]Config, 1)}
	cf.Configs[0] = DefaultConfig()
	return cf
}

func (cf *MemoryConfig) Join(groups map[int][]string) Err {
	lastConfig := cf.Configs[len(cf.Configs)-1]
	newConfig := Config{
		len(cf.Configs),
		lastConfig.Shards,
		deepCopy(lastConfig.Groups),
	}
	for gid, servers := range groups {
		if _, ok := newConfig.Groups[gid]; !ok {
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			newConfig.Groups[gid] = newServers
		}
	}
	group2Shards := Group2Shards(newConfig)
	for {
		source, target := GetGIDWithMaximumShards(group2Shards), GetGIDWithMinimumShards(group2Shards)
		if source != 0 && len(group2Shards[source])-len(group2Shards[target]) <= 1 {
			break
		}
		group2Shards[target] = append(group2Shards[target], group2Shards[source][0])
		group2Shards[source] = group2Shards[source][1:]
	}
	var newShards [NShards]int
	for gid, shards := range group2Shards {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}
	newConfig.Shards = newShards
	cf.Configs = append(cf.Configs, newConfig)
	return OK
}

func (cf *MemoryConfig) Leave(gids []int) Err {
	lastConifg := cf.Configs[len(cf.Configs)-1]
	newConfig := Config{
		len(cf.Configs),
		lastConifg.Shards,
		deepCopy(lastConifg.Groups),
	}
	group2Shards := Group2Shards(newConfig)
	orphanShards := make([]int, 0)
	for _, gid := range gids {
		if _, ok := newConfig.Groups[gid]; ok {
			delete(newConfig.Groups, gid)
		}
		if shards, ok := group2Shards[gid]; ok {
			delete(group2Shards, gid)
			orphanShards = append(orphanShards, shards...)
		}
	}

	var newShards [NShards]int
	if len(newConfig.Groups) > 0 {
		for _, shard := range orphanShards {
			gid := GetGIDWithMinimumShards(group2Shards)
			newShards[shard] = gid
			group2Shards[gid] = append(group2Shards[gid], shard)
		}

		for gid, shards := range group2Shards {
			for _, shard := range shards {
				newShards[shard] = gid
			}
		}
	}
	newConfig.Shards = newShards
	cf.Configs = append(cf.Configs, newConfig)
	return OK
}

func (cf *MemoryConfig) Move(shard, gid int) Err {
	lastConfig := cf.Configs[len(cf.Configs)-1]
	newConfig := Config{
		len(cf.Configs),
		lastConfig.Shards,
		deepCopy(lastConfig.Groups),
	}
	newConfig.Shards[shard] = gid
	cf.Configs = append(cf.Configs, newConfig)
	return OK
}

func (cf *MemoryConfig) Query(num int) (Config, Err) {
	if num < 0 || num >= len(cf.Configs) {
		return cf.Configs[len(cf.Configs)-1], OK
	}
	return cf.Configs[num], OK
}

func Group2Shards(config Config) map[int][]int {
	group2Shards := make(map[int][]int)
	for gid := range config.Groups {
		group2Shards[gid] = make([]int, 0)
	}
	for shard, gid := range config.Shards {
		group2Shards[gid] = append(group2Shards[gid], shard)
	}
	return group2Shards
}

func GetGIDWithMinimumShards(group2Shards map[int][]int) int {
	var gids []int
	for gid := range group2Shards {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	index, minShards := -1, NShards+1
	for _, gid := range gids {
		if gid != 0 && len(group2Shards[gid]) < minShards {
			index, minShards = gid, len(group2Shards[gid])
		}
	}
	return index
}

func GetGIDWithMaximumShards(group2Shards map[int][]int) int {
	if shards, ok := group2Shards[0]; ok && len(shards) != 0 {
		return 0
	}

	var gids []int
	for gid := range group2Shards {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	index, maxShards := -1, -1
	for _, gid := range gids {
		if len(group2Shards[gid]) > maxShards {
			index, maxShards = gid, len(group2Shards[gid])
		}
	}
	return index
}

func deepCopy(groups map[int][]string) map[int][]string {
	newGroups := make(map[int][]string)
	for gid, servers := range groups {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		newGroups[gid] = newServers
	}
	return newGroups
}
