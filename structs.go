package main

import (
	"io"

	as "github.com/aerospike/aerospike-client-go"
	"github.com/coocood/freecache"
)

type handler struct {
	argsCount    int
	argsLogCount int
	f            func(io.Writer, *context, [][]byte) error
	writeBack    bool
}

type context struct {
	client                *as.Client
	exitOnClusterLost			bool
	ns                    string
	set                   string
	readPolicy            *as.BasePolicy
	writePolicy           *as.WritePolicy
	backwardWriteCompat   bool
	counterWbOk           uint32
	counterOk             uint32
	counterErr            uint32
	gaugeConn             int32
	expandedMapDefaultTTL int
	expandedMapCache      *freecache.Cache
	expandedMapCacheTTL   int
	logCommands					  bool
}
