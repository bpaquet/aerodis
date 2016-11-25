package main

import (
	as "github.com/aerospike/aerospike-client-go"
	"github.com/coocood/freecache"
)

type writeFunc func([]byte) error

type handler struct {
	args_count int
	f          func(writeFunc, *context, [][]byte) error
}

type context struct {
	client                *as.Client
	ns                    string
	set                   string
	readPolicy            *as.BasePolicy
	writePolicy           *as.WritePolicy
	backwardWriteCompat   bool
	counterOk             uint32
	counterErr            uint32
	gaugeConn             int32
	expandedMapDefaultTTL int
	expandedMapCache      *freecache.Cache
	expandedMapCacheTTL   int
}
