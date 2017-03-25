package main

import (
	"io"

	as "github.com/aerospike/aerospike-client-go"
	"github.com/coocood/freecache"
)

type handler struct {
	argsCount int
	f         func(io.Writer, *context, [][]byte) error
}

type context struct {
	client                *as.Client
	ns                    string
	set                   string
	readPolicy            *as.BasePolicy
	writePolicy           *as.WritePolicy
	backwardWriteCompat   bool
	counterWbOk           int32
	counterOk             int32
	counterErr            int32
	gaugeConn             int32
	expandedMapDefaultTTL int
	expandedMapCache      *freecache.Cache
	expandedMapCacheTTL   int
}
