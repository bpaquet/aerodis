package main

import (
	as "github.com/aerospike/aerospike-client-go"
	"github.com/coocood/freecache"
)

type write_func func([]byte) error

type handler struct {
	args_count int
	f          func(write_func, *context, [][]byte) error
}

type context struct {
	client                   *as.Client
	ns                       string
	set                      string
	read_policy              *as.BasePolicy
	write_policy             *as.WritePolicy
	backward_write_compat    bool
	counter_ok               uint32
	counter_err              uint32
	gauge_conn               int32
	expanded_map_default_ttl int
	expanded_map_cache       *freecache.Cache
	expanded_map_cache_ttl   int
}
