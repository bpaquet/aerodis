package main

import (
	as "github.com/aerospike/aerospike-client-go"
	ase "github.com/aerospike/aerospike-client-go/types"
)

func fillReadPolicy(read_policy *as.BasePolicy) {
	read_policy.ConsistencyLevel = as.CONSISTENCY_ONE
	read_policy.ReplicaPolicy = as.MASTER_PROLES
}

func fillWritePolicy(write_policy *as.WritePolicy) {
	write_policy.CommitLevel = as.COMMIT_MASTER
}

func fillWritePolicyEx(ctx *context, ttl int, createOnly bool) *as.WritePolicy {
	policy := as.NewWritePolicy(0, 0)
	if ttl != -1 {
		policy = as.NewWritePolicy(0, uint32(ttl))
	}
	fillWritePolicy(policy)
	if createOnly {
		policy.RecordExistsAction = as.CREATE_ONLY
	}
	return policy
}

func buildKey(ctx *context, key []byte) (*as.Key, error) {
	return as.NewKey(ctx.ns, ctx.set, string(key))
}

func errResultCode(err error) ase.ResultCode {
	switch err.(type) {
	case ase.AerospikeError:
		return err.(ase.AerospikeError).ResultCode()
	}
	return -15000
}
