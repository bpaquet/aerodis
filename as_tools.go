package main

import (
	as "github.com/aerospike/aerospike-client-go"
	ase "github.com/aerospike/aerospike-client-go/types"
)

func createReadPolicy() *as.BasePolicy {
	policy := as.NewPolicy()
	policy.ConsistencyLevel = as.CONSISTENCY_ONE
	policy.ReplicaPolicy = as.MASTER_PROLES
	return policy
}

func fillWritePolicy(writePolicy *as.WritePolicy) {
	writePolicy.CommitLevel = as.COMMIT_MASTER
}

func createWritePolicyGeneration(generation uint32, ttl int) *as.WritePolicy {
	policy := as.NewWritePolicy(0, as.TTLDontUpdate)
	if ttl != -1 {
		policy = as.NewWritePolicy(0, uint32(ttl))
	}
	fillWritePolicy(policy)
	if generation > 0 {
		policy.GenerationPolicy = as.EXPECT_GEN_EQUAL
		policy.Generation = generation
	}
	return policy
}

func createWritePolicyEx(ttl int, createOnly bool) *as.WritePolicy {
	policy := as.NewWritePolicy(0, as.TTLDontUpdate)
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
