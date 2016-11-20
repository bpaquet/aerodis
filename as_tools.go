package main

import (
  as "github.com/aerospike/aerospike-client-go"
)

func FillReadPolicy(read_policy * as.BasePolicy) {
  read_policy.ConsistencyLevel = as.CONSISTENCY_ONE
  read_policy.ReplicaPolicy = as.MASTER_PROLES
}

func FillWritePolicy(write_policy * as.WritePolicy) {
  write_policy.CommitLevel = as.COMMIT_MASTER
}

func FillWritePolicyEx(ctx *context, ttl int, create_only bool) * as.WritePolicy {
  policy := as.NewWritePolicy(0, 0)
  if ttl != -1 {
    policy = as.NewWritePolicy(0, uint32(ttl))
  }
  FillWritePolicy(policy)
  if create_only {
    policy.RecordExistsAction = as.CREATE_ONLY
  }
  return policy
}

func BuildKey(ctx *context, key []byte) (*as.Key, error) {
  return as.NewKey(ctx.ns, ctx.set, string(key))
}
