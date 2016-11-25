package main

import (
  "math/rand"
  "time"
  "strconv"

  as "github.com/aerospike/aerospike-client-go"
  ase "github.com/aerospike/aerospike-client-go/types"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

const MAIN_SUFFIX = "____MAIN____"
const ROOT_BIN_NAME = "z"
const VALUE_BIN_NAME = "v"
const MAIN_KEY_BIN_NAME = "m"
const SECOND_KEY_BIN_NAME = "s"

func RandStringBytes(n int) string {
  b := make([]byte, n)
  for i := range b {
    b[i] = letterBytes[rand.Intn(len(letterBytes))]
  }
  return string(b)
}

func Now() int64 {
  return time.Now().UnixNano()
}

func format_composite_key(ctx *context, key string, field string) (*as.Key, error) {
  return as.NewKey(ctx.ns, ctx.set, "composite_" + key + "_" + field)
}

func composite_exists(ctx *context, k string) (*string, error) {
  if ctx.expanded_map_cache != nil {
    v, err := ctx.expanded_map_cache.Get([]byte(k))
    if err == nil {
      s := string(v)
      return &s, nil
    }
  }
  key, err := format_composite_key(ctx, k, MAIN_SUFFIX)
  if err != nil {
    return nil, err
  }
  rec, err := ctx.client.Get(ctx.read_policy, key, ROOT_BIN_NAME)
  if err != nil {
    return nil, err
  }
  if rec != nil && rec.Bins[ROOT_BIN_NAME] != nil {
    s := rec.Bins[ROOT_BIN_NAME].(string)
    if ctx.expanded_map_cache != nil {
      ctx.expanded_map_cache.Set([]byte(k), []byte(s), ctx.expanded_map_cache_ttl)
    }
    return &s, nil
  }
  return nil, nil
}

func composite_exists_or_create(ctx *context, k string, ttl int) (*string, bool, error) {
  return _composite_exists_or_create(ctx, k, ttl, true)
}

func _composite_exists_or_create(ctx *context, k string, ttl int, can_retry bool) (*string, bool, error) {
  suffixed_key, err := composite_exists(ctx, k)
  if err != nil {
    return nil, false, err
  }
  if suffixed_key != nil {
    if ttl == -1 {
      return suffixed_key, false, nil
    }
    key, err := format_composite_key(ctx, k, MAIN_SUFFIX)
    if err != nil {
      return nil, false, err
    }
    err = ctx.client.Touch(FillWritePolicyEx(ctx, ttl, false), key)
    if err != nil {
      return nil, false, err
    }
    return suffixed_key, false, nil
  }
  kk := k + "_" + RandStringBytes(8)
  key, err := format_composite_key(ctx, k, MAIN_SUFFIX)
  if err != nil {
    return nil, false, err
  }
  rec := as.BinMap {
    ROOT_BIN_NAME: kk,
    "created_at": Now(),
  }
  err = ctx.client.Put(FillWritePolicyEx(ctx, ttl, true), key , rec)
  if err != nil {
    if ResultCode(err) == ase.KEY_EXISTS_ERROR && can_retry {
      return _composite_exists_or_create(ctx, k, ttl, false)
    }
    return nil, false, err
  }
  if ctx.expanded_map_cache != nil {
    ctx.expanded_map_cache.Set([]byte(k), []byte(kk), ctx.expanded_map_cache_ttl)
  }
  return &kk, true, nil
}

func cmd_em_HGET(wf write_func, ctx *context, args [][]byte) (error) {
  suffixed_key, err := composite_exists(ctx, string(args[0]))
  if err != nil {
    return err
  }
  if suffixed_key == nil {
    return WriteLine(wf, "$-1")
  }

  key, err := format_composite_key(ctx, *suffixed_key, string(args[1]))
  if err != nil {
    return err
  }
  rec, err := ctx.client.Get(ctx.read_policy, key, VALUE_BIN_NAME)
  if err != nil {
    return err
  }
  return WriteBin(wf, rec, VALUE_BIN_NAME, "$-1")
}

func cmd_em_HSET(wf write_func, ctx *context, args [][]byte) (error) {
  suffixed_key, _, err := composite_exists_or_create(ctx, string(args[0]), -1)
  if err != nil {
    return err
  }
  key, err := format_composite_key(ctx, *suffixed_key, string(args[1]))
  if err != nil {
    return err
  }
  exists, err := ctx.client.Exists(ctx.read_policy, key)
  if err != nil {
    return err
  }
  rec := as.BinMap {
    MAIN_KEY_BIN_NAME: *suffixed_key,
    SECOND_KEY_BIN_NAME: string(args[1]),
    VALUE_BIN_NAME: Encode(ctx, args[2]),
    "created_at": Now(),
  }
  err = ctx.client.Put(ctx.write_policy, key, rec)
  if err != nil {
    return err
  }
  if exists {
    return WriteLine(wf, ":0")
  } else {
    return WriteLine(wf, ":1")
  }
}

func cmd_em_HDEL(wf write_func, ctx *context, args [][]byte) (error) {
  suffixed_key, err := composite_exists(ctx, string(args[0]))
  if err != nil {
    return err
  }
  if suffixed_key == nil {
    return WriteLine(wf, ":0")
  }
  key, err := format_composite_key(ctx, *suffixed_key, string(args[1]))
  if err != nil {
    return err
  }
  existed, err := ctx.client.Delete(ctx.write_policy, key)
  if err != nil  {
    return err
  }
  if existed {
    return WriteLine(wf, ":1")
  }
  return WriteLine(wf, ":0")
}

func cmd_em_EXPIRE(wf write_func, ctx *context, args [][]byte) (error) {
  ttl, err := strconv.Atoi(string(args[1]))
  if err != nil {
    return err
  }
  key, err := format_composite_key(ctx, string(args[0]), MAIN_SUFFIX)
  if err != nil {
    return err
  }
  err = ctx.client.Touch(FillWritePolicyEx(ctx, ttl, false), key)
  if err == nil {
    return WriteLine(wf, ":1")
  }
  if ResultCode(err) != ase.KEY_NOT_FOUND_ERROR {
    return err
  }
  return cmd_EXPIRE(wf, ctx, args)
}

func cmd_em_TTL(wf write_func, ctx *context, args [][]byte) (error) {
  key, err := format_composite_key(ctx, string(args[0]), MAIN_SUFFIX)
  if err != nil {
    return err
  }
  rec, err := ctx.client.GetHeader(ctx.read_policy, key)
  if err != nil {
    return err
  }
  if rec != nil {
   return WriteLine(wf, ":" + strconv.FormatUint(uint64(rec.Expiration), 10))
  }
  return cmd_TTL(wf, ctx, args)
}

func cmd_em_DEL(wf write_func, ctx *context, args [][]byte) (error) {
  key, err := format_composite_key(ctx, string(args[0]), MAIN_SUFFIX)
  if err != nil {
    return err
  }
  existed, err := ctx.client.Delete(ctx.write_policy, key)
  if err != nil  {
    return err
  }
  if existed {
    if ctx.expanded_map_cache != nil {
      ctx.expanded_map_cache.Del(args[0])
    }
    return WriteLine(wf, ":1")
  }
  return cmd_DEL(wf, ctx, args)
}

func cmd_em_HMSET(wf write_func, ctx *context, args [][]byte) (error) {
  suffixed_key, _, err := composite_exists_or_create(ctx, string(args[0]), -1)
  if err != nil {
    return err
  }
  for i := 1; i < len(args); i += 2 {
    key, err := format_composite_key(ctx, *suffixed_key, string(args[i]))
    if err != nil {
      return err
    }
    rec := as.BinMap {
      MAIN_KEY_BIN_NAME: *suffixed_key,
      SECOND_KEY_BIN_NAME: string(args[i]),
      VALUE_BIN_NAME: Encode(ctx, args[i + 1]),
      "created_at": Now(),
    }
    err = ctx.client.Put(FillWritePolicyEx(ctx, ctx.expanded_map_default_ttl, false), key, rec)
    if err != nil {
      return err
    }
  }
  return WriteLine(wf, "+OK")
}

func cmd_em_HMGET(wf write_func, ctx *context, args [][]byte) (error) {
  suffixed_key, err := composite_exists(ctx, string(args[0]))
  if err != nil {
    return err
  }
  res := make([]*as.Record, len(args) - 1)
  if suffixed_key != nil {
    for i := 0; i < len(args) - 1; i ++ {
      key, err := format_composite_key(ctx, *suffixed_key, string(args[i + 1]))
      if err != nil {
        return err
      }
      rec, err := ctx.client.Get(ctx.read_policy, key, VALUE_BIN_NAME)
      if err != nil {
        return err
      }
      res[i] = rec
    }
  }
  return WriteArrayBin(wf, res, VALUE_BIN_NAME, "")
}

func cmd_em_HGETALL(wf write_func, ctx *context, args [][]byte) (error) {
  suffixed_key, err := composite_exists(ctx, string(args[0]))
  if err != nil {
    return err
  }
  if suffixed_key == nil {
    return WriteArray(wf, make([]interface{}, 0))
  }
  statment := as.NewStatement(ctx.ns, ctx.set)
  statment.Addfilter(as.NewEqualFilter(MAIN_KEY_BIN_NAME, *suffixed_key))
  recordset, err := ctx.client.Query(nil, statment)
  if err != nil {
    return err
  }
  out := make([]*as.Record, 0)
  for res := range recordset.Results() {
    if res.Err != nil {
      return res.Err
    }
    out = append(out, res.Record)
  }
  return WriteArrayBin(wf, out, VALUE_BIN_NAME, SECOND_KEY_BIN_NAME)
}

func composite_incr(wf write_func, ctx *context, suffixed_key *string, field string, value int) (error) {
  key, err := format_composite_key(ctx, *suffixed_key, field)
  if err != nil {
    return err
  }
  rec, err := ctx.client.Operate(FillWritePolicyEx(ctx, ctx.expanded_map_default_ttl, false), key, as.PutOp(as.NewBin(MAIN_KEY_BIN_NAME, *suffixed_key)), as.PutOp(as.NewBin(SECOND_KEY_BIN_NAME, field)), as.AddOp(as.NewBin(VALUE_BIN_NAME, value)), as.GetOpForBin(VALUE_BIN_NAME))
  if err != nil {
    if ResultCode(err) == ase.BIN_TYPE_ERROR {
      return WriteLine(wf, "$-1")
    }
    return err
  }
  return WriteBinInt(wf, rec, VALUE_BIN_NAME)
}

func cmd_em_HINCRBYEX(wf write_func, ctx *context, args [][]byte) (error) {
  incr, err := strconv.Atoi(string(args[2]))
  if err != nil {
    return err
  }
  ttl, err := strconv.Atoi(string(args[3]))
  if err != nil {
    return err
  }
  suffixed_key, _, err := composite_exists_or_create(ctx, string(args[0]), ttl)
  if err != nil {
    return err
  }
  return composite_incr(wf, ctx, suffixed_key, string(args[1]), incr)
}

func cmd_em_HINCRBY(wf write_func, ctx *context, args [][]byte) (error) {
  incr, err := strconv.Atoi(string(args[2]))
  if err != nil {
    return err
  }
  suffixed_key, _, err := composite_exists_or_create(ctx, string(args[0]), -1)
  if err != nil {
    return err
  }
  return composite_incr(wf, ctx, suffixed_key, string(args[1]), incr)
}

func cmd_em_HMINCRBYEX(wf write_func, ctx *context, args [][]byte) (error) {
  ttl, err := strconv.Atoi(string(args[1]))
  if err != nil {
    return err
  }
  suffixed_key, _, err := composite_exists_or_create(ctx, string(args[0]), ttl)
  if err != nil {
    return err
  }
  if len(args) > 2 {
    a := args[2:]
    for i := 0; i < len(a); i += 2 {
      incr, err := strconv.Atoi(string(a[i + 1]))
      if err != nil {
        return err
      }
      key, err := format_composite_key(ctx, *suffixed_key, string(a[i]))
      if err != nil {
        return err
      }
      _, err = ctx.client.Operate(FillWritePolicyEx(ctx, ctx.expanded_map_default_ttl, false), key, as.PutOp(as.NewBin(MAIN_KEY_BIN_NAME, *suffixed_key)), as.PutOp(as.NewBin(SECOND_KEY_BIN_NAME, string(a[i]))), as.AddOp(as.NewBin(VALUE_BIN_NAME, incr)))
      if err != nil {
        return err
      }
    }
  }
  return WriteLine(wf, "+OK")
}