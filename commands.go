package main

import (
  "strconv"
  "strings"
  "encoding/base64"

  as "github.com/aerospike/aerospike-client-go"
)

func cmd_DEL(wf write_func, ctx *context, args [][]byte) (error) {
  key, err := BuildKey(ctx, args[0])
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

func get(wf write_func, ctx *context, k []byte, bin_name string) (error) {
  key, err := BuildKey(ctx, k)
  if err != nil {
    return err
  }
  rec, err := ctx.client.Get(ctx.read_policy, key, bin_name)
  if err != nil  {
    return err
  }
  return WriteBin(wf, rec, bin_name, "$-1")
}

func cmd_GET(wf write_func, ctx *context, args [][]byte) (error) {
  return get(wf, ctx, args[0], BIN_NAME)
}

func cmd_HGET(wf write_func, ctx *context, args [][]byte) (error) {
  return get(wf, ctx, args[0], string(args[1]))
}

func setex(wf write_func, ctx *context, k []byte, bin_name string, content []byte, ttl int, create_only bool) (error) {
  key, err := BuildKey(ctx, k)
  if err != nil {
    return err
  }
  rec := as.BinMap {
    bin_name: Encode(ctx, content),
  }
  err = ctx.client.Put(FillWritePolicyEx(ctx, ttl, create_only), key, rec)
  if err != nil  {
    if create_only && err.Error() == "Key already exists" {
      return WriteLine(wf, ":0")
    }
    return err
  }
  if create_only {
    return WriteLine(wf, ":1")
  }
  return WriteLine(wf, "+OK")
}

func cmd_SET(wf write_func, ctx *context, args [][]byte) (error) {
  return setex(wf, ctx, args[0], BIN_NAME, args[1], -1, false)
}

func cmd_SETEX(wf write_func, ctx *context, args [][]byte) (error) {
  ttl, err := strconv.Atoi(string(args[1]))
  if err != nil {
    return err
  }

  return setex(wf, ctx, args[0], BIN_NAME, args[2], ttl, false)
}

func cmd_SETNX(wf write_func, ctx *context, args [][]byte) (error) {
  return setex(wf, ctx, args[0], BIN_NAME, args[1], -1, true)
}

func cmd_SETNXEX(wf write_func, ctx *context, args [][]byte) (error) {
  ttl, err := strconv.Atoi(string(args[1]))
  if err != nil {
    return err
  }

  return setex(wf, ctx, args[0], BIN_NAME, args[2], ttl, true)
}

func cmd_HSET(wf write_func, ctx *context, args [][]byte) (error) {
  key, err := BuildKey(ctx, args[0])
  if err != nil {
    return err
  }
  rec, err := ctx.client.Execute(ctx.write_policy, key, module_name, "HSET", as.NewValue(string(args[1])), as.NewValue(Encode(ctx, args[2])))
  if err != nil  {
    return err
  }
  return WriteLine(wf, ":" + strconv.Itoa(rec.(int)))
}

func cmd_HDEL(wf write_func, ctx *context, args [][]byte) (error) {
  key, err := BuildKey(ctx, args[0])
  if err != nil {
    return err
  }
  rec, err := ctx.client.Execute(ctx.write_policy, key, module_name, "HDEL", as.NewValue(string(args[1])))
  if err != nil  {
    return err
  }
  return WriteLine(wf, ":" + strconv.Itoa(rec.(int)))
}

func array_push(wf write_func, ctx *context, args [][]byte, f string, ttl int) (error) {
  key, err := BuildKey(ctx, args[0])
  if err != nil {
    return err
  }
  rec, err := ctx.client.Execute(ctx.write_policy, key, module_name, f, as.NewValue(BIN_NAME), as.NewValue(Encode(ctx, args[1])), as.NewValue(ttl))
  if err != nil  {
    return err
  }
  return WriteLine(wf, ":" + strconv.Itoa(rec.(int)))
}

func cmd_RPUSH(wf write_func, ctx *context, args [][]byte) (error) {
  return array_push(wf, ctx, args, "RPUSH", -1)
}

func cmd_RPUSHEX(wf write_func, ctx *context, args [][]byte) (error) {
  ttl, err := strconv.Atoi(string(args[2]))
  if err != nil {
    return err
  }

  return array_push(wf, ctx, args, "RPUSH", ttl)
}

func cmd_LPUSH(wf write_func, ctx *context, args [][]byte) (error) {
  return array_push(wf, ctx, args, "LPUSH", -1)
}

func cmd_LPUSHEX(wf write_func, ctx *context, args [][]byte) (error) {
  ttl, err := strconv.Atoi(string(args[2]))
  if err != nil {
    return err
  }

  return array_push(wf, ctx, args, "LPUSH", ttl)
}

func array_pop(wf write_func, ctx *context, args [][]byte, f string) (error) {
  key, err := BuildKey(ctx, args[0])
  if err != nil {
    return err
  }
  rec, err := ctx.client.Execute(ctx.write_policy, key, module_name, f, as.NewValue(BIN_NAME), as.NewValue(1), as.NewValue(-1))
  if err != nil  {
    return err
  }
  if rec == nil {
    return WriteLine(wf, "$-1")
  }
  a := rec.([]interface{})
  if len(a) == 0 {
    return WriteLine(wf, "$-1")
  }
  x := rec.([]interface{})[0]
  // backward compat
  switch x.(type) {
  case int:
    return WriteByteArray(wf, []byte(strconv.Itoa(x.(int))))
  case string:
    s := x.(string)
    if strings.HasPrefix(s, "__64__") {
      bytes, err := base64.StdEncoding.DecodeString(s[6:])
      if err != nil {
        return err
      }
      return WriteByteArray(wf, bytes)
    }
    return WriteByteArray(wf, []byte(s))
  // end of backward compat
  default:
    return WriteByteArray(wf, x.([]byte))
  }
}

func cmd_RPOP(wf write_func, ctx *context, args [][]byte) (error) {
  return array_pop(wf, ctx, args, "RPOP")
}

func cmd_LPOP(wf write_func, ctx *context, args [][]byte) (error) {
  return array_pop(wf, ctx, args, "LPOP")
}

func cmd_LLEN(wf write_func, ctx *context, args [][]byte) (error) {
  key, err := BuildKey(ctx, args[0])
  if err != nil {
    return err
  }
  rec, err := ctx.client.Get(ctx.read_policy, key, BIN_NAME + "_size")
  if err != nil  {
    return err
  }
  return WriteBinInt(wf, rec, BIN_NAME + "_size")
}

func cmd_LRANGE(wf write_func, ctx *context, args [][]byte) (error) {
  key, err := BuildKey(ctx, args[0])
  if err != nil {
    return err
  }
  start, err := strconv.Atoi(string(args[1]))
  if err != nil {
    return err
  }
  stop, err := strconv.Atoi(string(args[2]))
  if err != nil {
    return err
  }
  rec, err := ctx.client.Execute(ctx.write_policy, key, module_name, "LRANGE", as.NewValue(BIN_NAME), as.NewValue(start), as.NewValue(stop))
  if err != nil  {
    return err
  }
  if rec == nil {
    return WriteLine(wf, "$-1")
  }
  return WriteArray(wf, rec.([]interface{}))
}

func cmd_LTRIM(wf write_func, ctx *context, args [][]byte) (error) {
  key, err := BuildKey(ctx, args[0])
  if err != nil {
    return err
  }
  start, err := strconv.Atoi(string(args[1]))
  if err != nil {
    return err
  }
  stop, err := strconv.Atoi(string(args[2]))
  if err != nil {
    return err
  }
  rec, err := ctx.client.Execute(ctx.write_policy, key, module_name, "LTRIM", as.NewValue(BIN_NAME), as.NewValue(start), as.NewValue(stop))
  if err != nil  {
    return err
  }
  if rec == nil {
    return WriteLine(wf, "$-1")
  }
  return WriteLine(wf, "+OK")
}

func hIncrByEx(wf write_func, ctx *context, k []byte, field string, incr int, ttl int) (error) {
  key, err := BuildKey(ctx, k)
  if err != nil {
    return err
  }
  bin := as.NewBin(field, incr)
  rec, err := ctx.client.Operate(FillWritePolicyEx(ctx, ttl, false), key, as.AddOp(bin), as.GetOpForBin(field))
  if err != nil  {
    if err.Error() == "Bin type error" {
      return WriteLine(wf, "$-1")
    }
    return err
  }
  return WriteBinInt(wf, rec, field)
}

func cmd_INCR(wf write_func, ctx *context, args [][]byte) (error) {
  return hIncrByEx(wf, ctx, args[0], BIN_NAME, 1, -1)
}

func cmd_DECR(wf write_func, ctx *context, args [][]byte) (error) {
  return hIncrByEx(wf, ctx, args[0], BIN_NAME, -1, -1)
}

func cmd_INCRBY(wf write_func, ctx *context, args [][]byte) (error) {
  incr, err := strconv.Atoi(string(args[1]))
  if err != nil {
    return err
  }
  return hIncrByEx(wf, ctx, args[0], BIN_NAME, incr, -1)
}

func cmd_HINCRBY(wf write_func, ctx *context, args [][]byte) (error) {
  incr, err := strconv.Atoi(string(args[2]))
  if err != nil {
    return err
  }
  return hIncrByEx(wf, ctx, args[0], string(args[1]), incr, -1)
}

func cmd_HINCRBYEX(wf write_func, ctx *context, args [][]byte) (error) {
  incr, err := strconv.Atoi(string(args[2]))
  if err != nil {
    return err
  }
  ttl, err := strconv.Atoi(string(args[3]))
  if err != nil {
    return err
  }
  return hIncrByEx(wf, ctx, args[0], string(args[1]), incr, ttl)
}

func cmd_DECRBY(wf write_func, ctx *context, args [][]byte) (error) {
  decr, err := strconv.Atoi(string(args[1]))
  if err != nil {
    return err
  }
  return hIncrByEx(wf, ctx, args[0], BIN_NAME, -decr, -1)
}

func cmd_HMGET(wf write_func, ctx *context, args [][]byte) (error) {
  key, err := BuildKey(ctx, args[0])
  if err != nil {
    return err
  }
  a := make([]string, len(args) - 1)
  for i, e := range args[1:] {
    a[i] = string(e)
  }
  rec, err := ctx.client.Get(ctx.read_policy, key, a...)
  if err != nil {
    return err
  }
  err = WriteLine(wf, "*" + strconv.Itoa(len(a)))
  if err != nil {
    return err
  }
  for _, e := range a {
    err = WriteBin(wf, rec, e, "$-1")
    if err != nil {
      return err
    }
  }
  return nil
}

func cmd_HMSET(wf write_func, ctx *context, args [][]byte) (error) {
  key, err := BuildKey(ctx, args[0])
  if err != nil {
    return err
  }
  m := make(map[string]interface{})
  for i := 1; i < len(args); i += 2 {
    m[string(args[i])] = Encode(ctx, args[i + 1])
  }
  rec, err := ctx.client.Execute(ctx.write_policy, key, module_name, "HMSET", as.NewValue(m))
  if err != nil {
    return err
  }
  return WriteLine(wf, "+" + rec.(string))
}


func cmd_HGETALL(wf write_func, ctx *context, args [][]byte) (error) {
  key, err := BuildKey(ctx, args[0])
  if err != nil {
    return err
  }
  rec, err := ctx.client.Execute(ctx.write_policy, key, module_name, "HGETALL")
  if err != nil {
    return err
  }
  a := rec.([]interface{})
  err = WriteLine(wf, "*" + strconv.Itoa(len(a)))
  if err != nil {
    return err
  }
  for i := 0; i < len(a); i += 2 {
    err = WriteByteArray(wf, []byte(a[i].(string)))
    if err != nil {
      return err
    }
    err = WriteValue(wf, a[i + 1])
    if err != nil {
      return err
    }
  }
  return nil
}

func cmd_EXPIRE(wf write_func, ctx *context, args [][]byte) (error) {
  key, err := BuildKey(ctx, args[0])
  if err != nil {
    return err
  }

  ttl, err := strconv.Atoi(string(args[1]))
  if err != nil {
    return err
  }

  err = ctx.client.Touch(FillWritePolicyEx(ctx, ttl, false), key)
  if err != nil {
    if err.Error() == "Key not found" {
      return WriteLine(wf, ":0")
    }
    return err
  }
  return WriteLine(wf, ":1")
}

func cmd_TTL(wf write_func, ctx *context, args [][]byte) (error) {
  key, err := BuildKey(ctx, args[0])
  if err != nil {
    return err
  }

  rec, err := ctx.client.GetHeader(ctx.read_policy, key)
  if err != nil {
    return err
  }
  if rec == nil {
    return WriteLine(wf, ":-2")
  }
  return WriteLine(wf, ":" + strconv.FormatUint(uint64(rec.Expiration), 10))
}

func cmd_FLUSHDB(wf write_func, ctx *context, args [][]byte) (error) {
  recordset, err := ctx.client.ScanAll(nil, ctx.ns, ctx.set)
  if err != nil {
    return err
  }

  err = nil
  for res := range recordset.Results() {
    if res.Err != nil {
      err = res.Err
      break
    }
    _, err = ctx.client.Delete(ctx.write_policy, res.Record.Key)
    if err != nil {
      break
    }
  }
  if err != nil {
    return err
  }

  return WriteLine(wf, "+OK")
}

func cmd_HMINCRBYEX(wf write_func, ctx *context, args [][]byte) (error) {
  key, err := BuildKey(ctx, args[0])
  if err != nil {
    return err
  }
  ttl, err := strconv.Atoi(string(args[1]))
  if err != nil {
    return err
  }
  if len(args) == 2 {
    err := ctx.client.Touch(FillWritePolicyEx(ctx, ttl, false), key)
    if err != nil {
      if err.Error() != "Key not found" {
        return err
      }
    }
    return WriteLine(wf, "+OK")
  }
  ops := make([]*as.Operation, 0)
  a := args[2:]
  for i := 0; i < len(a); i += 2 {
    incr, err := strconv.Atoi(string(a[i + 1]))
    if err != nil {
      return err
    }
    ops = append(ops, as.AddOp(as.NewBin(string(a[i]), incr)))
  }
  _, err = ctx.client.Operate(FillWritePolicyEx(ctx, ttl, false), key, ops...)
  if err != nil  {
    return err
  }
  return WriteLine(wf, "+OK")
}