package main

import (
	"encoding/base64"
	"strconv"
	"strings"

	as "github.com/aerospike/aerospike-client-go"
	ase "github.com/aerospike/aerospike-client-go/types"
)

func cmd_DEL(wf writeFunc, ctx *context, args [][]byte) error {
	key, err := buildKey(ctx, args[0])
	if err != nil {
		return err
	}
	existed, err := ctx.client.Delete(ctx.writePolicy, key)
	if err != nil {
		return err
	}
	if existed {
		return writeLine(wf, ":1")
	}
	return writeLine(wf, ":0")
}

func get(wf writeFunc, ctx *context, k []byte, binName string) error {
	key, err := buildKey(ctx, k)
	if err != nil {
		return err
	}
	rec, err := ctx.client.Get(ctx.readPolicy, key, binName)
	if err != nil {
		return err
	}
	return writeBin(wf, rec, binName, "$-1")
}

func cmd_GET(wf writeFunc, ctx *context, args [][]byte) error {
	return get(wf, ctx, args[0], binName)
}

func cmd_HGET(wf writeFunc, ctx *context, args [][]byte) error {
	return get(wf, ctx, args[0], string(args[1]))
}

func setex(wf writeFunc, ctx *context, k []byte, binName string, content []byte, ttl int, createOnly bool) error {
	key, err := buildKey(ctx, k)
	if err != nil {
		return err
	}
	rec := as.BinMap{
		binName: encode(ctx, content),
	}
	err = ctx.client.Put(fillWritePolicyEx(ctx, ttl, createOnly), key, rec)
	if err != nil {
		if createOnly && errResultCode(err) == ase.KEY_EXISTS_ERROR {
			return writeLine(wf, ":0")
		}
		return err
	}
	if createOnly {
		return writeLine(wf, ":1")
	}
	return writeLine(wf, "+OK")
}

func cmd_SET(wf writeFunc, ctx *context, args [][]byte) error {
	return setex(wf, ctx, args[0], binName, args[1], -1, false)
}

func cmd_SETEX(wf writeFunc, ctx *context, args [][]byte) error {
	ttl, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return err
	}

	return setex(wf, ctx, args[0], binName, args[2], ttl, false)
}

func cmd_SETNX(wf writeFunc, ctx *context, args [][]byte) error {
	return setex(wf, ctx, args[0], binName, args[1], -1, true)
}

func cmd_SETNXEX(wf writeFunc, ctx *context, args [][]byte) error {
	ttl, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return err
	}

	return setex(wf, ctx, args[0], binName, args[2], ttl, true)
}

func cmd_HSET(wf writeFunc, ctx *context, args [][]byte) error {
	key, err := buildKey(ctx, args[0])
	if err != nil {
		return err
	}
	rec, err := ctx.client.Execute(ctx.writePolicy, key, MODULE_NAME, "HSET", as.NewValue(string(args[1])), as.NewValue(encode(ctx, args[2])))
	if err != nil {
		return err
	}
	return writeLine(wf, ":"+strconv.Itoa(rec.(int)))
}

func cmd_HDEL(wf writeFunc, ctx *context, args [][]byte) error {
	key, err := buildKey(ctx, args[0])
	if err != nil {
		return err
	}
	rec, err := ctx.client.Execute(ctx.writePolicy, key, MODULE_NAME, "HDEL", as.NewValue(string(args[1])))
	if err != nil {
		return err
	}
	return writeLine(wf, ":"+strconv.Itoa(rec.(int)))
}

func array_push(wf writeFunc, ctx *context, args [][]byte, f string, ttl int) error {
	key, err := buildKey(ctx, args[0])
	if err != nil {
		return err
	}
	rec, err := ctx.client.Execute(ctx.writePolicy, key, MODULE_NAME, f, as.NewValue(binName), as.NewValue(encode(ctx, args[1])), as.NewValue(ttl))
	if err != nil {
		return err
	}
	return writeLine(wf, ":"+strconv.Itoa(rec.(int)))
}

func cmd_RPUSH(wf writeFunc, ctx *context, args [][]byte) error {
	return array_push(wf, ctx, args, "RPUSH", -1)
}

func cmd_RPUSHEX(wf writeFunc, ctx *context, args [][]byte) error {
	ttl, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return err
	}

	return array_push(wf, ctx, args, "RPUSH", ttl)
}

func cmd_LPUSH(wf writeFunc, ctx *context, args [][]byte) error {
	return array_push(wf, ctx, args, "LPUSH", -1)
}

func cmd_LPUSHEX(wf writeFunc, ctx *context, args [][]byte) error {
	ttl, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return err
	}

	return array_push(wf, ctx, args, "LPUSH", ttl)
}

func array_pop(wf writeFunc, ctx *context, args [][]byte, f string) error {
	key, err := buildKey(ctx, args[0])
	if err != nil {
		return err
	}
	rec, err := ctx.client.Execute(ctx.writePolicy, key, MODULE_NAME, f, as.NewValue(binName), as.NewValue(1), as.NewValue(-1))
	if err != nil {
		return err
	}
	if rec == nil {
		return writeLine(wf, "$-1")
	}
	a := rec.([]interface{})
	if len(a) == 0 {
		return writeLine(wf, "$-1")
	}
	x := rec.([]interface{})[0]
	// backward compat
	switch x.(type) {
	case int:
		return writeByteArray(wf, []byte(strconv.Itoa(x.(int))))
	case string:
		s := x.(string)
		if strings.HasPrefix(s, "__64__") {
			bytes, err := base64.StdEncoding.DecodeString(s[6:])
			if err != nil {
				return err
			}
			return writeByteArray(wf, bytes)
		}
		return writeByteArray(wf, []byte(s))
	// end of backward compat
	default:
		return writeByteArray(wf, x.([]byte))
	}
}

func cmd_RPOP(wf writeFunc, ctx *context, args [][]byte) error {
	return array_pop(wf, ctx, args, "RPOP")
}

func cmd_LPOP(wf writeFunc, ctx *context, args [][]byte) error {
	return array_pop(wf, ctx, args, "LPOP")
}

func cmd_LLEN(wf writeFunc, ctx *context, args [][]byte) error {
	key, err := buildKey(ctx, args[0])
	if err != nil {
		return err
	}
	rec, err := ctx.client.Get(ctx.readPolicy, key, binName+"_size")
	if err != nil {
		return err
	}
	return writeBinInt(wf, rec, binName+"_size")
}

func cmd_LRANGE(wf writeFunc, ctx *context, args [][]byte) error {
	key, err := buildKey(ctx, args[0])
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
	rec, err := ctx.client.Execute(ctx.writePolicy, key, MODULE_NAME, "LRANGE", as.NewValue(binName), as.NewValue(start), as.NewValue(stop))
	if err != nil {
		return err
	}
	if rec == nil {
		return writeLine(wf, "$-1")
	}
	return writeArray(wf, rec.([]interface{}))
}

func cmd_LTRIM(wf writeFunc, ctx *context, args [][]byte) error {
	key, err := buildKey(ctx, args[0])
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
	rec, err := ctx.client.Execute(ctx.writePolicy, key, MODULE_NAME, "LTRIM", as.NewValue(binName), as.NewValue(start), as.NewValue(stop))
	if err != nil {
		return err
	}
	if rec == nil {
		return writeLine(wf, "$-1")
	}
	return writeLine(wf, "+OK")
}

func hIncrByEx(wf writeFunc, ctx *context, k []byte, field string, incr int, ttl int) error {
	key, err := buildKey(ctx, k)
	if err != nil {
		return err
	}
	bin := as.NewBin(field, incr)
	rec, err := ctx.client.Operate(fillWritePolicyEx(ctx, ttl, false), key, as.AddOp(bin), as.GetOpForBin(field))
	if err != nil {
		if errResultCode(err) == ase.BIN_TYPE_ERROR {
			return writeLine(wf, "$-1")
		}
		return err
	}
	return writeBinInt(wf, rec, field)
}

func cmd_INCR(wf writeFunc, ctx *context, args [][]byte) error {
	return hIncrByEx(wf, ctx, args[0], binName, 1, -1)
}

func cmd_DECR(wf writeFunc, ctx *context, args [][]byte) error {
	return hIncrByEx(wf, ctx, args[0], binName, -1, -1)
}

func cmd_INCRBY(wf writeFunc, ctx *context, args [][]byte) error {
	incr, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return err
	}
	return hIncrByEx(wf, ctx, args[0], binName, incr, -1)
}

func cmd_HINCRBY(wf writeFunc, ctx *context, args [][]byte) error {
	incr, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return err
	}
	return hIncrByEx(wf, ctx, args[0], string(args[1]), incr, -1)
}

func cmd_HINCRBYEX(wf writeFunc, ctx *context, args [][]byte) error {
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

func cmd_DECRBY(wf writeFunc, ctx *context, args [][]byte) error {
	decr, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return err
	}
	return hIncrByEx(wf, ctx, args[0], binName, -decr, -1)
}

func cmd_HMGET(wf writeFunc, ctx *context, args [][]byte) error {
	key, err := buildKey(ctx, args[0])
	if err != nil {
		return err
	}
	a := make([]string, len(args)-1)
	for i, e := range args[1:] {
		a[i] = string(e)
	}
	rec, err := ctx.client.Get(ctx.readPolicy, key, a...)
	if err != nil {
		return err
	}
	err = writeLine(wf, "*"+strconv.Itoa(len(a)))
	if err != nil {
		return err
	}
	for _, e := range a {
		err = writeBin(wf, rec, e, "$-1")
		if err != nil {
			return err
		}
	}
	return nil
}

func cmd_HMSET(wf writeFunc, ctx *context, args [][]byte) error {
	key, err := buildKey(ctx, args[0])
	if err != nil {
		return err
	}
	m := make(map[string]interface{})
	for i := 1; i < len(args); i += 2 {
		m[string(args[i])] = encode(ctx, args[i+1])
	}
	rec, err := ctx.client.Execute(ctx.writePolicy, key, MODULE_NAME, "HMSET", as.NewValue(m))
	if err != nil {
		return err
	}
	return writeLine(wf, "+"+rec.(string))
}

func cmd_HGETALL(wf writeFunc, ctx *context, args [][]byte) error {
	key, err := buildKey(ctx, args[0])
	if err != nil {
		return err
	}
	rec, err := ctx.client.Execute(ctx.writePolicy, key, MODULE_NAME, "HGETALL")
	if err != nil {
		return err
	}
	a := rec.([]interface{})
	err = writeLine(wf, "*"+strconv.Itoa(len(a)))
	if err != nil {
		return err
	}
	for i := 0; i < len(a); i += 2 {
		err = writeByteArray(wf, []byte(a[i].(string)))
		if err != nil {
			return err
		}
		err = writeValue(wf, a[i+1])
		if err != nil {
			return err
		}
	}
	return nil
}

func cmd_EXPIRE(wf writeFunc, ctx *context, args [][]byte) error {
	key, err := buildKey(ctx, args[0])
	if err != nil {
		return err
	}

	ttl, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return err
	}

	err = ctx.client.Touch(fillWritePolicyEx(ctx, ttl, false), key)
	if err != nil {
		if errResultCode(err) == ase.KEY_NOT_FOUND_ERROR {
			return writeLine(wf, ":0")
		}
		return err
	}
	return writeLine(wf, ":1")
}

func cmd_TTL(wf writeFunc, ctx *context, args [][]byte) error {
	key, err := buildKey(ctx, args[0])
	if err != nil {
		return err
	}

	rec, err := ctx.client.GetHeader(ctx.readPolicy, key)
	if err != nil {
		return err
	}
	if rec == nil {
		return writeLine(wf, ":-2")
	}
	return writeLine(wf, ":"+strconv.FormatUint(uint64(rec.Expiration), 10))
}

func cmd_FLUSHDB(wf writeFunc, ctx *context, args [][]byte) error {
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
		_, err = ctx.client.Delete(ctx.writePolicy, res.Record.Key)
		if err != nil {
			break
		}
	}
	if err != nil {
		return err
	}

	return writeLine(wf, "+OK")
}

func cmd_HMINCRBYEX(wf writeFunc, ctx *context, args [][]byte) error {
	key, err := buildKey(ctx, args[0])
	if err != nil {
		return err
	}
	ttl, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return err
	}
	if len(args) == 2 {
		err := ctx.client.Touch(fillWritePolicyEx(ctx, ttl, false), key)
		if err != nil {
			if errResultCode(err) != ase.KEY_NOT_FOUND_ERROR {
				return err
			}
		}
		return writeLine(wf, "+OK")
	}
	ops := make([]*as.Operation, 0)
	a := args[2:]
	for i := 0; i < len(a); i += 2 {
		incr, err := strconv.Atoi(string(a[i+1]))
		if err != nil {
			return err
		}
		ops = append(ops, as.AddOp(as.NewBin(string(a[i]), incr)))
	}
	_, err = ctx.client.Operate(fillWritePolicyEx(ctx, ttl, false), key, ops...)
	if err != nil {
		return err
	}
	return writeLine(wf, "+OK")
}
