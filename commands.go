package main

import (
	"errors"
	"io"
	"strconv"

	as "github.com/aerospike/aerospike-client-go"
	ase "github.com/aerospike/aerospike-client-go/types"
)

func cmdDEL(wf io.Writer, ctx *context, args [][]byte) error {
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

func get(wf io.Writer, ctx *context, k []byte, binName string) error {
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

func cmdGET(wf io.Writer, ctx *context, args [][]byte) error {
	return get(wf, ctx, args[0], binName)
}

func cmdMGET(wf io.Writer, ctx *context, args [][]byte) error {
	res := make([]*as.Record, len(args))
	for i := 0; i < len(args); i++ {
		key, err := buildKey(ctx, args[i])
		if err != nil {
			return err
		}
		rec, err := ctx.client.Get(ctx.readPolicy, key, binName)
		if err != nil {
			return err
		}
		res[i] = rec
	}
	return writeArrayBin(wf, res, binName, "")
}

func cmdHGET(wf io.Writer, ctx *context, args [][]byte) error {
	return get(wf, ctx, args[0], string(args[1]))
}

func setex(wf io.Writer, ctx *context, k []byte, binName string, content []byte, ttl int, createOnly bool) error {
	key, err := buildKey(ctx, k)
	if err != nil {
		return err
	}
	err = ctx.client.PutBins(createWritePolicyEx(ttl, createOnly), key, as.NewBin(binName, encode(ctx, content)))
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

func cmdSET(wf io.Writer, ctx *context, args [][]byte) error {
	return setex(wf, ctx, args[0], binName, args[1], -1, false)
}

func cmdSETEX(wf io.Writer, ctx *context, args [][]byte) error {
	ttl, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return err
	}
	return setex(wf, ctx, args[0], binName, args[2], ttl, false)
}

func cmdMSET(wf io.Writer, ctx *context, args [][]byte) error {
	for i := 0; i+1 < len(args); i += 2 {
		key, err := buildKey(ctx, args[i])
		if err != nil {
			return err
		}
		err = ctx.client.PutBins(ctx.writePolicy, key, as.NewBin(binName, encode(ctx, args[i+1])))
		if err != nil {
			return err
		}
	}
	return writeLine(wf, "+OK")
}

func cmdSETNX(wf io.Writer, ctx *context, args [][]byte) error {
	return setex(wf, ctx, args[0], binName, args[1], -1, true)
}

func cmdSETNXEX(wf io.Writer, ctx *context, args [][]byte) error {
	ttl, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return err
	}

	return setex(wf, ctx, args[0], binName, args[2], ttl, true)
}

func tryHSet(ctx *context, key *as.Key, field string, value interface{}, ttl int) (error, bool) {
	policy := as.NewPolicy()
	policy.ReplicaPolicy = as.MASTER
	rec, err := ctx.client.Get(policy, key, field)
	if err != nil {
		return err, false
	}
	var generation uint32
	if rec != nil {
		generation = rec.Generation
	} else {
		generation = 0
	}
	err = ctx.client.PutBins(createWritePolicyGeneration(generation, ttl), key, as.NewBin(field, value))
	if err != nil {
		return err, false
	}
	if rec == nil {
		return nil, false
	}
	if rec.Bins[field] == nil {
		return nil, false
	}
	return nil, true
}

func hset(wf io.Writer, ctx *context, k []byte, kk []byte, v interface{}, ttl int, set bool) error {
	key, err := buildKey(ctx, k)
	if err != nil {
		return err
	}
	field := string(kk)
	for i := 0; i < ctx.generationRetries; i++ {
		err, existed := tryHSet(ctx, key, field, v, ttl)
		if err == nil {
			if ! set {
				existed = !existed
			}
			if existed {
				return writeLine(wf, ":0")
			}
			return writeLine(wf, ":1")
		}
		if errResultCode(err) != ase.GENERATION_ERROR {
			return err
		}
	}
	return errors.New("Too many retry for hset")
}

func cmdHSET(wf io.Writer, ctx *context, args [][]byte) error {
	return hset(wf, ctx, args[0], args[1], args[2], -1, true)
}

func cmdHSETEX(wf io.Writer, ctx *context, args [][]byte) error {
	ttl, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return err
	}
	return hset(wf, ctx, args[0], args[2], args[3], ttl, true)
}

func cmdHDEL(wf io.Writer, ctx *context, args [][]byte) error {
	return hset(wf, ctx, args[0], args[1], nil, -1, false)
}

func listOpReturnSize(wf io.Writer, ctx *context, args [][]byte, ttl int, op *as.Operation) error {
	key, err := buildKey(ctx, args[0])
	if err != nil {
		return err
	}
	rec, err := ctx.client.Operate(createWritePolicyEx(ttl, false), key, op, as.AddOp(as.NewBin(SIZE_ARRAY_FIELD, 1)))
	if err != nil {
		return err
	}
	return writeBinInt(wf, rec, binName)
}

func arrayRPush(wf io.Writer, ctx *context, args [][]byte, ttl int) error {
	// ListInsertOp does not like to be called on an empty list and -1
	// ListAppendOp is ok
	return listOpReturnSize(wf, ctx, args, ttl, as.ListAppendOp(binName, encode(ctx, args[1])))
}

func cmdRPUSH(wf io.Writer, ctx *context, args [][]byte) error {
	return arrayRPush(wf, ctx, args, -1)
}

func cmdRPUSHEX(wf io.Writer, ctx *context, args [][]byte) error {
	ttl, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return err
	}
	return arrayRPush(wf, ctx, args, ttl)
}

func arrayLPush(wf io.Writer, ctx *context, args [][]byte, ttl int) error {
	return listOpReturnSize(wf, ctx, args, ttl, as.ListInsertOp(binName, 0, encode(ctx, args[1])))
}

func cmdLPUSH(wf io.Writer, ctx *context, args [][]byte) error {
	return arrayLPush(wf, ctx, args, -1)
}

func cmdLPUSHEX(wf io.Writer, ctx *context, args [][]byte) error {
	ttl, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return err
	}
	return arrayLPush(wf, ctx, args, ttl)
}

func arrayPop(wf io.Writer, ctx *context, args [][]byte, index int) error {
	key, err := buildKey(ctx, args[0])
	if err != nil {
		return err
	}
	size, err := ctx.client.Get(ctx.readPolicy, key, SIZE_ARRAY_FIELD)
	if err != nil {
		return err
	}
	if size == nil {
		return writeLine(wf, "$-1")
	}
	if size.Bins[SIZE_ARRAY_FIELD] == nil {
		return writeLine(wf, "$-1")
	}
	if size.Bins[SIZE_ARRAY_FIELD].(int) == 0 {
		return writeLine(wf, "$-1")
	}
	rec, err := ctx.client.Operate(ctx.writePolicy, key, as.ListPopOp(binName, index), as.AddOp(as.NewBin(SIZE_ARRAY_FIELD, -1)))
	code := errResultCode(err)
	if code == ase.BIN_TYPE_ERROR || code == ase.PARAMETER_ERROR {
		return writeLine(wf, "$-1")
	}
	if err != nil {
		return err
	}
	result := rec.Bins[binName]
	switch result.(type) {
	case int:
		return writeByteArray(wf, []byte(strconv.Itoa(result.(int))))
	default:
		return writeByteArray(wf, result.([]byte))
	}
}

func cmdRPOP(wf io.Writer, ctx *context, args [][]byte) error {
	return arrayPop(wf, ctx, args, -1)
}

func cmdLPOP(wf io.Writer, ctx *context, args [][]byte) error {
	return arrayPop(wf, ctx, args, 0)
}

func cmdLLEN(wf io.Writer, ctx *context, args [][]byte) error {
	key, err := buildKey(ctx, args[0])
	if err != nil {
		return err
	}
	rec, err := ctx.client.Get(ctx.readPolicy, key, SIZE_ARRAY_FIELD)
	if err != nil {
		return err
	}
	return writeBinInt(wf, rec, SIZE_ARRAY_FIELD)
}

func cmdLRANGE(wf io.Writer, ctx *context, args [][]byte) error {
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
	err, result, _ := arrayRange(ctx, key, start, stop)
	if err != nil {
		return err
	}
	return writeArray(wf, result)
}

func arrayRange(ctx *context, key *as.Key, start int, stop int) (error, []interface{}, uint32) {
	if ((stop > 0 && start > 0) || (stop < 0 && start < 0)) && start > stop {
		return nil, make([]interface{}, 0), 0
	}
	count := stop - start + 1
	if stop < start {
		count -= 1
	}
	rec, err := ctx.client.Operate(ctx.writePolicy, key, as.ListGetRangeOp(binName, start, count), as.ListSizeOp(binName))
	if errResultCode(err) == ase.PARAMETER_ERROR {
		return nil, make([]interface{}, 0), 0
	}
	if err != nil {
		return err, nil, 0
	}
	if rec == nil {
		return nil, make([]interface{}, 0), 0
	}
	a := rec.Bins[binName].([]interface {})
	size, result := a[len(a)-1], a[:len(a)-1]
	if start < 0 && stop >= 0 {
		end := size.(int) + start + 1
		if end < len(result) {
			result = result[0:end]
		}
	}
	if start >= 0 && stop < 0 {
		end := size.(int) + stop + 1
		if end < len(result) {
			result = result[0:end]
		}
	}
	return nil, result, rec.Generation
}

func cmdLTRIM(wf io.Writer, ctx *context, args [][]byte) error {
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
	for i := 0; i < ctx.generationRetries; i++ {
		err := tryLTRIM(wf, ctx, key, start, stop)
		if err == nil {
			return writeLine(wf, "+OK")
		}
		if errResultCode(err) != ase.GENERATION_ERROR {
			return err
		}
	}
	return errors.New("Too many retry for ltrim")
}

func tryLTRIM(wf io.Writer, ctx *context, key *as.Key, start int, stop int) error {
	err, result, generation := arrayRange(ctx, key, start, stop)
	if err != nil {
		return err
	}
	ops := make([]*as.Operation, 1)
	ops[0] = as.ListClearOp(binName)
	if len(result) > 0 {
		ops = append(ops, as.ListAppendOp(binName, result...))
	}
	ops = append(ops, as.PutOp(as.NewBin(SIZE_ARRAY_FIELD, len(result))))
	policy := ctx.writePolicy
	if generation > 0 {
		policy = createWritePolicyGeneration(generation, -1)
	}
	_, err = ctx.client.Operate(policy, key, ops...)
	if err != nil {
		return err
	}
	return nil
}

func hIncrByEx(wf io.Writer, ctx *context, k []byte, field string, incr int, ttl int) error {
	key, err := buildKey(ctx, k)
	if err != nil {
		return err
	}
	bin := as.NewBin(field, incr)
	rec, err := ctx.client.Operate(createWritePolicyEx(ttl, false), key, as.AddOp(bin), as.GetOpForBin(field))
	if err != nil {
		if errResultCode(err) == ase.BIN_TYPE_ERROR {
			return writeLine(wf, "$-1")
		}
		return err
	}
	return writeBinInt(wf, rec, field)
}

func cmdINCR(wf io.Writer, ctx *context, args [][]byte) error {
	return hIncrByEx(wf, ctx, args[0], binName, 1, -1)
}

func cmdDECR(wf io.Writer, ctx *context, args [][]byte) error {
	return hIncrByEx(wf, ctx, args[0], binName, -1, -1)
}

func cmdINCRBY(wf io.Writer, ctx *context, args [][]byte) error {
	incr, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return err
	}
	return hIncrByEx(wf, ctx, args[0], binName, incr, -1)
}

func cmdINCRBYEX(wf io.Writer, ctx *context, args [][]byte) error {
	ttl, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return err
	}
	incr, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return err
	}
	return hIncrByEx(wf, ctx, args[0], binName, incr, ttl)
}

func cmdHINCRBY(wf io.Writer, ctx *context, args [][]byte) error {
	incr, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return err
	}
	return hIncrByEx(wf, ctx, args[0], string(args[1]), incr, -1)
}

func cmdHINCRBYEX(wf io.Writer, ctx *context, args [][]byte) error {
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

func cmdDECRBY(wf io.Writer, ctx *context, args [][]byte) error {
	decr, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return err
	}
	return hIncrByEx(wf, ctx, args[0], binName, -decr, -1)
}

func cmdDECRBYEX(wf io.Writer, ctx *context, args [][]byte) error {
	ttl, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return err
	}
	decr, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return err
	}
	return hIncrByEx(wf, ctx, args[0], binName, -decr, ttl)
}

func cmdHMGET(wf io.Writer, ctx *context, args [][]byte) error {
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

func cmdHMSET(wf io.Writer, ctx *context, args [][]byte) error {
	key, err := buildKey(ctx, args[0])
	if err != nil {
		return err
	}
	bins := make([]*as.Bin, (len(args) - 1) / 2)
	for i := 1; i+1 < len(args); i += 2 {
		bins[i/2] = as.NewBin(string(args[i]), encode(ctx, args[i+1]))
	}
	err = ctx.client.PutBins(ctx.writePolicy, key, bins...)
	if err != nil {
		return err
	}
	return writeLine(wf, "+OK")
}

func cmdHGETALL(wf io.Writer, ctx *context, args [][]byte) error {
	key, err := buildKey(ctx, args[0])
	if err != nil {
		return err
	}
	rec, err := ctx.client.Get(ctx.readPolicy, key)
	if err != nil {
		return err
	}
	if rec == nil {
		err = writeLine(wf, "*0")
		if err != nil {
			return err
		}
	} else {
		err = writeLine(wf, "*"+strconv.Itoa(len(rec.Bins) * 2))
		if err != nil {
			return err
		}
		for k, v := range rec.Bins {
			err = writeByteArray(wf, []byte(k))
			if err != nil {
				return err
			}
			err = writeValue(wf, v)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func cmdEXPIRE(wf io.Writer, ctx *context, args [][]byte) error {
	key, err := buildKey(ctx, args[0])
	if err != nil {
		return err
	}

	ttl, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return err
	}

	err = ctx.client.Touch(createWritePolicyEx(ttl, false), key)
	if err != nil {
		if errResultCode(err) == ase.KEY_NOT_FOUND_ERROR {
			return writeLine(wf, ":0")
		}
		return err
	}
	return writeLine(wf, ":1")
}

func cmdTTL(wf io.Writer, ctx *context, args [][]byte) error {
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

func cmdFLUSHDB(wf io.Writer, ctx *context, args [][]byte) error {
	policy := as.NewScanPolicy()
	recordset, err := ctx.client.ScanAll(policy, ctx.ns, ctx.set)
	if err != nil {
		return err
	}
	for res := range recordset.Results() {
		if res.Err != nil {
			return err
		}
		_, err := ctx.client.Delete(ctx.writePolicy, res.Record.Key)
		if err != nil {
			return err
		}
	}
	return writeLine(wf, "+OK")
}

func cmdHMINCRBYEX(wf io.Writer, ctx *context, args [][]byte) error {
	key, err := buildKey(ctx, args[0])
	if err != nil {
		return err
	}
	ttl, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return err
	}
	if len(args) == 2 {
		err := ctx.client.Touch(createWritePolicyEx(ttl, false), key)
		if err != nil {
			if errResultCode(err) != ase.KEY_NOT_FOUND_ERROR {
				return err
			}
		}
		return writeLine(wf, "+OK")
	}
	ops := make([]*as.Operation, (len(args) - 2) / 2)
	for i := 2; i+1 < len(args); i += 2 {
		incr, err := strconv.Atoi(string(args[i+1]))
		if err != nil {
			return err
		}
		ops[(i/2)-1] = as.AddOp(as.NewBin(string(args[i]), incr))
	}
	_, err = ctx.client.Operate(createWritePolicyEx(ttl, false), key, ops...)
	if err != nil {
		return err
	}
	return writeLine(wf, "+OK")
}
