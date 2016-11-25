package main

import (
	"errors"
	"net"
	"strconv"
)

type readingContext struct {
	conn  net.Conn
	buf   []byte
	start int
	end   int
}

func readLine(ctx *readingContext, prefix []byte) ([]byte, error) {
	if prefix != nil && prefix[len(prefix)-1] == '\r' && ctx.start == 0 && ctx.buf[0] == '\n' {
		ctx.start = 1
		return prefix[0 : len(prefix)-1], nil
	}
	for i := ctx.start; i < ctx.end; i++ {
		if i < ctx.end-1 && ctx.buf[i] == '\r' && ctx.buf[i+1] == '\n' {
			line := ctx.buf[ctx.start:i]
			if prefix != nil {
				line = append(prefix, line...)
			}
			ctx.start = i + 2
			return line, nil
		}
		if ctx.buf[i] == '\n' {
			line := ctx.buf[ctx.start:i]
			if prefix != nil {
				line = append(prefix, line...)
			}
			ctx.start = i + 1
			return line, nil
		}
	}

	if ctx.start == ctx.end {
		ctx.start = 0
		ctx.end = 0
	}
	if ctx.end == len(ctx.buf) {
		current := ctx.buf[ctx.start:]
		ctx.start = 0
		ctx.end = 0
		line, err := readLine(ctx, current)
		if err != nil {
			return nil, err
		}
		return line, nil
	}
	l, err := ctx.conn.Read(ctx.buf[ctx.end:])
	if err != nil {
		return nil, err
	}
	ctx.end += l
	return readLine(ctx, prefix)
}

func readByteArray(ctx *readingContext, size int) ([]byte, error) {
	if ctx.start+size+2 > ctx.end {
		size += 2
		local_buf := make([]byte, size)
		copy(local_buf, ctx.buf[ctx.start:ctx.end])
		current := ctx.end - ctx.start
		for current < size {
			l, err := ctx.conn.Read(local_buf[current:])
			if err != nil {
				return nil, err
			}
			current += l
		}
		ctx.start = 0
		ctx.end = 0
		return local_buf[:size-2], nil
	}
	res := ctx.buf[ctx.start : ctx.start+size]
	ctx.start += size + 2
	return res, nil
}

func parse(ctx *readingContext) ([][]byte, error) {
	line, err := readLine(ctx, nil)
	if err != nil {
		return nil, err
	}
	count := -1
	args := make([][]byte, 0)
	if len(line) > 0 && line[0] == '*' {
		arrayCount, err := strconv.Atoi(string(line[1:]))
		if err != nil {
			return nil, err
		}
		count = arrayCount
		args = make([][]byte, arrayCount)
		for i := 0; i < arrayCount; i++ {
			line, err = readLine(ctx, nil)
			if err != nil {
				return nil, err
			}
			if line[0] == '$' {
				arg_len, err := strconv.Atoi(string(line[1:]))
				if err != nil {
					return nil, err
				}
				res, err := readByteArray(ctx, arg_len)
				if err != nil {
					return nil, err
				}
				args[i] = res
				count -= 1
			} else {
				return nil, errors.New("Protocol error")
			}
		}
	}
	if count != 0 {
		args = make([][]byte, 0)
		last := 0
		for i := 0; i < len(line); i++ {
			if line[i] == ' ' {
				args = append(args, line[last:i])
				last = i + 1
			}
		}
		args = append(args, line[last:])
		return args, nil
	}
	return args, nil
}
