package main

import (
	"bufio"
	"errors"
	"io"
	"strconv"
)

func readLine(ctx *bufio.Reader, prefix []byte) ([]byte, error) {
	line, isPrefix, err := ctx.ReadLine()
	if err != nil {
		return nil, err
	}

	if len(prefix) > 0 {
		line = append(prefix, line...)
	}

	if isPrefix {
		return readLine(ctx, line)
	}

	return line, nil
}

func readByteArray(ctx *bufio.Reader, size int) ([]byte, error) {
	// read the \r\n as well
	size += 2
	res := make([]byte, size)
	n, err := io.ReadFull(ctx, res)
	if n != size {
		return nil, errors.New("protocol parse error: byte array size mismatch")
	}

	// don't return the \r\n
	return res[:size-2], err
}

func parse(ctx *bufio.Reader) ([][]byte, error) {
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
				argLen, err := strconv.Atoi(string(line[1:]))
				if err != nil {
					return nil, err
				}
				res, err := readByteArray(ctx, argLen)
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
