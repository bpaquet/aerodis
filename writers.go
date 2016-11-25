package main

import (
	"bytes"
	"encoding/base64"
	"log"
	"strconv"
	"strings"

	as "github.com/aerospike/aerospike-client-go"
)

func writeErr(wf writeFunc, error_prefix string, s string) error {
	log.Printf(error_prefix+"Client error : %s\n", s)
	return wf([]byte("-ERR " + s + "\n"))
}

func writeByteArray(wf writeFunc, buf []byte) error {
	err := wf([]byte("$" + strconv.Itoa(len(buf)) + "\r\n"))
	if err != nil {
		return err
	}
	err = wf(buf)
	if err != nil {
		return err
	}
	return wf([]byte("\r\n"))
}

func writeArray(wf writeFunc, array []interface{}) error {
	err := writeLine(wf, "*"+strconv.Itoa(len(array)))
	if err != nil {
		return err
	}
	for _, e := range array {
		// backward compat
		switch e.(type) {
		case string:
			s := e.(string)
			if strings.HasPrefix(s, "__64__") {
				bytes, err := base64.StdEncoding.DecodeString(s[6:])
				if err != nil {
					return err
				}
				err = writeByteArray(wf, bytes)
				if err != nil {
					return err
				}
			} else {
				err := writeByteArray(wf, []byte(s))
				if err != nil {
					return err
				}
			}
		default:
			// end of backward compat
			err := writeByteArray(wf, e.([]byte))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func writeLine(wf writeFunc, s string) error {
	return wf([]byte(s + "\r\n"))
}

func writeValue(wf writeFunc, x interface{}) error {
	switch x.(type) {
	case int:
		return writeByteArray(wf, []byte(strconv.Itoa(x.(int))))
	// backward compat
	case string:
		s := x.(string)
		if strings.HasPrefix(s, "__64__") {
			bytes, err := base64.StdEncoding.DecodeString(s[6:])
			if err != nil {
				return err
			}
			return writeByteArray(wf, bytes)
		} else {
			return writeByteArray(wf, []byte(s))
		}
	// end of backward compat
	default:
		return writeByteArray(wf, x.([]byte))
	}
}

func writeBin(wf writeFunc, rec *as.Record, bin_name string, nil_value string) error {
	if rec == nil {
		return writeLine(wf, nil_value)
	}
	x := rec.Bins[bin_name]
	if x == nil {
		return writeLine(wf, nil_value)
	}
	return writeValue(wf, x)
}

func writeBinInt(wf writeFunc, rec *as.Record, bin_name string) error {
	nil_value := ":0"
	if rec == nil {
		return writeLine(wf, nil_value)
	}
	x := rec.Bins[bin_name]
	if x == nil {
		return writeLine(wf, nil_value)
	}
	return writeLine(wf, ":"+strconv.Itoa(x.(int)))
}

func writeArrayBin(wf writeFunc, res []*as.Record, bin_name string, key_bin_name string) error {
	l := len(res)
	if key_bin_name != "" {
		l *= 2
	}
	err := writeLine(wf, "*"+strconv.Itoa(l))
	if err != nil {
		return err
	}
	for _, e := range res {
		if key_bin_name != "" {
			err := writeBin(wf, e, key_bin_name, "$-1")
			if err != nil {
				return err
			}
		}
		err := writeBin(wf, e, bin_name, "$-1")
		if err != nil {
			return err
		}
	}
	return nil
}

func encode(ctx *context, buf []byte) interface{} {
	if len(buf) < 10 {
		x, err := strconv.Atoi(string(buf))
		if err == nil {
			return x
		}
	}
	if !ctx.backwardWriteCompat {
		return buf
	}
	if bytes.IndexByte(buf, 0) == -1 {
		return string(buf)
	}
	return "__64__" + base64.StdEncoding.EncodeToString(buf)
}
