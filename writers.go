package main

import (
	"bytes"
	"encoding/base64"
	"log"
	"strconv"
	"strings"

	as "github.com/aerospike/aerospike-client-go"
)

func WriteErr(wf write_func, error_prefix string, s string) error {
	log.Printf(error_prefix+"Client error : %s\n", s)
	return wf([]byte("-ERR " + s + "\n"))
}

func WriteByteArray(wf write_func, buf []byte) error {
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

func WriteArray(wf write_func, array []interface{}) error {
	err := WriteLine(wf, "*"+strconv.Itoa(len(array)))
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
				err = WriteByteArray(wf, bytes)
				if err != nil {
					return err
				}
			} else {
				err := WriteByteArray(wf, []byte(s))
				if err != nil {
					return err
				}
			}
		default:
			// end of backward compat
			err := WriteByteArray(wf, e.([]byte))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func WriteLine(wf write_func, s string) error {
	return wf([]byte(s + "\r\n"))
}

func WriteValue(wf write_func, x interface{}) error {
	switch x.(type) {
	case int:
		return WriteByteArray(wf, []byte(strconv.Itoa(x.(int))))
	// backward compat
	case string:
		s := x.(string)
		if strings.HasPrefix(s, "__64__") {
			bytes, err := base64.StdEncoding.DecodeString(s[6:])
			if err != nil {
				return err
			}
			return WriteByteArray(wf, bytes)
		} else {
			return WriteByteArray(wf, []byte(s))
		}
	// end of backward compat
	default:
		return WriteByteArray(wf, x.([]byte))
	}
}

func WriteBin(wf write_func, rec *as.Record, bin_name string, nil_value string) error {
	if rec == nil {
		return WriteLine(wf, nil_value)
	}
	x := rec.Bins[bin_name]
	if x == nil {
		return WriteLine(wf, nil_value)
	}
	return WriteValue(wf, x)
}

func WriteBinInt(wf write_func, rec *as.Record, bin_name string) error {
	nil_value := ":0"
	if rec == nil {
		return WriteLine(wf, nil_value)
	}
	x := rec.Bins[bin_name]
	if x == nil {
		return WriteLine(wf, nil_value)
	}
	return WriteLine(wf, ":"+strconv.Itoa(x.(int)))
}

func WriteArrayBin(wf write_func, res []*as.Record, bin_name string, key_bin_name string) error {
	l := len(res)
	if key_bin_name != "" {
		l *= 2
	}
	err := WriteLine(wf, "*"+strconv.Itoa(l))
	if err != nil {
		return err
	}
	for _, e := range res {
		if key_bin_name != "" {
			err := WriteBin(wf, e, key_bin_name, "$-1")
			if err != nil {
				return err
			}
		}
		err := WriteBin(wf, e, bin_name, "$-1")
		if err != nil {
			return err
		}
	}
	return nil
}

func Encode(ctx *context, buf []byte) interface{} {
	if len(buf) < 10 {
		x, err := strconv.Atoi(string(buf))
		if err == nil {
			return x
		}
	}
	if !ctx.backward_write_compat {
		return buf
	}
	if bytes.IndexByte(buf, 0) == -1 {
		return string(buf)
	}
	return "__64__" + base64.StdEncoding.EncodeToString(buf)
}
