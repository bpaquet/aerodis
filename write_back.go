package main

import (
	"encoding/json"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
)

func sendMessage(wf io.Writer, conn *net.UDPConn, cacheName string, key string, m map[string]interface{}) error {
		v, err := json.Marshal(m)
		if err != nil {
			return err
		}
		s := strings.Replace(cacheName + "_" + key, "|", "_", -1) + "|" + string(v)
		udpSend(conn, s)
		return writeLine(wf, "+OK")
}

func writeBack(handlers map[string]handler, config map[string]interface{}, ctx *context) map[string]handler {
	if config["write_back_target"] == nil {
		return handlers
	}
	ra, err := net.ResolveUDPAddr("udp", config["write_back_target"].(string))
	if err != nil {
		panic(err)
	}
	conn, err := net.DialUDP("udp", nil, ra)
	if err != nil {
		panic(err)
	}
	if config["write_back_setTimeout"] != nil {
		cacheName := "CACHE_" + strings.ToUpper(ctx.set)
		m := make(map[string]interface{})
		m["cache_name"] = cacheName
		m["method"] = "setTimeout"
		a := make([]interface{}, 2)
		m["args"] = a
		log.Printf("%s: Using write back for setTimeout to %s", ctx.set, config["write_back_target"])
		f := func(wf io.Writer, ctx *context, args [][]byte) error {
			key := string(args[0])
			ttl, err := strconv.Atoi(string(args[1]))
			if err != nil {
				return err
			}
			a[0] = key
			a[1] = ttl
			return sendMessage(wf, conn, cacheName, key, m)
		}
		handlers["EXPIRE"] = handler{handlers["EXPIRE"].argsCount, f, true}
	}
	if config["write_back_hIncrBy"] != nil {
		cacheName := "CACHE_" + strings.ToUpper(ctx.set)
		m := make(map[string]interface{})
		m["cache_name"] = cacheName
		m["method"] = "hIncrBy"
		a := make([]interface{}, 3)
		m["args"] = a
		log.Printf("%s: Using write back for hIncrBy to %s", ctx.set, config["write_back_target"])
		f := func(wf io.Writer, ctx *context, args [][]byte) error {
			key := string(args[0])
			field := string(args[1])
			incr, err := strconv.Atoi(string(args[2]))
			if err != nil {
				return err
			}
			a[0] = key
			a[1] = field
			a[2] = incr
			return sendMessage(wf, conn, cacheName, key, m)
		}
		handlers["HINCRBY"] = handler{handlers["HINCRBY"].argsCount, f, true}
	}
	return handlers

}
