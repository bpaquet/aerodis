package main

import (
	"log"
	"net"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

func udpSend(conn *net.UDPConn, s string) {
	// log.Println(s)
	_, err := conn.Write([]byte(s))
	if err != nil {
		log.Println("Couldn't send udp message", err.Error())
	}
}

func statsd(target string, ctx *context) {
	ra, err := net.ResolveUDPAddr("udp", target)
	if err != nil {
		log.Fatal("Unable to open resolve udp addr", err.Error())
		return
	}
	conn, err := net.DialUDP("udp", nil, ra)
	if err != nil {
		log.Fatal("Unable to open statsd socket", err.Error())
		return
	}
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal("Unable to get hostname", err.Error())
		return
	}
	start := "redis_go." + hostname + "." + ctx.ns + "." + ctx.set + "."
	ticker := time.NewTicker(time.Second * time.Duration(10))
	for range ticker.C {
		ok := atomic.SwapUint32(&(*ctx).counter_ok, 0)
		err := atomic.SwapUint32(&(*ctx).counter_err, 0)
		c := atomic.LoadInt32(&(*ctx).gauge_conn)
		udpSend(conn, start+"ok:"+strconv.Itoa(int(ok/10))+"|g")
		udpSend(conn, start+"err:"+strconv.Itoa(int(err/10))+"|g")
		udpSend(conn, start+"conn:"+strconv.Itoa(int(c))+"|g")
	}
}
