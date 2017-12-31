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
	start := "redis_go." + hostname + "."
	end := ",ns=" + ctx.ns + ",set=" + ctx.set
	ticker := time.NewTicker(time.Second * time.Duration(5))
	for range ticker.C {
		ok := atomic.SwapUint32(&(*ctx).counterOk, 0)
		wbOk := atomic.SwapUint32(&(*ctx).counterWbOk, 0)
		err := atomic.SwapUint32(&(*ctx).counterErr, 0)
		c := atomic.LoadInt32(&(*ctx).gaugeConn)
		udpSend(conn, start+"ops,type=ok"+end+":"+strconv.Itoa(int(ok))+"|c")
		udpSend(conn, start+"ops,type=wbok"+end+":"+strconv.Itoa(int(wbOk))+"|c")
		udpSend(conn, start+"ops,type=err"+end+":"+strconv.Itoa(int(err))+"|c")
		udpSend(conn, start+"conn"+end+":"+strconv.Itoa(int(c))+"|g")
	}
}
