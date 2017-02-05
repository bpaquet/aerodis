package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"runtime/pprof"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"syscall"

	as "github.com/aerospike/aerospike-client-go"
	"github.com/coocood/freecache"
)

const binName = "r"
const MODULE_NAME = "redis"

func standardHandlers() map[string]handler {
	handlers := make(map[string]handler)
	handlers["DEL"] = handler{1, cmdDEL}
	handlers["GET"] = handler{1, cmdGET}
	handlers["SET"] = handler{2, cmdSET}
	handlers["SETEX"] = handler{3, cmdSETEX}
	handlers["SETNXEX"] = handler{3, cmdSETNXEX}
	handlers["SETNX"] = handler{2, cmdSETNX}
	handlers["MGET"] = handler{2, cmdMGET}
	handlers["MSET"] = handler{2, cmdMSET}
	handlers["LLEN"] = handler{1, cmdLLEN}
	handlers["RPUSH"] = handler{2, cmdRPUSH}
	handlers["LPUSH"] = handler{2, cmdLPUSH}
	handlers["RPUSHEX"] = handler{3, cmdRPUSHEX}
	handlers["LPUSHEX"] = handler{3, cmdLPUSHEX}
	handlers["RPOP"] = handler{1, cmdRPOP}
	handlers["LPOP"] = handler{1, cmdLPOP}
	handlers["LRANGE"] = handler{3, cmdLRANGE}
	handlers["LTRIM"] = handler{3, cmdLTRIM}
	handlers["INCR"] = handler{1, cmdINCR}
	handlers["INCRBY"] = handler{2, cmdINCRBY}
	handlers["HINCRBY"] = handler{3, cmdHINCRBY}
	handlers["HINCRBYEX"] = handler{4, cmdHINCRBYEX}
	handlers["DECR"] = handler{1, cmdDECR}
	handlers["DECRBY"] = handler{2, cmdDECRBY}
	handlers["HGET"] = handler{2, cmdHGET}
	handlers["HSET"] = handler{3, cmdHSET}
	handlers["HSETEX"] = handler{4, cmdHSETEX}
	handlers["HDEL"] = handler{2, cmdHDEL}
	handlers["HMGET"] = handler{2, cmdHMGET}
	handlers["HMSET"] = handler{3, cmdHMSET}
	handlers["HMINCRBYEX"] = handler{2, cmdHMINCRBYEX}
	handlers["HGETALL"] = handler{1, cmdHGETALL}
	handlers["EXPIRE"] = handler{2, cmdEXPIRE}
	handlers["TTL"] = handler{1, cmdTTL}
	handlers["FLUSHDB"] = handler{0, cmdFLUSHDB}
	return handlers
}

func expandedMapHandlers() map[string]handler {
	handlers := standardHandlers()
	handlers["DEL"] = handler{1, cmdExpandedMapDEL}
	handlers["HINCRBY"] = handler{3, cmdExpandedMapHINCRBY}
	handlers["HINCRBYEX"] = handler{4, cmdExpandedMapHINCRBYEX}
	handlers["HGET"] = handler{2, cmdExpandedMapHGET}
	handlers["HSET"] = handler{3, cmdExpandedMapHSET}
	handlers["HSETEX"] = handler{4, cmdExpandedMapHSETEX}
	handlers["HDEL"] = handler{2, cmdExpandedMapHDEL}
	handlers["HMGET"] = handler{2, cmdExpandedMapHMGET}
	handlers["HMSET"] = handler{3, cmdExpandedMapHMSET}
	handlers["HMINCRBYEX"] = handler{2, cmdExpandedMapHMINCRBYEX}
	handlers["HGETALL"] = handler{1, cmdExpandedMapHGETALL}
	handlers["EXPIRE"] = handler{2, cmdExpandedMapEXPIRE}
	handlers["TTL"] = handler{1, cmdExpandedMapTTL}
	return handlers
}

func getIntFromJson(x interface{}) int {
	switch x.(type) {
	case string:
		v, err := strconv.Atoi(x.(string))
		if err != nil {
			panic(err)
		}
		return v
	}
	return int(x.(float64))
}

func displayExpandedMapCacheStat(ctx *context) {
	for {
		time.Sleep(time.Duration(300) * time.Second)

		log.Printf("%s: cache ratio %d %.2f %%", ctx.set, ctx.expandedMapCache.LookupCount(), ctx.expandedMapCache.HitRate()*100)
		ctx.expandedMapCache.ResetStatistics()
	}
}

func main() {
	// to change the flags on the default logger
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	rand.Seed(time.Now().UnixNano())

	aeroHost := flag.String("aero_host", "localhost", "Aerospike server host")
	aeroPort := flag.Int("aero_port", 3000, "Aerospike server port")
	ns := flag.String("ns", "test", "Aerospike namespace")
	configFile := flag.String("config_file", "", "Configuration file")
	flag.Parse()

	config := []byte("{\"sets\":[{\"proto\":\"tcp\",\"listen\":\"127.0.0.1:6379\",\"set\":\"redis\"}]}")
	if *configFile != "" {
		bytes, err := ioutil.ReadFile(*configFile)
		if err != nil {
			panic(err)
		}
		config = bytes
	}

	var parsedConfig interface{}
	e := json.Unmarshal(config, &parsedConfig)
	if e != nil {
		panic(e)
	}

	m := parsedConfig.(map[string]interface{})

	if m["max_fds"] != nil {
		maxFds := getIntFromJson(m["max_fds"])
		var rLimit syscall.Rlimit
		rLimit.Max = uint64(maxFds)
		rLimit.Cur = uint64(maxFds)
		err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	  if err != nil {
	  	panic(err)
	  }
	  log.Printf("Set max openfile to %d", maxFds)
	}

	jsonAeroHost := m["aerospike_ips"]

	aPort := *aeroPort
	hosts := make([]string, 0)

	if jsonAeroHost != nil {
		for _, i := range jsonAeroHost.([]interface{}) {
			hosts = append(hosts, i.(string))
		}
	} else {
		hosts = append(hosts, *aeroHost)
	}

	var client *as.Client
	var err error

	for _, i := range hosts {
		log.Printf("Connecting to aero on %s:%d", i, aPort)
		client, err = as.NewClient(i, aPort)
		if err == nil {
			log.Printf("Connected to aero on %s:%d, namespace %s", i, aPort, *ns)
			break
		} else {
			log.Printf("Unable to connect to %s:%d, %s", i, aPort, err)
		}
	}
	if err != nil {
		panic(err)
	}

	readPolicy := as.NewPolicy()
	writePolicy := fillWritePolicyEx(-1, false)

	var wg sync.WaitGroup

	sets := m["sets"]

	statsdConfig := m["statsd"]

	for _, c := range sets.([]interface{}) {
		wg.Add(1)

		m := c.(map[string]interface{})
		proto := m["proto"].(string)
		listen := m["listen"].(string)
		set := m["set"].(string)

		if proto == "unix" {
			_, err := os.Stat(listen)
			if err == nil {
				os.Remove(listen)
			}
		}

		l, err := net.Listen(proto, listen)
		if err != nil {
			panic(err)
		}
		defer l.Close()

		if proto == "unix" {
			os.Chmod(listen, 0777)
		}

		log.Printf("%s: Listening on %s", set, listen)

		backwardWriteCompat := false
		if m["backwardWriteCompat"] != nil {
			backwardWriteCompat = true
			log.Printf("%s: Write backward compat", set)
		}
		ctx := context{client, *ns, set, readPolicy, writePolicy, backwardWriteCompat, 0, 0, 0, 0, nil, 0}

		if statsdConfig != nil {
			log.Printf("%s: Sending stats to statsd %s", set, statsdConfig)
			go statsd(statsdConfig.(string), &ctx)
		}

		if m["expanded_map"] != nil {
			if m["default_ttl"] != nil {
				ctx.expandedMapDefaultTTL = getIntFromJson(m["default_ttl"])
			} else {
				ctx.expandedMapDefaultTTL = 3600 * 24 * 31
			}
			log.Printf("%s: Expanded map mode, ttl %d", set, ctx.expandedMapDefaultTTL)
			if m["cache_size"] != nil {
				size := getIntFromJson(m["cache_size"])
				ctx.expandedMapCache = freecache.NewCache(size)
				ctx.expandedMapCacheTTL = 600
				if m["cache_ttl"] != nil {
					ctx.expandedMapCacheTTL = getIntFromJson(m["cache_ttl"])
				}
				log.Printf("%s: Using a cache of %d bytes, ttl %d", set, size, ctx.expandedMapCacheTTL)
				go displayExpandedMapCacheStat(&ctx)
			}
			go handlePort(&ctx, l, writeBack(expandedMapHandlers(), m, &ctx))
		} else {
			go handlePort(&ctx, l, writeBack(standardHandlers(), m, &ctx))
		}
	}

	wg.Wait()
}

func handlePort(ctx *context, l net.Listener, handlers map[string]handler) {
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Print("Error accepting: ", err.Error())
		} else {
			atomic.AddInt32(&ctx.gaugeConn, 1)
			go handleConnection(conn, handlers, ctx)
		}
	}
}

func handleConnection(conn net.Conn, handlers map[string]handler, ctx *context) error {
	multiBuffer := bytes.NewBuffer(nil)
	multiMode := false
	multiCounter := 0

	errorPrefix := "[" + (*ctx).set + "] "

	reader := bufio.NewReaderSize(conn, 1024)
	for {
		args, err := parse(reader)
		if err != nil {
			if err == io.EOF {
				return handleError(nil, ctx, conn)
			}
			writeErr(conn, errorPrefix, err.Error(), args)
			atomic.AddUint32(&ctx.counterErr, 1)
			return handleError(err, ctx, conn)
		}

		cmd := string(args[0])
		switch cmd {
		case "QUIT":
			return handleError(nil, ctx, conn)

		case "PROFILE":
			fname := "/tmp/redis_go_profile"
			f, err := os.Create(fname)
			if err != nil {
				return err
			}
			d := 60
			log.Printf("Start CPU Profiling for %d s", d)
			pprof.StartCPUProfile(f)
			writeLine(conn, "+OK In progress")
			time.Sleep(time.Duration(60) * time.Second)
			pprof.StopCPUProfile()
			log.Printf("End of CPU Profiling, output written to %s", fname)
			writeLine(conn, "+OK")
			return handleError(err, ctx, conn)
		}

		execErr := handleCommand(conn, args, handlers, ctx, &multiMode, &multiCounter, multiBuffer)
		if execErr != nil {
			writeErr(conn, errorPrefix, execErr.Error(), args)
			atomic.AddUint32(&ctx.counterErr, 1)
			return handleError(execErr, ctx, conn)
		}
		atomic.AddUint32(&ctx.counterOk, 1)
	}
}

func handleCommand(wf io.Writer, args [][]byte, handlers map[string]handler, ctx *context, multiMode *bool, multiCounter *int, multiBuffer *bytes.Buffer) error {
	cmd := string(args[0])
	switch cmd {
	case "MULTI":
		*multiCounter = 0
		multiBuffer.Reset()
		err := writeLine(wf, "+OK")
		if err != nil {
			return err
		}
		*multiMode = true

	case "EXEC":
		if !*multiMode {
			return errors.New("Exec received, but no MULTI before")
		}

		*multiMode = false
		err := writeLine(wf, "*"+strconv.Itoa(*multiCounter))
		if err != nil {
			return err
		}

		err = write(wf, multiBuffer.Bytes())
		if err != nil {
			return err
		}

	case "DISCARD":
		if !*multiMode {
			return errors.New("Exec received, but no MULTI before")
		}

		*multiMode = false
		err := writeLine(wf, "+OK")
		if err != nil {
			return err
		}

	default:
		args = args[1:]
		h, ok := handlers[cmd]
		if ok {
			if h.argsCount > len(args) {
				return fmt.Errorf("Wrong number of params for '%s': %d", cmd, len(args))
			}
			targetWriter := wf
			if *multiMode {
				*multiCounter += 1
				err := writeLine(wf, "+QUEUED")
				if err != nil {
					return err
				}
				targetWriter = multiBuffer
			}
			err := h.f(targetWriter, ctx, args)
			if err != nil {
				return fmt.Errorf("Aerospike error: '%s'", err)
			}
		} else {
			return fmt.Errorf("Unknown command '%s'", cmd)
		}
	}

	return nil
}

func handleError(err error, ctx *context, conn net.Conn) error {
	atomic.AddInt32(&ctx.gaugeConn, -1)
	conn.Close()
	return nil
}
