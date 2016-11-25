package main

import (
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
	fillReadPolicy(readPolicy)

	writePolicy := as.NewWritePolicy(0, 0)
	fillWritePolicy(writePolicy)

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
	errorPrefix := "[" + (*ctx).set + "] "
	var multiBuffer [][]byte
	multiCounter := 0
	multiMode := false
	wf := func(buffer []byte) error {
		_, err := conn.Write(buffer)
		return err
	}
	subWf := func(buffer []byte) error {
		if multiMode {
			multiBuffer = append(multiBuffer, buffer)
			return nil
		}
		return wf(buffer)
	}
	handleCommand := func(args [][]byte) error {
		cmd := string(args[0])
		if cmd == "MULTI" {
			multiCounter = 0
			multiBuffer = multiBuffer[:0]
			writeLine(wf, "+OK")
			multiMode = true
		} else if cmd == "EXEC" {
			if multiMode {
				multiMode = false
				err := writeLine(wf, "*"+strconv.Itoa(multiCounter))
				if err != nil {
					return err
				}
				for _, b := range multiBuffer {
					err := wf(b)
					if err != nil {
						return err
					}
				}
			} else {
				return errors.New("Exec received, bit no MULTI before")
			}
		} else if cmd == "DISCARD" {
			if multiMode {
				multiMode = false
				writeLine(wf, "+OK")
			} else {
				return errors.New("Exec received, bit no MULTI before")
			}
		} else {
			args = args[1:]
			h, ok := handlers[cmd]
			if ok {
				if h.argsCount > len(args) {
					return fmt.Errorf("Wrong number of params for '%s': %d", cmd, len(args))
				} else {
					if multiMode {
						multiCounter += 1
						err := writeLine(wf, "+QUEUED")
						if err != nil {
							return err
						}
					}
					err := h.f(subWf, ctx, args)
					if err != nil {
						return fmt.Errorf("Aerospike error: '%s'", err)
					}
				}
			} else {
				return fmt.Errorf("Unknown command '%s'", cmd)
			}
		}
		return nil
	}
	onError := func() error {
		atomic.AddInt32(&ctx.gaugeConn, -1)
		conn.Close()
		return nil
	}
	readingCtx := readingContext{conn, make([]byte, 1024), 0, 0}
	for {
		args, err := parse(&readingCtx)
		if err != nil {
			if err == io.EOF {
				log.Printf("EOF")
				return onError()
			}
			writeErr(wf, errorPrefix, err.Error())
			atomic.AddUint32(&ctx.counterErr, 1)
			return onError()
		}
		cmd := string(args[0])
		if cmd == "QUIT" {
			return onError()
		}
		if cmd == "PROFILE" {
			fname := "/tmp/redis_go_profile"
			f, err := os.Create(fname)
			if err != nil {
				return err
			}
			d := 60
			log.Printf("Start CPU Profiling for %d s", d)
			pprof.StartCPUProfile(f)
			writeLine(wf, "+OK In progress")
			time.Sleep(time.Duration(60) * time.Second)
			pprof.StopCPUProfile()
			log.Printf("End of CPU Profiling, output written to %s", fname)
			writeLine(wf, "+OK")
			return onError()
		}
		execErr := handleCommand(args)
		if execErr != nil {
			writeErr(wf, errorPrefix, execErr.Error())
			atomic.AddUint32(&ctx.counterErr, 1)
			return onError()
		}
		atomic.AddUint32(&ctx.counterOk, 1)
	}
}
