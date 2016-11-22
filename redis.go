package main

import (
  "fmt"
  "net"
  "flag"
  "strconv"
  "encoding/json"
  "sync"
  "sync/atomic"
  "io/ioutil"
  "log"
  "os"
  "errors"
  "math/rand"
  "time"
  "runtime/pprof"

  as "github.com/aerospike/aerospike-client-go"
  "github.com/coocood/freecache"
)

const BIN_NAME = "r"
const module_name = "redis"

func StandardHandlers() map[string]handler {
  handlers := make(map[string]handler)
  handlers["DEL"] = handler{1, cmd_DEL}
  handlers["GET"] = handler{1, cmd_GET}
  handlers["SET"] = handler{2, cmd_SET}
  handlers["SETEX"] = handler{3, cmd_SETEX}
  handlers["SETNXEX"] = handler{3, cmd_SETNXEX}
  handlers["SETNX"] = handler{2, cmd_SETNX}
  handlers["LLEN"] = handler{1, cmd_LLEN}
  handlers["RPUSH"] = handler{2, cmd_RPUSH}
  handlers["LPUSH"] = handler{2, cmd_LPUSH}
  handlers["RPUSHEX"] = handler{3, cmd_RPUSHEX}
  handlers["LPUSHEX"] = handler{3, cmd_LPUSHEX}
  handlers["RPOP"] = handler{1, cmd_RPOP}
  handlers["LPOP"] = handler{1, cmd_LPOP}
  handlers["LRANGE"] = handler{3, cmd_LRANGE}
  handlers["LTRIM"] = handler{3, cmd_LTRIM}
  handlers["INCR"] = handler{1, cmd_INCR}
  handlers["INCRBY"] = handler{2, cmd_INCRBY}
  handlers["HINCRBY"] = handler{3, cmd_HINCRBY}
  handlers["HINCRBYEX"] = handler{4, cmd_HINCRBYEX}
  handlers["DECR"] = handler{1, cmd_DECR}
  handlers["DECRBY"] = handler{2, cmd_DECRBY}
  handlers["HGET"] = handler{2, cmd_HGET}
  handlers["HSET"] = handler{3, cmd_HSET}
  handlers["HDEL"] = handler{2, cmd_HDEL}
  handlers["HMGET"] = handler{2, cmd_HMGET}
  handlers["HMSET"] = handler{3, cmd_HMSET}
  handlers["HMINCRBYEX"] = handler{2, cmd_HMINCRBYEX}
  handlers["HGETALL"] = handler{1, cmd_HGETALL}
  handlers["EXPIRE"] = handler{2, cmd_EXPIRE}
  handlers["TTL"] = handler{1, cmd_TTL}
  handlers["FLUSHDB"] = handler{0, cmd_FLUSHDB}
  return handlers
}

func ExpandedMapHandlers() (map[string]handler) {
  handlers := StandardHandlers()
  handlers["DEL"] = handler{1, cmd_em_DEL}
  handlers["HINCRBY"] = handler{3, cmd_em_HINCRBY}
  handlers["HINCRBYEX"] = handler{4, cmd_em_HINCRBYEX}
  handlers["HGET"] = handler{2, cmd_em_HGET}
  handlers["HSET"] = handler{3, cmd_em_HSET}
  handlers["HDEL"] = handler{2, cmd_em_HDEL}
  handlers["HMGET"] = handler{2, cmd_em_HMGET}
  handlers["HMSET"] = handler{3, cmd_em_HMSET}
  handlers["HMINCRBYEX"] = handler{2, cmd_em_HMINCRBYEX}
  handlers["HGETALL"] = handler{1, cmd_em_HGETALL}
  handlers["EXPIRE"] = handler{2, cmd_em_EXPIRE}
  handlers["TTL"] = handler{1, cmd_em_TTL}
  return handlers
}

func getIntFromJson(x interface{}) (int) {
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

func DisplayExpandedMapCacheStat(ctx *context) {
  for {
    time.Sleep(time.Duration(300) * time.Second)

    log.Printf("%s: cache ratio %d %.2f %%", ctx.set, ctx.expanded_map_cache.LookupCount(), ctx.expanded_map_cache.HitRate() * 100)
    ctx.expanded_map_cache.ResetStatistics()
  }
}

func main() {
  rand.Seed(time.Now().UnixNano())

  aero_host := flag.String("aero_host", "localhost", "Aerospike server host")
  aero_port := flag.Int("aero_port", 3000, "Aerospike server port")
  ns := flag.String("ns", "test", "Aerospike namespace")
  config_file := flag.String("config_file", "", "Configuration file")
  flag.Parse()

  config := []byte("{\"sets\":[{\"proto\":\"tcp\",\"listen\":\"127.0.0.1:6379\",\"set\":\"redis\"}]}")
  if *config_file != "" {
    bytes, err := ioutil.ReadFile(*config_file)
    if err != nil {
      panic(err)
    }
    config = bytes
  }

  var parsed_config interface{}
  e := json.Unmarshal(config, &parsed_config)
  if e != nil {
    panic(e)
  }

  m := parsed_config.(map[string]interface{})
  json_aero_host := m["aerospike_ips"]

  a_port := *aero_port
  hosts := make([]string, 0)

  if json_aero_host != nil {
    for _, i := range json_aero_host.([]interface{}) {
      hosts = append(hosts, i.(string))
    }
  } else {
    hosts = append(hosts, *aero_host)
  }

  var client * as.Client = nil
  var err error = nil

  for _, i := range hosts {
    log.Printf("Connecting to aero on %s:%d", i, a_port)
    client, err = as.NewClient(i, a_port)
    if err == nil {
      log.Printf("Connected to aero on %s:%d, namespace %s", i, a_port, *ns)
      break
    } else {
      log.Printf("Unable to connect to %s:%d, %s", i, a_port, err)
    }
  }
  if err != nil {
    panic(err);
  }

  read_policy := as.NewPolicy()
  FillReadPolicy(read_policy)

  write_policy := as.NewWritePolicy(0, 0)
  FillWritePolicy(write_policy)

  var wg sync.WaitGroup

  sets := m["sets"]

  statsd_config := m["statsd"]

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

    backward_write_compat := false
    if m["backward_write_compat"] != nil {
      backward_write_compat = true
      log.Printf("%s: Write backward compat", set)
    }
    ctx := context{client, *ns, set, read_policy, write_policy, backward_write_compat, 0, 0, 0, 0, nil, 0}

    if statsd_config != nil {
      log.Printf("%s: Sending stats to statsd %s", set, statsd_config)
      go statsd(statsd_config.(string), &ctx)
    }

    if m["expanded_map"] != nil {
      if m["default_ttl"] != nil {
        ctx.expanded_map_default_ttl = getIntFromJson(m["default_ttl"])
      } else {
        ctx.expanded_map_default_ttl = 3600 * 24 * 31
      }
      log.Printf("%s: Expanded map mode, ttl %d", set, ctx.expanded_map_default_ttl)
      if m["cache_size"] != nil {
        size := getIntFromJson(m["cache_size"])
        ctx.expanded_map_cache = freecache.NewCache(size)
        ctx.expanded_map_cache_ttl = 600
        if m["cache_ttl"] != nil {
          ctx.expanded_map_cache_ttl = getIntFromJson(m["cache_ttl"])
        }
        log.Printf("%s: Using a cache of %d bytes, ttl %d", set, size, ctx.expanded_map_cache_ttl)
        go DisplayExpandedMapCacheStat(&ctx)
      }
      go HandlePort(&ctx, l, WriteBack(ExpandedMapHandlers(), m, &ctx))
    } else {
      go HandlePort(&ctx, l, WriteBack(StandardHandlers(), m, &ctx))
    }
  }

  wg.Wait()
}

func HandlePort(ctx *context, l net.Listener, handlers map[string]handler) {
  for {
    conn, err := l.Accept()
    if err != nil {
      log.Print("Error accepting: ", err.Error())
    } else {
      atomic.AddInt32(&ctx.gauge_conn, 1)
      go HandleConnection(conn, handlers, ctx)
    }
  }
}

func HandleConnection(conn net.Conn, handlers map[string]handler, ctx *context) error {
  error_prefix := "[" + (*ctx).set + "] "
  var multi_buffer [][]byte
  multi_counter := 0
  multi_mode := false
  wf := func(buffer []byte) (error) {
    _, err := conn.Write(buffer)
    return err
  }
  sub_wf := func(buffer []byte) (error) {
    if multi_mode {
      multi_buffer = append(multi_buffer, buffer)
      return nil
    }
    return wf(buffer)
  }
  handleCommand := func(args [][] byte) error {
    cmd := string(args[0])
    if cmd == "MULTI" {
      multi_counter = 0
      multi_buffer = multi_buffer[:0]
      WriteLine(wf, "+OK")
      multi_mode = true
    } else if cmd == "EXEC" {
      if multi_mode {
        multi_mode = false
        err := WriteLine(wf, "*" + strconv.Itoa(multi_counter))
        if err != nil {
          return err
        }
        for _, b := range multi_buffer {
          err := wf(b)
          if err != nil {
            return err
          }
        }
      } else {
        return errors.New("Exec received, bit no MULTI before")
      }
    } else if cmd == "DISCARD" {
      if multi_mode {
        multi_mode = false
        WriteLine(wf, "+OK")
      } else {
        return errors.New("Exec received, bit no MULTI before")
      }
    } else {
      args = args[1:]
      h, ok := handlers[cmd]
      if ok {
        if h.args_count > len(args) {
          return errors.New(fmt.Sprintf("Wrong number of params for '%s': %d", cmd, len(args)))
        } else {
          if multi_mode {
            multi_counter += 1
            err := WriteLine(wf, "+QUEUED")
            if err != nil {
              return err
            }
          }
          err := h.f(sub_wf, ctx, args)
          if err != nil {
            return errors.New(fmt.Sprintf("Aerospike error: '%s'", err))
          }
        }
      } else {
        return errors.New(fmt.Sprintf("unknown command '%s'", cmd))
      }
    }
    return nil
  }
  on_error := func() error {
    atomic.AddInt32(&ctx.gauge_conn, -1)
    conn.Close()
    return nil
  }
  reading_ctx := reading_context{conn, make([]byte, 1024), 0, 0}
  for {
    args, err := Parse(&reading_ctx)
    if err != nil {
      if err.Error() == "EOF" {
        return on_error()
      }
      WriteErr(wf, error_prefix, err.Error())
      atomic.AddUint32(&ctx.counter_err, 1)
      return on_error()
    }
    cmd := string(args[0])
    if cmd == "QUIT" {
      return on_error();
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
      WriteLine(wf, "+OK In progress")
      time.Sleep(time.Duration(60) * time.Second)
      pprof.StopCPUProfile()
      log.Printf("End of CPU Profiling, output written to %s", fname)
      WriteLine(wf, "+OK")
      return on_error()
    }
    exec_err := handleCommand(args)
    if exec_err != nil {
      WriteErr(wf, error_prefix, exec_err.Error())
      atomic.AddUint32(&ctx.counter_err, 1)
      return on_error()
    } else {
      atomic.AddUint32(&ctx.counter_ok, 1)
    }
  }
}
