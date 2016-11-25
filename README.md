# Aerodis

[![Aerodis](https://goreportcard.com/badge/github.com/bpaquet/aerodis)](https://goreportcard.com/report/github.com/bpaquet/aerodis)

Use Aerospike through a Redis interface.

# Why

Redis is a great product, but can be difficult to scale.
Redis cluster solves some issues, but still uses one thread for each server.
Aerospike is natively distributed, multi-threaded and has excellent performance.

Aerodis implements most of Redis primitives above an Aerospike Cluster.

Architecture: Application (which use Redis driver) => Aerodis => Aerospike cluster.

I'm using Aerodis from a big PHP application which have used Redis from a long time.

I have deployed one instance of Aerodis on each PHP server, and have achieved 50k queries per second on each server,
and have reached 500k queries per second on five Aerospike nodes.

# Why in Go ?

First attempt was to implement the [Redis interface in PHP, using the PHP Aerospike drivers](https://github.com/bpaquet/aerospike_redis_php). This solution has two problems:
* the Aerospike PHP driver has a slow update cycle, and some problems (this [one](https://github.com/aerospike/aerospike-client-php/issues/111) for example).
* PHP is still a mono-thread multi-process, so some optimizations cannot be done easily (like an in-memory cache for example).

Why use Go to write a new proxy ? Because it's easier than C, and Go Aerospike driver has good performance.

# What is implemented ?

Connection between application / Aerodis : ``tcp`` or ``unix socket``.

Multi-database: Aerodis does not manage multi database on one socket, but can manage multiple socket to manage multiple databases.

## Implemented functions:
* key / value: ``get`` / ``set`` / ``setex`` / ``setnx`` / ``del`` / ``incr`` / ``decr`` / ``incrby`` / ``decrby``
* ttl: ``expire`` / ``ttl``
* array: ``lpush`` / ``rpush`` / ``rpop`` / ``lpop`` / ``llen`` / ``ltrim`` / ``lRange``
* flush: ``flushdb`` (using scan, poor performance)
* map: ``hget`` / ``hset`` / ``hmget`` / ``hmset`` / ``hincrby``/ ``hdel``/ ``hgetall`` (see below)
<<<<<<< HEAD
* transaction: ``exec``/ ``multi``. Supported for compatibility, but commands are executed between ``exec``/``multi``.
Answers are send when calling ``multi``, like with Redis.
=======
* transaction: ``exec``/ ``multi``. Supported for compatibility, but command are executed even between ``exec``/``multi``.
Answers are dispatched when calling ``multi``, like with Redis.
>>>>>>> origin/master

## Added functions:

Some functions which do not exist in Aerospike are implemented:
* ``rpushex``/ ``lpushex``: ``rpush`` / ``lpush`` with a TTL. TTL is the last params.
* ``setnex``: ``setex``, but only if the entry does not exists.
* `hincrbyex`: ``hincrby`` with a TTL. TTL is the last params.
* ``hmincrybyex``: mutiple hincrby in the same call. Syntax: ``key ttl [field1 incr1] [field2 incr2]``
Note: modification of the PHP driver is needed to use these functions from PHP: [v5.x](https://github.com/bpaquet/phpredis/tree/2.2.7_patched) and [v7](https://github.com/bpaquet/phpredis/tree/3.0.0_patched).

## Map functions:
There are two implementations of map:

### Standard map implementation

Each Redis map is stored into Aerospike in a single entry. A Redis field corresponds to an Aerospike bin.

Limitations:
* Aerospike bin name is limited to 14 chars
* Total number of bin names is limited in Aerospike, so you cannot use random field names.

The main advantage is performance.

This is the default mode.

### Expanded map implementation

This implementation allows use random field names. For each key, Aerodis computes a secondary key and stores it in Aerospike.
The key / field value is stored in Aerospike under the Aerospike key: ``secondary_key`` + ``field``.

There is some limitations:
* Each Redis access requires two Aerospike accesses. A cache can be added, I achieve a hit ratio above 90% on my platform.
* TTL management is complicated. You have to specify the max TTL for all entries. So you cannot use this mode without TTL.
* ``hGetAll`` use a [secondary Aerospike index](http://www.aerospike.com/docs/architecture/secondary-index.html), so performance can be poor.

# How to use it:

## On Aerospike:

* Install the (``redis.lua``)[redis.lua] module: ``register module 'redis.lua'``
* For expanded map, create the secondary index: ``create index expanded_map_xxx_yyy on xxx.yyy (m) STRING'``,
where ``xxx.yyy`` is the namespace / set which will use expanded map.

## Compile aerodis

* Aerodis is written in GO, and tested with Go 1.7
* Clone aerodis in ``$GOPATH/src``. Aerodis should be in ``$GOPATH/src/aerodis``.
* Install dependencies, using [trash](https://github.com/rancher/trash): ``trash``.
Dependencies will be installed in ``$GOPATH/src/aerodis/vendor``
* Compile aerodis: ``go build``

## Run and configure aerodis

To launch aerodis, use: ``aerodis --config_file config.json [--ns redis]``.

Example of config file:
````json
{
  "aerospike_ips": [
    "192.168.56.80"
  ],
  "sets": [{
    "proto": "tcp",
    "listen": "0.0.0.0:6379",
    "set": "set1"
  }, {
    "proto": "unix",
    "listen": "/tmp/my_socket",
    "set": "expanded_map",
    "expanded_map": 1,
    "cache_size": 2097152
  }]
}
````

* ``aerospike_ips``: Add some Aerospike ips to start the Aerospike connection. Usually two ips are enough.
* This config file will
** open a Redis interface on the TCP port ``6379``, using standard map implementation,
which will be backed on ``redis.set1`` in Aerospike.
You can specify the namespace to use in the command line.
** open a Redis interface in the unix socket ``/tmp/my_socket``, using expanded_map map implementation, with a 2M cache.
Do not forget to create the secondary index on the set ``redis.expanded_map``in Aerospike.

## Tests

Aerodis has been heavily tested with a PHP application. It should work from any language.
Please feel free to open an issue if you discover problems.

Tests are only integration tests, and are written in PHP.

## Undocumented functions

* Statsd statistics
* Write back
* Backward compatibility

## License

[Apache 2 license](license.txt)












