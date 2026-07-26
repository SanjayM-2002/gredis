# Gredis

An in-memory key-value store written in Go, compatible with the Redis RESP protocol.

Gredis runs a single-threaded event loop built on I/O multiplexing (`kqueue` on macOS, `epoll` on Linux), so one thread serves many concurrent clients. It supports pipelining, key expiration (lazy and active), approximated LRU eviction, AOF rewrite, `MULTI`/`EXEC` transactions and graceful shutdown. Any Redis client, including `redis-cli`, can connect to it.

## Getting started

Requires Go 1.21+.

```bash
go run .
```

The server listens on `0.0.0.0:6363` by default. You can override the host and port with flags:

```bash
go run . --host 127.0.0.1 --port 7000
```

Build a binary:

```bash
go build -o bin/gredis .
./bin/gredis
```

`Ctrl+C` (SIGINT) or SIGTERM triggers a graceful shutdown (see [Graceful shutdown](#graceful-shutdown)).

## Connecting

### redis-cli

```bash
redis-cli -p 6363
```

```
127.0.0.1:6363> SET name sanjay
OK
127.0.0.1:6363> GET name
"sanjay"
127.0.0.1:6363> INCR visits
(integer) 1
```

If Redis runs in Docker, you can use the `redis-cli` inside the container:

```bash
docker exec -it <container> redis-cli -h host.docker.internal -p 6363
```

### Raw RESP over TCP

The server only accepts RESP-encoded input, so you can talk to it with `nc` by writing RESP by hand:

```bash
printf '*1\r\n$4\r\nPING\r\n' | nc localhost 6363
```

```
+PONG
```

## RESP protocol

Clients and server exchange data using **RESP** (REdis Serialization Protocol), a text-based, length-prefixed protocol. Each value starts with a one-byte type prefix and ends with CRLF (`\r\n`).

| Prefix | Type | Example | Decoded value |
|---|---|---|---|
| `+` | Simple string | `+OK\r\n` | `"OK"` |
| `-` | Error | `-ERR syntax error\r\n` | error `ERR syntax error` |
| `:` | Integer | `:42\r\n` | `42` |
| `$` | Bulk string | `$6\r\nsanjay\r\n` | `"sanjay"` (6 bytes) |
| `$-1` | Null bulk string | `$-1\r\n` | `nil` |
| `*` | Array | `*2\r\n:1\r\n:2\r\n` | `[1, 2]` |

A client sends every command as an **array of bulk strings**. For example, `SET name sanjay` is encoded as:

```
*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$6\r\nsanjay\r\n
```

Because bulk strings are length-prefixed, they are binary-safe: values can contain spaces, CRLF or any other bytes.

The decoder and encoder are in [`core/resp.go`](core/resp.go). Unit tests are in [`core/resp_test.go`](core/resp_test.go).

## Supported commands

| Command | Description | Reply |
|---|---|---|
| `PING [message]` | Health check | `+PONG`, or `message` as a bulk string |
| `SET key value [EX seconds]` | Store a string, optionally with a TTL | `+OK` |
| `GET key` | Read a value | bulk string, or null if the key is missing or expired |
| `DEL key [key ...]` | Delete keys | integer: number of keys deleted |
| `TTL key` | Remaining time to live, in seconds | integer; `-1` = no TTL, `-2` = key doesn't exist |
| `EXPIRE key seconds` | Set a TTL on an existing key | `:1` if set, `:0` if the key doesn't exist |
| `INCR key` | Increment an integer value by 1 (a missing key starts at 0) | integer: the new value |
| `MULTI` | Start a transaction | `+OK` |
| `EXEC` | Execute queued commands | array of replies, one per queued command |
| `DISCARD` | Drop queued commands | `+OK` |
| `INFO` | Keyspace stats | bulk string: `# Keyspace` / `db0:keys=N,...` |
| `BGREWRITEAOF` | Rewrite the AOF file from current data | `+OK` |
| `SLEEP seconds` | Block the event loop (for testing shutdown) | `+OK` |
| `LRU` | Run one LRU eviction cycle (for testing) | `+OK` |

`CLIENT` and `LATENCY` are stubs, so monitoring tools like `redis_exporter` can connect. Unknown commands are handled as `PING`.

## Architecture

### Event loop and I/O multiplexing

Gredis uses the same concurrency model as Redis: a **single-threaded event loop** over non-blocking sockets.

1. The listening socket and every client socket are registered with the kernel's multiplexer: `epoll` on Linux, `kqueue` on macOS.
2. The loop blocks in `epoll_wait` / `kevent` until one or more file descriptors are readable.
3. For each ready fd, it either accepts a new connection (the listening socket) or reads, decodes and executes the client's commands, then writes the reply.
4. Back to step 2.

Only one command runs at a time, so the store needs no locks, and every command is atomic.

The OS-specific code is split by Go build constraints (filename suffixes):

- [`server/async_tcp_linux.go`](server/async_tcp_linux.go): `epoll`
- [`server/async_tcp_darwin.go`](server/async_tcp_darwin.go): `kqueue`

### Pipelining

A client can send multiple commands in a single write without waiting for replies. The server decodes every command in the buffer, executes them in order, collects the replies in one buffer, and sends them back in a single `write` syscall.

```bash
printf '*1\r\n$4\r\nPING\r\n*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\n' | nc localhost 6363
```

```
+PONG
+OK
$1
v
```

### Object types and encodings

Each stored value (`Obj`) carries a type and an encoding, packed into one byte: the type in the upper 4 bits, the encoding in the lower 4. Strings get one of three encodings:

| Encoding | When |
|---|---|
| `INT` | the value parses as a 64-bit integer |
| `EMBSTR` | the value is 44 bytes or less |
| `RAW` | anything longer |

`INCR` checks that the encoding is `INT` before operating on the value.

### Key expiration

TTLs are stored in a separate `expires` map. Expired keys are removed in two ways:

- **Passive (lazy)**: on access, an expired key is deleted and treated as missing.
- **Active**: a cron step in the event loop runs about once a second. It samples up to 20 keys and deletes the expired ones, and repeats while at least 25% of a sample had expired. This is the same sampling algorithm Redis uses.

### Eviction

The keyspace is capped at `KeysLimit` keys (100 by default, in [`config/main.go`](config/main.go)). When a write hits the limit, keys are evicted based on `EvictionStrategy`:

| Strategy | Behavior |
|---|---|
| `simple-first` | evict one arbitrary key |
| `allkeys-random` | evict a batch (`EvictionRatio`, 40% by default) of random keys |
| `allkeys-lru` (default) | **approximated LRU** |

Approximated LRU works the way Redis does it: every object stores a 24-bit last-access clock. Each eviction cycle samples a few keys into a small **eviction pool** sorted by idle time, and evicts the keys idle the longest. This avoids maintaining an exact LRU list over all keys.

### Persistence (AOF rewrite)

`BGREWRITEAOF` serializes the whole keyspace to `gredis-master.aof`, as RESP-encoded `SET` commands. These are the same bytes a client would send, so the file can be replayed through the normal command path. A rewrite also runs on shutdown.

The AOF is not loaded on startup yet.

### Transactions

Each connection has its own client state (`core.Client`): a transaction flag and a command queue.

- `MULTI` sets the flag. Later commands are queued and the server replies `+QUEUED`.
- `EXEC` runs the whole queue back to back in the event loop and returns an array of replies. No other client's command can interleave.
- `DISCARD` clears the queue.

```
> MULTI
OK
> SET x 5
QUEUED
> INCR x
QUEUED
> EXEC
1) OK
2) (integer) 6
```

### Graceful shutdown

A signal handler goroutine listens for SIGINT and SIGTERM. The event loop and the handler coordinate through an atomic engine state (`WAITING` → `BUSY` → `WAITING`, or → `SHUTTING_DOWN`):

- If a command is running (`BUSY`), the handler waits for it to finish.
- It then sets `SHUTTING_DOWN`, so the loop cannot pick up new work.
- Finally it rewrites the AOF and exits.

## Project structure

```
main.go                     entry point: flags, signal handling, starts the server
config/main.go              host, port, key limit, eviction strategy, AOF path
server/
  async_tcp_linux.go        event loop (epoll)
  async_tcp_darwin.go       event loop (kqueue)
  sync_tcp.go               reads and decodes commands from a connection
core/
  resp.go                   RESP decoder / encoder
  eval.go                   command handlers and dispatcher
  comm.go                   per-client state and transaction queue
  store.go                  keyspace (map) and expiry map
  object.go                 Obj struct, type and encoding constants
  type_string.go            encoding detection for strings
  typeencoding.go           bit helpers for type/encoding
  expire.go                 expiry checks and active expiration
  eviction.go               eviction strategies, LRU clock
  evictionpool.go           LRU eviction pool
  aof.go                    AOF rewrite
  stats.go                  keyspace stats for INFO
  events.go                 shutdown hook
storm/set/main.go           load generator (concurrent random SETs)
```

## Testing

```bash
go test ./...
```

## Load generation

`storm/set` opens 5 concurrent connections and keeps sending random `SET` commands. It's useful for watching eviction:

```bash
go run storm/set/main.go
```

Watch the key count from another terminal:

```bash
redis-cli -p 6363 INFO
```
