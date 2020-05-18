<p align="center">
<img 
    src="logo.png" 
    width="336" height="75" border="0" alt="REDCON">
<br>
<a href="https://travis-ci.org/smallnest/redcon"><img src="https://img.shields.io/travis/smallnest/redcon.svg?style=flat-square" alt="Build Status"></a>
<a href="https://godoc.org/github.com/smallnest/redcon"><img src="https://img.shields.io/badge/api-reference-blue.svg?style=flat-square" alt="GoDoc"></a>
</p>

<p align="center">Fast Redis compatible server framework for Go</p>

cloned from [tidwall/redcon](https://github.com/tidwall/redcon) but have add some features independently.


Features
--------
- Create a [Fast](#benchmarks) custom Redis compatible server in Go
- Simple interface. One function `ListenAndServe` and two types `Conn` & `Command`
- Support for pipelining and telnet commands
- Works with Redis clients such as [redigo](https://github.com/garyburd/redigo), [redis-py](https://github.com/andymccurdy/redis-py), [node_redis](https://github.com/NodeRedis/node_redis), and [jedis](https://github.com/xetorthio/jedis)
- [TLS Support](#tls-example)

Installing
----------

```
go get -u github.com/smallnest/redcon
```

Example
-------

Here's a full example of a Redis clone that accepts:

- SET key value
- GET key
- DEL key
- PING
- QUIT

You can run this example from a terminal:


```go
package main

import (
	"log"
	"strings"
	"sync"

	"github.com/smallnest/redcon"
)

var addr = ":6380"

func main() {
	var mu sync.RWMutex
	var items = make(map[string][]byte)
	go log.Printf("started server at %s", addr)
	err := redcon.ListenAndServe(addr,
		func(conn redcon.Conn, cmd redcon.Command) {
			switch strings.ToLower(string(cmd.Args[0])) {
			default:
				conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
			case "ping":
				conn.WriteString("PONG")
			case "quit":
				conn.WriteString("OK")
				conn.Close()
			case "set":
				if len(cmd.Args) != 3 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				mu.Lock()
				items[string(cmd.Args[1])] = cmd.Args[2]
				mu.Unlock()
				conn.WriteString("OK")
			case "get":
				if len(cmd.Args) != 2 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				mu.RLock()
				val, ok := items[string(cmd.Args[1])]
				mu.RUnlock()
				if !ok {
					conn.WriteNull()
				} else {
					conn.WriteBulk(val)
				}
			case "del":
				if len(cmd.Args) != 2 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				mu.Lock()
				_, ok := items[string(cmd.Args[1])]
				delete(items, string(cmd.Args[1]))
				mu.Unlock()
				if !ok {
					conn.WriteInt(0)
				} else {
					conn.WriteInt(1)
				}
			}
		},
		func(conn redcon.Conn) bool {
			// use this function to accept or deny the connection.
			// log.Printf("accept: %s", conn.RemoteAddr())
			return true
		},
		func(conn redcon.Conn, err error) {
			// this is called when the connection has been closed
			// log.Printf("closed: %s, err: %v", conn.RemoteAddr(), err)
		},
	)
	if err != nil {
		log.Fatal(err)
	}
}
```

License
-------
Redcon source code is available under the MIT [License](/LICENSE).
