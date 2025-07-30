package internal

import (
	"fmt"
	"strconv"
	"sync"
)

var Handlers = map[string]func([]Value) Value{
	"PING":     ping,
	"SET":      set,
	"GET":      get,
	"HSET":     hset,
	"HGET":     hget,
	"HGETALL":  hgetall,
	"HDEL":     hdel,
	"HLEN":     hlen,
	"FLUSHALL": flushall,
	"DEL":      del,
	"INFO":     info,
	"INCR":     incr,
	"DECR":     decr,
}

func ping(args []Value) Value {
	if len(args) == 0 {
		return Value{typ: "string", str: "PONG"}
	}

	return Value{typ: "string", str: args[0].bulk}
}

var SETs = map[string]string{}
var SETsMu = sync.RWMutex{}

func set(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'set' command"}
	}

	key := args[0].bulk
	value := args[1].bulk

	SETsMu.Lock()
	SETs[key] = value
	SETsMu.Unlock()

	return Value{typ: "string", str: "OK"}
}

func get(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'get' command"}
	}

	key := args[0].bulk

	SETsMu.RLock()
	value, ok := SETs[key]
	SETsMu.RUnlock()

	if !ok {
		return Value{typ: "null"}
	}

	return Value{typ: "bulk", bulk: value}
}

var HSETs = map[string]map[string]string{}
var HSETsMu = sync.RWMutex{}

func hset(args []Value) Value {
	if len(args) != 3 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hset' command"}
	}

	hash := args[0].bulk
	key := args[1].bulk
	value := args[2].bulk

	HSETsMu.Lock()
	if _, ok := HSETs[hash]; !ok {
		HSETs[hash] = map[string]string{}
	}
	HSETs[hash][key] = value
	HSETsMu.Unlock()

	return Value{typ: "string", str: "OK"}
}

func hget(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hget' command"}
	}

	hash := args[0].bulk
	key := args[1].bulk

	HSETsMu.RLock()
	value, ok := HSETs[hash][key]
	HSETsMu.RUnlock()

	if !ok {
		return Value{typ: "null"}
	}

	return Value{typ: "bulk", bulk: value}
}

func hgetall(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hgetall' command"}
	}

	hash := args[0].bulk

	HSETsMu.RLock()
	value, ok := HSETs[hash]
	HSETsMu.RUnlock()

	if !ok {
		return Value{typ: "null"}
	}

	values := []Value{}
	for k, v := range value {
		values = append(values, Value{typ: "bulk", bulk: k})
		values = append(values, Value{typ: "bulk", bulk: v})
	}

	return Value{typ: "array", array: values}
}

// hdel command which deletes a key-value pair in the hash
func hdel(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hdel' command"}
	}

	hash := args[0].bulk
	key := args[1].bulk

	HSETsMu.Lock()
	delete(HSETs[hash], key)
	HSETsMu.Unlock()

	return Value{typ: "string", str: "OK"}
}

// hlen command which returns the number of key-value pairs in the hash
func hlen(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hlen' command"}
	}

	hash := args[0].bulk

	HSETsMu.RLock()
	defer HSETsMu.RUnlock()

	hashMap, exists := HSETs[hash]
	if !exists {
		return Value{typ: "integer", num: 0} // Return 0 if hash does not exist
	}

	return Value{typ: "integer", num: len(hashMap)}
}

// flushall command which clears all the key-value pairs in the SET and the hash
func flushall(args []Value) Value {
	// clear all the key-value pairs in the SET
	SETsMu.Lock()
	SETs = map[string]string{}
	SETsMu.Unlock()

	// clear all the key-value pairs in the hash
	HSETsMu.Lock()
	HSETs = map[string]map[string]string{}
	HSETsMu.Unlock()

	return Value{typ: "string", str: "OK"}
}

// del command which deletes a key-value pair in the SET or the hash
func del(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'del' command"}
	}

	key := args[0].bulk

	SETsMu.Lock()
	delete(SETs, key)
	SETsMu.Unlock()

	HSETsMu.Lock()
	delete(HSETs, key)
	HSETsMu.Unlock()

	return Value{typ: "string", str: "OK"}
}

// info command which shows stats about the server
func info(args []Value) Value {
	if len(args) != 0 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'info' command"}
	}

	SETsMu.RLock()
	totalKeys := len(SETs)
	SETsMu.RUnlock()

	HSETsMu.RLock()
	totalHashes := len(HSETs)
	totalFields := 0
	for _, m := range HSETs {
		totalFields += len(m)
	}
	HSETsMu.RUnlock()

	infoStr := fmt.Sprintf(
		"# Server Info\nkeys:%d\nhashes:%d\nfields:%d\n",
		totalKeys, totalHashes, totalFields,
	)

	return Value{typ: "bulk", bulk: infoStr}
}

// incr command which increments the value of a key in the SET
func incr(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'incr' command"}
	}

	key := args[0].bulk

	SETsMu.Lock()
	value, ok := SETs[key]
	SETsMu.Unlock()

	if !ok {
		SETs[key] = "1"
		return Value{typ: "integer", num: 1}
	}

	num, err := strconv.Atoi(value)
	if err != nil {
		return Value{typ: "error", str: "ERR value is not an integer"}
	}

	num++
	SETs[key] = strconv.Itoa(num)
	return Value{typ: "integer", num: num}
}

// decr command which decrements the value of a key in the SET
func decr(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'decr' command"}
	}

	key := args[0].bulk

	SETsMu.Lock()
	value, ok := SETs[key]
	SETsMu.Unlock()

	if !ok {
		SETs[key] = "0"
		return Value{typ: "integer", num: 0}
	}

	num, err := strconv.Atoi(value)
	if err != nil {
		return Value{typ: "error", str: "ERR value is not an integer"}
	}

	num--
	SETs[key] = strconv.Itoa(num)
	return Value{typ: "integer", num: num}
}
