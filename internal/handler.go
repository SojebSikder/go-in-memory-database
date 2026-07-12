package internal

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
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
	"EXPIRE":   expire,
	"TTL":      ttl,
	"PERSIST":  persist,
	"SADD":     sadd,
	// connection handshake
	"HELLO":   hello,
	"CLIENT":  clientCmd,
	"EVALSHA": evalsha,
	"EVAL":    eval,
}

func ping(args []Value) Value {
	if len(args) == 0 {
		return Value{typ: "string", str: "PONG"}
	}

	return Value{typ: "string", str: args[0].bulk}
}

var (
	SETs   = map[string]string{}
	SETsMu = sync.RWMutex{}

	// For TTL
	Expirations   = map[string]int64{}
	ExpirationsMu = sync.RWMutex{}
)

func StartKeyExpiryCleaner() {
	go func() {
		for {
			time.Sleep(1 * time.Second)
			now := time.Now().Unix()

			ExpirationsMu.Lock()
			for k, exp := range Expirations {
				if now > exp {
					// Remove from SETs
					SETsMu.Lock()
					delete(SETs, k)
					SETsMu.Unlock()

					// Remove from HSETs
					HSETsMu.Lock()
					delete(HSETs, k)
					HSETsMu.Unlock()

					// Remove from Sets
					SetsMu.Lock()
					delete(Sets, k)
					SetsMu.Unlock()

					// Remove from expirations
					delete(Expirations, k)
				}
			}
			ExpirationsMu.Unlock()
		}
	}()
}

// checkAndDeleteIfExpired returns true if the key existed but was expired
func checkAndDeleteIfExpired(key string) (*string, bool) {
	ExpirationsMu.RLock()
	expireAt, exists := Expirations[key]
	ExpirationsMu.RUnlock()

	if exists && time.Now().Unix() > expireAt {
		// Key expired, remove it
		SETsMu.Lock()
		delete(SETs, key)
		SETsMu.Unlock()

		HSETsMu.Lock()
		delete(HSETs, key)
		HSETsMu.Unlock()

		SetsMu.Lock()
		delete(Sets, key)
		SetsMu.Unlock()

		ExpirationsMu.Lock()
		delete(Expirations, key)
		ExpirationsMu.Unlock()

		return nil, true
	}

	// Check in SETs
	SETsMu.RLock()
	value, ok := SETs[key]
	SETsMu.RUnlock()
	if ok {
		return &value, false
	}

	// Check in HSETs
	HSETsMu.RLock()
	_, ok = HSETs[key]
	HSETsMu.RUnlock()
	if ok {
		return nil, false // key exists as a hash, no single string value
	}

	// Check in Sets
	SetsMu.RLock()
	_, ok = Sets[key]
	SetsMu.RUnlock()
	if ok {
		return nil, false // key exists as a set, no single string value
	}

	return nil, false // key does not exist
}

// hello handles the Redis connection handshake (RESP3 negotiation)
func hello(args []Value) Value {
	return Value{
		typ: "array",
		array: []Value{
			{typ: "string", str: "server"}, {typ: "string", str: "smem"},
			{typ: "string", str: "version"}, {typ: "string", str: "0.1.0"},
			{typ: "string", str: "proto"}, {typ: "integer", num: 2},
			{typ: "string", str: "mode"}, {typ: "string", str: "standalone"},
		},
	}
}

// clientCmd handles connection management subcommands like CLIENT SETNAME, CLIENT LIST, etc.
func clientCmd(args []Value) Value {
	if len(args) > 0 {
		subCmd := strings.ToUpper(args[0].bulk)

		if subCmd == "SETNAME" || subCmd == "TRACKING" {
			return Value{typ: "string", str: "OK"}
		}
	}
	return Value{typ: "string", str: "OK"}
}

// evalsha handles executing cached Lua scripts.
// returning an NOSCRIPT error forces the client to fall back to standard EVAL (sending the raw script)
func evalsha(args []Value) Value {
	return Value{typ: "error", str: "NOSCRIPT No matching script. Please use EVAL."}
}

// eval stub that returns an empty array or null to satisfy basic script checks
func eval(args []Value) Value {
	if len(args) < 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'eval' command"}
	}

	fmt.Printf("Client attempted to run Lua Script: %s\n", args[0].bulk)

	// return an empty array response
	return Value{typ: "array", array: []Value{}}
}

func set(args []Value) Value {
	if len(args) < 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'set' command"}
	}

	key := args[0].bulk
	value := args[1].bulk

	var expireSeconds int64 = 0

	// Check for optional EX argument
	if len(args) == 4 {
		option := args[2].bulk
		if strings.ToLower(option) != "ex" {
			return Value{typ: "error", str: "ERR syntax error"}
		}

		secs, err := strconv.ParseInt(args[3].bulk, 10, 64)
		if err != nil || secs <= 0 {
			return Value{typ: "error", str: "ERR invalid expire time"}
		}
		expireSeconds = secs
	} else if len(args) != 2 {
		return Value{typ: "error", str: "ERR syntax error"}
	}

	// Store value
	SETsMu.Lock()
	SETs[key] = value
	SETsMu.Unlock()

	// Handle expiration if EX is provided
	if expireSeconds > 0 {
		ExpirationsMu.Lock()
		Expirations[key] = time.Now().Unix() + expireSeconds
		ExpirationsMu.Unlock()
	} else {
		ExpirationsMu.Lock()
		delete(Expirations, key)
		ExpirationsMu.Unlock()
	}

	return Value{typ: "string", str: "OK"}
}

func get(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'get' command"}
	}

	key := args[0].bulk

	// Check expiration
	value, expired := checkAndDeleteIfExpired(key)
	if expired || value == nil {
		return Value{typ: "null"}
	}

	return Value{typ: "bulk", bulk: *value}
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
	defer HSETsMu.RUnlock()

	hashMap, exists := HSETs[hash]
	if !exists {
		return Value{typ: "null"}
	}

	value, ok := hashMap[key]
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
	defer HSETsMu.RUnlock()

	hashMap, exists := HSETs[hash]
	if !exists {
		return Value{typ: "null"}
	}

	values := make([]Value, 0, len(hashMap)*2)
	for k, v := range hashMap {
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
	defer HSETsMu.Unlock()

	if _, ok := HSETs[hash]; ok {
		delete(HSETs[hash], key)
		// delete the hash itself if it becomes empty
		if len(HSETs[hash]) == 0 {
			delete(HSETs, hash)
		}
	}

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

var Sets = map[string]map[string]bool{}
var SetsMu = sync.RWMutex{}

// sadd command adds the specified members to the set stored at key.
func sadd(args []Value) Value {
	if len(args) < 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'sadd' command"}
	}

	key := args[0].bulk

	SetsMu.Lock()
	defer SetsMu.Unlock()

	if _, ok := Sets[key]; !ok {
		Sets[key] = map[string]bool{}
	}

	addedCount := 0
	for i := 1; i < len(args); i++ {
		member := args[i].bulk
		if !Sets[key][member] {
			Sets[key][member] = true
			addedCount++
		}
	}

	return Value{typ: "integer", num: addedCount}
}

// flushall command which clears all the key-value pairs in the SET, the hash, and the sets
func flushall(args []Value) Value {
	// clear all the key-value pairs in the SET
	SETsMu.Lock()
	SETs = map[string]string{}
	SETsMu.Unlock()

	// clear all the key-value pairs in the hash
	HSETsMu.Lock()
	HSETs = map[string]map[string]string{}
	HSETsMu.Unlock()

	// clear all the key-value pairs in the sets
	SetsMu.Lock()
	Sets = map[string]map[string]bool{}
	SetsMu.Unlock()

	// clear expirations
	ExpirationsMu.Lock()
	Expirations = map[string]int64{}
	ExpirationsMu.Unlock()

	return Value{typ: "string", str: "OK"}
}

// del command which deletes a key-value pair in the SET, the hash, or the set
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

	SetsMu.Lock()
	delete(Sets, key)
	SetsMu.Unlock()

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

	SetsMu.RLock()
	totalSets := len(Sets)
	totalMembers := 0
	for _, m := range Sets {
		totalMembers += len(m)
	}
	SetsMu.RUnlock()

	infoStr := fmt.Sprintf(
		"# Server Info\nkeys:%d\nhashes:%d\nfields:%d\nsets:%d\nmembers:%d\n",
		totalKeys, totalHashes, totalFields, totalSets, totalMembers,
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
	defer SETsMu.Unlock()

	value, ok := SETs[key]
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
	defer SETsMu.Unlock()

	value, ok := SETs[key]
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

// EXPIRE sets a timeout on a key
func expire(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'expire' command"}
	}

	key := args[0].bulk
	seconds, err := strconv.ParseInt(args[1].bulk, 10, 64)
	if err != nil || seconds <= 0 {
		return Value{typ: "error", str: "ERR invalid expire time"}
	}

	// check if key exists in either SETs, HSETs, or Sets
	SETsMu.RLock()
	_, existsSET := SETs[key]
	SETsMu.RUnlock()

	HSETsMu.RLock()
	_, existsHSET := HSETs[key]
	HSETsMu.RUnlock()

	SetsMu.RLock()
	_, existsSet := Sets[key]
	SetsMu.RUnlock()

	if !existsSET && !existsHSET && !existsSet {
		return Value{typ: "integer", num: 0} // Key does not exist
	}

	ExpirationsMu.Lock()
	Expirations[key] = time.Now().Unix() + seconds
	ExpirationsMu.Unlock()

	return Value{typ: "integer", num: 1}
}

// TTL returns the remaining time to live of a key
func ttl(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'ttl' command"}
	}

	key := args[0].bulk

	ExpirationsMu.RLock()
	expireAt, exists := Expirations[key]
	ExpirationsMu.RUnlock()

	// Check if key exists in SETs, HSETs, or Sets
	SETsMu.RLock()
	_, existsSET := SETs[key]
	SETsMu.RUnlock()

	HSETsMu.RLock()
	_, existsHSET := HSETs[key]
	HSETsMu.RUnlock()

	SetsMu.RLock()
	_, existsSet := Sets[key]
	SetsMu.RUnlock()

	if !existsSET && !existsHSET && !existsSet {
		return Value{typ: "integer", num: -2} // Key does not exist
	}

	if !exists {
		return Value{typ: "integer", num: -1} // Key exists but has no expiration
	}

	remaining := expireAt - time.Now().Unix()
	if remaining < 0 {
		return Value{typ: "integer", num: -2} // Key expired
	}

	return Value{typ: "integer", num: int(remaining)}
}

// PERSIST removes the expiration from a key
func persist(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'persist' command"}
	}

	key := args[0].bulk

	// Check if key exists in either SETs, HSETs, or Sets
	SETsMu.RLock()
	_, existsSET := SETs[key]
	SETsMu.RUnlock()

	HSETsMu.RLock()
	_, existsHSET := HSETs[key]
	HSETsMu.RUnlock()

	SetsMu.RLock()
	_, existsSet := Sets[key]
	SetsMu.RUnlock()

	if !existsSET && !existsHSET && !existsSet {
		return Value{typ: "integer", num: 0} // Key does not exist
	}

	ExpirationsMu.Lock()
	defer ExpirationsMu.Unlock()

	if _, exists := Expirations[key]; !exists {
		return Value{typ: "integer", num: 0} // Key had no expiration
	}

	delete(Expirations, key)
	return Value{typ: "integer", num: 1}
}
