In-Memory database like Redis. Created using Go

# Install
Run the server
```
go run .
```
we can specify the port
```
go run . --port 6380
```
To test the database,
Run redis cli on docker
```
docker run --rm -it redis redis-cli -h host.docker.internal
```

Now we can run redis command on redis cli:
```
set name sojeb
```
# Build
```
./build.sh
```

# Supported commands
- PING
- SET
- GET
- HSET
- HGET
- HGETALL
- HDEL
- HLEN
- FLUSHALL
- DEL
- INFO
- INCR
- DECR