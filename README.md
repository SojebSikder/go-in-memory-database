In-Memory database like Redis. Created using Go

# Install
Run the server
```
go run .
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

# Supported commands
- PING
- SET
- GET
- HSET
- HGET
- HGETALL