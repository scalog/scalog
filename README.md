# scalog
reimplementing scalog from scratch

## Build Scalog

Run the `go build` command to build Scalog, and `go test -v ./...` to run
unit-tests.

## Run the code

To run the server side code, we have to install `goreman` by running
```go
go get github.com/mattn/goreman
```

Use `goreman start` to run the server side code. The goreman configuration is
in `Procfile` and the Scalog configuration file is in `.scalog.yaml`

To run the client, we should use
```go
./scalog client --config .scalog.yaml
```

After that, we can use command `append [record]` to append a record to the
log, and use `read [GlobalSequenceNumber] [ShardID]` to read a record.
