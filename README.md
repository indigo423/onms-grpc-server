OpenNMS IPC gRPC Server
====

This is a [Go](https://golang.org/) version of the [Java-based](https://github.com/OpenNMS/grpc-server) gRPC-IPC Server.

The gRPC IPC Server acts as a bridge between minions running gRPC IPC strategy and OpenNMS running Kafka strategy.

## Compile:

Make sure to have Go 1.14 installed on your system.

```
go build
```

## Compile with Docker:

```
docker build -t agalue/onms-grpc-server .
```

## Prerequisites

* Minions running GRPC strategy.
* OpenNMS running Kafka strategy with single topic.

## Start the server

```bash
./onms-grpc-server \
  -instance-id Apex \
  -tls-enabled \
  -tls-key /grpc/key.pem \
  -tls-cert /grpc/cert.pem \
  -port 8990 \
  -max-buffer-size 10485760 \
  -producer-cfg acks=1 \
  -bootstrap kafka1.example.com:9092
```

Use `--help` for more details.

## Start the server using Docker

```bash
docker run -d --name grpc-server \
  -e INSTANCE_ID=Apex \
  -e TLS_ENABLED=true \
  -e TLS_KEY=/grpc/key.pem \
  -e TLS_CERT=/grpc/cert.pem \
  -e PORT=8990 \
  -e MAX_BUFFER_SIZE=10485760 \
  -e PRODUCER_ACKS=1 \
  -e BOOTSTRAP_SERVER kafka1.example.com:9092
  -v $(pwd)/grpc:/grpc
  agalue/onms-grpc-server
```


