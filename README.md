OpenNMS IPC gRPC Server [![Go Report Card](https://goreportcard.com/badge/github.com/agalue/onms-grpc-server)](https://goreportcard.com/report/github.com/agalue/onms-grpc-server)
====

This is a [Go](https://golang.org/) version of the [Java-based](https://github.com/OpenNMS/grpc-server) gRPC-IPC Server.

The gRPC IPC Server acts as a bridge between minions running gRPC IPC strategy and OpenNMS running Kafka strategy.

## Compile:

To compile directly on your machine, Make sure to have at least Go 1.16 and the latest [librdkafka](https://github.com/edenhill/librdkafka) installed on your system.

```
go build
```

## Compile with Docker:

```
docker build -t agalue/onms-grpc-server .
```

## Prerequisites

* Minions running GRPC strategy.
* OpenNMS running Kafka strategy with `single-topic` enabled.

## Start the server

Make sure the latest [librdkafka](https://github.com/edenhill/librdkafka) installed on your system.

```bash
./onms-grpc-server \
  -instance-id Apex \
  -tls-enabled \
  -tls-key /grpc/key.pem \
  -tls-cert /grpc/cert.pem \
  -port 8990 \
  -http-port 2112 \
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
  -e TLS_SERVER_KEY=/grpc/key.pem \
  -e TLS_SERVER_CERT=/grpc/cert.pem \
  -e TLS_CLIENT_CA_CERT=/grpc/ca.pem \
  -e PORT=8990 \
  -e HTTP_PORT=2112 \
  -e MAX_BUFFER_SIZE=10485760 \
  -e PRODUCER_ACKS=1 \
  -e BOOTSTRAP_SERVER kafka1.example.com:9092
  -v $(pwd)/grpc:/grpc
  agalue/onms-grpc-server
```

> The HTTP port is for accessing metrics in Prometheus format

## Pending

* Implement certificate-based client authentication.
