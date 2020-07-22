Test environment based on Docker
====

This environment starts all the infrastructure required to test the Go version of the gRPC server.

There are going to be 3 Minions using gRPC to talk with OpenNMS; two in location Durham, and one in location Raleigh.

A Kafka broker is in between, required when using an external gRPC server.

```bash
docker-compose up -d
```

The above command will build an image for the gRPC server and use Horizon 26.1.2 with the latest Kafka and PostgreSQL for the rest of the environment.
