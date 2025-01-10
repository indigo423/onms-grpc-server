FROM golang:alpine AS builder
WORKDIR /app
ENV GRPC_HEALTH_PROBE_VERSION=v0.4.36
ADD ./ /app/
RUN echo "@edgecommunity http://dl-cdn.alpinelinux.org/alpine/edge/community" >> /etc/apk/repositories && \
    apk update && \
    apk add --no-cache alpine-sdk git cyrus-sasl-dev librdkafka-dev@edgecommunity && \
    wget -qOgrpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64 && \
    CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -tags static_all,netgo,musl -o onms-grpc-server .

FROM alpine
COPY --from=builder /app/onms-grpc-server /bin
COPY --from=builder /app/grpc_health_probe /bin
COPY ./docker-entrypoint.sh /
RUN apk update && \
    apk add --no-cache bash tzdata && \
    rm -rf /var/cache/apk/* && \
    addgroup -S onms && \
    adduser -S -G onms onms && \
    chmod +x /bin/*grpc*
USER onms
LABEL maintainer="Alejandro Galue <agalue@opennms.org>" name="OpenNMS gRPC Server"
ENTRYPOINT [ "/docker-entrypoint.sh" ]
