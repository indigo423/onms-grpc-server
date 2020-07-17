FROM golang:alpine AS builder
RUN mkdir /app && \
    echo "@edgecommunity http://dl-cdn.alpinelinux.org/alpine/edge/community" >> /etc/apk/repositories && \
    apk update && \
    apk add --no-cache alpine-sdk git librdkafka-dev@edgecommunity
ADD ./ /app/
WORKDIR /app
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -tags musl -a -o onms-grpc-server .

FROM alpine
RUN echo "@edgecommunity http://dl-cdn.alpinelinux.org/alpine/edge/community" >> /etc/apk/repositories && \
    apk update && \
    apk add --no-cache bash librdkafka@edgecommunity && \
    rm -rf /var/cache/apk/* && \
    addgroup -S onms && adduser -S -G onms onms
COPY --from=builder /app/onms-grpc-server /onms-grpc-server
COPY ./docker-entrypoint.sh /
USER onms
LABEL maintainer="Alejandro Galue <agalue@opennms.org>" \
      name="OpenNMS gRPC Server"
ENTRYPOINT [ "/docker-entrypoint.sh" ]
