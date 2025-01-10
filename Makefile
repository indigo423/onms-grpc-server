PROTOBUF_PATH := ./protobuf

.PHONY deps:
deps:
	command -v go

.PHONY deps-protobuf:
deps-protobuf:
	command -v protoc

.PHONY protobuf:
protobuf: deps-protobuf
	protoc --proto_path=$(PROTOBUF_PATH)/proto --go_out=$(PROTOBUF_PATH) sink-message.proto
	protoc --proto_path=$(PROTOBUF_PATH)/proto --go_out=$(PROTOBUF_PATH) kafka-rpc.proto
	protoc --proto_path=$(PROTOBUF_PATH)/proto --go_out=$(PROTOBUF_PATH) --go-grpc_out=$(PROTOBUF_PATH) ipc.proto

onms-grpc-server: deps
	go build
