package main

import (
	"context"
	"testing"
	"time"

	"github.com/agalue/onms-grpc-server/protobuf/ipc"
	"github.com/agalue/onms-grpc-server/protobuf/rpc"
	"github.com/agalue/onms-grpc-server/protobuf/sink"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc/metadata"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"gotest.tools/assert"
)

// Mock Interfaces

type mockConsumer struct {
	msgChannel chan *kafka.Message
}

func (mock *mockConsumer) Send(msg *kafka.Message) {
	mock.msgChannel <- msg
}

func (mock *mockConsumer) Subscribe(topic string, rebalanceCb kafka.RebalanceCb) error {
	return nil
}

func (mock *mockConsumer) Poll(timeoutMs int) (event kafka.Event) {
	return <-mock.msgChannel
}

func (mock *mockConsumer) CommitMessage(m *kafka.Message) ([]kafka.TopicPartition, error) {
	return []kafka.TopicPartition{}, nil
}

func (mock *mockConsumer) Close() error {
	close(mock.msgChannel)
	return nil
}

type mockProducer struct {
	eventChannel chan kafka.Event
	messages     []*kafka.Message
}

func (mock *mockProducer) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	mock.messages = append(mock.messages, msg)
	return nil
}

func (mock *mockProducer) Events() chan kafka.Event {
	return mock.eventChannel
}

func (mock *mockProducer) Close() {
	close(mock.eventChannel)
}

var mockBuffer map[string][]interface{}

type mockRPCStreamingServer struct {
	id string
}

func (mock *mockRPCStreamingServer) Send(*ipc.RpcRequestProto) error {
	return nil
}

func (mock *mockRPCStreamingServer) Recv() (*ipc.RpcResponseProto, error) {
	return nil, nil
}

func (mock *mockRPCStreamingServer) SetHeader(metadata.MD) error {
	return nil
}

func (mock *mockRPCStreamingServer) SendHeader(metadata.MD) error {
	return nil
}

func (mock *mockRPCStreamingServer) SetTrailer(metadata.MD) {
}

func (mock *mockRPCStreamingServer) Context() context.Context {
	return context.WithValue(context.Background(), "id", mock.id)
}

func (mock *mockRPCStreamingServer) SendMsg(m interface{}) error {
	if mockBuffer == nil {
		mockBuffer = make(map[string][]interface{})
	}
	mockBuffer[mock.id] = append(mockBuffer[mock.id], m)
	return nil
}

func (mock *mockRPCStreamingServer) RecvMsg(m interface{}) error {
	return nil
}

// Tests

func TestPropertiesFlag(t *testing.T) {
	properties := PropertiesFlag{}
	properties.Set("a")
	properties.Set("b")
	assert.Equal(t, 2, len(properties))
	assert.Equal(t, "a, b", properties.String())
}

func TestRoundRobinHandlerMap(t *testing.T) {
	handlerMap := new(RoundRobinHandlerMap)
	handlerMap.Add("001", &mockRPCStreamingServer{"H1"})
	handlerMap.Add("002", &mockRPCStreamingServer{"H2"})
	handlerMap.Add("003", &mockRPCStreamingServer{"H3"})
	// Verify round-robin logic
	h := handlerMap.Get()
	assert.Equal(t, 1, handlerMap.current)
	assert.Equal(t, "H2", h.Context().Value("id"))
	h = handlerMap.Get()
	assert.Equal(t, 2, handlerMap.current)
	assert.Equal(t, "H3", h.Context().Value("id"))
	h = handlerMap.Get()
	assert.Equal(t, 0, handlerMap.current)
	assert.Equal(t, "H1", h.Context().Value("id"))
	// Verify contains logic
	assert.Assert(t, handlerMap.Contains("001"))
	assert.Assert(t, !handlerMap.Contains("004"))
}

func TestUpdateKafkaConfig(t *testing.T) {
	srv, _, _ := createMockServer()
	properties := PropertiesFlag{
		"acks=1",
		"max.poll.records=50",
		"max.partition.fetch.bytes=1000000",
	}
	cfg := &kafka.ConfigMap{}
	err := srv.updateKafkaConfig(cfg, properties)
	assert.NilError(t, err)
	acks, ok := cfg.Get("acks", "0")
	assert.Assert(t, ok)
	assert.Equal(t, "1", acks)
}

func TestGetSinkTopic(t *testing.T) {
	srv, _, _ := createMockServer()
	topic := srv.getSinkTopic("Syslog")
	assert.Equal(t, "OpenNMS.Sink.Syslog", topic)
}

func TestTransformAndSendSinkMessageSingle(t *testing.T) {
	srv, producer, _ := createMockServer()
	msg := &ipc.SinkMessage{
		MessageId: "001",
		Location:  "Test",
		SystemId:  "minion01",
		ModuleId:  "Heartbeat",
		Content:   []byte("This is a test"),
	}
	srv.transformAndSendSinkMessage(msg)
	// Verification
	assert.Equal(t, 1, len(producer.messages))
	received := producer.messages[0]
	sinkReceived := &sink.SinkMessage{}
	proto.Unmarshal(received.Value, sinkReceived)
	assert.Equal(t, msg.MessageId, *sinkReceived.MessageId)
	assert.Equal(t, string(msg.Content), string(sinkReceived.Content))
}

func TestTransformAndSendSinkMessageMultiple(t *testing.T) {
	srv, producer, _ := createMockServer()
	srv.MaxBufferSize = 4
	totalChunks := 5
	data := []byte("ABCDEFGHIJKLMNOPQRST")
	assert.Equal(t, len(data), totalChunks*srv.MaxBufferSize)
	for chunk := 0; chunk < totalChunks; chunk++ {
		idx := chunk * srv.MaxBufferSize
		content := data[idx : idx+srv.MaxBufferSize]
		msg := &ipc.SinkMessage{
			MessageId: "001",
			Location:  "Test",
			SystemId:  "minion01",
			ModuleId:  "Echo",
			Content:   content,
		}
		srv.transformAndSendSinkMessage(msg)
	}
	// Verification
	assert.Equal(t, totalChunks, len(producer.messages))
	var message []byte
	for chunk := 0; chunk < totalChunks; chunk++ {
		msg := producer.messages[chunk]
		received := &sink.SinkMessage{}
		proto.Unmarshal(msg.Value, received)
		message = append(message, received.Content...)
	}
	assert.Equal(t, string(data), string(message))
}

func TestIsHeaders(t *testing.T) {
	srv, _, _ := createMockServer()
	assert.Equal(t, false, srv.isHeaders(&ipc.RpcResponseProto{}))
	assert.Equal(t, false, srv.isHeaders(&ipc.RpcResponseProto{
		RpcId:    "001",
		SystemId: "minion01",
	}))
	assert.Equal(t, true, srv.isHeaders(&ipc.RpcResponseProto{
		RpcId:    "minion01",
		SystemId: "minion01",
	}))
}

func TestAddRPCHandler(t *testing.T) {
	srv, _, _ := createMockServer()
	srv.addRPCHandler("Durham", "minion01", &mockRPCStreamingServer{"H1"})
	srv.addRPCHandler("Raleigh", "minion02", &mockRPCStreamingServer{"H2"})
	srv.addRPCHandler("Raleigh", "minion03", &mockRPCStreamingServer{"H3"})
	// Verify Map by Minion
	obj, ok := srv.rpcHandlerByMinionID.Load("minion01")
	assert.Assert(t, ok)
	h := obj.(ipc.OpenNMSIpc_RpcStreamingServer)
	assert.Equal(t, "H1", h.Context().Value("id"))
	obj, ok = srv.rpcHandlerByMinionID.Load("minion02")
	assert.Assert(t, ok)
	h = obj.(ipc.OpenNMSIpc_RpcStreamingServer)
	assert.Equal(t, "H2", h.Context().Value("id"))
	obj, ok = srv.rpcHandlerByMinionID.Load("minion03")
	assert.Assert(t, ok)
	h = obj.(ipc.OpenNMSIpc_RpcStreamingServer)
	assert.Equal(t, "H3", h.Context().Value("id"))
	// Verify Map by Location (single minion)
	obj, ok = srv.rpcHandlerByLocation.Load("Durham")
	assert.Assert(t, ok)
	durham := obj.(*RoundRobinHandlerMap)
	assert.Equal(t, 1, len(durham.handlerIDs))
	obj, ok = durham.handlerMap.Load(durham.handlerIDs[0])
	assert.Assert(t, ok)
	h = obj.(ipc.OpenNMSIpc_RpcStreamingServer)
	assert.Equal(t, "H1", h.Context().Value("id"))
	// Verify Map by Location (multiple minions)
	obj, ok = srv.rpcHandlerByLocation.Load("Raleigh")
	assert.Assert(t, ok)
	raleigh := obj.(*RoundRobinHandlerMap)
	assert.Equal(t, 2, len(raleigh.handlerIDs))
	obj, ok = raleigh.handlerMap.Load(raleigh.handlerIDs[0])
	assert.Assert(t, ok)
	h = obj.(ipc.OpenNMSIpc_RpcStreamingServer)
	assert.Equal(t, "H2", h.Context().Value("id"))
	obj, ok = raleigh.handlerMap.Load(raleigh.handlerIDs[1])
	assert.Assert(t, ok)
	h = obj.(ipc.OpenNMSIpc_RpcStreamingServer)
	assert.Equal(t, "H3", h.Context().Value("id"))
}

func TestGetRPCHandler(t *testing.T) {
	srv, _, _ := createMockServer()
	srv.addRPCHandler("Durham", "minion01", &mockRPCStreamingServer{"H1"})
	srv.addRPCHandler("Raleigh", "minion02", &mockRPCStreamingServer{"H2"})
	srv.addRPCHandler("Raleigh", "minion03", &mockRPCStreamingServer{"H3"})
	// Get by System ID
	handler := srv.getRPCHandler("Durham", "minion01")
	assert.Assert(t, handler != nil)
	assert.Equal(t, "H1", handler.Context().Value("id"))
	// Get by Location (round-robin)
	handler = srv.getRPCHandler("Raleigh", "")
	assert.Assert(t, handler != nil)
	assert.Equal(t, "H3", handler.Context().Value("id"))
	handler = srv.getRPCHandler("Raleigh", "")
	assert.Assert(t, handler != nil)
	assert.Equal(t, "H2", handler.Context().Value("id"))
	handler = srv.getRPCHandler("Raleigh", "")
	assert.Assert(t, handler != nil)
	assert.Equal(t, "H3", handler.Context().Value("id"))
}

func TestTransformAndSendRPCMessageSingle(t *testing.T) {
	srv, producer, _ := createMockServer()
	msg := &ipc.RpcResponseProto{
		Location:   "Test",
		ModuleId:   "Echo",
		SystemId:   "minion01",
		RpcId:      "001",
		RpcContent: []byte("This is a test"),
	}
	srv.transformAndSendRPCMessage(msg)
	// Verification
	assert.Equal(t, 1, len(producer.messages))
	received := producer.messages[0]
	rpcMsg := &rpc.RpcMessageProto{}
	proto.Unmarshal(received.Value, rpcMsg)
	assert.Equal(t, msg.RpcId, rpcMsg.RpcId)
	assert.Equal(t, string(msg.RpcContent), string(rpcMsg.RpcContent))
}

func TestTransformAndSendRPCMessageMultiple(t *testing.T) {
	srv, producer, _ := createMockServer()
	srv.MaxBufferSize = 4
	totalChunks := 5
	data := []byte("ABCDEFGHIJKLMNOPQRST")
	assert.Equal(t, len(data), totalChunks*srv.MaxBufferSize)
	for chunk := 0; chunk < totalChunks; chunk++ {
		idx := chunk * srv.MaxBufferSize
		content := data[idx : idx+srv.MaxBufferSize]
		msg := &ipc.RpcResponseProto{
			Location:   "Test",
			ModuleId:   "Echo",
			SystemId:   "minion01",
			RpcId:      "001",
			RpcContent: content,
		}
		srv.transformAndSendRPCMessage(msg)
	}
	// Verification
	assert.Equal(t, totalChunks, len(producer.messages))
	var message []byte
	for chunk := 0; chunk < totalChunks; chunk++ {
		msg := producer.messages[chunk]
		received := &rpc.RpcMessageProto{}
		proto.Unmarshal(msg.Value, received)
		message = append(message, received.RpcContent...)
	}
	assert.Equal(t, string(data), string(message))
}

func TestGetRequestTopicAtLocation(t *testing.T) {
	srv, _, _ := createMockServer()
	topic := srv.getRequestTopicAtLocation("Test")
	assert.Equal(t, "OpenNMS.Test.rpc-request", topic)
}

func TestGetResponseTopic(t *testing.T) {
	srv, _, _ := createMockServer()
	topic := srv.getResponseTopic()
	assert.Equal(t, "OpenNMS.rpc-response", topic)
}

func TestCreateRPCRequest(t *testing.T) {
	srv, _, _ := createMockServer()
	rpcMsg := &rpc.RpcMessageProto{
		ModuleId:           "Echo",
		SystemId:           "minion01",
		RpcId:              "001",
		RpcContent:         []byte("This is a test"),
		CurrentChunkNumber: int32(0),
		TotalChunks:        int32(1),
		ExpirationTime:     uint64(time.Now().Unix()),
	}
	request := srv.createRPCRequest("Test", rpcMsg)
	assert.Equal(t, "Test", request.Location)
	assert.Equal(t, string(rpcMsg.RpcContent), string(request.RpcContent))
}

func TestHandleChunksSimple(t *testing.T) {
	srv, _, _ := createMockServer()
	rpcMsg := &rpc.RpcMessageProto{
		RpcId:              "001",
		RpcContent:         []byte("This is a test"),
		CurrentChunkNumber: int32(0),
		TotalChunks:        int32(1),
	}
	assert.Assert(t, srv.handleChunks(rpcMsg))
	obj, ok := srv.messageCache.Load(rpcMsg.RpcId)
	assert.Assert(t, ok)
	data := obj.([]byte)
	assert.Equal(t, string(rpcMsg.RpcContent), string(data))
}

func TestHandleChunksMultiple(t *testing.T) {
	srv, _, _ := createMockServer()
	totalChunks := 5
	data := []byte("ABCDEFGHIJKLMNOPQRST")
	msgSize := 4
	assert.Equal(t, len(data), totalChunks*msgSize)
	for chunk := 0; chunk < totalChunks; chunk++ {
		idx := chunk * msgSize
		content := data[idx : idx+msgSize]
		rpcMsg := &rpc.RpcMessageProto{
			RpcId:              "001",
			RpcContent:         content,
			CurrentChunkNumber: int32(chunk),
			TotalChunks:        int32(totalChunks),
		}
		done := srv.handleChunks(rpcMsg)
		assert.Equal(t, chunk == totalChunks-1, done)
	}
	obj, ok := srv.messageCache.Load("001")
	assert.Assert(t, ok)
	finalData := obj.([]byte)
	assert.Equal(t, string(data), string(finalData))
}

func TestSendRequest(t *testing.T) {
	srv, _, _ := createMockServer()
	location := "Durham"
	systemID := "minion01"
	srv.addRPCHandler(location, systemID, &mockRPCStreamingServer{"H1"})
	request := &ipc.RpcRequestProto{
		Location:   location,
		ModuleId:   "Echo",
		SystemId:   systemID,
		RpcId:      "001",
		RpcContent: []byte("Test"),
	}
	srv.sendRequest(location, request)
	messages, ok := mockBuffer["H1"]
	assert.Assert(t, ok)
	assert.Equal(t, 1, len(messages))
	r := messages[0].(*ipc.RpcRequestProto)
	assert.Equal(t, "001", r.RpcId)
}

func TestEndToEnd(t *testing.T) {
	srv, _, _ := createMockServer()
	err := srv.Start()
	assert.NilError(t, err)
	time.Sleep(5 * time.Second)
	srv.Stop()
}

// Helper methods

func createMockServer() (*OnmsGrpcIpcServer, *mockProducer, *mockConsumer) {
	producer := &mockProducer{
		eventChannel: make(chan kafka.Event),
	}
	consumer := &mockConsumer{
		msgChannel: make(chan *kafka.Message),
	}
	consumers := make(map[string]KafkaConsumer)
	consumers["Test"] = consumer
	srv := &OnmsGrpcIpcServer{
		OnmsInstanceID: "OpenNMS",
		GrpcPort:       defaultGrpcPort,
		HTTPPort:       defaultHTTPPort,
		MaxBufferSize:  defaultMaxByfferSize,
		producer:       producer,
		consumers:      consumers,
	}
	srv.initVariables()
	return srv, producer, consumer
}
