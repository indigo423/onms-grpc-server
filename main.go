// Package main
//
// GO version of the Java based gRPC Server.
//
// TODO
// - Client Certificate Validation
// - Prometheus Metrics
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/agalue/onms-grpc-server/protobuf/ipc"
	"github.com/agalue/onms-grpc-server/protobuf/rpc"
	"github.com/agalue/onms-grpc-server/protobuf/sink"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

const (
	topicNameAtLocation          = "%s.%s.%s"
	topicNameWithoutLocation     = "%s.%s"
	sinkTopicNameWithoutLocation = "%s.%s.%s"
	sinkModulePrefix             = "Sink"
	rpcRequestTopicName          = "rpc-request"
	rpcResponseTopicName         = "rpc-response"
	defaultPort                  = 8990
	defaultMaxByfferSize         = 921600
	defaultInstanceID            = "OpenNMS"
)

// PropertiesFlag represents an array of string flags
type PropertiesFlag []string

func (p *PropertiesFlag) String() string {
	return strings.Join(*p, ", ")
}

// Set stores a string flag in the array
func (p *PropertiesFlag) Set(value string) error {
	*p = append(*p, value)
	return nil
}

// OnmsGrpcIpcServer represents an OpenNMS gRPC Server instance
type OnmsGrpcIpcServer struct {
	Port                    int
	KafkaBootstrap          string
	KafkaProducerProperties PropertiesFlag
	KafkaConsumerProperties PropertiesFlag
	OnmsInstanceID          string
	MaxBufferSize           int
	TLSEnabled              bool
	TLSCertFile             string
	TLSKeyFile              string

	server    *grpc.Server
	hs        *health.Server
	producer  *kafka.Producer
	consumers map[string]*kafka.Consumer

	rpcHandlerByLocation sync.Map
	rpcHandlerByMinionID sync.Map
	currentChunkCache    sync.Map
	messageCache         sync.Map
	rpcDelayQueue        sync.Map
}

// Start initiates the gRPC server and the Kafka Producer instances
func (srv *OnmsGrpcIpcServer) Start() error {
	jsonBytes, _ := json.MarshalIndent(srv, "", "  ")
	log.Printf("initializing gRPC server: %s", string(jsonBytes))

	// Initialize Maps
	var err error
	srv.consumers = make(map[string]*kafka.Consumer)
	srv.rpcHandlerByLocation = sync.Map{}
	srv.rpcHandlerByMinionID = sync.Map{}
	srv.currentChunkCache = sync.Map{}
	srv.messageCache = sync.Map{}
	srv.rpcDelayQueue = sync.Map{}

	// Initialize Kafka Producer
	kafkaConfig := &kafka.ConfigMap{"bootstrap.servers": srv.KafkaBootstrap}
	if srv.KafkaProducerProperties != nil {
		for _, kv := range srv.KafkaProducerProperties {
			array := strings.Split(kv, "=")
			if err = kafkaConfig.SetKey(array[0], array[1]); err != nil {
				return err
			}
		}
	}
	srv.producer, err = kafka.NewProducer(kafkaConfig)
	if err != nil {
		return fmt.Errorf("could not create producer: %v", err)
	}

	// Initialize Producer Message Logger
	go func() {
		for e := range srv.producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("delivery failed: %v\n", ev.TopicPartition)
				} else {
					log.Printf("delivered message of %d bytes with key %s to %v\n", len(ev.Value), ev.Key, ev.TopicPartition)
				}
			default:
				log.Printf("kafka event: %s\n", ev)
			}
		}
	}()

	// Initialize TCP Listener
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", srv.Port))
	if err != nil {
		return fmt.Errorf("cannot initialize listener: %v", err)
	}

	// Initialize gRPC Server
	if srv.TLSEnabled {
		creds, err := credentials.NewServerTLSFromFile(srv.TLSCertFile, srv.TLSKeyFile)
		if err != nil {
			return fmt.Errorf("Failed to setup TLS: %v", err)
		}
		srv.server = grpc.NewServer(grpc.Creds(creds))
	} else {
		srv.server = grpc.NewServer()
	}
	srv.hs = health.NewServer()

	// Configure Services
	ipc.RegisterOpenNMSIpcServer(srv.server, srv)
	grpc_health_v1.RegisterHealthServer(srv.server, srv.hs)

	// Start gRPC Server
	jsonBytes, _ = json.MarshalIndent(srv.server.GetServiceInfo(), "", "  ")
	log.Printf("gRPC server info: %s", string(jsonBytes))
	go func() {
		log.Printf("starting gRPC server on port %d\n", srv.Port)
		if err = srv.server.Serve(listener); err != nil {
			log.Fatalf("could not serve: %v", err)
		}
	}()

	// Initialize Delay Queue Processor
	go func() {
		for now := range time.Tick(time.Second) {
			srv.rpcDelayQueue.Range(func(key interface{}, value interface{}) bool {
				rpcID := key.(string)
				expiration := value.(uint64)
				if uint64(now.Unix()) > expiration {
					log.Printf("RPC message %s expired", rpcID)
					srv.rpcDelayQueue.Delete(rpcID)
					srv.messageCache.Delete(rpcID)
					srv.currentChunkCache.Delete(rpcID)
				}
				return true
			})
		}
	}()

	return nil
}

// Stop gracefully stop the gRPC server and the Kafka Producer/Consumer instances
func (srv *OnmsGrpcIpcServer) Stop() {
	log.Println("shutting down...")
	srv.server.Stop()
	srv.producer.Close()
	for _, consumer := range srv.consumers {
		consumer.Close()
	}
	srv.hs.Shutdown()
	log.Println("done!")
}

// Main gRPC Methods

// SinkStreaming Streams Sink messages from Minion to OpenNMS (client-side streaming RPC)
func (srv *OnmsGrpcIpcServer) SinkStreaming(stream ipc.OpenNMSIpc_SinkStreamingServer) error {
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&ipc.Empty{})
		}
		if err != nil {
			return err
		}
		srv.transformAndSendSinkMessage(msg)
	}
}

// RpcStreaming Streams RPC messages between OpenNMS and Minion (bidirectional streaming RPC)
func (srv *OnmsGrpcIpcServer) RpcStreaming(stream ipc.OpenNMSIpc_RpcStreamingServer) error {
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if srv.isHeaders(msg) {
			srv.addRPCHandler(msg.Location, msg.SystemId, stream)
			if err := srv.startConsumingForLocation(msg.Location); err != nil {
				log.Printf("error: %v", err)
			}
		} else {
			srv.transformAndSendRPCMessage(msg)
		}
	}
}

// Sink API Methods

func (srv *OnmsGrpcIpcServer) transformAndSendSinkMessage(msg *ipc.SinkMessage) {
	totalChunks := srv.getTotalChunks(msg.Content)
	for chunk := int32(0); chunk < totalChunks; chunk++ {
		bufferSize := srv.getRemainingBufferSize(int32(len(msg.Content)), chunk)
		offset := chunk * srv.getMaxBufferSize()
		data := msg.Content[offset : offset+bufferSize]
		sinkMsg := &sink.SinkMessage{
			MessageId:          &msg.MessageId,
			CurrentChunkNumber: &chunk,
			TotalChunks:        &totalChunks,
			Content:            data,
		}
		if bytes, err := proto.Marshal(sinkMsg); err != nil {
			log.Printf("error cannot serialize sink message: %v\n", err)
		} else {
			srv.sendToKafka(srv.getSinkTopic(msg.ModuleId), msg.MessageId, bytes)
		}
	}
}

func (srv *OnmsGrpcIpcServer) getSinkTopic(module string) string {
	return fmt.Sprintf(sinkTopicNameWithoutLocation, srv.OnmsInstanceID, sinkModulePrefix, module)
}

// RPC API Methods

func (srv *OnmsGrpcIpcServer) isHeaders(msg *ipc.RpcResponseProto) bool {
	return msg.SystemId != "" && msg.RpcId == msg.SystemId
}

func (srv *OnmsGrpcIpcServer) addRPCHandler(location string, systemID string, rpcHandler ipc.OpenNMSIpc_RpcStreamingServer) {
	if location == "" || systemID == "" {
		log.Printf("invalid metadata received with location = '%s', systemId = '%s'", location, systemID)
		return
	}
	if _, ok := srv.rpcHandlerByLocation.Load(location); !ok {
		srv.rpcHandlerByLocation.Store(location, rpcHandler)
		srv.rpcHandlerByMinionID.Store(systemID, rpcHandler)
		log.Printf("added RPC handler for minion %s at location %s", systemID, location)
	}
}

func (srv *OnmsGrpcIpcServer) startConsumingForLocation(location string) error {
	if srv.consumers[location] != nil {
		return fmt.Errorf("a consumer already exists for location %s", location)
	}

	kafkaConfig := &kafka.ConfigMap{
		"bootstrap.servers":       srv.KafkaBootstrap,
		"group.id":                srv.OnmsInstanceID,
		"auto.commit.interval.ms": 1000,
	}
	if srv.KafkaConsumerProperties != nil {
		for _, kv := range srv.KafkaConsumerProperties {
			array := strings.Split(kv, "=")
			if err := kafkaConfig.SetKey(array[0], array[1]); err != nil {
				return err
			}
		}
	}

	consumer, err := kafka.NewConsumer(kafkaConfig)
	if err != nil {
		return fmt.Errorf("could not create producer: %v", err)
	}

	topic := srv.getRequestTopicAtLocation(location)
	if err := consumer.Subscribe(topic, nil); err != nil {
		return fmt.Errorf("cannot subscribe to topic %s: %v", topic, err)
	}
	log.Printf("subscribed to topic %s", topic)

	go func() {
		log.Printf("starting RPC consumer for location %s", location)
		for {
			event := consumer.Poll(100)
			switch e := event.(type) {
			case *kafka.Message:
				rpcMsg := &rpc.RpcMessageProto{}
				if err := proto.Unmarshal(e.Value, rpcMsg); err != nil {
					log.Printf("invalid message received: %v", err)
					continue
				}
				log.Printf("processing RPC message %s", rpcMsg.RpcId)
				srv.rpcDelayQueue.Store(rpcMsg.RpcId, rpcMsg.ExpirationTime)
				rpcContent := rpcMsg.RpcContent
				// For larger messages which get split into multiple chunks, cache them until all of them arrive
				if rpcMsg.TotalChunks > 1 {
					// Handle multiple chunks
					if !srv.handleChunks(rpcMsg) {
						continue
					}
					if data, ok := srv.messageCache.Load(rpcMsg.RpcId); ok {
						rpcContent = data.([]byte)
					}
					// Remove rpcId from cache
					srv.messageCache.Delete(rpcMsg.RpcId)
					srv.currentChunkCache.Delete(rpcMsg.RpcId)
				}
				rpcRequest := &ipc.RpcRequestProto{
					RpcId:          rpcMsg.RpcId,
					ModuleId:       rpcMsg.ModuleId,
					ExpirationTime: rpcMsg.ExpirationTime,
					SystemId:       rpcMsg.SystemId,
					Location:       location,
					RpcContent:     rpcContent,
				}
				srv.sendRequest(location, rpcRequest)
			case kafka.Error:
				log.Printf("consumer error %v", e)
			}
		}
	}()

	srv.consumers[location] = consumer
	return nil
}

func (srv *OnmsGrpcIpcServer) transformAndSendRPCMessage(msg *ipc.RpcResponseProto) {
	totalChunks := srv.getTotalChunks(msg.RpcContent)
	for chunk := int32(0); chunk < totalChunks; chunk++ {
		bufferSize := srv.getRemainingBufferSize(int32(len(msg.RpcContent)), chunk)
		offset := chunk * srv.getMaxBufferSize()
		data := msg.RpcContent[offset : offset+bufferSize]
		rpcMsg := &rpc.RpcMessageProto{
			RpcId:              msg.RpcId,
			ModuleId:           msg.ModuleId,
			SystemId:           msg.SystemId,
			RpcContent:         data,
			CurrentChunkNumber: chunk,
			TotalChunks:        totalChunks,
			TracingInfo:        msg.TracingInfo,
		}
		if bytes, err := proto.Marshal(rpcMsg); err != nil {
			log.Printf("error cannot serialize sink message: %v", err)
		} else {
			srv.sendToKafka(srv.getResponseTopic(), msg.RpcId, bytes)
		}
	}
}

func (srv *OnmsGrpcIpcServer) handleChunks(rpcMsg *rpc.RpcMessageProto) bool {
	data, _ := srv.currentChunkCache.LoadOrStore(rpcMsg.RpcId, 0)
	chunkNumber := data.(int32)
	if chunkNumber != rpcMsg.CurrentChunkNumber {
		log.Printf("expected chunk = %d but got chunk = %d, ignoring.", chunkNumber, rpcMsg.CurrentChunkNumber)
		return false
	}
	data, _ = srv.messageCache.LoadOrStore(rpcMsg.RpcId, make([]byte, 0))
	srv.messageCache.Store(rpcMsg.RpcId, append(data.([]byte), rpcMsg.RpcContent...))
	chunkNumber++
	srv.currentChunkCache.Store(rpcMsg.RpcId, chunkNumber)
	return rpcMsg.TotalChunks == chunkNumber
}

func (srv *OnmsGrpcIpcServer) getRPCHandler(location string, systemID string) ipc.OpenNMSIpc_RpcStreamingServer {
	if systemID != "" {
		stream, _ := srv.rpcHandlerByMinionID.Load(systemID)
		if stream == nil {
			return nil
		}
		return stream.(ipc.OpenNMSIpc_RpcStreamingServer)
	}
	stream, _ := srv.rpcHandlerByLocation.Load(location)
	if stream == nil {
		return nil
	}
	return stream.(ipc.OpenNMSIpc_RpcStreamingServer)
}

func (srv *OnmsGrpcIpcServer) sendRequest(location string, rpcRequest *ipc.RpcRequestProto) {
	stream := srv.getRPCHandler(location, rpcRequest.SystemId)
	if stream == nil {
		log.Printf("no RPC handler found for location %s", location)
		return
	}
	if rpcRequest.SystemId == "" {
		log.Printf("sending gRPC request %s for location %s", rpcRequest.RpcId, location)
	} else {
		log.Printf("sending gRPC request %s to minion %s for location %s", rpcRequest.RpcId, rpcRequest.SystemId, location)
	}
	if err := stream.SendMsg(rpcRequest); err != nil {
		log.Printf("error while sending RPC request: %v", err)
	}
}

func (srv *OnmsGrpcIpcServer) getRequestTopicAtLocation(location string) string {
	return fmt.Sprintf(topicNameAtLocation, srv.OnmsInstanceID, location, rpcRequestTopicName)
}

func (srv *OnmsGrpcIpcServer) getResponseTopic() string {
	return fmt.Sprintf(topicNameWithoutLocation, srv.OnmsInstanceID, rpcResponseTopicName)
}

// Common/Helper Methods

func (srv *OnmsGrpcIpcServer) getMaxBufferSize() int32 {
	return int32(srv.MaxBufferSize)
}

func (srv *OnmsGrpcIpcServer) getTotalChunks(data []byte) int32 {
	if srv.MaxBufferSize == 0 {
		return int32(1)
	}
	chunks := int32(math.Ceil(float64(len(data) / srv.MaxBufferSize)))
	if len(data)%srv.MaxBufferSize > 0 {
		chunks++
	}
	return chunks
}

func (srv *OnmsGrpcIpcServer) getRemainingBufferSize(messageSize, chunk int32) int32 {
	if srv.MaxBufferSize > 0 && messageSize > srv.getMaxBufferSize() {
		remaining := messageSize - chunk*srv.getMaxBufferSize()
		if remaining > srv.getMaxBufferSize() {
			return srv.getMaxBufferSize()
		}
		return remaining
	}
	return messageSize
}

func (srv *OnmsGrpcIpcServer) sendToKafka(topic string, key string, value []byte) {
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(key),
		Value:          value,
	}
	if err := srv.producer.Produce(msg, nil); err != nil {
		log.Printf("error while sending message %s to topic %s: %v", key, topic, err)
	}
}

// Main Method

func main() {
	srv := &OnmsGrpcIpcServer{}
	flag.IntVar(&srv.Port, "port", defaultPort, "gRPC Server Listener Port")
	flag.StringVar(&srv.OnmsInstanceID, "instance-id", defaultInstanceID, "OpenNMS Instance ID")
	flag.StringVar(&srv.KafkaBootstrap, "bootstrap", "localhost:9092", "Kafka Bootstrap Server")
	flag.Var(&srv.KafkaProducerProperties, "producer-cfg", "Kafka Producer configuration entry (can be used multiple times)\nfor instance: acks=1")
	flag.Var(&srv.KafkaConsumerProperties, "consumer-cfg", "Kafka Consumer configuration entry (can be used multiple times)\nfor instance: acks=1")
	flag.IntVar(&srv.MaxBufferSize, "max-buffer-size", defaultMaxByfferSize, "Maximum Buffer Size for RPC/Sink API Messages")
	flag.StringVar(&srv.TLSCertFile, "tls-cert", "", "Path to the TLS Certificate file")
	flag.StringVar(&srv.TLSKeyFile, "tls-key", "", "Path to the TLS Key file")
	flag.BoolVar(&srv.TLSEnabled, "tls-enabled", false, "Enable TLS for the gRPC Server")
	flag.Parse()

	if err := srv.Start(); err != nil {
		log.Fatal(err)
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	<-stop
	srv.Stop()
}
