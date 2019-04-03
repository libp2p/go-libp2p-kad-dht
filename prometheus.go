package dht

import (
	"fmt"

	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	messageSizeBytesBuckets      = []float64{0, 1, 10, 30, 70, 100, 200, 1000, 10000}
	networkLatencySecondsBuckets = []float64{0, 0.001, 0.010, 0.100, 0.300, 1, 10, 100}
)

const (
	namespace = "libp2p"
	subsystem = "kad_dht"

	messageType    = "message_type"
	instanceId     = "instance_id"
	localPeerId    = "local_peer_id"
	errorLabelName = "error"
)

var constLabels = prometheus.Labels{"stream_pooling": "race_wait_and_new_pipeline_sends"}

// See https://groups.google.com/d/msg/prometheus-developers/ntZHQz216c0/DSbqaA-4EwAJ
func newGaugeFunc(name string, f func() float64, labels prometheus.Labels) prometheus.GaugeFunc {
	opts := prometheus.GaugeOpts(newOpts(name))
	opts.ConstLabels = combineLabels(opts.ConstLabels, labels)
	return promauto.NewGaugeFunc(opts, f)
}

// Don't forget to also change newHistogramOpts.
func newOpts(name string) prometheus.Opts {
	return prometheus.Opts{
		Namespace:   namespace,
		Subsystem:   subsystem,
		Name:        name,
		ConstLabels: constLabels,
	}
}

func newHistogramOpts(name string, buckets []float64) prometheus.HistogramOpts {
	return prometheus.HistogramOpts{
		Namespace:   namespace,
		Subsystem:   subsystem,
		Name:        name,
		ConstLabels: constLabels,
		Buckets:     buckets,
	}
}

func dhtInstanceLabels() []string {
	return []string{localPeerId, instanceId}
}

func (dht *IpfsDHT) instanceLabelValues() []string {
	return []string{dht.self.String(), fmt.Sprintf("%p", dht)}
}

// This could be cached on the instance for better performance.
func (dht *IpfsDHT) instanceLabels() prometheus.Labels {
	vs := dht.instanceLabelValues()
	return prometheus.Labels{
		localPeerId: vs[0],
		instanceId:  vs[1],
	}
}

func messageLabels() []string {
	return append(dhtInstanceLabels(), messageType)
}

func (dht *IpfsDHT) messageLabelValues(m *pb.Message) []string {
	return append(dht.instanceLabelValues(), m.Type.String())
}

var (
	receivedMessages = promauto.NewCounterVec(
		prometheus.CounterOpts(newOpts("received_messages")),
		messageLabels())
	receivedMessageSizeBytes = promauto.NewHistogramVec(
		newHistogramOpts("received_message_size_bytes", messageSizeBytesBuckets),
		messageLabels())
	inboundRequestHandlingTimeSeconds = promauto.NewHistogramVec(
		newHistogramOpts("inbound_request_handling_time_seconds", networkLatencySecondsBuckets),
		messageLabels())

	sentMessages = promauto.NewCounterVec(
		prometheus.CounterOpts(newOpts("sent_messages")),
		messageLabels())
	sentMessageSizeBytes = promauto.NewHistogramVec(
		newHistogramOpts("sent_message_size_bytes", messageSizeBytesBuckets),
		messageLabels())

	sendMessageLatencySeconds = promauto.NewHistogramVec(
		newHistogramOpts("send_message_latency_seconds", networkLatencySecondsBuckets),
		append(messageLabels(), errorLabelName))
	outboundRequestResponseLatencySeconds = promauto.NewHistogramVec(
		newHistogramOpts("outbound_request_response_latency_seconds", networkLatencySecondsBuckets),
		append(messageLabels(), errorLabelName))
	messageWriteLatencySeconds = promauto.NewHistogramVec(
		newHistogramOpts("message_write_latency_seconds",
			// We're only looking for large spikes due to contention.
			[]float64{0, 0.001, 0.01, 0.1, 1, 10, 100, 1000}),
		messageLabels())
	newStreamTimeSeconds = promauto.NewHistogramVec(
		newHistogramOpts("new_stream_time_seconds", networkLatencySecondsBuckets),
		dhtInstanceLabels())
	newStreamTimeErrorSeconds = promauto.NewHistogramVec(
		newHistogramOpts("new_stream_time_error_seconds", networkLatencySecondsBuckets),
		dhtInstanceLabels())

	routingTablePeersAdded = promauto.NewCounterVec(
		prometheus.CounterOpts(newOpts("routing_table_peers_added")),
		dhtInstanceLabels())
	routingTablePeersRemoved = promauto.NewCounterVec(
		prometheus.CounterOpts(newOpts("routing_table_peers_removed")),
		dhtInstanceLabels())
)

// GaugeFuncs can't have variable labels, so we must unregister them manually.
func (dht *IpfsDHT) initRoutingTableNumEntriesGaugeFunc() {
	gf := newGaugeFunc(
		"routing_table_num_entries",
		func() float64 {
			return float64(dht.routingTable.Size())
		},
		dht.instanceLabels())
	go func() {
		<-dht.Context().Done()
		prometheus.DefaultRegisterer.Unregister(gf)
	}()
}

// Create a new Labels instance, favouring values in later instances.
func combineLabels(lss ...prometheus.Labels) prometheus.Labels {
	ret := make(prometheus.Labels)
	for _, ls := range lss {
		for k, v := range ls {
			ret[k] = v
		}
	}
	return ret
}
