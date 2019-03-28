package dht

import (
	"fmt"

	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	messageSizeBytesBuckets = []float64{0, 1, 10, 30, 70, 100, 200, 1000, 10000}
	latencySecondsBuckets   = []float64{0, 0.001, 0.010, 0.100, 0.300, 1, 10, 100}
)

const (
	namespace   = "libp2p"
	subsystem   = "kad_dht"
	messageType = "message_type"
	instanceId  = "instance_id"
	localPeerId = "local_peer_id"
)

// See https://groups.google.com/d/msg/prometheus-developers/ntZHQz216c0/DSbqaA-4EwAJ
func newGaugeFunc(name string, f func() float64, labels prometheus.Labels) prometheus.GaugeFunc {
	opts := prometheus.GaugeOpts(newOpts(name))
	opts.ConstLabels = labels
	return promauto.NewGaugeFunc(opts, f)
}

func newOpts(name string) prometheus.Opts {
	return prometheus.Opts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      name,
	}
}

func newHistogramOpts(name string, buckets []float64) prometheus.HistogramOpts {
	return prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      name,
		Buckets:   buckets,
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
		newHistogramOpts("inbound_request_handling_time_seconds", latencySecondsBuckets),
		messageLabels())

	sentMessages = promauto.NewCounterVec(
		prometheus.CounterOpts(newOpts("sent_messages")),
		messageLabels())
	sentMessageSizeBytes = promauto.NewHistogramVec(
		newHistogramOpts("sent_message_size_bytes", messageSizeBytesBuckets),
		messageLabels())

	outboundRequestResponseLatencySeconds = promauto.NewHistogramVec(
		newHistogramOpts("outbound_request_response_latency_seconds", latencySecondsBuckets),
		messageLabels())
	messageWriteLatencySeconds = promauto.NewHistogramVec(
		newHistogramOpts("message_write_latency_seconds",
			// We're only looking for large spikes due to contention.
			prometheus.ExponentialBuckets(0.001, 10, 6)),
		messageLabels())

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
