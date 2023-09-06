package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/urfave/cli/v2"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"golang.org/x/exp/slog"

	"github.com/libp2p/go-libp2p-kad-dht/v2"
	"github.com/libp2p/go-libp2p-kad-dht/v2/tele"
)

type Config struct {
	Host        string
	Port        int
	ProtocolID  string
	MetricsHost string
	MetricsPort int
	TraceHost   string
	TracePort   int
}

func (c Config) String() string {
	data, _ := json.MarshalIndent(c, "", "  ")
	return string(data)
}

var cfg = Config{
	Host:        "127.0.0.1",
	Port:        0,
	ProtocolID:  string(dht.ProtocolIPFS),
	MetricsHost: "127.0.0.1",
	MetricsPort: 3232,
	TraceHost:   "127.0.0.1",
	TracePort:   14268,
}

func main() {
	app := &cli.App{
		Name:   "musa",
		Usage:  "a lean bootstrapper process for any network",
		Action: daemonAction,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "host",
				Usage:       "the network musa should bind on",
				Value:       cfg.Host,
				Destination: &cfg.Host,
				EnvVars:     []string{"MUSA_HOST"},
			},
			&cli.IntFlag{
				Name:        "port",
				Usage:       "the port on which musa should listen on",
				Value:       cfg.Port,
				Destination: &cfg.Port,
				EnvVars:     []string{"MUSA_PORT"},
				DefaultText: "random",
			},
			&cli.StringFlag{
				Name:        "protocol",
				Usage:       "the libp2p protocol for the DHT",
				Value:       cfg.ProtocolID,
				Destination: &cfg.ProtocolID,
				EnvVars:     []string{"MUSA_PROTOCOL"},
			},
			&cli.StringFlag{
				Name:        "metrics-host",
				Usage:       "the network musa metrics should bind on",
				Value:       cfg.MetricsHost,
				Destination: &cfg.MetricsHost,
				EnvVars:     []string{"MUSA_METRICS_HOST"},
			},
			&cli.IntFlag{
				Name:        "metrics-port",
				Usage:       "the port on which musa metrics should listen on",
				Value:       cfg.MetricsPort,
				Destination: &cfg.MetricsPort,
				EnvVars:     []string{"MUSA_METRICS_PORT"},
				DefaultText: "random",
			},
			&cli.StringFlag{
				Name:        "trace-host",
				Usage:       "the network musa trace should be pushed to",
				Value:       cfg.TraceHost,
				Destination: &cfg.TraceHost,
				EnvVars:     []string{"MUSA_TRACE_HOST"},
			},
			&cli.IntFlag{
				Name:        "trace-port",
				Usage:       "the port to which musa should push traces",
				Value:       cfg.TracePort,
				Destination: &cfg.TracePort,
				EnvVars:     []string{"MUSA_TRACE_PORT"},
				DefaultText: "random",
			},
		},
	}

	sigs := make(chan os.Signal, 1)
	ctx, cancel := context.WithCancel(context.Background())

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, os.Interrupt, os.Kill)
	go func() {
		sig := <-sigs
		slog.Info("Received signal - Stopping...", "signal", sig.String())
		signal.Stop(sigs)
		cancel()
	}()

	if err := app.RunContext(ctx, os.Args); err != nil {
		slog.Error("application error", "err", err)
		os.Exit(1)
	}
}

func daemonAction(cCtx *cli.Context) error {
	slog.Info("Starting musa daemon process with configuration:")
	fmt.Println(cfg.String())

	exporter, err := prometheus.New()
	if err != nil {
		return fmt.Errorf("new prometheus exporter: :%w", err)
	}
	meterProvider := metric.NewMeterProvider(append(tele.MeterProviderOpts, metric.WithReader(exporter))...)
	traceProvider, err := traceProvider()
	if err != nil {
		return fmt.Errorf("new trace provider: %w", err)
	}

	go serveMetrics()

	dhtConfig := dht.DefaultConfig()
	dhtConfig.Mode = dht.ModeOptServer
	dhtConfig.Logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	dhtConfig.ProtocolID = protocol.ID(cfg.ProtocolID)
	dhtConfig.MeterProvider = meterProvider
	dhtConfig.TracerProvider = traceProvider

	if dhtConfig.ProtocolID == dht.ProtocolIPFS {
		dhtConfig.Datastore = datastore.NewNullDatastore()
	}

	var d *dht.DHT
	opts := []libp2p.Option{
		libp2p.ListenAddrStrings(
			fmt.Sprintf("/ip4/%s/tcp/%d", cfg.Host, cfg.Port),
			fmt.Sprintf("/ip4/%s/udp/%d/quic-v1", cfg.Host, cfg.Port),
			fmt.Sprintf("/ip4/%s/udp/%d/quic-v1/webtransport", cfg.Host, cfg.Port),
		),
		libp2p.Routing(func(h host.Host) (routing.PeerRouting, error) {
			d, err = dht.New(h, dhtConfig)
			return d, err
		}),
	}

	h, err := libp2p.New(opts...)
	if err != nil {
		return fmt.Errorf("new libp2p host: %w", err)
	}

	slog.Info("Created libp2p host", "peerID", h.ID().String())
	for i, addr := range h.Addrs() {
		slog.Info(fmt.Sprintf("  [%d] %s", i, addr.String()))
	}

	if err := d.Bootstrap(cCtx.Context); err != nil {
		return err
	}

	slog.Info("Initialized")
	<-cCtx.Context.Done()

	return nil
}

func serveMetrics() {
	addr := fmt.Sprintf("%s:%d", cfg.MetricsHost, cfg.MetricsPort)

	slog.Info("serving metrics", "endpoint", addr+"/metrics")
	http.Handle("/metrics", promhttp.Handler())
	err := http.ListenAndServe(addr, nil)
	if err != nil {
		slog.Warn("error serving metrics", "err", err.Error())
		return
	}
}

func traceProvider() (*trace.TracerProvider, error) {
	endpoint := fmt.Sprintf("http://%s:%d/api/traces", cfg.TraceHost, cfg.TracePort)
	exp, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(endpoint)))
	if err != nil {
		return nil, err
	}

	tp := trace.NewTracerProvider(
		trace.WithBatcher(exp),
		trace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String("musa"),
			semconv.DeploymentEnvironmentKey.String("production"),
		)),
	)

	return tp, nil
}
