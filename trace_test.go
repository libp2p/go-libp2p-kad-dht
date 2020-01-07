package dht

import (
	"testing"

	"github.com/uber/jaeger-client-go"
	jaegercfg "github.com/uber/jaeger-client-go/config"
)

func TestTracing(t *testing.T) {
	cfg := jaegercfg.Configuration{
		Sampler: &jaegercfg.SamplerConfig{
			Type:  jaeger.SamplerTypeConst,
			Param: 1,
		},
		Reporter: &jaegercfg.ReporterConfig{
			LogSpans: true,
		},
	}

	cfg.InitGlobalTracer("libp2p")

	//mem := &basictracer.InMemorySpanRecorder{}
	// opentracing.SetGlobalTracer(basictracer.New(mem))

	TestProvides(t)

	// spans := mem.GetSpans()

	// w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', 0)
	// for _, sp := range spans {
	// 	var sb strings.Builder
	// 	for k, v := range sp.Tags {
	// 		sb.WriteString(fmt.Sprintf("%s=%s;", k, v))
	// 	}

	// 	fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", sp.Operation, sp.Start, sp.Duration, sb.String(), sp.Logs)

	// }
	// w.Flush()
}
