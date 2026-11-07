package exporterkit_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"

	"github.com/Trysac/u135-exporter-kit"
)

type queueCollector struct{}

func (q *queueCollector) Name() string {
	return "queue_worker"
}

func (q *queueCollector) Describe() []exporterkit.Desc {
	return []exporterkit.Desc{
		{
			Name:   "queue_jobs_total",
			Help:   "Number of processed queue jobs.",
			Type:   exporterkit.Counter,
			Labels: []string{"queue"},
		},
	}
}

func (q *queueCollector) Collect(ctx context.Context, emit func(exporterkit.Sample)) error {
	emit(exporterkit.Sample{
		Name:        "queue_jobs_total",
		LabelValues: []string{"critical"},
		Value:       15,
	})
	return nil
}

func ExampleNewRegistry() {
	reg := exporterkit.NewRegistry(exporterkit.Options{})
	_ = reg.Register(&queueCollector{})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	reg.Handler().ServeHTTP(rec, req)

	fmt.Printf("Status: %d\n", rec.Code)
	fmt.Println(rec.Body.String())
	// Output:
	// Status: 200
	// # HELP exporter_collector_up Whether the collector succeeded (1) or failed (0).
	// # TYPE exporter_collector_up gauge
	// exporter_collector_up{collector="queue_worker"} 1
	// # HELP queue_jobs_total Number of processed queue jobs.
	// # TYPE queue_jobs_total counter
	// queue_jobs_total{queue="critical"} 15
}
