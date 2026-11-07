package exporterkit_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Trysac/u135-exporter-kit"
)

// mockCollector implements exporterkit.Collector for testing.
type mockCollector struct {
	name      string
	descs     []exporterkit.Desc
	collectFn func(ctx context.Context, emit func(exporterkit.Sample)) error
}

func (m *mockCollector) Name() string {
	return m.name
}

func (m *mockCollector) Describe() []exporterkit.Desc {
	return m.descs
}

func (m *mockCollector) Collect(ctx context.Context, emit func(exporterkit.Sample)) error {
	if m.collectFn != nil {
		return m.collectFn(ctx, emit)
	}
	return nil
}

func TestWriteTextAcceptanceJobs(t *testing.T) {
	// Acceptance: WriteText for one counter family 'jobs_total' with help 'Jobs processed.', label 'queue' and samples a=3 and b=0.5
	// writes exactly "# HELP jobs_total Jobs processed.\n# TYPE jobs_total counter\njobs_total{queue=\"a\"} 3\njobs_total{queue=\"b\"} 0.5\n"
	fam := exporterkit.Family{
		Desc: exporterkit.Desc{
			Name:   "jobs_total",
			Help:   "Jobs processed.",
			Type:   exporterkit.Counter,
			Labels: []string{"queue"},
		},
		Samples: []exporterkit.Sample{
			{Name: "jobs_total", LabelValues: []string{"a"}, Value: 3},
			{Name: "jobs_total", LabelValues: []string{"b"}, Value: 0.5},
		},
	}

	var buf bytes.Buffer
	err := exporterkit.WriteText(&buf, []exporterkit.Family{fam})
	if err != nil {
		t.Fatalf("WriteText returned unexpected error: %v", err)
	}

	want := "# HELP jobs_total Jobs processed.\n# TYPE jobs_total counter\njobs_total{queue=\"a\"} 3\njobs_total{queue=\"b\"} 0.5\n"
	if got := buf.String(); got != want {
		t.Fatalf("WriteText mismatch:\ngot:\n%q\nwant:\n%q", got, want)
	}
}

func TestEscapingAndFloats(t *testing.T) {
	// Acceptance: Label values escape backslash, double quote and newline as \\, \" and \n;
	// help text escapes backslash and newline; values NaN, +Inf and -Inf print as "NaN", "+Inf" and "-Inf" and 1e21 prints as "1e+21"
	fam := exporterkit.Family{
		Desc: exporterkit.Desc{
			Name:   "test_escaping",
			Help:   "line1\\slash\nline2",
			Type:   exporterkit.Gauge,
			Labels: []string{"target"},
		},
		Samples: []exporterkit.Sample{
			{Name: "test_escaping", LabelValues: []string{"a\\b\"c\nd"}, Value: math.NaN()},
			{Name: "test_escaping", LabelValues: []string{"+inf"}, Value: math.Inf(1)},
			{Name: "test_escaping", LabelValues: []string{"-inf"}, Value: math.Inf(-1)},
			{Name: "test_escaping", LabelValues: []string{"exp"}, Value: 1e21},
		},
	}

	var buf bytes.Buffer
	if err := exporterkit.WriteText(&buf, []exporterkit.Family{fam}); err != nil {
		t.Fatalf("WriteText failed: %v", err)
	}

	want := "# HELP test_escaping line1\\\\slash\\nline2\n# TYPE test_escaping gauge\ntest_escaping{target=\"a\\\\b\\\"c\\nd\"} NaN\ntest_escaping{target=\"+inf\"} +Inf\ntest_escaping{target=\"-inf\"} -Inf\ntest_escaping{target=\"exp\"} 1e+21\n"
	if got := buf.String(); got != want {
		t.Fatalf("Escaping mismatch:\ngot:\n%q\nwant:\n%q", got, want)
	}
}

func TestSortingDeterministic(t *testing.T) {
	// Acceptance: Collectors emitting 'b_total' before 'a_total' and label values "z" before "y" produce output
	// where "a_total" precedes "b_total" and {queue="y"} precedes {queue="z"}, byte-identical across 2 Gathers
	reg := exporterkit.NewRegistry(exporterkit.Options{})
	col := &mockCollector{
		name: "worker",
		descs: []exporterkit.Desc{
			{Name: "b_total", Help: "b help", Type: exporterkit.Counter, Labels: []string{"queue"}},
			{Name: "a_total", Help: "a help", Type: exporterkit.Counter, Labels: []string{"queue"}},
		},
		collectFn: func(ctx context.Context, emit func(exporterkit.Sample)) error {
			emit(exporterkit.Sample{Name: "b_total", LabelValues: []string{"z"}, Value: 1})
			emit(exporterkit.Sample{Name: "b_total", LabelValues: []string{"y"}, Value: 2})
			emit(exporterkit.Sample{Name: "a_total", LabelValues: []string{"z"}, Value: 3})
			emit(exporterkit.Sample{Name: "a_total", LabelValues: []string{"y"}, Value: 4})
			return nil
		},
	}
	if err := reg.Register(col); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	fams1, err := reg.Gather(context.Background())
	if err != nil {
		t.Fatalf("Gather 1 failed: %v", err)
	}
	fams2, err := reg.Gather(context.Background())
	if err != nil {
		t.Fatalf("Gather 2 failed: %v", err)
	}

	var b1, b2 bytes.Buffer
	if err := exporterkit.WriteText(&b1, fams1); err != nil {
		t.Fatalf("WriteText 1 failed: %v", err)
	}
	if err := exporterkit.WriteText(&b2, fams2); err != nil {
		t.Fatalf("WriteText 2 failed: %v", err)
	}

	out1 := b1.String()
	out2 := b2.String()
	if out1 != out2 {
		t.Fatalf("Scrapes were not byte-identical:\nScrape 1:\n%s\nScrape 2:\n%s", out1, out2)
	}

	// Check order: exporter_collector_up comes first, then a_total, then b_total
	idxUp := strings.Index(out1, "exporter_collector_up")
	idxA := strings.Index(out1, "a_total")
	idxB := strings.Index(out1, "b_total")
	if !(idxUp < idxA && idxA < idxB) {
		t.Fatalf("Order mismatch: expected up < a_total < b_total, got idxUp=%d idxA=%d idxB=%d", idxUp, idxA, idxB)
	}

	// Inside a_total: queue="y" must precede queue="z"
	idxAY := strings.Index(out1, `a_total{queue="y"}`)
	idxAZ := strings.Index(out1, `a_total{queue="z"}`)
	if !(idxAY < idxAZ) {
		t.Fatalf("Order mismatch for a_total labels: got y=%d, z=%d", idxAY, idxAZ)
	}
}

func TestCollectorUpAndFailureIsolation(t *testing.T) {
	// Acceptance: Every scrape adds family exporter_collector_up with one sample per collector, value 1 on success and 0 on error or panic;
	// a failing collector's own samples are dropped and the others are still written
	reg := exporterkit.NewRegistry(exporterkit.Options{})

	successCol := &mockCollector{
		name: "good",
		descs: []exporterkit.Desc{
			{Name: "success_metric", Help: "good metric", Type: exporterkit.Untyped, Labels: []string{}},
		},
		collectFn: func(ctx context.Context, emit func(exporterkit.Sample)) error {
			emit(exporterkit.Sample{Name: "success_metric", Value: 42})
			return nil
		},
	}

	errCol := &mockCollector{
		name: "bad_err",
		descs: []exporterkit.Desc{
			{Name: "err_metric", Help: "failing metric", Type: exporterkit.Gauge, Labels: []string{}},
		},
		collectFn: func(ctx context.Context, emit func(exporterkit.Sample)) error {
			emit(exporterkit.Sample{Name: "err_metric", Value: 1})
			return errors.New("database timeout")
		},
	}

	panicCol := &mockCollector{
		name: "bad_panic",
		descs: []exporterkit.Desc{
			{Name: "panic_metric", Help: "panicking metric", Type: exporterkit.Gauge, Labels: []string{}},
		},
		collectFn: func(ctx context.Context, emit func(exporterkit.Sample)) error {
			emit(exporterkit.Sample{Name: "panic_metric", Value: 1})
			panic("unexpected memory corruption")
		},
	}

	if err := reg.Register(successCol); err != nil {
		t.Fatalf("Register successCol failed: %v", err)
	}
	if err := reg.Register(errCol); err != nil {
		t.Fatalf("Register errCol failed: %v", err)
	}
	if err := reg.Register(panicCol); err != nil {
		t.Fatalf("Register panicCol failed: %v", err)
	}

	fams, err := reg.Gather(context.Background())
	if err != nil {
		t.Fatalf("Gather failed: %v", err)
	}

	var buf bytes.Buffer
	if err := exporterkit.WriteText(&buf, fams); err != nil {
		t.Fatalf("WriteText failed: %v", err)
	}
	out := buf.String()

	// Check that exporter_collector_up values are correct
	if !strings.Contains(out, `exporter_collector_up{collector="good"} 1`) {
		t.Fatalf("missing good collector up=1 in output:\n%s", out)
	}
	if !strings.Contains(out, `exporter_collector_up{collector="bad_err"} 0`) {
		t.Fatalf("missing bad_err collector up=0 in output:\n%s", out)
	}
	if !strings.Contains(out, `exporter_collector_up{collector="bad_panic"} 0`) {
		t.Fatalf("missing bad_panic collector up=0 in output:\n%s", out)
	}

	// Check that failing collector samples are dropped
	if strings.Contains(out, "err_metric") {
		t.Fatalf("err_metric should have been dropped:\n%s", out)
	}
	if strings.Contains(out, "panic_metric") {
		t.Fatalf("panic_metric should have been dropped:\n%s", out)
	}

	// Check that good collector sample was preserved
	if !strings.Contains(out, "success_metric 42\n") {
		t.Fatalf("success_metric 42 missing from output:\n%s", out)
	}
}

func TestBlockingCollectorTimeout(t *testing.T) {
	// Acceptance: A collector that ignores ctx and blocks, with Options{Timeout: 20 * time.Millisecond},
	// makes Gather return in under 1 s with exporter_collector_up{collector="slow"} 0
	reg := exporterkit.NewRegistry(exporterkit.Options{
		Timeout: 20 * time.Millisecond,
	})

	col := &mockCollector{
		name: "slow",
		descs: []exporterkit.Desc{
			{Name: "slow_metric", Help: "slow help", Type: exporterkit.Gauge, Labels: []string{}},
		},
		collectFn: func(ctx context.Context, emit func(exporterkit.Sample)) error {
			// Deliberately block and ignore ctx
			time.Sleep(2 * time.Second)
			emit(exporterkit.Sample{Name: "slow_metric", Value: 100})
			return nil
		},
	}

	if err := reg.Register(col); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	start := time.Now()
	fams, err := reg.Gather(context.Background())
	duration := time.Since(start)

	if err != nil {
		t.Fatalf("Gather failed: %v", err)
	}
	if duration >= 1*time.Second {
		t.Fatalf("Gather took %v, expected under 1s", duration)
	}

	var buf bytes.Buffer
	if err := exporterkit.WriteText(&buf, fams); err != nil {
		t.Fatalf("WriteText failed: %v", err)
	}
	out := buf.String()

	if !strings.Contains(out, `exporter_collector_up{collector="slow"} 0`) {
		t.Fatalf("Expected slow collector to fail with up=0:\n%s", out)
	}
	if strings.Contains(out, "slow_metric") {
		t.Fatalf("Expected slow_metric to be dropped:\n%s", out)
	}
}

func TestInvalidSampleHandling(t *testing.T) {
	// Acceptance: A sample named "undeclared_total", one with 2 label values for a 1-label Desc,
	// or a repeated {queue="a"} marks the collector failed: exporter_collector_up is 0 and OnError receives an error matching ErrInvalidSample via errors.Is
	cases := []struct {
		name   string
		emitFn func(emit func(exporterkit.Sample))
	}{
		{
			name: "undeclared metric",
			emitFn: func(emit func(exporterkit.Sample)) {
				emit(exporterkit.Sample{Name: "undeclared_total", Value: 1})
			},
		},
		{
			name: "label count mismatch",
			emitFn: func(emit func(exporterkit.Sample)) {
				emit(exporterkit.Sample{Name: "declared_total", LabelValues: []string{"v1", "v2"}, Value: 1})
			},
		},
		{
			name: "repeated label values",
			emitFn: func(emit func(exporterkit.Sample)) {
				emit(exporterkit.Sample{Name: "declared_total", LabelValues: []string{"a"}, Value: 1})
				emit(exporterkit.Sample{Name: "declared_total", LabelValues: []string{"a"}, Value: 2})
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var caughtErr error
			var caughtCol string
			reg := exporterkit.NewRegistry(exporterkit.Options{
				OnError: func(collector string, err error) {
					caughtCol = collector
					caughtErr = err
				},
			})

			col := &mockCollector{
				name: "flawed",
				descs: []exporterkit.Desc{
					{Name: "declared_total", Help: "desc", Type: exporterkit.Counter, Labels: []string{"queue"}},
				},
				collectFn: func(ctx context.Context, emit func(exporterkit.Sample)) error {
					tc.emitFn(emit)
					return nil
				},
			}

			if err := reg.Register(col); err != nil {
				t.Fatalf("Register failed: %v", err)
			}

			fams, err := reg.Gather(context.Background())
			if err != nil {
				t.Fatalf("Gather failed: %v", err)
			}

			if caughtCol != "flawed" {
				t.Fatalf("expected caughtCol to be flawed, got %q", caughtCol)
			}
			if !errors.Is(caughtErr, exporterkit.ErrInvalidSample) {
				t.Fatalf("expected caughtErr to match ErrInvalidSample, got: %v", caughtErr)
			}

			var buf bytes.Buffer
			if err := exporterkit.WriteText(&buf, fams); err != nil {
				t.Fatalf("WriteText failed: %v", err)
			}
			out := buf.String()
			if !strings.Contains(out, `exporter_collector_up{collector="flawed"} 0`) {
				t.Fatalf("expected exporter_collector_up{collector=\"flawed\"} 0, got:\n%s", out)
			}
		})
	}
}

func TestRegisterValidationAndSampleLimit(t *testing.T) {
	// Acceptance: Register returns ErrInvalidName for metric name "1bad" or label name "__x",
	// and ErrDuplicateCollector for a repeated collector name; a scrape with 100_001 samples returns ErrTooManySamples and the handler answers 500
	reg := exporterkit.NewRegistry(exporterkit.Options{
		MaxSamples: 100_000,
	})

	// Test nil collector
	if err := reg.Register(nil); !errors.Is(err, exporterkit.ErrInvalidName) {
		t.Fatalf("expected ErrInvalidName for nil collector, got: %v", err)
	}

	// Test empty collector name
	emptyCol := &mockCollector{name: ""}
	if err := reg.Register(emptyCol); !errors.Is(err, exporterkit.ErrInvalidName) {
		t.Fatalf("expected ErrInvalidName for empty collector name, got: %v", err)
	}

	// Test invalid metric name "1bad"
	badMetricCol := &mockCollector{
		name: "c1",
		descs: []exporterkit.Desc{
			{Name: "1bad", Help: "help", Type: exporterkit.Gauge},
		},
	}
	if err := reg.Register(badMetricCol); !errors.Is(err, exporterkit.ErrInvalidName) {
		t.Fatalf("expected ErrInvalidName for '1bad', got: %v", err)
	}

	// Test reserved metric name exporter_collector_up
	reservedCol := &mockCollector{
		name: "c_res",
		descs: []exporterkit.Desc{
			{Name: "exporter_collector_up", Help: "reserved", Type: exporterkit.Gauge},
		},
	}
	if err := reg.Register(reservedCol); !errors.Is(err, exporterkit.ErrInvalidName) {
		t.Fatalf("expected ErrInvalidName for exporter_collector_up, got: %v", err)
	}

	// Test invalid label name "__x"
	badLabelCol := &mockCollector{
		name: "c2",
		descs: []exporterkit.Desc{
			{Name: "good_metric", Help: "help", Type: exporterkit.Gauge, Labels: []string{"__x"}},
		},
	}
	if err := reg.Register(badLabelCol); !errors.Is(err, exporterkit.ErrInvalidName) {
		t.Fatalf("expected ErrInvalidName for '__x', got: %v", err)
	}

	// Test valid registration
	goodCol := &mockCollector{
		name: "c3",
		descs: []exporterkit.Desc{
			{Name: "shared_metric", Help: "help", Type: exporterkit.Gauge},
		},
	}
	if err := reg.Register(goodCol); err != nil {
		t.Fatalf("unexpected error registering goodCol: %v", err)
	}

	// Test repeated collector name
	dupCol := &mockCollector{
		name: "c3",
	}
	if err := reg.Register(dupCol); !errors.Is(err, exporterkit.ErrDuplicateCollector) {
		t.Fatalf("expected ErrDuplicateCollector, got: %v", err)
	}

	// Test conflicting family name between collectors
	conflictingCol := &mockCollector{
		name: "c4",
		descs: []exporterkit.Desc{
			{Name: "shared_metric", Help: "help 2", Type: exporterkit.Gauge},
		},
	}
	if err := reg.Register(conflictingCol); !errors.Is(err, exporterkit.ErrInvalidName) {
		t.Fatalf("expected ErrInvalidName for conflicting metric name across collectors, got: %v", err)
	}

	// Test sample limit exceed
	limitReg := exporterkit.NewRegistry(exporterkit.Options{
		MaxSamples: 100_000,
	})
	hugeCol := &mockCollector{
		name: "huge",
		descs: []exporterkit.Desc{
			{Name: "huge_metric", Help: "huge", Type: exporterkit.Counter, Labels: []string{"id"}},
		},
		collectFn: func(ctx context.Context, emit func(exporterkit.Sample)) error {
			for i := 0; i <= 100_000; i++ {
				emit(exporterkit.Sample{
					Name:        "huge_metric",
					LabelValues: []string{strconv.Itoa(i)},
					Value:       1,
				})
			}
			return nil
		},
	}
	if err := limitReg.Register(hugeCol); err != nil {
		t.Fatalf("register hugeCol failed: %v", err)
	}

	_, err := limitReg.Gather(context.Background())
	if !errors.Is(err, exporterkit.ErrTooManySamples) {
		t.Fatalf("expected ErrTooManySamples, got: %v", err)
	}

	// Handler answers 500 when Gather returns error
	h := limitReg.Handler()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected status 500 on too many samples, got: %d", rec.Code)
	}
	if rec.Body.String() != "scrape failed\n" {
		t.Fatalf("expected 500 body 'scrape failed\n', got: %q", rec.Body.String())
	}
}

func TestHandlerMethodsAndHeaders(t *testing.T) {
	// Acceptance: Handler answers GET with 200 and Content-Type "text/plain; version=0.0.4; charset=utf-8",
	// HEAD with the same headers and an empty body, and POST with 405 and Allow "GET, HEAD"
	reg := exporterkit.NewRegistry(exporterkit.Options{})
	col := &mockCollector{
		name: "worker",
		descs: []exporterkit.Desc{
			{Name: "tasks", Help: "task count", Type: exporterkit.Gauge},
		},
		collectFn: func(ctx context.Context, emit func(exporterkit.Sample)) error {
			emit(exporterkit.Sample{Name: "tasks", Value: 10})
			return nil
		},
	}
	if err := reg.Register(col); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	h := reg.Handler()

	// GET
	{
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("GET: expected 200, got %d", rec.Code)
		}
		const wantCT = "text/plain; version=0.0.4; charset=utf-8"
		if gotCT := rec.Header().Get("Content-Type"); gotCT != wantCT {
			t.Fatalf("GET: Content-Type got %q, want %q", gotCT, wantCT)
		}
		if !strings.Contains(rec.Body.String(), "tasks 10\n") {
			t.Fatalf("GET: body missing sample: %q", rec.Body.String())
		}
	}

	// HEAD
	{
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodHead, "/metrics", nil)
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("HEAD: expected 200, got %d", rec.Code)
		}
		const wantCT = "text/plain; version=0.0.4; charset=utf-8"
		if gotCT := rec.Header().Get("Content-Type"); gotCT != wantCT {
			t.Fatalf("HEAD: Content-Type got %q, want %q", gotCT, wantCT)
		}
		if rec.Body.Len() != 0 {
			t.Fatalf("HEAD: expected empty body, got %q", rec.Body.String())
		}
	}

	// POST
	{
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/metrics", nil)
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("POST: expected 405, got %d", rec.Code)
		}
		if gotAllow := rec.Header().Get("Allow"); gotAllow != "GET, HEAD" {
			t.Fatalf("POST: Allow header got %q, want %q", gotAllow, "GET, HEAD")
		}
	}
}

func TestZeroCollectorsAndEmptyFamilies(t *testing.T) {
	reg := exporterkit.NewRegistry(exporterkit.Options{})
	fams, err := reg.Gather(context.Background())
	if err != nil {
		t.Fatalf("Gather failed: %v", err)
	}
	if len(fams) != 0 {
		t.Fatalf("expected 0 families for zero collectors, got %d", len(fams))
	}

	var buf bytes.Buffer
	if err := exporterkit.WriteText(&buf, fams); err != nil {
		t.Fatalf("WriteText failed: %v", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("expected empty output for zero collectors, got: %q", buf.String())
	}

	// Also test empty samples in a family is omitted
	emptyFam := exporterkit.Family{
		Desc: exporterkit.Desc{
			Name: "no_samples",
			Help: "empty",
			Type: exporterkit.Gauge,
		},
		Samples: nil,
	}
	if err := exporterkit.WriteText(&buf, []exporterkit.Family{emptyFam}); err != nil {
		t.Fatalf("WriteText with empty samples failed: %v", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("expected empty output for empty family samples, got: %q", buf.String())
	}
}

func TestConcurrentRegistrationAndScrape(t *testing.T) {
	reg := exporterkit.NewRegistry(exporterkit.Options{})
	col := &mockCollector{
		name: "init_col",
		descs: []exporterkit.Desc{
			{Name: "init_metric", Help: "desc", Type: exporterkit.Counter},
		},
		collectFn: func(ctx context.Context, emit func(exporterkit.Sample)) error {
			time.Sleep(5 * time.Millisecond)
			emit(exporterkit.Sample{Name: "init_metric", Value: 1})
			return nil
		},
	}
	_ = reg.Register(col)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c := &mockCollector{
				name: string(rune('a' + id)),
				descs: []exporterkit.Desc{
					{Name: string(rune('m' + id)), Help: "desc", Type: exporterkit.Gauge},
				},
			}
			_ = reg.Register(c)
		}(i)

		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = reg.Gather(context.Background())
		}()
	}
	wg.Wait()
}

type failWriter struct{}

func (failWriter) Write(p []byte) (n int, err error) {
	return 0, io.ErrClosedPipe
}

func TestWriteTextErrorPaths(t *testing.T) {
	fam := exporterkit.Family{
		Desc: exporterkit.Desc{
			Name: "failed_write",
			Help: "test",
			Type: exporterkit.Gauge,
		},
		Samples: []exporterkit.Sample{
			{Name: "failed_write", Value: 1},
		},
	}
	fw := failWriter{}
	if err := exporterkit.WriteText(fw, []exporterkit.Family{fam}); err == nil {
		t.Fatalf("expected write error with failWriter, got nil")
	}
}
