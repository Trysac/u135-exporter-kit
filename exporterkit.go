package exporterkit

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Sentinel errors returned or wrapped by exporterkit functions.
var (
	// ErrInvalidName is returned when a collector name, metric name, or label name fails validation.
	ErrInvalidName = errors.New("exporterkit: invalid name")
	// ErrDuplicateCollector is returned when a collector with the same name is already registered.
	ErrDuplicateCollector = errors.New("exporterkit: duplicate collector")
	// ErrInvalidSample is returned when a sample emits undeclared metric names, mismatched label counts, or duplicate label values.
	ErrInvalidSample = errors.New("exporterkit: invalid sample")
	// ErrTooManySamples is returned when the total number of collected samples across all collectors exceeds MaxSamples.
	ErrTooManySamples = errors.New("exporterkit: sample limit exceeded")
)

var (
	metricNameRegex = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*$`)
	labelNameRegex  = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
)

const defaultTimeout = 10 * time.Second
const defaultMaxSamples = 100_000

// MetricType represents a Prometheus metric type.
type MetricType string

// Supported Prometheus metric types.
const (
	Counter MetricType = "counter"
	Gauge   MetricType = "gauge"
	Untyped MetricType = "untyped"
)

// Desc represents the descriptor of a metric family.
type Desc struct {
	Name   string
	Help   string
	Type   MetricType
	Labels []string
}

// Sample represents a single metric observation emitted by a collector.
type Sample struct {
	Name        string
	LabelValues []string
	Value       float64
}

// Family represents a metric family with its descriptor and all matching samples.
type Family struct {
	Desc    Desc
	Samples []Sample
}

// Collector gathers metrics on each scrape.
type Collector interface {
	// Name returns the unique identifier for the collector.
	Name() string
	// Describe returns descriptors for all metric families emitted by this collector.
	Describe() []Desc
	// Collect runs a single collection cycle, passing gathered samples to emit.
	// Implementations MUST honor ctx cancellation.
	Collect(ctx context.Context, emit func(Sample)) error
}

// Options configures the behaviour of a Registry.
type Options struct {
	// Timeout specifies the per-collector execution timeout. Defaults to 10 seconds if <= 0.
	Timeout time.Duration
	// MaxSamples limits the total number of samples collected in a single scrape. Defaults to 100,000 if <= 0.
	MaxSamples int
	// OnError is invoked when an individual collector fails, panics, times out, or emits invalid samples.
	OnError func(collector string, err error)
}

// Registry manages collectors and coordinates metric scraping.
type Registry struct {
	mu         sync.RWMutex
	opts       Options
	collectors map[string]Collector
	famOwners  map[string]string
}

// NewRegistry returns a new initialized Registry with the given options.
func NewRegistry(opts Options) *Registry {
	if opts.Timeout <= 0 {
		opts.Timeout = defaultTimeout
	}
	if opts.MaxSamples <= 0 {
		opts.MaxSamples = defaultMaxSamples
	}
	return &Registry{
		opts:       opts,
		collectors: make(map[string]Collector),
		famOwners:  make(map[string]string),
	}
}

// Register registers a Collector with the registry.
// Returns ErrInvalidName if a metric or label name is invalid, or if two collectors declare the same metric name.
// Returns ErrDuplicateCollector if a collector with the same Name() has already been registered.
func (r *Registry) Register(c Collector) error {
	if c == nil {
		return fmt.Errorf("%w: nil collector", ErrInvalidName)
	}
	cName := c.Name()
	if cName == "" {
		return fmt.Errorf("%w: empty collector name", ErrInvalidName)
	}

	descs := c.Describe()
	for _, d := range descs {
		if !isValidMetricName(d.Name) {
			return fmt.Errorf("%w: invalid metric name %q", ErrInvalidName, d.Name)
		}
		if d.Name == "exporter_collector_up" {
			return fmt.Errorf("%w: metric name %q is reserved for built-in metrics", ErrInvalidName, d.Name)
		}
		for _, l := range d.Labels {
			if !isValidLabelName(l) {
				return fmt.Errorf("%w: invalid label name %q in metric %q", ErrInvalidName, l, d.Name)
			}
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.collectors[cName]; exists {
		return fmt.Errorf("%w: collector %q", ErrDuplicateCollector, cName)
	}

	for _, d := range descs {
		if existing, exists := r.famOwners[d.Name]; exists {
			return fmt.Errorf("%w: metric %q already declared by collector %q", ErrInvalidName, d.Name, existing)
		}
	}

	r.collectors[cName] = c
	for _, d := range descs {
		r.famOwners[d.Name] = cName
	}

	return nil
}

type collectorResult struct {
	name    string
	success bool
	samples []Sample
	err     error
}

// Gather executes all registered collectors concurrently with bounded timeouts and returns the collected metric families.
// If total samples exceed MaxSamples, ErrTooManySamples is returned.
func (r *Registry) Gather(ctx context.Context) ([]Family, error) {
	r.mu.RLock()
	collectors := make([]Collector, 0, len(r.collectors))
	for _, c := range r.collectors {
		collectors = append(collectors, c)
	}
	opts := r.opts
	r.mu.RUnlock()

	sort.Slice(collectors, func(i, j int) bool {
		return collectors[i].Name() < collectors[j].Name()
	})

	numCollectors := len(collectors)
	resCh := make(chan collectorResult, numCollectors)

	for _, c := range collectors {
		go func(col Collector) {
			colName := col.Name()
			descs := col.Describe()
			descMap := make(map[string]Desc, len(descs))
			for _, d := range descs {
				descMap[d.Name] = d
			}

			colCtx, cancel := context.WithTimeout(ctx, opts.Timeout)
			defer cancel()

			var (
				emitMu  sync.Mutex
				samples []Sample
				colErr  error
				seenMap = make(map[string]bool)
			)

			emit := func(s Sample) {
				emitMu.Lock()
				defer emitMu.Unlock()
				if colErr != nil {
					return
				}
				d, ok := descMap[s.Name]
				if !ok {
					colErr = fmt.Errorf("%w: undeclared sample metric name %q", ErrInvalidSample, s.Name)
					return
				}
				if len(s.LabelValues) != len(d.Labels) {
					colErr = fmt.Errorf("%w: metric %q expects %d label values, got %d", ErrInvalidSample, s.Name, len(d.Labels), len(s.LabelValues))
					return
				}
				var b strings.Builder
				b.WriteString(s.Name)
				for _, lv := range s.LabelValues {
					b.WriteByte(0)
					b.WriteString(strconv.Itoa(len(lv)))
					b.WriteByte(':')
					b.WriteString(lv)
				}
				labelKey := b.String()
				if seenMap[labelKey] {
					colErr = fmt.Errorf("%w: duplicate sample for metric %q", ErrInvalidSample, s.Name)
					return
				}
				seenMap[labelKey] = true
				samples = append(samples, s)
			}

			runCh := make(chan error, 1)
			go func() {
				defer func() {
					if r := recover(); r != nil {
						runCh <- fmt.Errorf("panic: %v", r)
					}
				}()
				runCh <- col.Collect(colCtx, emit)
			}()

			var runErr error
			select {
			case <-colCtx.Done():
				runErr = colCtx.Err()
			case runErr = <-runCh:
			}

			emitMu.Lock()
			defer emitMu.Unlock()

			if runErr != nil {
				resCh <- collectorResult{name: colName, success: false, err: runErr}
				return
			}
			if colErr != nil {
				resCh <- collectorResult{name: colName, success: false, err: colErr}
				return
			}

			resCh <- collectorResult{name: colName, success: true, samples: samples}
		}(c)
	}

	results := make(map[string]collectorResult, numCollectors)
	for i := 0; i < numCollectors; i++ {
		res := <-resCh
		results[res.name] = res
		if !res.success && opts.OnError != nil {
			opts.OnError(res.name, res.err)
		}
	}

	var upSamples []Sample
	for _, col := range collectors {
		cName := col.Name()
		res := results[cName]
		val := 0.0
		if res.success {
			val = 1.0
		}
		upSamples = append(upSamples, Sample{
			Name:        "exporter_collector_up",
			LabelValues: []string{cName},
			Value:       val,
		})
	}

	r.mu.RLock()
	descByName := make(map[string]Desc)
	for _, col := range r.collectors {
		for _, d := range col.Describe() {
			descByName[d.Name] = d
		}
	}
	r.mu.RUnlock()

	famSamples := make(map[string][]Sample)
	totalSamples := len(upSamples)

	for _, col := range collectors {
		res := results[col.Name()]
		if !res.success {
			continue
		}
		totalSamples += len(res.samples)
		for _, s := range res.samples {
			famSamples[s.Name] = append(famSamples[s.Name], s)
		}
	}

	if totalSamples > opts.MaxSamples {
		return nil, fmt.Errorf("%w: got %d, limit is %d", ErrTooManySamples, totalSamples, opts.MaxSamples)
	}

	var fams []Family
	if len(upSamples) > 0 {
		fams = append(fams,
			Family{
				Desc: Desc{
					Name:   "exporter_collector_up",
					Help:   "Whether the collector succeeded (1) or failed (0).",
					Type:   Gauge,
					Labels: []string{"collector"},
				},
				Samples: upSamples,
			},
		)
	}

	metricNames := make([]string, 0, len(famSamples))
	for name := range famSamples {
		metricNames = append(metricNames, name)
	}
	sort.Strings(metricNames)

	for _, name := range metricNames {
		smps := famSamples[name]
		d := descByName[name]
		sortSamples(smps)
		fams = append(fams, Family{
			Desc:    d,
			Samples: smps,
		})
	}

	return fams, nil
}

func sortSamples(samples []Sample) {
	sort.Slice(samples, func(i, j int) bool {
		si, sj := samples[i].LabelValues, samples[j].LabelValues
		minLen := len(si)
		if len(sj) < minLen {
			minLen = len(sj)
		}
		for idx := 0; idx < minLen; idx++ {
			if si[idx] != sj[idx] {
				return si[idx] < sj[idx]
			}
		}
		return len(si) < len(sj)
	})
}

// Handler returns an http.Handler that executes Gather and writes Prometheus text exposition format.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case http.MethodGet, http.MethodHead:
		default:
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}

		fams, err := r.Gather(req.Context())
		if err != nil {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusInternalServerError)
			if req.Method == http.MethodGet {
				_, _ = io.WriteString(w, "scrape failed\n")
			}
			return
		}

		var buf bytes.Buffer
		if err := WriteText(&buf, fams); err != nil {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusInternalServerError)
			if req.Method == http.MethodGet {
				_, _ = io.WriteString(w, "scrape failed\n")
			}
			return
		}

		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		if req.Method == http.MethodGet {
			_, _ = w.Write(buf.Bytes())
		}
	})
}

// WriteText formats metric families using the Prometheus text exposition format 0.0.4.
// Empty families (with no samples) are omitted.
func WriteText(w io.Writer, fams []Family) error {
	for _, fam := range fams {
		if len(fam.Samples) == 0 {
			continue
		}
		if _, err := fmt.Fprintf(w, "# HELP %s %s\n", fam.Desc.Name, escapeHelp(fam.Desc.Help)); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "# TYPE %s %s\n", fam.Desc.Name, fam.Desc.Type); err != nil {
			return err
		}
		for _, s := range fam.Samples {
			if len(fam.Desc.Labels) == 0 {
				if _, err := fmt.Fprintf(w, "%s %s\n", s.Name, formatFloat(s.Value)); err != nil {
					return err
				}
			} else {
				var labels []string
				for idx, labelName := range fam.Desc.Labels {
					lVal := ""
					if idx < len(s.LabelValues) {
						lVal = s.LabelValues[idx]
					}
					labels = append(labels, fmt.Sprintf("%s=\"%s\"", labelName, escapeLabelValue(lVal)))
				}
				if _, err := fmt.Fprintf(w, "%s{%s} %s\n", s.Name, strings.Join(labels, ","), formatFloat(s.Value)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func isValidMetricName(name string) bool {
	if len(name) == 0 || len(name) > 200 {
		return false
	}
	return metricNameRegex.MatchString(name)
}

func isValidLabelName(name string) bool {
	if len(name) == 0 || len(name) > 200 {
		return false
	}
	if strings.HasPrefix(name, "__") {
		return false
	}
	return labelNameRegex.MatchString(name)
}

func escapeHelp(help string) string {
	var b strings.Builder
	b.Grow(len(help))
	for i := 0; i < len(help); i++ {
		c := help[i]
		switch c {
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}

func escapeLabelValue(val string) string {
	var b strings.Builder
	b.Grow(len(val))
	for i := 0; i < len(val); i++ {
		c := val[i]
		switch c {
		case '\\':
			b.WriteString(`\\`)
		case '"':
			b.WriteString(`\"`)
		case '\n':
			b.WriteString(`\n`)
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}

func formatFloat(v float64) string {
	if math.IsNaN(v) {
		return "NaN"
	}
	if math.IsInf(v, 1) {
		return "+Inf"
	}
	if math.IsInf(v, -1) {
		return "-Inf"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}
