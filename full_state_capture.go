package tubing_cdc

import (
	"fmt"
	"strings"
	"sync"
)

// FullStateJobKind classifies work planned for a full-state capture driver.
type FullStateJobKind int

const (
	// FullStateJobChunkedTable means run PK-ordered LIMIT chunks until the table is exhausted
	// (progress via ChunkProgressStore / BuildPKOrderedChunkSelect).
	FullStateJobChunkedTable FullStateJobKind = iota
	// FullStateJobSelectedPKs means read exactly the listed primary-key tuples (BuildPKRowsInSelect).
	FullStateJobSelectedPKs
)

// FullStateTableSpec describes one replicated table for chunked snapshotting.
type FullStateTableSpec struct {
	TableKey  string
	PKColumns []string
	ChunkSize int
	RunID     string
}

// Validate checks FQN, PK columns, and chunk size.
func (s FullStateTableSpec) Validate() error {
	if strings.TrimSpace(s.TableKey) == "" {
		return fmt.Errorf("full-state: table_key is empty")
	}
	if _, err := ParseTableIdentity(strings.TrimSpace(s.TableKey)); err != nil {
		return fmt.Errorf("full-state: %w", err)
	}
	if len(s.PKColumns) == 0 {
		return fmt.Errorf("full-state: pk columns are empty")
	}
	for _, col := range s.PKColumns {
		if strings.TrimSpace(col) == "" {
			return fmt.Errorf("full-state: empty pk column name")
		}
	}
	if s.ChunkSize <= 0 {
		return fmt.Errorf("full-state: chunk_size must be positive")
	}
	return nil
}

// FullStateJob is one unit of work for an operator or in-process driver (watermark + SELECT cycle).
type FullStateJob struct {
	Kind        FullStateJobKind
	Spec        FullStateTableSpec
	SelectedPKs [][]any
}

// FullStateCaptureConfig lists tables that participate in full-state capture planning.
type FullStateCaptureConfig struct {
	Tables []FullStateTableSpec
}

// PlanFullStateMode selects how PlanFullStateJobs expands work items.
type PlanFullStateMode int

const (
	// PlanFullStateAllTables emits one chunked-table job per entry in cfg.Tables.
	PlanFullStateAllTables PlanFullStateMode = iota
	// PlanFullStateOneTable emits a single chunked-table job for opts.TableKey.
	PlanFullStateOneTable
	// PlanFullStateSelectedPKs emits one selected-PKs job for opts.TableKey and opts.SelectedPKs.
	PlanFullStateSelectedPKs
)

// PlanFullStateJobsOptions configures planning. DefaultChunkSize applies when a table spec has ChunkSize 0.
type PlanFullStateJobsOptions struct {
	Mode             PlanFullStateMode
	TableKey         string
	SelectedPKs      [][]any
	DefaultChunkSize int
}

// PlanFullStateJobs returns concrete jobs for a capture run. It does not execute SQL or touch Badger.
func PlanFullStateJobs(cfg *FullStateCaptureConfig, opts PlanFullStateJobsOptions) ([]FullStateJob, error) {
	if cfg == nil {
		return nil, fmt.Errorf("full-state: config is nil")
	}
	switch opts.Mode {
	case PlanFullStateAllTables:
		if len(cfg.Tables) == 0 {
			return nil, fmt.Errorf("full-state: no tables in config")
		}
		out := make([]FullStateJob, 0, len(cfg.Tables))
		for _, t := range cfg.Tables {
			spec, err := normalizeFullStateTableSpec(t, opts.DefaultChunkSize)
			if err != nil {
				return nil, err
			}
			if err := spec.Validate(); err != nil {
				return nil, err
			}
			out = append(out, FullStateJob{Kind: FullStateJobChunkedTable, Spec: spec})
		}
		return out, nil
	case PlanFullStateOneTable:
		tk := strings.TrimSpace(opts.TableKey)
		if tk == "" {
			return nil, fmt.Errorf("full-state: TableKey is empty")
		}
		spec, ok := findFullStateTableSpec(cfg.Tables, tk)
		if !ok {
			return nil, fmt.Errorf("full-state: unknown table %q", tk)
		}
		spec, err := normalizeFullStateTableSpec(spec, opts.DefaultChunkSize)
		if err != nil {
			return nil, err
		}
		if err := spec.Validate(); err != nil {
			return nil, err
		}
		return []FullStateJob{{Kind: FullStateJobChunkedTable, Spec: spec}}, nil
	case PlanFullStateSelectedPKs:
		tk := strings.TrimSpace(opts.TableKey)
		if tk == "" {
			return nil, fmt.Errorf("full-state: TableKey is empty")
		}
		if len(opts.SelectedPKs) == 0 {
			return nil, fmt.Errorf("full-state: SelectedPKs is empty")
		}
		spec, ok := findFullStateTableSpec(cfg.Tables, tk)
		if !ok {
			return nil, fmt.Errorf("full-state: unknown table %q", tk)
		}
		spec, err := normalizeFullStateTableSpec(spec, opts.DefaultChunkSize)
		if err != nil {
			return nil, err
		}
		if err := validatePKRows(spec.PKColumns, opts.SelectedPKs); err != nil {
			return nil, err
		}
		if err := spec.Validate(); err != nil {
			return nil, err
		}
		return []FullStateJob{{
			Kind:        FullStateJobSelectedPKs,
			Spec:        spec,
			SelectedPKs: clonePKRows(opts.SelectedPKs),
		}}, nil
	default:
		return nil, fmt.Errorf("full-state: unknown plan mode %d", opts.Mode)
	}
}

func normalizeFullStateTableSpec(s FullStateTableSpec, defaultChunk int) (FullStateTableSpec, error) {
	s.TableKey = strings.TrimSpace(s.TableKey)
	s.RunID = strings.TrimSpace(s.RunID)
	pk := make([]string, len(s.PKColumns))
	for i, c := range s.PKColumns {
		pk[i] = strings.TrimSpace(c)
	}
	s.PKColumns = pk
	if s.ChunkSize <= 0 {
		if defaultChunk <= 0 {
			return s, fmt.Errorf("full-state: chunk_size is zero and DefaultChunkSize is unset")
		}
		s.ChunkSize = defaultChunk
	}
	return s, nil
}

func findFullStateTableSpec(tables []FullStateTableSpec, tableKey string) (FullStateTableSpec, bool) {
	want := strings.TrimSpace(tableKey)
	for _, t := range tables {
		if strings.TrimSpace(t.TableKey) == want {
			return t, true
		}
	}
	return FullStateTableSpec{}, false
}

func validatePKRows(pkColumns []string, rows [][]any) error {
	n := len(pkColumns)
	for i, row := range rows {
		if len(row) != n {
			return fmt.Errorf("full-state: selected PK row %d has length %d, want %d", i, len(row), n)
		}
	}
	return nil
}

func clonePKRows(rows [][]any) [][]any {
	out := make([][]any, len(rows))
	for i, row := range rows {
		cp := make([]any, len(row))
		copy(cp, row)
		out[i] = cp
	}
	return out
}

// FullStateJobQueue is a small FIFO for jobs produced by PlanFullStateJobs (optional P4 wiring on TubingCDC).
type FullStateJobQueue struct {
	mu   sync.Mutex
	jobs []FullStateJob
}

// NewFullStateJobQueue returns an empty queue.
func NewFullStateJobQueue() *FullStateJobQueue {
	return &FullStateJobQueue{}
}

// Enqueue appends jobs to the tail.
func (q *FullStateJobQueue) Enqueue(jobs ...FullStateJob) {
	if q == nil || len(jobs) == 0 {
		return
	}
	q.mu.Lock()
	q.jobs = append(q.jobs, jobs...)
	q.mu.Unlock()
}

// TryDequeue removes and returns the head job, if any.
func (q *FullStateJobQueue) TryDequeue() (FullStateJob, bool) {
	if q == nil {
		return FullStateJob{}, false
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.jobs) == 0 {
		return FullStateJob{}, false
	}
	j := q.jobs[0]
	copy(q.jobs, q.jobs[1:])
	q.jobs = q.jobs[:len(q.jobs)-1]
	return j, true
}

// Len returns the number of queued jobs.
func (q *FullStateJobQueue) Len() int {
	if q == nil {
		return 0
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.jobs)
}

// Clear drops all queued jobs.
func (q *FullStateJobQueue) Clear() {
	if q == nil {
		return
	}
	q.mu.Lock()
	q.jobs = nil
	q.mu.Unlock()
}
