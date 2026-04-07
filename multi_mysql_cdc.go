package tubing_cdc

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

// MySQLSourceSpec identifies one MySQL replication source for P6 multi-instance composition.
// Each spec gets its own canal-backed TubingCDC; combine with ApplyMySQLSourcePersistenceScope
// when several sources share one BadgerDir or Redis key space.
type MySQLSourceSpec struct {
	// ID is a stable label (must be unique in a slice passed to NewMultiMySQLCDC). Use only
	// ASCII letters, digits, dot, underscore, or hyphen so it is safe inside storage keys.
	ID string
	// Config is the per-instance canal configuration. Must not be nil.
	Config *Configs
}

// ValidateMySQLSourceSpecs checks IDs and configs before NewMultiMySQLCDC.
func ValidateMySQLSourceSpecs(specs []MySQLSourceSpec) error {
	if len(specs) == 0 {
		return fmt.Errorf("tubingcdc: no MySQL sources")
	}
	seen := make(map[string]struct{}, len(specs))
	for i, s := range specs {
		id, err := sanitizeMySQLSourceID(s.ID)
		if err != nil {
			return fmt.Errorf("tubingcdc: source[%d] ID: %w", i, err)
		}
		if _, dup := seen[id]; dup {
			return fmt.Errorf("tubingcdc: duplicate source ID %q", id)
		}
		seen[id] = struct{}{}
		if s.Config == nil {
			return fmt.Errorf("tubingcdc: source %q: Config is nil", id)
		}
	}
	return nil
}

// ApplyMySQLSourcePersistenceScope mutates cfg so binlog position and chunk-progress keys are
// namespaced by sourceID. Call once per source before NewTubingCDC when sharing BadgerDir or
// Redis between multiple MySQL instances. It is a no-op when both persistence sections are nil.
func ApplyMySQLSourcePersistenceScope(cfg *Configs, sourceID string) error {
	if cfg == nil {
		return fmt.Errorf("tubingcdc: Configs is nil")
	}
	id, err := sanitizeMySQLSourceID(sourceID)
	if err != nil {
		return err
	}
	if cfg.PositionPersistence != nil {
		pp := cfg.PositionPersistence
		if strings.TrimSpace(pp.BadgerKey) == "" {
			pp.BadgerKey = defaultBadgerStateKey + "/" + id
		} else {
			pp.BadgerKey = strings.TrimSpace(pp.BadgerKey) + "/" + id
		}
		if strings.TrimSpace(pp.RedisKey) == "" {
			pp.RedisKey = defaultRedisPositionKey + ":" + id
		} else {
			pp.RedisKey = strings.TrimSpace(pp.RedisKey) + ":" + id
		}
	}
	if cfg.ChunkProgressPersistence != nil {
		cp := cfg.ChunkProgressPersistence
		base := strings.TrimSpace(cp.BadgerKeyPrefix)
		if base == "" {
			base = strings.TrimSuffix(DefaultChunkProgressKeyPrefix, "/")
		} else {
			base = strings.TrimSuffix(base, "/")
		}
		cp.BadgerKeyPrefix = base + "/" + id + "/"
	}
	return nil
}

func sanitizeMySQLSourceID(id string) (string, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return "", fmt.Errorf("source ID is empty")
	}
	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			continue
		case r == '_', r == '-', r == '.':
			continue
		default:
			return "", fmt.Errorf("source ID %q: invalid character %q (use [A-Za-z0-9._-])", id, r)
		}
	}
	return id, nil
}

// MultiMySQLCDC runs several independent TubingCDC instances (one MySQL server each).
// PostgreSQL or other log protocols are not implemented here; see roadmap P6 TODO.
type MultiMySQLCDC struct {
	ids       []string
	cdc       []*TubingCDC
	closeOnce sync.Once
}

// NewMultiMySQLCDC builds one TubingCDC per spec. On partial failure it closes instances
// already created and returns an error.
func NewMultiMySQLCDC(specs []MySQLSourceSpec) (*MultiMySQLCDC, error) {
	if err := ValidateMySQLSourceSpecs(specs); err != nil {
		return nil, err
	}
	ids := make([]string, len(specs))
	cdc := make([]*TubingCDC, len(specs))
	for i, s := range specs {
		id, _ := sanitizeMySQLSourceID(s.ID)
		ids[i] = id
		c, err := NewTubingCDC(s.Config)
		if err != nil {
			for j := 0; j < i; j++ {
				cdc[j].Close()
			}
			return nil, fmt.Errorf("tubingcdc: source %q: %w", id, err)
		}
		cdc[i] = c
	}
	return &MultiMySQLCDC{ids: ids, cdc: cdc}, nil
}

// SourceIDs returns the ordered spec IDs (same order as Instances).
func (m *MultiMySQLCDC) SourceIDs() []string {
	if m == nil {
		return nil
	}
	out := make([]string, len(m.ids))
	copy(out, m.ids)
	return out
}

// Instances returns the underlying TubingCDC handles in spec order.
func (m *MultiMySQLCDC) Instances() []*TubingCDC {
	if m == nil {
		return nil
	}
	out := make([]*TubingCDC, len(m.cdc))
	copy(out, m.cdc)
	return out
}

// Close stops drivers and closes every instance. Safe to call more than once (e.g. after Run returns).
func (m *MultiMySQLCDC) Close() {
	if m == nil {
		return
	}
	m.closeOnce.Do(func() {
		for _, c := range m.cdc {
			if c != nil {
				c.Close()
			}
		}
		m.cdc = nil
	})
}

// Run starts each instance's canal Run in its own goroutine. It returns when ctx is cancelled
// (after Close, with ctx.Err()) or when any Run returns a non-nil error. All instances are
// closed before this method returns.
func (m *MultiMySQLCDC) Run(ctx context.Context) error {
	if m == nil {
		return fmt.Errorf("tubingcdc: MultiMySQLCDC is nil")
	}
	if ctx == nil {
		return fmt.Errorf("tubingcdc: context is nil")
	}
	n := len(m.cdc)
	if n == 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		return nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, n)
	var wg sync.WaitGroup
	for _, c := range m.cdc {
		c := c
		wg.Add(1)
		go func() {
			defer wg.Done()
			errCh <- c.Run()
		}()
	}

	waitDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitDone)
	}()

	var reason error
	select {
	case <-ctx.Done():
		reason = ctx.Err()
	case err := <-errCh:
		reason = err
	}

	m.Close()
	cancel()
	<-waitDone

	if reason != nil {
		return reason
	}
	return nil
}
