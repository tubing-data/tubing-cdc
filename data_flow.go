package tubing_cdc

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
)

type TubingCDC struct {
	river        *canal.Canal
	posStore     *twoTierPositionStore
	chunkStore   *ChunkProgressStore
	sharedBadger *badger.DB
	chunkControl *ChunkProcessingControl
	fullStateQ   *FullStateJobQueue

	drvMu            sync.Mutex
	algoDriverCancel context.CancelFunc
	algoDriverWG     sync.WaitGroup
}

func NewTubingCDC(cfg *Configs) (*TubingCDC, error) {
	if cfg == nil {
		return nil, fmt.Errorf("configs is nil")
	}
	if cfg.PositionPersistence != nil && strings.TrimSpace(cfg.PositionPersistence.BadgerDir) == "" {
		return nil, fmt.Errorf("position persistence: BadgerDir is empty")
	}
	if cfg.ChunkProgressPersistence != nil {
		if err := cfg.ChunkProgressPersistence.validate(); err != nil {
			return nil, err
		}
	}

	cancalCfg := canal.NewDefaultConfig()
	cancalCfg.Addr = cfg.Address
	cancalCfg.User = cfg.Username
	cancalCfg.Password = cfg.Password
	cancalCfg.Dump.ExecutionPath = ""

	for _, tbl := range cfg.Tables {
		re, err := tableIncludeRegex(tbl)
		if err != nil {
			return nil, err
		}
		cancalCfg.IncludeTableRegex = append(cancalCfg.IncludeTableRegex, re)
	}

	if cfg.Watermark != nil {
		if err := cfg.Watermark.Validate(); err != nil {
			return nil, err
		}
		wmKey := strings.TrimSpace(cfg.Watermark.TableKey)
		already := false
		for _, t := range cfg.Tables {
			if strings.TrimSpace(t) == wmKey {
				already = true
				break
			}
		}
		if !already {
			re, err := tableIncludeRegex(wmKey)
			if err != nil {
				return nil, err
			}
			cancalCfg.IncludeTableRegex = append(cancalCfg.IncludeTableRegex, re)
		}
	}

	river, err := canal.NewCanal(cancalCfg)
	if err != nil {
		return nil, err
	}
	handler := cfg.EventHandler
	if handler == nil {
		handler = &MyEventHandler{}
	}

	if cfg.Watermark != nil && cfg.WatermarkNotifier != nil {
		handler = wrapHandlerWithWatermark(handler, cfg.Watermark, cfg.WatermarkNotifier, cfg.ForwardWatermarkRowsToEventHandler)
	}

	if cfg.Algorithm1 != nil {
		if err := cfg.Algorithm1.validate(cfg.Watermark); err != nil {
			river.Close()
			return nil, err
		}
		handler = wrapHandlerWithAlgorithm1(handler, cfg.Algorithm1.Tracker, cfg.Algorithm1.TargetTableKey)
	}

	var posStore *twoTierPositionStore
	var chunkStore *ChunkProgressStore
	var sharedDB *badger.DB

	posOn := cfg.PositionPersistence != nil && strings.TrimSpace(cfg.PositionPersistence.BadgerDir) != ""
	chunkOn := cfg.ChunkProgressPersistence != nil

	if posOn && chunkOn {
		pDir := strings.TrimSpace(cfg.PositionPersistence.BadgerDir)
		cDir := strings.TrimSpace(cfg.ChunkProgressPersistence.BadgerDir)
		if pDir == cDir {
			var oerr error
			sharedDB, oerr = openBadgerDB(pDir)
			if oerr != nil {
				river.Close()
				return nil, oerr
			}
			var perr error
			posStore, perr = newTwoTierPositionStoreWithDB(sharedDB, cfg.PositionPersistence, false)
			if perr != nil {
				_ = sharedDB.Close()
				river.Close()
				return nil, perr
			}
			var cerr error
			chunkStore, cerr = newChunkProgressStoreWithDB(sharedDB, cfg.ChunkProgressPersistence, false)
			if cerr != nil {
				_ = posStore.Close()
				_ = sharedDB.Close()
				river.Close()
				return nil, cerr
			}
		} else {
			var perr error
			posStore, perr = newTwoTierPositionStore(cfg.PositionPersistence)
			if perr != nil {
				river.Close()
				return nil, perr
			}
			var cerr error
			chunkStore, cerr = newChunkProgressStore(cfg.ChunkProgressPersistence)
			if cerr != nil {
				_ = posStore.Close()
				river.Close()
				return nil, cerr
			}
		}
	} else if posOn {
		var perr error
		posStore, perr = newTwoTierPositionStore(cfg.PositionPersistence)
		if perr != nil {
			river.Close()
			return nil, perr
		}
	} else if chunkOn {
		var cerr error
		chunkStore, cerr = newChunkProgressStore(cfg.ChunkProgressPersistence)
		if cerr != nil {
			river.Close()
			return nil, cerr
		}
	}

	if posStore != nil {
		handler = wrapHandlerWithPositionStore(handler, posStore)
	}
	river.SetEventHandler(handler)

	return &TubingCDC{
		river:        river,
		posStore:     posStore,
		chunkStore:   chunkStore,
		sharedBadger: sharedDB,
		chunkControl: cfg.ChunkProcessingControl,
		fullStateQ:   cfg.FullStateJobQueue,
	}, nil
}

func tableIncludeRegex(dbTable string) (string, error) {
	parts := strings.SplitN(dbTable, ".", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", fmt.Errorf("tables entry must be database.table, got %q", dbTable)
	}
	return regexp.QuoteMeta(parts[0]) + `\.` + regexp.QuoteMeta(parts[1]), nil
}

func (t *TubingCDC) Run() error {
	return t.river.Run()
}

func (t *TubingCDC) RunFrom(pos mysql.Position) error {
	return t.river.RunFrom(pos)
}

// Canal returns the underlying go-mysql canal for advanced use (e.g. WatermarkUpdateValueSQL + Execute
// during P3 chunk cycles). Nil when TubingCDC is nil.
func (t *TubingCDC) Canal() *canal.Canal {
	if t == nil {
		return nil
	}
	return t.river
}

func (t *TubingCDC) Close() {
	t.StopAlgorithm1ChunkDriver()
	// Close canal while position/chunk Badger stores are still open: canal.Close may invoke
	// OnPosSynced on the handler chain, which must not run against a nil or closed Badger DB.
	t.river.Close()
	if t.posStore != nil {
		_ = t.posStore.Close()
		t.posStore = nil
	}
	if t.chunkStore != nil {
		_ = t.chunkStore.Close()
		t.chunkStore = nil
	}
	if t.sharedBadger != nil {
		_ = t.sharedBadger.Close()
		t.sharedBadger = nil
	}
}

// ChunkProgressStore returns the P2 chunk cursor store when Configs.ChunkProgressPersistence was set, or nil.
func (t *TubingCDC) ChunkProgressStore() *ChunkProgressStore {
	if t == nil {
		return nil
	}
	return t.chunkStore
}

// ChunkProcessingControl returns Configs.ChunkProcessingControl when it was set, or nil.
func (t *TubingCDC) ChunkProcessingControl() *ChunkProcessingControl {
	if t == nil {
		return nil
	}
	return t.chunkControl
}

// FullStateJobQueue returns Configs.FullStateJobQueue when it was set, or nil.
func (t *TubingCDC) FullStateJobQueue() *FullStateJobQueue {
	if t == nil {
		return nil
	}
	return t.fullStateQ
}

// EnqueueFullStateJobs runs PlanFullStateJobs and appends jobs to Configs.FullStateJobQueue. It returns
// the number of jobs enqueued. An error is returned when planning fails or no queue was configured.
func (t *TubingCDC) EnqueueFullStateJobs(cfg *FullStateCaptureConfig, opts PlanFullStateJobsOptions) (int, error) {
	if t == nil {
		return 0, fmt.Errorf("tubing cdc: nil receiver")
	}
	if t.fullStateQ == nil {
		return 0, fmt.Errorf("tubing cdc: FullStateJobQueue is not configured")
	}
	jobs, err := PlanFullStateJobs(cfg, opts)
	if err != nil {
		return 0, err
	}
	t.fullStateQ.Enqueue(jobs...)
	return len(jobs), nil
}

// StartAlgorithm1ChunkDriver starts a background goroutine that dequeues FullStateJob values from
// cfg.JobQueue and runs DBLog Algorithm 1 cycles (watermark updates via Canal.Execute, SELECT,
// ReconcileChunkRows, RowSink.Emit). See docs/algorithm1-chunk-driver.md for semantics and limits.
// Idempotent only when no driver is already running. Call StopAlgorithm1ChunkDriver before Close (Close invokes it).
func (t *TubingCDC) StartAlgorithm1ChunkDriver(ctx context.Context, cfg Algorithm1ChunkDriverConfig) error {
	if t == nil || t.river == nil {
		return fmt.Errorf("tubing cdc: nil receiver or canal")
	}
	if err := cfg.validate(); err != nil {
		return err
	}
	t.drvMu.Lock()
	defer t.drvMu.Unlock()
	if t.algoDriverCancel != nil {
		return fmt.Errorf("tubing cdc: algorithm1 chunk driver already running")
	}
	runCtx, cancel := context.WithCancel(ctx)
	d := newAlgorithm1ChunkDriver(t.river, cfg, cancel)
	t.algoDriverCancel = cancel
	t.algoDriverWG.Add(1)
	go func() {
		defer func() {
			t.drvMu.Lock()
			t.algoDriverCancel = nil
			t.drvMu.Unlock()
			t.algoDriverWG.Done()
		}()
		d.run(runCtx)
	}()
	return nil
}

// StopAlgorithm1ChunkDriver cancels the driver context and waits for the goroutine to exit.
func (t *TubingCDC) StopAlgorithm1ChunkDriver() {
	if t == nil {
		return
	}
	t.drvMu.Lock()
	cancel := t.algoDriverCancel
	t.drvMu.Unlock()
	if cancel != nil {
		cancel()
	}
	t.algoDriverWG.Wait()
}
