package tubing_cdc

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// ErrLeadershipLost is returned from RunTubingCDCWithLeaderElection when the Redis lease could not be
// renewed or the session was released after another instance took over.
var ErrLeadershipLost = errors.New("tubingcdc: leadership lost")

const defaultLeaderLockKey = "tubing-cdc:leader"

// LeaderSession is a single hold on the distributed leader lock. Call Release when tearing down;
// Lost is closed when the lease expires without renewal, renewal fails, or Release runs.
type LeaderSession interface {
	Lost() <-chan struct{}
	Release()
}

// LeaderElectionConfig selects Redis for DBLog-style active/standby coordination (P5). Only one
// holder should run canal at a time; standbys retry AcquireRedisLeaderSession or use
// RunTubingCDCWithLeaderElection, which recreates TubingCDC each term (go-mysql canal is not
// restartable after Close).
type LeaderElectionConfig struct {
	RedisAddr     string
	RedisPassword string
	RedisDB       int
	// LockKey is the Redis key for the lease; empty defaults to "tubing-cdc:leader".
	LockKey string
	// Token uniquely identifies this process instance; empty defaults to a random UUID string.
	Token string
	// Lease is the key TTL. Renewals run on RenewInterval. Minimum 3 seconds.
	Lease time.Duration
	// RenewInterval defaults to Lease/3 when zero.
	RenewInterval time.Duration
	// AcquireRetryInterval is the sleep between failed non-blocking acquire attempts; default 500ms.
	AcquireRetryInterval time.Duration
}

func (c *LeaderElectionConfig) validate() error {
	if c == nil {
		return fmt.Errorf("leader election: config is nil")
	}
	if strings.TrimSpace(c.RedisAddr) == "" {
		return fmt.Errorf("leader election: RedisAddr is empty")
	}
	if c.Lease < 3*time.Second {
		return fmt.Errorf("leader election: Lease must be at least 3s")
	}
	return nil
}

func (c *LeaderElectionConfig) applyDefaults() {
	if strings.TrimSpace(c.LockKey) == "" {
		c.LockKey = defaultLeaderLockKey
	}
	if c.Lease == 0 {
		c.Lease = 15 * time.Second
	}
	if c.RenewInterval <= 0 {
		c.RenewInterval = c.Lease / 3
	}
	if c.RenewInterval < time.Second {
		c.RenewInterval = time.Second
	}
	if c.AcquireRetryInterval <= 0 {
		c.AcquireRetryInterval = 500 * time.Millisecond
	}
}

// RunTubingCDCWithLeaderElection runs canal as leader only: it acquires cfg.LeaderElection, builds
// TubingCDC, and calls Run until leadership is lost or ctx is cancelled, then repeats as standby.
// cfg.LeaderElection must be non-nil. MySQL errors from Run are returned and stop the loop.
func RunTubingCDCWithLeaderElection(ctx context.Context, cfg *Configs) error {
	return runTubingCDCWithLeaderElection(ctx, cfg, nil)
}

// RunTubingCDCWithLeaderElectionFrom is like RunTubingCDCWithLeaderElection but uses RunFrom(pos)
// for each leader term.
func RunTubingCDCWithLeaderElectionFrom(ctx context.Context, cfg *Configs, pos mysql.Position) error {
	p := pos
	return runTubingCDCWithLeaderElection(ctx, cfg, &p)
}

func runTubingCDCWithLeaderElection(ctx context.Context, cfg *Configs, pos *mysql.Position) error {
	if cfg == nil {
		return fmt.Errorf("tubingcdc: configs is nil")
	}
	if cfg.LeaderElection == nil {
		return fmt.Errorf("tubingcdc: LeaderElection is not configured")
	}
	lec := *cfg.LeaderElection
	lec.applyDefaults()
	if err := lec.validate(); err != nil {
		return err
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		sess, err := AcquireRedisLeaderSession(ctx, &lec)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		runErr := runOneCDCLeaderTerm(ctx, cfg, pos, sess)
		sess.Release()
		if runErr != nil {
			if errors.Is(runErr, ErrLeadershipLost) {
				continue
			}
			return runErr
		}
	}
}

func runOneCDCLeaderTerm(ctx context.Context, cfg *Configs, pos *mysql.Position, sess LeaderSession) error {
	cdc, err := NewTubingCDC(cfg)
	if err != nil {
		return err
	}
	runErr := make(chan error, 1)
	go func() {
		if pos != nil {
			runErr <- cdc.RunFrom(*pos)
			return
		}
		runErr <- cdc.Run()
	}()
	select {
	case <-ctx.Done():
		cdc.Close()
		<-runErr
		return ctx.Err()
	case <-sess.Lost():
		cdc.Close()
		<-runErr
		return ErrLeadershipLost
	case err := <-runErr:
		cdc.Close()
		if err != nil {
			return err
		}
		return ErrLeadershipLost
	}
}
