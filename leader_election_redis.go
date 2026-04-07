package tubing_cdc

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const (
	luaLeaderRenew   = `if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("PEXPIRE", KEYS[1], ARGV[2]) else return 0 end`
	luaLeaderRelease = `if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("DEL", KEYS[1]) else return 0 end`
)

type redisLeaderSession struct {
	client *redis.Client
	key    string
	token  string
	lease  time.Duration

	lost       chan struct{}
	lostOnce   sync.Once
	closeOnce  sync.Once
	stopRenew  context.CancelFunc
	mu         sync.Mutex
	released   bool
}

func (s *redisLeaderSession) Lost() <-chan struct{} {
	return s.lost
}

func (s *redisLeaderSession) Release() {
	s.mu.Lock()
	if s.released {
		s.mu.Unlock()
		return
	}
	s.released = true
	stop := s.stopRenew
	s.mu.Unlock()
	if stop != nil {
		stop()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _ = s.client.Eval(ctx, luaLeaderRelease, []string{s.key}, s.token).Result()
	s.signalLost()
	s.closeClient()
}

func (s *redisLeaderSession) signalLost() {
	s.lostOnce.Do(func() { close(s.lost) })
}

func (s *redisLeaderSession) closeClient() {
	s.closeOnce.Do(func() { _ = s.client.Close() })
}

// AcquireRedisLeaderSession blocks until this instance holds the Redis lease or ctx is cancelled.
func AcquireRedisLeaderSession(ctx context.Context, cfg *LeaderElectionConfig) (LeaderSession, error) {
	if cfg == nil {
		return nil, fmt.Errorf("leader election: config is nil")
	}
	c := *cfg
	c.applyDefaults()
	if err := c.validate(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(c.Token) == "" {
		c.Token = uuid.NewString()
	}
	rdb := redis.NewClient(&redis.Options{
		Addr:     c.RedisAddr,
		Password: c.RedisPassword,
		DB:       c.RedisDB,
	})
	for {
		if err := ctx.Err(); err != nil {
			_ = rdb.Close()
			return nil, err
		}
		ok, err := rdb.SetNX(ctx, c.LockKey, c.Token, c.Lease).Result()
		if err != nil {
			_ = rdb.Close()
			return nil, err
		}
		if ok {
			sess := &redisLeaderSession{
				client: rdb,
				key:    c.LockKey,
				token:  c.Token,
				lease:  c.Lease,
				lost:   make(chan struct{}),
			}
			renewCtx, cancel := context.WithCancel(ctx)
			sess.stopRenew = cancel
			go sess.renewLoop(renewCtx, c.RenewInterval)
			return sess, nil
		}
		select {
		case <-ctx.Done():
			_ = rdb.Close()
			return nil, ctx.Err()
		case <-time.After(c.AcquireRetryInterval):
		}
	}
}

func (s *redisLeaderSession) renewLoop(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			rctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			n, err := s.client.Eval(rctx, luaLeaderRenew, []string{s.key}, s.token, s.lease.Milliseconds()).Int64()
			cancel()
			if err != nil || n == 0 {
				s.failRenew()
				return
			}
		}
	}
}

func (s *redisLeaderSession) failRenew() {
	s.mu.Lock()
	if s.released {
		s.mu.Unlock()
		return
	}
	s.released = true
	stop := s.stopRenew
	s.mu.Unlock()
	if stop != nil {
		stop()
	}
	s.signalLost()
	s.closeClient()
}
