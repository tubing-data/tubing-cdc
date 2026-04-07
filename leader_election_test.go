package tubing_cdc

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/redis/go-redis/v9"
)

func TestRunTubingCDCWithLeaderElection_validation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tests := []struct {
		name    string
		cfg     *Configs
		wantSub string
	}{
		{
			name:    "nil configs",
			cfg:     nil,
			wantSub: "configs is nil",
		},
		{
			name: "nil leader election",
			cfg: &Configs{
				Address:         "x",
				Username:        "u",
				Password:        "p",
				Tables:          []string{"db.t"},
				LeaderElection:  nil,
			},
			wantSub: "LeaderElection is not configured",
		},
		{
			name: "empty redis addr",
			cfg: &Configs{
				Address:  "x",
				Username: "u",
				Password: "p",
				Tables:   []string{"db.t"},
				LeaderElection: &LeaderElectionConfig{
					Lease: 5 * time.Second,
				},
			},
			wantSub: "RedisAddr is empty",
		},
		{
			name: "lease too short",
			cfg: &Configs{
				Address:  "x",
				Username: "u",
				Password: "p",
				Tables:   []string{"db.t"},
				LeaderElection: &LeaderElectionConfig{
					RedisAddr: "127.0.0.1:6379",
					Lease:     time.Second,
				},
			},
			wantSub: "Lease must be at least 3s",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := RunTubingCDCWithLeaderElection(ctx, tt.cfg)
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.wantSub) {
				t.Fatalf("error %q does not contain %q", err.Error(), tt.wantSub)
			}
		})
	}
}

func TestAcquireRedisLeaderSession_exclusive(t *testing.T) {
	mr := miniredis.RunT(t)
	base := LeaderElectionConfig{
		RedisAddr:            mr.Addr(),
		Lease:                10 * time.Second,
		RenewInterval:        time.Second,
		AcquireRetryInterval: 20 * time.Millisecond,
		LockKey:              "tubing-cdc:test:exclusive",
		Token:                "owner-a",
	}
	ctx := context.Background()
	s1, err := AcquireRedisLeaderSession(ctx, &base)
	if err != nil {
		t.Fatal(err)
	}

	ctx2, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	_, err = AcquireRedisLeaderSession(ctx2, &LeaderElectionConfig{
		RedisAddr:            mr.Addr(),
		Lease:                10 * time.Second,
		RenewInterval:        time.Second,
		AcquireRetryInterval: 10 * time.Millisecond,
		LockKey:              base.LockKey,
		Token:                "owner-b",
	})
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}

	s1.Release()

	s2, err := AcquireRedisLeaderSession(ctx, &LeaderElectionConfig{
		RedisAddr:            mr.Addr(),
		Lease:                10 * time.Second,
		RenewInterval:        time.Second,
		AcquireRetryInterval: 10 * time.Millisecond,
		LockKey:              base.LockKey,
		Token:                "owner-b",
	})
	if err != nil {
		t.Fatal(err)
	}
	s2.Release()
}

func TestRedisLeaderSession_lostWhenKeyStolen(t *testing.T) {
	mr := miniredis.RunT(t)
	key := "tubing-cdc:test:stolen"
	cfg := LeaderElectionConfig{
		RedisAddr:            mr.Addr(),
		Lease:                5 * time.Second,
		RenewInterval:        30 * time.Millisecond,
		AcquireRetryInterval: 10 * time.Millisecond,
		LockKey:              key,
		Token:                "leader-1",
	}
	sess, err := AcquireRedisLeaderSession(context.Background(), &cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Release()

	r2 := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	if err := r2.Set(context.Background(), key, "intruder", 0).Err(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-sess.Lost():
	case <-time.After(2 * time.Second):
		t.Fatal("expected Lost after key stolen")
	}
}

func TestRunTubingCDCWithLeaderElectionFrom_acceptsPosition(t *testing.T) {
	t.Parallel()
	// Only checks that mysql.Position is accepted by the API surface; validation fails before MySQL.
	ctx := context.Background()
	err := RunTubingCDCWithLeaderElectionFrom(ctx, &Configs{
		Address:  "x",
		Username: "u",
		Password: "p",
		Tables:   []string{"db.t"},
		LeaderElection: &LeaderElectionConfig{
			RedisAddr: "127.0.0.1:1",
			Lease:     5 * time.Second,
		},
	}, mysql.Position{Name: "f", Pos: 4})
	if err == nil {
		t.Fatal("expected error (no redis)")
	}
}

func TestLeaderElectionConfig_applyDefaults(t *testing.T) {
	t.Parallel()
	c := LeaderElectionConfig{RedisAddr: "127.0.0.1:6379"}
	c.applyDefaults()
	if c.LockKey != defaultLeaderLockKey {
		t.Fatalf("LockKey = %q", c.LockKey)
	}
	if c.Lease != 15*time.Second {
		t.Fatalf("Lease = %v", c.Lease)
	}
	if c.RenewInterval != c.Lease/3 {
		t.Fatalf("RenewInterval = %v", c.RenewInterval)
	}
	if err := c.validate(); err != nil {
		t.Fatal(err)
	}
}

func TestLeaderElectionConfig_validate_nilReceiver(t *testing.T) {
	t.Parallel()
	var c *LeaderElectionConfig
	if err := c.validate(); err == nil {
		t.Fatal("expected error")
	}
}

func TestAcquireRedisLeaderSession_nilConfig(t *testing.T) {
	t.Parallel()
	_, err := AcquireRedisLeaderSession(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error")
	}
}
