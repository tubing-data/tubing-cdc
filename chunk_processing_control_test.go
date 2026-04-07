package tubing_cdc

import (
	"context"
	"testing"
	"time"
)

func TestChunkProcessingControl_PauseResume(t *testing.T) {
	tests := []struct {
		name       string
		pauseFirst bool
		wantPaused bool
	}{
		{name: "running", pauseFirst: false, wantPaused: false},
		{name: "paused", pauseFirst: true, wantPaused: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewChunkProcessingControl()
			if tt.pauseFirst {
				c.Pause()
			}
			if got := c.Paused(); got != tt.wantPaused {
				t.Fatalf("Paused() = %v, want %v", got, tt.wantPaused)
			}
			c.Resume()
			if c.Paused() {
				t.Fatal("expected not paused after Resume")
			}
			c.Resume()
			if c.Paused() {
				t.Fatal("idempotent Resume should leave running")
			}
		})
	}
}

func TestChunkProcessingControl_NilSafe(t *testing.T) {
	var c *ChunkProcessingControl
	c.Pause()
	c.Resume()
	if c.Paused() {
		t.Fatal("nil should report not paused")
	}
	if err := c.WaitIfPaused(context.Background()); err != nil {
		t.Fatalf("WaitIfPaused: %v", err)
	}
}

func TestChunkProcessingControl_WaitIfPaused(t *testing.T) {
	tests := []struct {
		name   string
		cancel bool
	}{
		{name: "resume unblocks", cancel: false},
		{name: "cancel returns", cancel: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewChunkProcessingControl()
			c.Pause()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			errCh := make(chan error, 1)
			go func() {
				errCh <- c.WaitIfPaused(ctx)
			}()
			if tt.cancel {
				cancel()
				select {
				case err := <-errCh:
					if err != context.Canceled {
						t.Fatalf("got %v, want %v", err, context.Canceled)
					}
				case <-time.After(500 * time.Millisecond):
					t.Fatal("timeout waiting for cancel")
				}
				return
			}
			time.Sleep(20 * time.Millisecond)
			c.Resume()
			select {
			case err := <-errCh:
				if err != nil {
					t.Fatalf("WaitIfPaused: %v", err)
				}
			case <-time.After(500 * time.Millisecond):
				t.Fatal("timeout waiting for resume")
			}
		})
	}
}
