package tubing_cdc

import (
	"context"
	"sync"
	"time"
)

// ChunkProcessingControl coordinates pause/resume for DBLog-style chunk drivers (P4).
// Drivers should call WaitIfPaused between chunks (or watermark steps) so operators can back off load.
type ChunkProcessingControl struct {
	mu     sync.Mutex
	paused bool
}

// NewChunkProcessingControl returns a control in the running (not paused) state.
func NewChunkProcessingControl() *ChunkProcessingControl {
	return &ChunkProcessingControl{}
}

// Pause marks chunk processing as paused. Idempotent.
func (c *ChunkProcessingControl) Pause() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.paused = true
	c.mu.Unlock()
}

// Resume clears the paused flag. Idempotent.
func (c *ChunkProcessingControl) Resume() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.paused = false
	c.mu.Unlock()
}

// Paused reports whether Pause is in effect.
func (c *ChunkProcessingControl) Paused() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.paused
}

// WaitIfPaused blocks until the control is not paused, ctx is cancelled, or a short poll interval elapses
// and the state is checked again. Returns nil when unpaused; ctx.Err() when the context ends while paused.
func (c *ChunkProcessingControl) WaitIfPaused(ctx context.Context) error {
	if c == nil || ctx == nil {
		return nil
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		c.mu.Lock()
		p := c.paused
		c.mu.Unlock()
		if !p {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
