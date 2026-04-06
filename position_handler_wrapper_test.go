package tubing_cdc

import (
	"errors"
	"testing"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

type posSyncStub struct {
	canal.DummyEventHandler
	onPosErr   error
	innerCalls int
}

func (p *posSyncStub) OnPosSynced(_ *replication.EventHeader, _ mysql.Position, _ mysql.GTIDSet, _ bool) error {
	p.innerCalls++
	return p.onPosErr
}

func (p *posSyncStub) String() string {
	return "posSyncStub"
}

func TestPositionHandlerWrapper_OnPosSynced(t *testing.T) {
	tests := []struct {
		name        string
		innerErr    error
		wantInner   int
		wantPersist bool
	}{
		{name: "persists after inner ok", innerErr: nil, wantInner: 1, wantPersist: true},
		{name: "skips store on inner error", innerErr: errors.New("boom"), wantInner: 1, wantPersist: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			store, err := newTwoTierPositionStore(&PositionPersistence{BadgerDir: dir})
			if err != nil {
				t.Fatal(err)
			}

			stub := &posSyncStub{onPosErr: tt.innerErr}
			h := wrapHandlerWithPositionStore(stub, store)
			pos := mysql.Position{Name: "w.Bin", Pos: 7}
			gotErr := h.OnPosSynced(nil, pos, nil, false)
			if tt.innerErr != nil {
				if gotErr == nil || gotErr.Error() != tt.innerErr.Error() {
					t.Fatalf("error: got %v want %v", gotErr, tt.innerErr)
				}
			} else if gotErr != nil {
				t.Fatal(gotErr)
			}
			if stub.innerCalls != tt.wantInner {
				t.Fatalf("inner calls: got %d want %d", stub.innerCalls, tt.wantInner)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			_, _, rerr := ReadBinlogStateFromBadger(dir, "")
			if tt.wantPersist {
				if rerr != nil {
					t.Fatal(rerr)
				}
			} else {
				if rerr == nil {
					t.Fatal("expected read error when inner failed")
				}
			}
		})
	}
}
