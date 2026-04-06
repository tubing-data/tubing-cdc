package tubing_cdc_test

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/docker/docker/client"
	_ "github.com/go-sql-driver/mysql"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	tcmysql "github.com/testcontainers/testcontainers-go/modules/mysql"

	tubingcdc "tubing-cdc"
)

type rowCapture struct {
	canal.DummyEventHandler
	rows chan *canal.RowsEvent
}

func (r *rowCapture) OnRow(e *canal.RowsEvent) error {
	r.rows <- e
	return nil
}

func (r *rowCapture) String() string {
	return "rowCapture"
}

func moduleDir(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Dir(file)
}

func masterPosition(t *testing.T, db *sql.DB) mysql.Position {
	t.Helper()
	var file string
	var pos uint32
	var a, b, c sql.NullString
	if err := db.QueryRow("SHOW MASTER STATUS").Scan(&file, &pos, &a, &b, &c); err != nil {
		t.Fatalf("SHOW MASTER STATUS: %v", err)
	}
	return mysql.Position{Name: file, Pos: pos}
}

func waitDemTestAction(t *testing.T, ch <-chan *canal.RowsEvent, want string, timeout time.Duration) *canal.RowsEvent {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case e := <-ch:
			if e == nil || e.Table == nil {
				continue
			}
			if e.Table.Schema == "cdc_test" && e.Table.Name == "dem_test" && e.Action == want {
				return e
			}
		case <-deadline:
			t.Fatalf("timeout waiting for dem_test action %q", want)
		}
	}
}

func ensureDocker(t *testing.T) {
	t.Helper()
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		t.Skipf("docker client: %v", err)
	}
	pctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := cli.Ping(pctx); err != nil {
		t.Skipf("docker daemon: %v", err)
	}
}

func TestCDC_dem_test_row_events_integration(t *testing.T) {
	ensureDocker(t)

	ctx := context.Background()
	root := moduleDir(t)
	binlogCNF := filepath.Join(root, "docker", "mysql", "conf.d", "binlog.cnf")
	initSQL := filepath.Join(root, "docker", "mysql", "init", "01-init.sql")

	mysqlC, err := tcmysql.RunContainer(ctx,
		tcmysql.WithUsername("root"),
		tcmysql.WithPassword("root"),
		tcmysql.WithDatabase("cdc_test"),
		tcmysql.WithConfigFile(binlogCNF),
		tcmysql.WithScripts(initSQL),
	)
	if err != nil {
		t.Fatalf("start mysql container: %v", err)
	}
	t.Cleanup(func() {
		_ = mysqlC.Terminate(context.Background())
	})

	host, err := mysqlC.Host(ctx)
	if err != nil {
		t.Fatalf("container host: %v", err)
	}
	mapped, err := mysqlC.MappedPort(ctx, "3306/tcp")
	if err != nil {
		t.Fatalf("mapped port: %v", err)
	}
	tcpAddr := net.JoinHostPort(host, mapped.Port())

	adminDSN := fmt.Sprintf("root:root@tcp(%s)/cdc_test?parseTime=true", tcpAddr)
	adminDB, err := sql.Open("mysql", adminDSN)
	if err != nil {
		t.Fatalf("sql open admin: %v", err)
	}
	t.Cleanup(func() { _ = adminDB.Close() })
	if err := adminDB.PingContext(ctx); err != nil {
		t.Fatalf("ping admin: %v", err)
	}

	pos := masterPosition(t, adminDB)

	appDSN := fmt.Sprintf("cdc:cdc_pass@tcp(%s)/cdc_test?parseTime=true", tcpAddr)
	capture := &rowCapture{rows: make(chan *canal.RowsEvent, 64)}
	cfg := &tubingcdc.Configs{
		Address:      tcpAddr,
		Username:     "cdc",
		Password:     "cdc_pass",
		Tables:       []string{"cdc_test.dem_test"},
		EventHandler: capture,
	}
	cdc, err := tubingcdc.NewTubingCDC(cfg)
	if err != nil {
		t.Fatalf("NewTubingCDC: %v", err)
	}
	t.Cleanup(func() { cdc.Close() })

	go func() {
		_ = cdc.RunFrom(pos)
	}()

	appDB, err := sql.Open("mysql", appDSN)
	if err != nil {
		t.Fatalf("sql open app: %v", err)
	}
	t.Cleanup(func() { _ = appDB.Close() })
	if err := appDB.PingContext(ctx); err != nil {
		t.Fatalf("ping app: %v", err)
	}

	tests := []struct {
		name       string
		sql        string
		wantAction string
	}{
		{name: "insert", sql: `INSERT INTO dem_test (name) VALUES ('cdc_row1')`, wantAction: canal.InsertAction},
		{name: "update", sql: `UPDATE dem_test SET name='cdc_row2' WHERE name='cdc_row1'`, wantAction: canal.UpdateAction},
		{name: "delete", sql: `DELETE FROM dem_test WHERE name='cdc_row2'`, wantAction: canal.DeleteAction},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := appDB.ExecContext(ctx, tt.sql); err != nil {
				t.Fatalf("exec: %v", err)
			}
			ev := waitDemTestAction(t, capture.rows, tt.wantAction, 45*time.Second)
			t.Logf("action=%s schema=%s table=%s rows=%v", ev.Action, ev.Table.Schema, ev.Table.Name, ev.Rows)
		})
	}
}
