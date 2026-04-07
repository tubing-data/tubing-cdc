package tubing_cdc_test

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	tcmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
)

// mysqlIntegrationTCP opens a binlog-enabled MySQL container (same image wiring as cdc_dem_test)
// and returns host:port and a *sql.DB as root on cdc_test. Container and DB are closed via t.Cleanup.
func mysqlIntegrationTCP(t *testing.T) (tcpAddr string, adminDB *sql.DB) {
	t.Helper()
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
	t.Cleanup(func() { _ = mysqlC.Terminate(context.Background()) })

	host, err := mysqlC.Host(ctx)
	if err != nil {
		t.Fatalf("container host: %v", err)
	}
	mapped, err := mysqlC.MappedPort(ctx, "3306/tcp")
	if err != nil {
		t.Fatalf("mapped port: %v", err)
	}
	tcpAddr = net.JoinHostPort(host, mapped.Port())

	adminDSN := fmt.Sprintf("root:root@tcp(%s)/cdc_test?parseTime=true", tcpAddr)
	db, err := sql.Open("mysql", adminDSN)
	if err != nil {
		t.Fatalf("sql open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	return tcpAddr, db
}

func waitCondition(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.After(timeout)
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-deadline:
			t.Fatal("timeout waiting condition")
		case <-tick.C:
			if fn() {
				return
			}
		}
	}
}
