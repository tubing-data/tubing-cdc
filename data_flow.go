package tubing_cdc

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
)

type TubingCDC struct {
	river *canal.Canal
}

func NewTubingCDC(cfg *Configs) (*TubingCDC, error) {
	if cfg == nil {
		return nil, fmt.Errorf("configs is nil")
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

	river, err := canal.NewCanal(cancalCfg)
	if err != nil {
		return nil, err
	}
	handler := cfg.EventHandler
	if handler == nil {
		handler = &MyEventHandler{}
	}
	river.SetEventHandler(handler)

	return &TubingCDC{river: river}, nil
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

func (t *TubingCDC) Close() {
	t.river.Close()
}
