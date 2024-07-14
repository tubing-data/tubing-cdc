package tubing_cdc

import "github.com/go-mysql-org/go-mysql/canal"

type TubingCDC struct {
	river *canal.Canal
}

func NewTubingCDC(cfg *Configs) (t *TubingCDC, err error) {
	cancalCfg := canal.NewDefaultConfig()
	cancalCfg.Addr = cfg.Address
	cancalCfg.User = cfg.Username
	cancalCfg.Password = cfg.Password
	river, err := canal.NewCanal(cancalCfg)
	river.SetEventHandler(&MyEventHandler{})
	if err != nil {
		return nil, err
	}
	return &TubingCDC{river: river}, err
}

func (t *TubingCDC) Run() error {
	return t.river.Run()
}
