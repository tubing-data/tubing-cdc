package tubing_cdc

import "github.com/go-mysql-org/go-mysql/canal"

type Configs struct {
	Address  string
	Username string
	Password string
	// Tables lists fully-qualified names as "database.table" for canal IncludeTableRegex.
	Tables []string
	// EventHandler is optional; when nil, MyEventHandler is used.
	// Use NewDynamicTableEventHandler(Tables, tubingcdc.WithRowEventSink(...)) to emit each row as JSON;
	// default sink is LoggerRowSink; use StdoutRowSink or KafkaRowEventSink for other destinations.
	EventHandler canal.EventHandler
}
