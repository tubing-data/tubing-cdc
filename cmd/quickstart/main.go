package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	tubingcdc "tubing-cdc"
)

const tableKey = "cdc_test.orders"

func main() {
	esSink, err := tubingcdc.NewElasticsearchRowEventSink(tubingcdc.ElasticsearchSinkConfig{
		Addresses:         []string{env("ELASTICSEARCH_URL", "http://elasticsearch:9200")},
		Index:             env("ELASTICSEARCH_INDEX", "cdc-orders"),
		Refresh:           "true",
		StoreLatestEntity: true,
	})
	if err != nil {
		log.Fatal(err)
	}

	handler := tubingcdc.NewDynamicTableEventHandler(
		[]string{tableKey},
		tubingcdc.WithRowEventSink(esSink),
		tubingcdc.WithPipeline(tubingcdc.Pipe(
			tubingcdc.Tap(func(event tubingcdc.Event) error {
				return logEvent("received CDC event", event)
			}),
			tubingcdc.Map(func(event tubingcdc.Event) tubingcdc.Event {
				processRow(event.Before)
				processRow(event.After)
				return event
			}),
			tubingcdc.Tap(func(event tubingcdc.Event) error {
				return logEvent("processed for Elasticsearch", event)
			}),
		)),
	)

	cdc, err := tubingcdc.NewTubingCDC(&tubingcdc.Configs{
		Address:      env("MYSQL_ADDRESS", "mysql:3306"),
		Username:     env("MYSQL_USERNAME", "cdc"),
		Password:     env("MYSQL_PASSWORD", "cdc_pass"),
		Tables:       []string{tableKey},
		EventHandler: handler,
	})
	if err != nil {
		log.Fatal(err)
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(stop)
	runErr := make(chan error, 1)
	go func() {
		runErr <- cdc.Run()
	}()

	log.Printf("quickstart CDC running: %s -> %s/%s", tableKey, env("ELASTICSEARCH_URL", "http://elasticsearch:9200"), env("ELASTICSEARCH_INDEX", "cdc-orders"))
	select {
	case err := <-runErr:
		cdc.Close()
		if err != nil {
			log.Fatal(err)
		}
	case <-stop:
		cdc.Close()
		<-runErr
	}
}

func logEvent(stage string, event tubingcdc.Event) error {
	payload := map[string]any{
		"action": event.Action,
		"table":  event.TableKey(),
		"before": event.Before,
		"after":  event.After,
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	log.Printf("%s: %s", stage, encoded)
	return nil
}

// processRow is the sample processing stage between the CDC event and Elasticsearch.
func processRow(row tubingcdc.Row) {
	if row == nil {
		return
	}
	status := strings.ToUpper(asString(row["status"]))
	row["status"] = status
	row["email_domain"] = emailDomain(asString(row["customer_email"]))
	row["search_text"] = strings.TrimSpace(asString(row["customer_name"]) + " " + status)
}

func asString(value any) string {
	switch value := value.(type) {
	case string:
		return value
	case []byte:
		return string(value)
	default:
		return fmt.Sprint(value)
	}
}

func emailDomain(email string) string {
	parts := strings.SplitN(email, "@", 2)
	if len(parts) != 2 {
		return ""
	}
	return strings.ToLower(parts[1])
}

func env(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
