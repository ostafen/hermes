package main

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	httpapi "github.com/ostafen/hermes/internal/api/http"
	"github.com/ostafen/hermes/internal/config"
	"github.com/ostafen/hermes/internal/processor"
	"github.com/ostafen/hermes/internal/service"
	log "github.com/sirupsen/logrus"
)

func makeProcessorConfig(cfg *config.Config) processor.Config {
	procCfg := processor.DefaultConfig(cfg.Kafka.Brokers)
	if cfg.Processor.Replication > 0 {
		procCfg.Replication = cfg.Processor.Replication
	}

	if cfg.Processor.Partitions > 0 {
		procCfg.Partitions = cfg.Processor.Partitions
	}

	if cfg.Processor.StoragePath != "" {
		procCfg.StoragePath = cfg.Processor.StoragePath
	}

	return procCfg
}

func main() {
	cfg, err := config.Read()
	if err != nil {
		log.Fatal(err)
	}

	setupLogging(cfg.Logging)

	svc := service.NewProjectionService(makeProcessorConfig(cfg))
	defer svc.Shutdown()

	setupRouter(svc)

	log.WithField("port", cfg.Server.Port).
		Info("starting http server")

	http.ListenAndServe(fmt.Sprintf(":%d", cfg.Server.Port), nil)
}

func setupLogging(config config.Log) {
	log.SetReportCaller(true)
	log.SetLevel(getLogLevel(config.Level))
	log.SetFormatter(getFormatter(config.Format))
}

func getLogLevel(level string) log.Level {
	switch strings.ToUpper(level) {
	case "TRACE":
		return log.TraceLevel
	case "DEBUG":
		return log.DebugLevel
	case "INFO":
		return log.InfoLevel
	case "FATAL":
		return log.FatalLevel
	case "PANIC":
		return log.PanicLevel
	}
	return log.InfoLevel
}

func getFormatter(format string) log.Formatter {
	switch format {
	case "JSON":
		return &log.JSONFormatter{}
	case "TEXT":
		return &log.TextFormatter{}
	}
	return &log.JSONFormatter{}
}

func setupRouter(svc service.ProjectionService) {
	r := mux.NewRouter()

	controller := httpapi.NewProjectionsController(svc)

	r.HandleFunc("/projections", controller.Create).Methods("POST")
	r.HandleFunc("/projections/{id}", controller.Get).Methods("GET")
	r.HandleFunc("/projections/{id}", controller.Delete).Methods("DELETE")

	http.Handle("/", r)
}
