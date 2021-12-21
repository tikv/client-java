package main

import (
	"flag"
	"time"

	log "github.com/sirupsen/logrus"
	slog "github.com/tikv/client-java/tools/slow-log-timeline/log"
)

var (
	logPath   = flag.String("path", "", "path of slow log file")
	threshold = flag.String("threshold", "10ms", "warning threshold for slow log")
	factor    = flag.Int("factor", 70, "factor for bars")
)

func main() {
	flag.Parse()

	warningThreshold, err := time.ParseDuration(*threshold)
	if err != nil {
		log.Fatalf("invalid threshold: %v", err)
	}

	opts := slog.Opts{
		LogPath:       *logPath,
		BarLen:        *factor,
		WarnThreshold: warningThreshold,
	}

	slowLogs, err := slog.ParseLog(opts)
	if err != nil {
		log.Fatal(err)
	}
	for _, slowLog := range slowLogs {
		slowLog.Log()
	}
}
