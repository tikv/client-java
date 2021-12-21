package log

import (
	"bufio"
	"encoding/json"
	"os"
	"regexp"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

type Opts struct {
	LogPath       string
	BarLen        int
	WarnThreshold time.Duration
}

func ParseLog(opts Opts) ([]*SlowLog, error) {
	file, err := os.Open(opts.LogPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	logPrefix := "SlowLog:"
	expr, err := regexp.Compile(logPrefix + "{.+}")
	if err != nil {
		return nil, err
	}

	var logs []*SlowLog
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		filtered := expr.FindString(scanner.Text())
		if filtered == "" {
			continue
		}
		logText := strings.TrimPrefix(filtered, "SlowLog:")

		slowLog := &SlowLog{}
		err := json.Unmarshal([]byte(logText), slowLog)
		if err != nil {
			log.Errorf("Fail to read slow log: %s", filtered)
			continue
		}
		slowLog.opts = opts

		logs = append(logs, slowLog)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return logs, nil
}
