package log

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/fatih/color"
)

type SlowLog struct {
	Name     string
	Start    time.Duration
	End      time.Duration
	Duration time.Duration
	Region   string
	Key      string
	Spans    []Span
	opts     Opts
}

type Span struct {
	Name     string
	Start    time.Duration
	End      time.Duration
	Duration time.Duration
}

func (l *SlowLog) UnmarshalJSON(data []byte) error {
	type span struct {
		Name     string `json:"name"`
		Start    string `json:"start"`
		End      string `json:"end"`
		Duration string `json:"duration"`
	}
	type slowLog struct {
		Name     string `json:"func"`
		Start    string `json:"start"`
		End      string `json:"end"`
		Duration string `json:"duration"`
		Region   string `json:"region"`
		Key      string `json:"key"`
		Spans    []span `json:"spans"`
	}
	tmp := &slowLog{}
	err := json.Unmarshal(data, tmp)
	if err != nil {
		return err
	}

	var (
		start    time.Duration
		end      time.Duration
		duration time.Duration
	)
	start, err = parseTime(tmp.Start)
	if err != nil {
		return err
	}
	end, err = parseTime(tmp.End)
	if err != nil {
		return err
	}
	duration, err = time.ParseDuration(tmp.Duration)
	if err != nil {
		return err
	}

	*l = SlowLog{
		Name:     tmp.Name,
		Start:    start,
		End:      end,
		Duration: duration,
		Region:   tmp.Region,
		Key:      tmp.Key,
	}

	for _, s := range tmp.Spans {
		newSpan := &Span{
			Name: s.Name,
		}
		if s.End == "N/A" {
			s.End = tmp.End
		}
		start, err = parseTime(s.Start)
		if err != nil {
			return err
		}
		end, err = parseTime(s.End)
		if err != nil {
			return err
		}
		newSpan.Start = start
		newSpan.End = end
		newSpan.Duration = end - start

		l.Spans = append(l.Spans, *newSpan)
	}
	return nil
}

func parseTime(timeStr string) (time.Duration, error) {
	var (
		h, m, s, ms time.Duration
	)
	if timeStr == "N/A" {
	}
	_, err := fmt.Sscanf(timeStr, "%d:%d:%d.%d", &h, &m, &s, &ms)
	if err != nil {
		return 0, err
	}
	return h*time.Hour + m*time.Minute + s*time.Second + ms*time.Millisecond, nil
}

func (l *SlowLog) Log() {
	factor := float64(l.opts.BarLen) / float64(l.Duration)
	fmt.Printf("function: %s\n", color.YellowString(l.Name))
	fmt.Printf("start: %s\n", color.YellowString("%v", l.Start))
	fmt.Printf("end: %s\n", color.YellowString("%v", l.End))
	fmt.Printf("duration: %s\n", color.YellowString("%v", l.Duration))
	fmt.Printf("region: %s\n", color.YellowString("%v", l.Region))
	fmt.Printf("key: %s\n", color.YellowString("%v", l.Key))

	fmt.Printf("%s  %v total\n", strings.Repeat("=", l.opts.BarLen), l.Duration)

	for _, span := range l.Spans {
		leadingSpaceLen := int(float64(span.Start-l.Start) * factor)
		fmt.Print(strings.Repeat(" ", leadingSpaceLen))

		barLen := int(float64(span.Duration) * factor)
		bars := strings.Repeat("=", barLen)
		if span.Duration >= l.opts.WarnThreshold {
			bars = color.RedString("%s", bars)
		}
		fmt.Print(bars)

		tailingSpaceLen := l.opts.BarLen - leadingSpaceLen - barLen + 1
		fmt.Print(strings.Repeat(" ", tailingSpaceLen))

		fmt.Printf(" %v %s\n", span.Duration, span.Name)
	}
}
