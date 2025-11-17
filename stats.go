package funnydb

import (
	"context"
	"sync"
	"time"

	"github.com/funny/funnydb-go-sdk/v2/internal"
)

const (
	statsEventName = "#sdk_send_stats"
)

type StatCollector struct {
	producer       internal.Producer
	instanceID     string
	hostname       string
	reportMode     Mode
	accessKeyId    string
	initTime       time.Time
	reportInterval time.Duration
	stop           chan struct{}
	die            chan struct{}
	mu             sync.Mutex
	stats          map[statKey]int64
}

func newStatCollector(producer internal.Producer, instanceID string, hostname string, reportMode Mode, accessKeyId string, reportInterval time.Duration) (*StatCollector, error) {
	sc := &StatCollector{
		producer:       producer,
		instanceID:     instanceID,
		hostname:       hostname,
		reportMode:     reportMode,
		accessKeyId:    accessKeyId,
		initTime:       time.Now(),
		reportInterval: reportInterval,
		stop:           make(chan struct{}),
		die:            make(chan struct{}),
		stats:          map[statKey]int64{},
	}
	go sc.ioLoop()
	return sc, nil
}

func (sc *StatCollector) Collect(eventTime time.Time, event string) {
	beginTime := eventTime.Round(sc.reportInterval)
	sc.mu.Lock()
	defer sc.mu.Unlock()
	key := statKey{
		reportEvent: event,
		beginTime:   beginTime,
	}
	sc.stats[key]++
}

func (sc *StatCollector) Close() {
	close(sc.stop)
	<-sc.die
}

func (sc *StatCollector) ioLoop() {
	defer close(sc.die)

	ticker := time.NewTicker(sc.reportInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			sc.reportStats()
		case <-sc.stop:
			sc.reportStats()
			return
		}
	}
}

func (sc *StatCollector) reportStats() {
	sc.mu.Lock()
	statsCopy := sc.stats
	sc.stats = map[statKey]int64{}
	sc.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for key, count := range statsCopy {
		event := sc.makeEvent(key.beginTime, key.beginTime.Add(sc.reportInterval), key.reportEvent, count)
		data, err := event.transformToReportableData(sc.hostname)
		if err != nil {
			internal.DefaultLogger.Errorf("StatCollector reportStats transformToReportableData error: %s", err)
			continue
		}
		err = sc.producer.Add(ctx, data)
		if err != nil {
			internal.DefaultLogger.Errorf("StatCollector reportStats producer.Add error: %s", err)
			continue
		}
	}
}

func (sc *StatCollector) makeEvent(beginTime, endTime time.Time, event string, count int64) *Event {
	return &Event{
		Name: statsEventName,
		Props: map[string]any{
			"instance_id":      sc.instanceID,
			"client_mode":      string(sc.reportMode),
			"access_key_id":    sc.accessKeyId,
			"client_init_time": sc.initTime.UnixMilli(),
			"begin_time":       beginTime.UnixMilli(),
			"end_time":         endTime.UnixMilli(),
			"report_event":     event,
			"report_total":     count,
		},
	}
}

type statKey struct {
	reportEvent string
	beginTime   time.Time
}
