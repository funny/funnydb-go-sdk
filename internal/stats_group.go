package internal

import "time"

type StatsGroup struct {
	beginTimeMils int64
	endTimeMils   int64
	event         string
}

func NewStatsGroup(beginTime time.Time, statisticalInterval time.Duration, event string) StatsGroup {
	beginTruncateTime := beginTime.Truncate(statisticalInterval)
	return StatsGroup{
		beginTimeMils: beginTruncateTime.UnixMilli(),
		endTimeMils:   beginTruncateTime.UnixMilli() + statisticalInterval.Milliseconds(),
		event:         event,
	}
}
