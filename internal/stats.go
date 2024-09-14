package internal

import (
	"context"
	"errors"
	client "git.sofunny.io/data-analysis/ingest-client-go-sdk"
	"github.com/google/uuid"
	"os"
	"sort"
	"strings"
	"time"
)

const (
	StatsEventName = "collector_report_status"
)

var ErrStatisticianIngestEndpointNotExist = errors.New("statistician ingest endpoint illegal")

var ingestEndpointConnectInfoMap = map[string]string{
	"https://ingest.zh-cn.xmfunny.com": "FDI_hpwyjj0ewWTuMExV1K7D:FDS_X1pUw4DapBNvPaTvHPANTqUJ8uOw",
	"https://ingest.sg.xmfunny.com":    "FDI_oO1rlJgiPdY7zXxJd09f:FDS_f2BHPDUlPGeYeKbV4rWfxq8ief3O",
}

type statistician struct {
	initTime int64
	initMode string

	instanceId       string
	instanceIp       string
	instanceHostname string

	beginTime int64
	endTime   int64
	total     int64

	ingestClient *client.Client

	closeChan         chan struct{}
	reportChan        chan []int64
	reporterExistChan chan struct{}
}

func NewStatistician(mode string, ingestEndpoint string, reportInterval time.Duration) (*statistician, error) {
	return createStatistician(mode, ingestEndpoint, reportInterval, time.Now())
}

func createStatistician(mode string, ingestEndpoint string, reportInterval time.Duration, timePoint time.Time) (*statistician, error) {
	instanceId, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}

	v4Ip, err := getFirstIPv4Ip()
	if err != nil {
		return nil, err
	}

	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	info, exist := ingestEndpointConnectInfoMap[ingestEndpoint]
	if !exist {
		return nil, ErrStatisticianIngestEndpointNotExist
	}
	infoArray := strings.Split(info, ":")
	ingestClient, err := client.NewClient(client.Config{
		Endpoint:        ingestEndpoint,
		AccessKeyID:     infoArray[0],
		AccessKeySecret: infoArray[1],
	})

	m := &statistician{
		initTime:          timePoint.UnixMilli(),
		initMode:          mode,
		instanceId:        instanceId.String(),
		instanceIp:        v4Ip,
		instanceHostname:  hostname,
		ingestClient:      ingestClient,
		closeChan:         make(chan struct{}),
		reportChan:        make(chan []int64),
		reporterExistChan: make(chan struct{}),
	}

	m.reset(timePoint.UnixMilli())

	go m.initReporter(reportInterval)

	return m, nil
}

func (m *statistician) initReporter(reportInterval time.Duration) {
	reportIntervalTicker := time.NewTicker(reportInterval)
	defer reportIntervalTicker.Stop()

	for {
		select {
		case <-m.closeChan:
			m.report(time.Now().UnixMilli())
			close(m.reporterExistChan)
			return
		case msgEventTimeSlice := <-m.reportChan:
			// 上报前会先进行排序
			for _, msgEventTime := range msgEventTimeSlice {
				if m.isTimeToReport(msgEventTime) {
					m.report(msgEventTime)
				}
				m.increaseTotal()
			}
		case t := <-reportIntervalTicker.C:
			if m.isTimeToReport(t.UnixMilli()) {
				m.report(t.UnixMilli())
			}
		}
	}
}

const (
	StatsDataFieldNameHostname    = "hostname"
	StatsDataFieldNameInstanceId  = "instance_id"
	StatsDataFieldNameMode        = "mode"
	StatsDataFieldNameInitTime    = "init_time"
	StatsDataFieldNameBeginTime   = "begin_time"
	StatsDataFieldNameEndTime     = "end_time"
	StatsDataFieldNameReportTotal = "report_total"
)

func (m *statistician) report(msTimePoint int64) {
	if m.total > 0 {
		logId, err := GenerateLogId()
		if err != nil {
			DefaultLogger.Errorf("GenerateLogId error when statistician report : %s", err)
		} else {
			ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*5)
			defer cancelFunc()
			msgs := &client.Messages{}
			msgs.Messages = append(msgs.Messages, client.Message{
				Type: EventTypeValue,
				Data: map[string]interface{}{
					DataFieldNameLogId:            logId,
					DataFieldNameSdkType:          SdkType,
					DataFieldNameSdkVersion:       SdkVersion,
					DataFieldNameTime:             time.Now().UnixMilli(),
					DataFieldNameEvent:            StatsEventName,
					DataFieldNameIp:               m.instanceIp,
					StatsDataFieldNameHostname:    m.instanceHostname,
					StatsDataFieldNameInstanceId:  m.instanceId,
					StatsDataFieldNameMode:        m.initMode,
					StatsDataFieldNameInitTime:    m.initTime,
					StatsDataFieldNameBeginTime:   m.beginTime,
					StatsDataFieldNameEndTime:     m.endTime,
					StatsDataFieldNameReportTotal: m.total,
				},
			})
			if err := m.ingestClient.Collect(ctx, msgs); err != nil {
				DefaultLogger.Errorf("Collect error when statistician report : %s", err)
			}
		}
	}
	m.reset(msTimePoint)
}

func (m *statistician) isTimeToReport(msTimePoint int64) bool {
	return msTimePoint >= m.endTime
}

func (m *statistician) increaseTotal() {
	m.total = m.total + 1
}

func (m *statistician) reset(msTimePoint int64) {
	timePoint := time.UnixMilli(msTimePoint)
	beginHourTime := timePoint.Truncate(time.Hour)
	endHourTime := beginHourTime.Add(time.Hour)
	m.beginTime = beginHourTime.UnixMilli()
	m.endTime = endHourTime.UnixMilli()
	m.total = 0
}

func (m *statistician) Count(msgEventTimeSlice []int64) {
	sort.Slice(msgEventTimeSlice, func(i, j int) bool {
		return msgEventTimeSlice[i] < msgEventTimeSlice[j]
	})
	m.reportChan <- msgEventTimeSlice
}

func (m *statistician) Close() {
	close(m.closeChan)
	<-m.reporterExistChan
}
