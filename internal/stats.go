package internal

import (
	"context"
	"errors"
	"math"
	"os"
	"strings"
	"time"

	client "github.com/funny/ingest-client-go-sdk/v2"
	"github.com/google/uuid"
)

const (
	StatsEventName     = "collector_report_status"
	IngestMockEndpoint = "https://ingest.com"
	IngestCnEndpoint   = "https://ingest.zh-cn.xmfunny.com"
	IngestSgEndpoint   = "https://ingest.sg.xmfunny.com"
)

var ErrStatisticianIngestEndpointNotExist = errors.New("statistician ingest endpoint illegal")

var ingestEndpointConnectInfoMap = map[string]string{
	IngestCnEndpoint: "FDI_hpwyjj0ewWTuMExV1K7D:FDS_X1pUw4DapBNvPaTvHPANTqUJ8uOw",
	IngestSgEndpoint: "FDI_oO1rlJgiPdY7zXxJd09f:FDS_f2BHPDUlPGeYeKbV4rWfxq8ief3O",
}

type statistician struct {
	initTime    int64
	initMode    string
	accessKeyId string

	instanceId       string
	instanceIp       string
	instanceHostname string

	statisticalInterval time.Duration

	minRecordEndTimeMils int64
	recordMap            map[StatsGroup]int64

	ingestClient *client.Client

	closeChan         chan struct{}
	reportChan        chan []StatsGroup
	reporterExistChan chan struct{}
}

func NewStatistician(mode, accessKeyId, ingestEndpoint string, reportInterval time.Duration, statisticalInterval time.Duration) (*statistician, error) {
	return createStatistician(mode, accessKeyId, ingestEndpoint, reportInterval, time.Now(), statisticalInterval)
}

func createStatistician(mode, accessKeyId, ingestEndpoint string, reportInterval time.Duration, timePoint time.Time, statisticalInterval time.Duration) (*statistician, error) {
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
		initTime:             timePoint.UnixMilli(),
		initMode:             mode,
		accessKeyId:          accessKeyId,
		instanceId:           instanceId.String(),
		instanceIp:           v4Ip,
		instanceHostname:     hostname,
		statisticalInterval:  statisticalInterval,
		ingestClient:         ingestClient,
		minRecordEndTimeMils: 0,
		recordMap:            map[StatsGroup]int64{},
		closeChan:            make(chan struct{}),
		reportChan:           make(chan []StatsGroup),
		reporterExistChan:    make(chan struct{}),
	}

	go m.initReporter(reportInterval)

	return m, nil
}

func (m *statistician) initReporter(reportInterval time.Duration) {
	reportIntervalTicker := time.NewTicker(reportInterval)
	defer reportIntervalTicker.Stop()

	for {
		select {
		case <-m.closeChan:
			m.reportAll()
			close(m.reporterExistChan)
			return
		case items := <-m.reportChan:
			reportEventMaxBeginTimeMils := m.increaseRecord(items)
			m.report(reportEventMaxBeginTimeMils)
		case t := <-reportIntervalTicker.C:
			m.report(t.UnixMilli())
		}
	}
}

const (
	StatsDataFieldNameHostname    = "hostname"
	StatsDataFieldNameInstanceId  = "instance_id"
	StatsDataFieldNameMode        = "mode"
	StatsDataFieldNameAccessKeyId = "accessKeyId"
	StatsDataFieldNameInitTime    = "init_time"
	StatsDataFieldNameBeginTime   = "begin_time"
	StatsDataFieldNameEndTime     = "end_time"
	StatsDataFieldNameEvent       = "stats_event"
	StatsDataFieldNameReportTotal = "report_total"
)

func (m *statistician) reportAll() {
	m.report(math.MaxInt64)
}

func (m *statistician) report(reachTimeMils int64) {
	recordLen := len(m.recordMap)
	if recordLen > 0 && m.minRecordEndTimeMils <= reachTimeMils {
		msgs := &client.Messages{}
		removeGroup := make([]StatsGroup, 0, recordLen)
		for g, total := range m.recordMap {
			if reachTimeMils >= g.endTimeMils {
				logId, err := GenerateLogId()
				if err != nil {
					DefaultLogger.Errorf("GenerateLogId error when statistician report : %s", err)
				} else {
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
							StatsDataFieldNameAccessKeyId: m.accessKeyId,
							StatsDataFieldNameInitTime:    m.initTime,
							StatsDataFieldNameBeginTime:   g.beginTimeMils,
							StatsDataFieldNameEndTime:     g.endTimeMils,
							StatsDataFieldNameEvent:       g.event,
							StatsDataFieldNameReportTotal: total,
						},
					})
					removeGroup = append(removeGroup, g)
				}
			}
		}

		if len(msgs.Messages) > 0 {
			ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*5)
			defer cancelFunc()
			if err := m.ingestClient.Collect(ctx, msgs); err != nil {
				DefaultLogger.Errorf("Collect error when statistician report : %s", err)
			} else {
				for _, g := range removeGroup {
					delete(m.recordMap, g)
				}
				m.updateMinRecordEndTimeMils()
			}
		}
	}
}

func (m *statistician) updateMinRecordEndTimeMils() {
	for g, _ := range m.recordMap {
		if g.endTimeMils < m.minRecordEndTimeMils {
			m.minRecordEndTimeMils = g.endTimeMils
		}
	}
}

func (m *statistician) increaseRecord(gs []StatsGroup) int64 {
	var reportEventMaxBeginTimeMils int64 = 0
	for _, g := range gs {
		total, exist := m.recordMap[g]
		if exist {
			m.recordMap[g] = total + 1
		} else {
			m.recordMap[g] = 1
		}
		if g.beginTimeMils > reportEventMaxBeginTimeMils {
			reportEventMaxBeginTimeMils = g.beginTimeMils
		}
		if m.minRecordEndTimeMils == 0 {
			m.minRecordEndTimeMils = g.endTimeMils
			continue
		}
		if g.endTimeMils < m.minRecordEndTimeMils {
			m.minRecordEndTimeMils = g.endTimeMils
		}
	}
	return reportEventMaxBeginTimeMils
}

func (m *statistician) Count(gs []StatsGroup) {
	m.reportChan <- gs
}

func (m *statistician) Close() {
	close(m.closeChan)
	<-m.reporterExistChan
}
