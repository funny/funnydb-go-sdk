package internal

import (
	"context"
	"errors"
	"math/rand"
	"os"
	"sync/atomic"
	"time"

	"github.com/funny/funnydb-go-sdk/v2/internal/diskqueue"
	client "github.com/funny/ingest-client-go-sdk/v2"
	"golang.org/x/sync/errgroup"
)

type AsyncProducerConfig struct {
	Mode                      string
	Directory                 string
	IngestEndpoint            string
	AccessKey                 string
	AccessSecret              string
	MaxBufferRecords          int
	SendInterval              time.Duration
	SendTimeout               time.Duration
	BatchSize                 int64
	StatisticalInterval       time.Duration
	StatisticalReportInterval time.Duration
	DisableReportStats        bool
}

type AsyncProducer struct {
	status       int32
	config       *AsyncProducerConfig
	q            diskqueue.Interface
	eg           *errgroup.Group
	egCtx        context.Context
	closeCh      chan interface{}
	ingestClient *client.Client
	statistician *statistician
	existErr     error
}

func NewAsyncProducer(config AsyncProducerConfig) (Producer, error) {
	ingestClient, err := client.NewClient(client.Config{
		Endpoint:        config.IngestEndpoint,
		AccessKeyID:     config.AccessKey,
		AccessKeySecret: config.AccessSecret,
	})
	if err != nil {
		return nil, err
	}

	_, err = os.Stat(config.Directory)
	if err != nil && os.IsNotExist(err) {
		e := os.MkdirAll(config.Directory, os.ModePerm)
		if e != nil {
			return nil, e
		} else {
			DefaultLogger.Info("日志存储文件夹不存在，已自动创建")
		}
	}

	dq := diskqueue.New(
		"funnydb",
		config.Directory,
		128*1024*1024, // 128MB
		1,
		20*1024*1024, // 20MB
		250,
		100*time.Millisecond,
		true,
		NewAppLogFunc(),
	)

	var s *statistician
	if !config.DisableReportStats {
		s, err = NewStatistician(config.Mode, config.AccessKey, config.IngestEndpoint, config.StatisticalReportInterval, config.StatisticalInterval)
		if err != nil && !errors.Is(err, ErrStatisticianIngestEndpointNotExist) {
			return nil, err
		}
	}

	eg, ctx := errgroup.WithContext(context.Background())

	p := AsyncProducer{
		status:       running,
		config:       &config,
		q:            dq,
		eg:           eg,
		egCtx:        ctx,
		closeCh:      make(chan interface{}),
		ingestClient: ingestClient,
		existErr:     ErrProducerClosed,
		statistician: s,
	}
	return &p, p.init()
}

func (p *AsyncProducer) Add(ctx context.Context, data map[string]interface{}) error {
	var err error = nil

	if atomic.LoadInt32(&p.status) == stop {
		err = p.existErr
	} else {
		jsonData, jsonErr := marshalToBytes(data)
		if jsonErr != nil {
			err = jsonErr
		} else {
			err = p.q.Put(jsonData)
		}
	}

	return err
}

func (p *AsyncProducer) Close(ctx context.Context) error {
	if atomic.CompareAndSwapInt32(&p.status, running, stop) {
		close(p.closeCh)
		p.eg.Wait()
		if err := p.q.Close(); err != nil {
			DefaultLogger.Errorf("Close diskQ error : %s", err)
		}
		if p.statistician != nil {
			p.statistician.Close()
		}
		return nil
	} else {
		return p.existErr
	}
}

func (p *AsyncProducer) init() error {

	p.eg.Go(p.runSender)

	DefaultLogger.Infof("ModeAsync staring, log path: %s", p.config.Directory)

	return nil
}

func (p *AsyncProducer) runSender() error {
	ingestSendIntervalTicker := time.NewTicker(p.config.SendInterval)

	var minBackoff = time.Duration(200+rand.Int63n(100)) * time.Millisecond
	var maxBackoff = 10 * time.Second

	var (
		lastCommitedAt time.Time
		msgs           [][]byte
		msgSize        int
	)

	reset := func() {
		lastCommitedAt = time.Now()
		msgs = [][]byte{}
		msgSize = 0
	}
	reset()

	send := func() {
		clientMsgs := &client.Messages{}
		var msg client.Message
		for _, bytesMsg := range msgs {
			err := numberEncoding.Unmarshal(bytesMsg, &msg)
			if err != nil {
				DefaultLogger.Errorf("unmarshal message error when send data : %s", err)
				continue
			}
			clientMsgs.Messages = append(clientMsgs.Messages, msg)
		}

		var batchSendSuccess = true
		var restTime = minBackoff

	lp:
		for {
			select {
			case <-p.closeCh:
				DefaultLogger.Info("Collect loop receive close sig, exit")
				return
			case <-p.egCtx.Done():
				DefaultLogger.Info("Collect loop error sig, exit")
				return
			default:
				ctx, cancel := context.WithTimeout(context.Background(), p.config.SendTimeout)

				err := p.ingestClient.Collect(ctx, clientMsgs)
				cancel()
				if err != nil {
					DefaultLogger.Errorf("send data failed : %s", err)
					var innerErr client.Error
					if errors.As(err, &innerErr) {
						if innerErr.StatusCode < 500 && !(innerErr.StatusCode == 412 || innerErr.StatusCode == 401 || innerErr.StatusCode == 422) {
							// 剩下的基本上是 4xx 的客户端错误，数据有问题无法修复，因此跳过该批次数据
							batchSendSuccess = false
							break lp
						}
					}
				} else {
					break lp
				}
			}

			DefaultLogger.Warnf("batch need send again, will retry after %s", restTime)
			time.Sleep(restTime)
			restTime = restTime * 2
			if restTime > maxBackoff {
				restTime = maxBackoff
			}
		}

		if p.statistician != nil && batchSendSuccess {
			p.statistician.Count(getStatsGroupSlice(clientMsgs, p.statistician.statisticalInterval))
		}

		p.q.Advance()
		reset()
	}

	AppendAndCheckProcess := func(msgBytes []byte) {
		if msgBytes != nil {
			if int64(msgSize+len(msgBytes)) > p.config.BatchSize {
				send()
			}

			msgs = append(msgs, msgBytes)
			msgSize += len(msgBytes)

			if len(msgs) >= p.config.MaxBufferRecords {
				send()
			}
		}
	}

	defer func() {
		ingestSendIntervalTicker.Stop()

		if atomic.CompareAndSwapInt32(&p.status, running, stop) {
			close(p.closeCh)
			if err := p.q.Close(); err != nil {
				DefaultLogger.Errorf("Close diskQ error : %s", err)
			}
			if p.statistician != nil {
				p.statistician.Close()
			}
		}
	}()

	for {
		select {
		case <-p.closeCh:
			DefaultLogger.Info("Sender receive close sig, exist")
			return nil
		case <-p.egCtx.Done():
			DefaultLogger.Info("Sender receive error sig, exist")
			return nil
		case <-ingestSendIntervalTicker.C:
			// 以下流程检测是否太久没有发送数据
			if time.Since(lastCommitedAt) >= p.config.SendInterval && len(msgs) > 0 {
				send()
			}
		case line := <-p.q.ReadChan():
			AppendAndCheckProcess(line)
		}
	}
}
