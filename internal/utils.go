package internal

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	client "github.com/funny/ingest-client-go-sdk/v2"
	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
)

var numberEncoding = jsoniter.Config{
	EscapeHTML:             true,
	SortMapKeys:            true,
	ValidateJsonRawMessage: true,
	UseNumber:              true,
}.Froze()

func marshalToString(data interface{}) (string, error) {
	return numberEncoding.MarshalToString(data)
}

func marshalToBytes(data interface{}) ([]byte, error) {
	return numberEncoding.Marshal(data)
}

func GenerateLogId() (string, error) {
	uuid, err := uuid.NewV7()
	if err == nil {
		return uuid.String(), nil
	}
	return "", err
}

func GetLogFileInfo(timePoint time.Time, directory string, dateFormat string, logFileIndex int) (string, string, string) {
	logDirectory := generateLogDirectory(directory, timePoint)
	timeStr := timePoint.Format(dateFormat)
	logName := generateLogFileName(timeStr, logFileIndex)
	logPath := logDirectory + "/" + logName
	return logDirectory, logName, logPath
}

func generateLogDirectory(directory string, timePoint time.Time) string {
	return fmt.Sprintf("%s/%d/%d/%d", directory, timePoint.Year(), timePoint.Month(), timePoint.Day())
}

func generateLogFileName(timeStr string, logFileIndex int) string {
	return fmt.Sprintf("%s.%d.log", timeStr, logFileIndex)
}

func calculateLogFileIndex(logDirectory string, dateFormat string, timePoint time.Time) int {
	var index int = 0
	timeStr := timePoint.Format(dateFormat)
	for {
		logFileName := generateLogFileName(timeStr, index)
		logPath := logDirectory + "/" + logFileName
		if fileExists(logPath) {
			index++
			continue
		} else {
			break
		}
	}
	return index
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	// 处理其他可能的错误
	DefaultLogger.Errorf("fileExists execute error: %s", err)
	return false
}

func checkNeedLogRotate(currentPath, writePath string, fileSize, maxFileSize int64) bool {
	if currentPath != writePath {
		return true
	}
	if fileSize >= maxFileSize {
		return true
	}
	return false
}

func closeLogFile(file *os.File) error {
	err := file.Sync()
	if err != nil {
		return err
	}
	err = file.Close()
	if err != nil {
		return err
	}
	return nil
}

func writeToFile(file *os.File, line []byte) (int, error) {
	return file.Write(append(line, '\n'))
}

func GunzipData(compressedData []byte) ([]byte, error) {
	// 创建一个字节缓冲区，存放压缩数据
	buffer := bytes.NewBuffer(compressedData)

	// 创建一个 gzip.Reader 读取压缩数据
	reader, err := gzip.NewReader(buffer)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	// 解压缩数据到 bytes.Buffer
	var result bytes.Buffer
	if _, err := io.Copy(&result, reader); err != nil {
		return nil, err
	}

	return result.Bytes(), nil
}

func getFirstIPv4Ip() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}

	for _, addr := range addrs {
		// 检查 IP 地址类型并排除环回地址
		if ipNet, ok := addr.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
			if ipNet.IP.To4() != nil {
				return ipNet.IP.String(), nil
			}
		}
	}
	return "", fmt.Errorf("没有找到非环回的 IPv4 地址")
}

func getStatsGroupSlice(batch *client.Messages, statisticalInterval time.Duration) []StatsGroup {
	messages := batch.Messages
	var statsGroups = make([]StatsGroup, 0, len(messages))
	for _, msg := range messages {
		if msg.Type == EventTypeValue {
			dataContent := msg.Data.(map[string]interface{})
			timeFieldObj := dataContent[DataFieldNameTime]
			timeFieldValue, ok := timeFieldObj.(int64)
			if !ok {
				timeFieldNumber := timeFieldObj.(json.Number)
				timeFieldValue, _ = timeFieldNumber.Int64()
			}
			beginTime := time.UnixMilli(timeFieldValue).Truncate(statisticalInterval)
			endTIme := beginTime.Add(statisticalInterval)
			eventName := dataContent[DataFieldNameEvent].(string)
			statsGroups = append(statsGroups, StatsGroup{
				beginTimeMils: beginTime.UnixMilli(),
				endTimeMils:   endTIme.UnixMilli(),
				event:         eventName,
			})
		}
	}
	return statsGroups
}
