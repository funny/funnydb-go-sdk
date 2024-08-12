package internal

import (
	"fmt"
	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
	"os"
	"time"
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
