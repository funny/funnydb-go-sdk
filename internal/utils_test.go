package internal

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestGetCurrentTimeLogFileInfo(t *testing.T) {
	now := time.Now()
	parentDirectory := "/test"
	logDateFormat := time.DateOnly
	logFileIndex := 0

	logDirectory, logName, logPath := GetLogFileInfo(now, parentDirectory, logDateFormat, logFileIndex)

	expectLogDirectory := fmt.Sprintf("%s/%d/%d/%d", parentDirectory, now.Year(), now.Month(), now.Day())
	expectLogName := fmt.Sprintf("%s.%d.log", now.Format(logDateFormat), logFileIndex)
	expectLogPath := expectLogDirectory + "/" + expectLogName

	assert.Equal(t, logDirectory, expectLogDirectory)
	assert.Equal(t, logName, expectLogName)
	assert.Equal(t, logPath, expectLogPath)
}
