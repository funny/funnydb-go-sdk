package tests

import (
	"bufio"
	"context"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest" // synctest was introduced in Go 1.24, but we must make sure sdk is compatible with 1.23 and newer, so we need create a separate module for tests
	"time"

	sdk "github.com/funny/funnydb-go-sdk/v2"
	"github.com/stretchr/testify/require"
)

func TestClientWithPersistOnly(t *testing.T) {
	t.Run("add", func(t *testing.T) {
		logDir := t.TempDir()
		c, err := sdk.NewClient(&sdk.Config{
			Mode:      sdk.ModePersistOnly,
			Directory: logDir,
			FileSize:  100, // 100mb
		})
		require.NoError(t, err)
		require.NotNil(t, c)

		defer c.Close(context.Background())

		err = c.ReportEvent(context.Background(), &sdk.Event{
			Name: "test-event",
			Props: map[string]interface{}{
				"testprop": "propvalue",
			},
		})
		require.NoError(t, err)

		// check log file created
		files, err := filepath.Glob(logDir + "/*/*.log")
		require.NoError(t, err)
		require.Equal(t, 1, len(files), "Expected 1 log file to be created")

		// check file contains 1 line
		file, err := os.Open(files[0])
		require.NoError(t, err)
		defer file.Close()

		scanner := bufio.NewScanner(file)
		var lineCount int
		for scanner.Scan() {
			lineCount++
		}
		require.NoError(t, scanner.Err())
		require.Equal(t, 1, lineCount, "Expected 1 line to be written to log file")
	})

	t.Run("auto-create-log-dir", func(t *testing.T) {
		logDir := t.TempDir() + "/subdir1/subdir2"

		c, err := sdk.NewClient(&sdk.Config{
			Mode:      sdk.ModePersistOnly,
			Directory: logDir,
			FileSize:  100, // 100mb
		})
		require.NoError(t, err)
		require.NotNil(t, c)

		defer c.Close(context.Background())

		err = c.ReportEvent(context.Background(), &sdk.Event{
			Name: "test-event",
			Props: map[string]interface{}{
				"testprop": "propvalue",
			},
		})
		require.NoError(t, err)

		// check log file created
		files, err := filepath.Glob(logDir + "/*/*.log")
		require.NoError(t, err)
		require.Equal(t, 1, len(files), "Expected 1 log file to be created in auto-created directory")
	})

	t.Run("rotate-by-file-size", func(t *testing.T) {
		logDir := t.TempDir()
		c, err := sdk.NewClient(&sdk.Config{
			Mode:      sdk.ModePersistOnly,
			Directory: logDir,
			FileSize:  1, // 1mb
		})
		require.NoError(t, err)
		require.NotNil(t, c)
		defer c.Close(context.Background())

		err = c.ReportEvent(context.Background(), &sdk.Event{
			Name: "test-event",
			Props: map[string]interface{}{
				"testprop": "propvalue",
			},
		})
		require.NoError(t, err)

		// get single event size by check log file size
		files, err := filepath.Glob(logDir + "/*/*.log")
		require.NoError(t, err)
		require.Equal(t, 1, len(files), "Expected 1 log file to be created")

		fileInfo, err := os.Stat(files[0])
		require.NoError(t, err)
		singleEventSize := fileInfo.Size()

		// send more events to exceed 1mb
		numEvents := int((1*1024*1024)/singleEventSize) + 10
		for i := 0; i < numEvents; i++ {
			err = c.ReportEvent(context.Background(), &sdk.Event{
				Name: "test-event",
				Props: map[string]interface{}{
					"testprop": "propvalue",
				},
			})
			require.NoError(t, err)
		}

		files, err = filepath.Glob(logDir + "/*/*.log")
		require.NoError(t, err)
		require.Equal(t, 2, len(files), "Expected 2 log files to be created after rotation")

		// check file size not exceed 1mb
		for _, f := range files {
			fileInfo, err := os.Stat(f)
			require.NoError(t, err)
			require.LessOrEqual(t, fileInfo.Size(), int64(1*1024*1024), "Log file size should not exceed configured max size")
		}
	})

	t.Run("rotate-by-date", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			logDir := t.TempDir()
			c, err := sdk.NewClient(&sdk.Config{
				Mode:      sdk.ModePersistOnly,
				Directory: logDir,
				FileSize:  100, // 100mb
			})
			require.NoError(t, err)
			require.NotNil(t, c)

			defer c.Close(context.Background())

			err = c.ReportEvent(context.Background(), &sdk.Event{
				Name: "test-event",
				Props: map[string]interface{}{
					"testprop": "propvalue",
				},
			})
			require.NoError(t, err)

			files, err := filepath.Glob(logDir + "/*/*.log")
			require.NoError(t, err)
			require.Equal(t, 1, len(files), "Expected 1 log file to be created")

			time.Sleep(24 * time.Hour)

			err = c.ReportEvent(context.Background(), &sdk.Event{
				Name: "test-event-2",
				Props: map[string]interface{}{
					"testprop2": "propvalue2",
				},
			})
			require.NoError(t, err)

			files, err = filepath.Glob(logDir + "/*/*.log")
			require.NoError(t, err)
			require.Equal(t, 2, len(files), "Expected 2 log files after date change")
		})
	})
}
