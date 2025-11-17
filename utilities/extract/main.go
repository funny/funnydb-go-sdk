package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"
)

/*
提取 Async 模式存储在本地磁盘的数据，转换为 json 打印到标准输出
*/

func main() {
	inputFile := flag.String("input", "", "input file")
	skipBeforeStr := flag.String("skip-before", "", "skip message before certain timestamp (format: 2006-01-02T15:04:05Z07:00)")
	skipAfterStr := flag.String("skip-after", "", "skip message after certain timestamp (format: 2006-01-02T15:04:05Z07:00)")

	flag.Parse()

	log.Println("read file", "input_file", *inputFile)
	if err := run(*inputFile, *skipBeforeStr, *skipAfterStr); err != nil {
		panic(err)
	}
}

func run(inputFile string, skipBeforeStr string, skipAfterStr string) error {
	var (
		skipBefore time.Time
		skipAfter  time.Time
		err        error
	)

	if skipBeforeStr != "" {
		skipBefore, err = time.Parse(time.RFC3339, skipBeforeStr)
		if err != nil {
			return fmt.Errorf("invald skip-before: %s", err)
		}
	}
	if skipAfterStr != "" {
		skipAfter, err = time.Parse(time.RFC3339, skipAfterStr)
		if err != nil {
			return fmt.Errorf("invalid skip-after: %s", err)
		}
	}

	f, err := os.Open(inputFile)
	if err != nil {
		return fmt.Errorf("open file: %s", err)
	}
	defer f.Close()

	reader := bufio.NewReader(f)

	var msgSize int32

	readCount := 0
	skipCount := 0

	for {
		err = binary.Read(reader, binary.BigEndian, &msgSize)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("get next message size: %s", err)
		}

		// check msg is in reasonable size
		if !(0 < msgSize && msgSize <= 10*1024*1024) {
			return fmt.Errorf("possible file corruption: msg size is abow 10MB or a negative number")
		}

		readBuf := make([]byte, msgSize)
		_, err = io.ReadFull(reader, readBuf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("failed to read msg: %s", err)
		}

		if readBuf[0] != '{' || readBuf[len(readBuf)-1] != '}' {
			return fmt.Errorf("possible file corruption: malformed msg: %s", string(readBuf))
		}

		readCount += 1

		if !skipBefore.IsZero() || !skipAfter.IsZero() {
			var event Event
			err := json.Unmarshal(readBuf, &event)
			if err != nil {
				return fmt.Errorf("possible file corruption: malformed msg: %s", err)
			}
			if (!skipBefore.IsZero() && event.Data.Time < skipBefore.UnixMilli()) || (!skipAfter.IsZero() && event.Data.Time > skipAfter.UnixMilli()) {
				skipCount += 1
				continue
			}
		}

		if _, err := os.Stdout.Write(append(readBuf, '\n')); err != nil {
			return err
		}
	}

	log.Println("read", readCount, "msgs", "skip", skipCount)
	return nil
}

type Event struct {
	Data struct {
		Time int64 `json:"#time"`
	}
}
