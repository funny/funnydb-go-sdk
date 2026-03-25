package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	neturl "net/url"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	client "github.com/funny/ingest-client-go-sdk/v2"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"gopkg.in/yaml.v3"
)

// Default values used when the config file or CLI flags do not provide them.
const (
	defaultWorkDir              = "./work"
	defaultBatchSize            = 1000
	defaultConfigReloadInterval = 5 * time.Second
	defaultProgressLogInterval  = 15 * time.Second
	badLineModeStrict           = "strict"
	badLineModeSkip             = "skip_bad_line"
)

// Config is the full YAML configuration loaded from -config.
type Config struct {
	Ingest  IngestConfig  `yaml:"ingest"`
	Import  ImportConfig  `yaml:"import"`
	Runtime RuntimeConfig `yaml:"runtime"`
}

// IngestConfig controls how requests are sent to FunnyDB ingest.
type IngestConfig struct {
	Endpoint                 string        `yaml:"endpoint"`
	AccessKeyID              string        `yaml:"access_key_id"`
	AccessKeySecret          string        `yaml:"access_key_secret"`
	ClientID                 string        `yaml:"client_id"`
	Encoding                 string        `yaml:"encoding"`
	NoCompression            bool          `yaml:"no_compression"`
	CompressionAlgo          string        `yaml:"compression_algo"`
	RetryTimeIntervalInitial time.Duration `yaml:"retry_time_interval_initial"`
	RetryTimeIntervalMax     time.Duration `yaml:"retry_time_interval_max"`
	RequestTimeout           time.Duration `yaml:"request_timeout"`
	Concurrency              int           `yaml:"concurrency"`
}

// ImportConfig controls batching, throttling and bad-line handling.
type ImportConfig struct {
	BatchSize   int             `yaml:"batch_size"`
	RateLimit   RateLimitConfig `yaml:"rate_limit"`
	BadLineMode string          `yaml:"bad_line_mode"`
}

// RateLimitConfig throttles sending by records per second.
type RateLimitConfig struct {
	RecordsPerSec int `yaml:"records_per_sec"`
}

// RuntimeConfig controls local directories and periodic background tasks.
type RuntimeConfig struct {
	WorkDir              string        `yaml:"work_dir"`
	TempDir              string        `yaml:"temp_dir"`
	DataDir              string        `yaml:"data_dir"`
	ProgressDir          string        `yaml:"progress_dir"`
	ConfigReloadInterval time.Duration `yaml:"config_reload_interval"`
	ProgressLogInterval  time.Duration `yaml:"progress_log_interval"`
}

// FileProgress is the persisted checkpoint for a single URL.
//
// The importer only advances this state after a successful batch send,
// which gives the tool at-least-once delivery semantics on restart.
type FileProgress struct {
	URL               string    `json:"url"`
	FileName          string    `json:"file_name"`
	LocalDataPath     string    `json:"local_data_path"`
	DownloadCompleted bool      `json:"download_completed"`
	DownloadSize      int64     `json:"download_size"`
	ProcessedLines    int64     `json:"processed_lines"`
	ProcessedBytes    int64     `json:"processed_bytes"`
	SentBatches       int64     `json:"sent_batches"`
	SentRecords       int64     `json:"sent_records"`
	SkippedBadLines   int64     `json:"skipped_bad_lines,omitempty"`
	Done              bool      `json:"done"`
	LastError         string    `json:"last_error"`
	UpdatedAt         time.Time `json:"updated_at"`
}

// runtimePaths holds the resolved on-disk layout under work-dir.
type runtimePaths struct {
	WorkDir     string
	TempDir     string
	DataDir     string
	ProgressDir string
}

// dynamicSnapshot is the hot-reloadable subset of config used during import.
type dynamicSnapshot struct {
	BatchSize     int
	RecordsPerSec int
}

// dynamicSettings stores the current batch size and rate limiter.
// It can be updated by the config reload goroutine while import is running.
type dynamicSettings struct {
	mu      sync.RWMutex
	snap    dynamicSnapshot
	limiter *rate.Limiter
}

// appState is a small in-memory snapshot used only for periodic progress logs.
type appState struct {
	mu            sync.RWMutex
	totalURLs     int
	completedURLs int
	current       *FileProgress
}

// cliOptions contains command-line inputs for a single run.
type cliOptions struct {
	URLListPath             string
	ConfigPath              string
	WorkDir                 string
	Resume                  bool
	ProgressLogIntervalFlag time.Duration
}

// transformBatchTask represents one ordered raw batch.
//
// The producer sends tasks to the sender in file order. Each task transforms in a
// separate goroutine and closes Done when Messages are ready. The sender simply
// waits on Done and sends the transformed batch in the same order the tasks were produced.
type transformBatchTask struct {
	RawLines     [][]byte
	StartLineNo  int64
	Messages     []client.Message
	LineCount    int64
	ByteCount    int64
	BadLineCount int64
	Err          error
	Done         chan struct{}
}

func main() {
	opts := parseFlags()

	if err := run(opts); err != nil {
		slog.Error("import failed", "err", err)
		os.Exit(1)
	}
}

// parseFlags defines the CLI surface and returns parsed values.
func parseFlags() cliOptions {
	var opts cliOptions
	flag.StringVar(&opts.URLListPath, "url-list", "", "path to URL list file")
	flag.StringVar(&opts.ConfigPath, "config", "", "path to YAML config file")
	flag.StringVar(&opts.WorkDir, "work-dir", defaultWorkDir, "work directory")
	flag.BoolVar(&opts.Resume, "resume", true, "resume from existing progress")
	flag.DurationVar(&opts.ProgressLogIntervalFlag, "progress-log-interval", 0, "override progress log interval, e.g. 15s")
	flag.Parse()
	return opts
}

// run wires everything together:
// 1. load config and URL list
// 2. create work directories
// 3. start background logging/reload loops
// 4. process each URL sequentially
func run(opts cliOptions) error {
	if opts.URLListPath == "" {
		return fmt.Errorf("-url-list is required")
	}
	if opts.ConfigPath == "" {
		return fmt.Errorf("-config is required")
	}

	cfg, err := loadConfig(opts.ConfigPath)
	if err != nil {
		return err
	}
	paths, err := resolveRuntimePaths(cfg, opts.WorkDir)
	if err != nil {
		return err
	}
	if err := ensureDirs(paths); err != nil {
		return err
	}

	urls, err := loadURLList(opts.URLListPath)
	if err != nil {
		return err
	}
	if len(urls) == 0 {
		return fmt.Errorf("no URL found in %s", opts.URLListPath)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dyn := newDynamicSettings(cfg.Import.BatchSize, cfg.Import.RateLimit.RecordsPerSec)
	state := &appState{totalURLs: len(urls)}

	progressInterval := cfg.Runtime.ProgressLogInterval
	if opts.ProgressLogIntervalFlag > 0 {
		progressInterval = opts.ProgressLogIntervalFlag
	}
	if progressInterval <= 0 {
		progressInterval = defaultProgressLogInterval
	}

	go startProgressLogger(ctx, state, dyn, progressInterval)
	go startConfigReloader(ctx, opts.ConfigPath, dyn, cfg.Runtime.ConfigReloadInterval)

	ingestClient, err := newIngestClient(cfg)
	if err != nil {
		return err
	}

	for _, rawURL := range urls {
		progress, err := processURL(ctx, rawURL, cfg, paths, dyn, state, ingestClient, opts.Resume)
		if err != nil {
			state.setCurrent(progress)
			return err
		}
		state.markCompleted()
		state.setCurrent(progress)
	}

	slog.Info("all URLs processed", "total_urls", len(urls))
	return nil
}

// loadConfig reads YAML from disk, unmarshals it and applies validation/defaults.
func loadConfig(configPath string) (Config, error) {
	content, err := os.ReadFile(configPath)
	if err != nil {
		return Config{}, fmt.Errorf("read config %s: %w", configPath, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(content, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse config %s: %w", configPath, err)
	}
	if err := validateConfig(&cfg); err != nil {
		return Config{}, fmt.Errorf("validate config %s: %w", configPath, err)
	}
	return cfg, nil
}

// validateConfig checks required fields and fills simple defaults.
func validateConfig(cfg *Config) error {
	if cfg.Ingest.Endpoint == "" {
		return fmt.Errorf("ingest.endpoint is required")
	}
	if cfg.Import.BatchSize <= 0 {
		cfg.Import.BatchSize = defaultBatchSize
	}
	if cfg.Import.BadLineMode == "" {
		cfg.Import.BadLineMode = badLineModeStrict
	}
	if cfg.Import.BadLineMode != badLineModeStrict && cfg.Import.BadLineMode != badLineModeSkip {
		return fmt.Errorf("import.bad_line_mode must be %q or %q", badLineModeStrict, badLineModeSkip)
	}
	if cfg.Runtime.ConfigReloadInterval <= 0 {
		cfg.Runtime.ConfigReloadInterval = defaultConfigReloadInterval
	}
	if cfg.Runtime.ProgressLogInterval <= 0 {
		cfg.Runtime.ProgressLogInterval = defaultProgressLogInterval
	}
	if cfg.Import.RateLimit.RecordsPerSec < 0 {
		return fmt.Errorf("import.rate_limit.records_per_sec must be >= 0")
	}
	return nil
}

// resolveRuntimePaths merges CLI and config values into concrete filesystem paths.
func resolveRuntimePaths(cfg Config, workDirFlag string) (runtimePaths, error) {
	workDir := workDirFlag
	if workDir == "" {
		workDir = cfg.Runtime.WorkDir
	}
	if workDir == "" {
		workDir = defaultWorkDir
	}
	workDir = filepath.Clean(workDir)

	tempDir := cfg.Runtime.TempDir
	if tempDir == "" {
		tempDir = filepath.Join(workDir, "tmp")
	}
	dataDir := cfg.Runtime.DataDir
	if dataDir == "" {
		dataDir = filepath.Join(workDir, "data")
	}
	progressDir := cfg.Runtime.ProgressDir
	if progressDir == "" {
		progressDir = filepath.Join(workDir, "progress")
	}

	return runtimePaths{
		WorkDir:     workDir,
		TempDir:     filepath.Clean(tempDir),
		DataDir:     filepath.Clean(dataDir),
		ProgressDir: filepath.Clean(progressDir),
	}, nil
}

// ensureDirs creates the working directories used for temp files, data files and progress files.
func ensureDirs(paths runtimePaths) error {
	for _, dir := range []string{paths.WorkDir, paths.TempDir, paths.DataDir, paths.ProgressDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("create dir %s: %w", dir, err)
		}
	}
	return nil
}

// loadURLList reads one URL per line and ignores blanks and comment lines.
func loadURLList(urlListPath string) ([]string, error) {
	file, err := os.Open(urlListPath)
	if err != nil {
		return nil, fmt.Errorf("open url list %s: %w", urlListPath, err)
	}
	defer file.Close()

	var urls []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		urls = append(urls, line)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan url list %s: %w", urlListPath, err)
	}
	return urls, nil
}

// newIngestClient adapts our YAML config into the FunnyDB ingest SDK client config.
func newIngestClient(cfg Config) (*client.Client, error) {
	c, err := client.NewClient(client.Config{
		Endpoint:                 cfg.Ingest.Endpoint,
		AccessKeyID:              cfg.Ingest.AccessKeyID,
		AccessKeySecret:          cfg.Ingest.AccessKeySecret,
		ClientId:                 cfg.Ingest.ClientID,
		Encoding:                 cfg.Ingest.Encoding,
		NoCompression:            cfg.Ingest.NoCompression,
		CompressionAlgo:          cfg.Ingest.CompressionAlgo,
		RetryTimeIntervalInitial: cfg.Ingest.RetryTimeIntervalInitial,
		RetryTimeIntervalMax:     cfg.Ingest.RetryTimeIntervalMax,
	})
	if err != nil {
		return nil, fmt.Errorf("create ingest client: %w", err)
	}
	return c, nil
}

// newDynamicSettings creates the initial hot-reloadable import settings.
func newDynamicSettings(batchSize, recordsPerSec int) *dynamicSettings {
	d := &dynamicSettings{}
	d.update(batchSize, recordsPerSec)
	return d
}

// update swaps in a new batch size and limiter built from the latest config.
func (d *dynamicSettings) update(batchSize, recordsPerSec int) {
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}
	var limiter *rate.Limiter
	if recordsPerSec > 0 {
		burst := batchSize
		if burst < recordsPerSec {
			burst = recordsPerSec
		}
		limiter = rate.NewLimiter(rate.Limit(recordsPerSec), burst)
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	d.snap = dynamicSnapshot{BatchSize: batchSize, RecordsPerSec: recordsPerSec}
	d.limiter = limiter
}

func (d *dynamicSettings) snapshot() dynamicSnapshot {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.snap
}

func (d *dynamicSettings) waitN(ctx context.Context, n int) error {
	d.mu.RLock()
	limiter := d.limiter
	d.mu.RUnlock()
	if limiter == nil || n <= 0 {
		return nil
	}
	return limiter.WaitN(ctx, n)
}

// startConfigReloader periodically checks the config file and hot-reloads
// only the fields that are safe to change while importing.
func startConfigReloader(ctx context.Context, configPath string, dyn *dynamicSettings, interval time.Duration) {
	if interval <= 0 {
		interval = defaultConfigReloadInterval
	}

	stat, err := os.Stat(configPath)
	if err != nil {
		slog.Warn("config reload disabled", "path", configPath, "err", err)
		return
	}
	lastModTime := stat.ModTime()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		stat, err := os.Stat(configPath)
		if err != nil {
			slog.Warn("stat config failed", "path", configPath, "err", err)
			continue
		}
		if !stat.ModTime().After(lastModTime) {
			continue
		}

		cfg, err := loadConfig(configPath)
		if err != nil {
			slog.Error("reload config failed, keep previous settings", "path", configPath, "err", err)
			lastModTime = stat.ModTime()
			continue
		}
		lastModTime = stat.ModTime()
		dyn.update(cfg.Import.BatchSize, cfg.Import.RateLimit.RecordsPerSec)
		snap := dyn.snapshot()
		slog.Info("config reloaded", "path", configPath, "batch_size", snap.BatchSize, "rate_limit_records_per_sec", snap.RecordsPerSec)
	}
}

// startProgressLogger prints a periodic snapshot of overall and current-file progress.
func startProgressLogger(ctx context.Context, state *appState, dyn *dynamicSettings, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		progress, completed, total := state.snapshot()
		snap := dyn.snapshot()
		attrs := []any{
			"completed_urls", completed,
			"total_urls", total,
			"batch_size", snap.BatchSize,
			"rate_limit_records_per_sec", snap.RecordsPerSec,
		}
		if progress != nil {
			attrs = append(attrs,
				"current_url", progress.URL,
				"file_name", progress.FileName,
				"download_completed", progress.DownloadCompleted,
				"download_size", progress.DownloadSize,
				"processed_lines", progress.ProcessedLines,
				"processed_bytes", progress.ProcessedBytes,
				"sent_batches", progress.SentBatches,
				"sent_records", progress.SentRecords,
				"skipped_bad_lines", progress.SkippedBadLines,
				"done", progress.Done,
			)
		}
		slog.Info("progress", attrs...)
	}
}

// processURL handles one URL from start to finish:
// load progress, download if needed, import the file, then delete local data.
func processURL(
	ctx context.Context,
	rawURL string,
	cfg Config,
	paths runtimePaths,
	dyn *dynamicSettings,
	state *appState,
	ingestClient *client.Client,
	resume bool,
) (*FileProgress, error) {
	progress, progressPath, tempPath, err := loadOrInitProgress(rawURL, paths, resume)
	if err != nil {
		return nil, err
	}
	state.setCurrent(progress)

	if progress.Done {
		slog.Info("URL already completed, skip", "url", rawURL, "progress_file", progressPath)
		return progress, nil
	}

	slog.Info("start processing URL", "url", rawURL, "progress_file", progressPath)

	if !progress.DownloadCompleted || !fileExists(progress.LocalDataPath) {
		progress.DownloadCompleted = false
		progress.Done = false
		progress.LastError = ""
		if err := saveProgress(progressPath, progress); err != nil {
			return progress, err
		}

		slog.Info("start download", "url", rawURL, "temp_path", tempPath)
		size, err := downloadToFile(ctx, rawURL, tempPath, progress.LocalDataPath)
		if err != nil {
			_ = saveErrorProgress(progressPath, progress, fmt.Errorf("download %s: %w", rawURL, err))
			return progress, fmt.Errorf("download %s: %w", rawURL, err)
		}
		progress.DownloadCompleted = true
		progress.DownloadSize = size
		progress.LastError = ""
		if err := saveProgress(progressPath, progress); err != nil {
			return progress, err
		}
		slog.Info("download completed", "url", rawURL, "local_path", progress.LocalDataPath, "size", size)
	} else {
		if progress.DownloadSize == 0 {
			if info, err := os.Stat(progress.LocalDataPath); err == nil {
				progress.DownloadSize = info.Size()
			}
		}
		slog.Info("reuse existing local file", "url", rawURL, "local_path", progress.LocalDataPath, "processed_lines", progress.ProcessedLines)
	}

	if err := importDownloadedFile(ctx, cfg, dyn, ingestClient, progressPath, progress, state); err != nil {
		_ = saveErrorProgress(progressPath, progress, err)
		return progress, err
	}

	if err := os.Remove(progress.LocalDataPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		_ = saveErrorProgress(progressPath, progress, fmt.Errorf("remove data file %s: %w", progress.LocalDataPath, err))
		return progress, fmt.Errorf("remove data file %s: %w", progress.LocalDataPath, err)
	}
	slog.Info(
		"file completed",
		"url", rawURL,
		"local_path", progress.LocalDataPath,
		"processed_lines", progress.ProcessedLines,
		"processed_bytes", progress.ProcessedBytes,
		"sent_batches", progress.SentBatches,
		"sent_records", progress.SentRecords,
		"skipped_bad_lines", progress.SkippedBadLines,
	)

	return progress, nil
}

// loadOrInitProgress restores an existing checkpoint or creates a fresh one for the URL.
func loadOrInitProgress(rawURL string, paths runtimePaths, resume bool) (*FileProgress, string, string, error) {
	fileName, err := localFileNameFromURL(rawURL)
	if err != nil {
		return nil, "", "", err
	}

	localDataPath := filepath.Join(paths.DataDir, fileName)
	progressPath := filepath.Join(paths.ProgressDir, progressFileName(rawURL, fileName))
	tempPath := filepath.Join(paths.TempDir, fileName+".part")

	if resume && fileExists(progressPath) {
		content, err := os.ReadFile(progressPath)
		if err != nil {
			return nil, "", "", fmt.Errorf("read progress %s: %w", progressPath, err)
		}
		var progress FileProgress
		if err := json.Unmarshal(content, &progress); err != nil {
			return nil, "", "", fmt.Errorf("parse progress %s: %w", progressPath, err)
		}
		if progress.URL != rawURL {
			return nil, "", "", fmt.Errorf("progress file %s belongs to another URL: %s", progressPath, progress.URL)
		}
		if progress.FileName == "" {
			progress.FileName = fileName
		}
		if progress.LocalDataPath == "" {
			progress.LocalDataPath = localDataPath
		}
		return &progress, progressPath, tempPath, nil
	}

	progress := &FileProgress{
		URL:           rawURL,
		FileName:      fileName,
		LocalDataPath: localDataPath,
		UpdatedAt:     time.Now().UTC(),
	}
	return progress, progressPath, tempPath, nil
}

// saveProgress writes progress through a temporary file and rename,
// so crashes do not leave behind a partially written JSON file.
func saveProgress(progressPath string, progress *FileProgress) error {
	progress.UpdatedAt = time.Now().UTC()
	content, err := json.MarshalIndent(progress, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal progress %s: %w", progressPath, err)
	}
	tmpPath := progressPath + ".tmp"
	if err := os.WriteFile(tmpPath, append(content, '\n'), 0o644); err != nil {
		return fmt.Errorf("write progress tmp %s: %w", tmpPath, err)
	}
	if err := os.Rename(tmpPath, progressPath); err != nil {
		return fmt.Errorf("rename progress tmp %s -> %s: %w", tmpPath, progressPath, err)
	}
	return nil
}

// saveErrorProgress records the latest failure message in the progress file.
func saveErrorProgress(progressPath string, progress *FileProgress, err error) error {
	progress.LastError = err.Error()
	if saveErr := saveProgress(progressPath, progress); saveErr != nil {
		return fmt.Errorf("original error: %v; save progress error: %w", err, saveErr)
	}
	return nil
}

// downloadToFile downloads the whole source object to a .part file first,
// then atomically renames it into the final local data path.
func downloadToFile(ctx context.Context, rawURL, tempPath, finalPath string) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return 0, fmt.Errorf("build request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("http get: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("unexpected HTTP status: %s", resp.Status)
	}

	if err := os.MkdirAll(filepath.Dir(tempPath), 0o755); err != nil {
		return 0, fmt.Errorf("create temp dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(finalPath), 0o755); err != nil {
		return 0, fmt.Errorf("create data dir: %w", err)
	}

	tmpFile, err := os.Create(tempPath)
	if err != nil {
		return 0, fmt.Errorf("create temp file %s: %w", tempPath, err)
	}

	size, copyErr := io.Copy(tmpFile, resp.Body)
	closeErr := tmpFile.Close()
	if copyErr != nil {
		return 0, fmt.Errorf("copy response body: %w", copyErr)
	}
	if closeErr != nil {
		return 0, fmt.Errorf("close temp file %s: %w", tempPath, closeErr)
	}
	if err := os.Rename(tempPath, finalPath); err != nil {
		return 0, fmt.Errorf("rename temp file %s -> %s: %w", tempPath, finalPath, err)
	}
	return size, nil
}

// importDownloadedFile resumes from progress.ProcessedLines, transforms JSONL records,
// rate-limits and sends them in batches, and checkpoints after each successful batch.
//
// Processing model:
//   - file read and raw batching are sequential
//   - each full raw batch is handed to a transform goroutine
//   - transformed batches are sent strictly in producer order
func importDownloadedFile(
	ctx context.Context,
	cfg Config,
	dyn *dynamicSettings,
	ingestClient *client.Client,
	progressPath string,
	progress *FileProgress,
	state *appState,
) error {
	file, err := os.Open(progress.LocalDataPath)
	if err != nil {
		return fmt.Errorf("open data file %s: %w", progress.LocalDataPath, err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	skippedBytes, err := skipLines(reader, progress.ProcessedLines)
	if err != nil {
		return fmt.Errorf("restore from processed_lines=%d: %w", progress.ProcessedLines, err)
	}
	if progress.ProcessedBytes == 0 && skippedBytes > 0 {
		progress.ProcessedBytes = skippedBytes
	}
	state.setCurrent(progress)

	workerCount := cfg.Ingest.Concurrency
	if workerCount <= 0 {
		workerCount = 1
	}

	pipelineCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	batchTasks := make(chan *transformBatchTask, workerCount*2)
	g, gctx := errgroup.WithContext(pipelineCtx)
	g.SetLimit(workerCount + 1)

	transformBatch := func(ctx context.Context, fileName string, badLineMode string, task *transformBatchTask) error {
		defer close(task.Done)

		messages := make([]client.Message, 0, len(task.RawLines))
		for i, rawLine := range task.RawLines {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			lineNo := task.StartLineNo + int64(i)
			msg, err := transformJSONLLine(rawLine)
			if err != nil {
				if badLineMode == badLineModeSkip {
					task.BadLineCount++
					slog.Warn("skip bad line", "file_name", fileName, "line_no", lineNo, "err", err)
					continue
				}
				return fmt.Errorf("invalid JSONL line near line %d: %w", lineNo, err)
			}
			messages = append(messages, *msg)
		}

		task.Messages = messages
		return nil
	}

	g.Go(func() error {
		defer close(batchTasks)

		makeTask := func(rawLines [][]byte, startLineNo, lineCount, byteCount int64) *transformBatchTask {
			copiedRawLines := make([][]byte, len(rawLines))
			for i := range rawLines {
				copiedRawLines[i] = append([]byte(nil), rawLines[i]...)
			}
			return &transformBatchTask{
				RawLines:    copiedRawLines,
				StartLineNo: startLineNo,
				LineCount:   lineCount,
				ByteCount:   byteCount,
				Done:        make(chan struct{}),
			}
		}

		emitTask := func(task *transformBatchTask) error {
			g.Go(func() error {
				err := transformBatch(gctx, progress.FileName, cfg.Import.BadLineMode, task)
				task.Err = err
				return err
			})

			select {
			case batchTasks <- task:
				return nil
			case <-gctx.Done():
				return gctx.Err()
			}
		}

		snap := dyn.snapshot()
		currentBatchSize := snap.BatchSize
		if currentBatchSize <= 0 {
			currentBatchSize = defaultBatchSize
		}
		rawLines := make([][]byte, 0, currentBatchSize)
		lineNo := progress.ProcessedLines
		batchStartLineNo := lineNo + 1
		var batchByteCount int64

		for {
			rawLineBytes, readErr := reader.ReadBytes('\n')
			if readErr != nil && !errors.Is(readErr, io.EOF) {
				return fmt.Errorf("read data file %s: %w", progress.LocalDataPath, readErr)
			}
			if len(rawLineBytes) == 0 && errors.Is(readErr, io.EOF) {
				break
			}

			lineNo++
			rawLines = append(rawLines, append([]byte(nil), bytes.TrimRight(rawLineBytes, "\r\n")...))
			batchByteCount += int64(len(rawLineBytes))

			if len(rawLines) >= currentBatchSize {
				if err := emitTask(makeTask(rawLines, batchStartLineNo, int64(len(rawLines)), batchByteCount)); err != nil {
					return err
				}
				snap = dyn.snapshot()
				currentBatchSize = snap.BatchSize
				if currentBatchSize <= 0 {
					currentBatchSize = defaultBatchSize
				}
				rawLines = make([][]byte, 0, currentBatchSize)
				batchStartLineNo = lineNo + 1
				batchByteCount = 0
			}

			if errors.Is(readErr, io.EOF) {
				break
			}
		}

		if len(rawLines) > 0 {
			if err := emitTask(makeTask(rawLines, batchStartLineNo, int64(len(rawLines)), batchByteCount)); err != nil {
				return err
			}
		}

		return nil
	})

	for task := range batchTasks {
		select {
		case <-task.Done:
		case <-ctx.Done():
			cancel()
			_ = g.Wait()
			return ctx.Err()
		}

		if task.Err != nil {
			cancel()
			_ = g.Wait()
			return task.Err
		}

		if len(task.Messages) > 0 {
			if err := dyn.waitN(ctx, len(task.Messages)); err != nil {
				cancel()
				_ = g.Wait()
				return fmt.Errorf("rate limit wait: %w", err)
			}
			if err := sendBatch(ctx, cfg.Ingest.RequestTimeout, ingestClient, progress, task.Messages); err != nil {
				cancel()
				_ = g.Wait()
				return err
			}
			progress.SentBatches++
			progress.SentRecords += int64(len(task.Messages))
		}

		progress.ProcessedLines += task.LineCount
		progress.ProcessedBytes += task.ByteCount
		progress.SkippedBadLines += task.BadLineCount
		progress.LastError = ""
		if err := saveProgress(progressPath, progress); err != nil {
			cancel()
			_ = g.Wait()
			return err
		}
		state.setCurrent(progress)
	}

	if err := g.Wait(); err != nil {
		if errors.Is(err, context.Canceled) && ctx.Err() == nil {
			return nil
		}
		return err
	}

	progress.Done = true
	progress.LastError = ""
	if err := saveProgress(progressPath, progress); err != nil {
		return err
	}
	state.setCurrent(progress)
	return nil
}

// skipLines replays the file from the beginning and skips already committed lines.
// This keeps resume logic simple and line-aligned for JSONL input.
func skipLines(reader *bufio.Reader, n int64) (int64, error) {
	if n <= 0 {
		return 0, nil
	}
	var skippedBytes int64
	for i := int64(0); i < n; i++ {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if errors.Is(err, io.EOF) && len(line) > 0 {
				skippedBytes += int64(len(line))
				if i == n-1 {
					return skippedBytes, nil
				}
			}
			return skippedBytes, err
		}
		skippedBytes += int64(len(line))
	}
	return skippedBytes, nil
}

// transformJSONLLine applies the required jq-equivalent transformation:
// add metadata fields, then wrap the object as a FunnyDB Event message.
func transformJSONLLine(line []byte) (*client.Message, error) {
	line = bytes.TrimSpace(line)
	if len(line) == 0 {
		return nil, fmt.Errorf("empty line")
	}

	var data map[string]any
	if err := json.Unmarshal(line, &data); err != nil {
		return nil, err
	}
	data["#sdk_type"] = "go-sdk"
	data["#sdk_version"] = "funnydb-import-0.0.0"

	return &client.Message{
		Type: "Event",
		Data: data,
	}, nil
}

// sendBatch sends one batch through the SDK and optionally wraps it in a timeout.
func sendBatch(ctx context.Context, requestTimeout time.Duration, ingestClient *client.Client, progress *FileProgress, batch []client.Message) error {
	messages := &client.Messages{
		BatchId:  fmt.Sprintf("%s-%d", progress.FileName, progress.SentBatches+1),
		Messages: append([]client.Message(nil), batch...),
	}

	sendCtx := ctx
	cancel := func() {}
	if requestTimeout > 0 {
		sendCtx, cancel = context.WithTimeout(ctx, requestTimeout)
	}
	defer cancel()

	if err := ingestClient.Collect(sendCtx, messages); err != nil {
		return fmt.Errorf("send batch %s: %w", messages.BatchId, err)
	}
	return nil
}

// localFileNameFromURL derives a readable local file name from the URL path.
func localFileNameFromURL(rawURL string) (string, error) {
	parsed, err := neturl.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("parse URL %s: %w", rawURL, err)
	}
	base := path.Base(parsed.Path)
	if base == "." || base == "/" || base == "" {
		base = "download.jsonl"
	}
	return base, nil
}

// progressFileName adds a short URL hash so same-basename URLs do not collide.
func progressFileName(rawURL, baseName string) string {
	sum := sha1.Sum([]byte(rawURL))
	return fmt.Sprintf("%s.%s.progress.json", baseName, hex.EncodeToString(sum[:4]))
}

// fileExists is a small helper used for progress/data file checks.
func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// setCurrent stores a copy so log printing does not race with the importer mutating progress.
func (s *appState) setCurrent(progress *FileProgress) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if progress == nil {
		s.current = nil
		return
	}
	copyProgress := *progress
	s.current = &copyProgress
}

// markCompleted increments the number of URLs that reached terminal success.
func (s *appState) markCompleted() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.completedURLs++
}

// snapshot returns a copy of the latest log state.
func (s *appState) snapshot() (*FileProgress, int, int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var progress *FileProgress
	if s.current != nil {
		copyProgress := *s.current
		progress = &copyProgress
	}
	return progress, s.completedURLs, s.totalURLs
}
