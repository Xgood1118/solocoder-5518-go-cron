package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/solo-coder/go-cron/internal/metrics"
	"github.com/solo-coder/go-cron/internal/model"
	"github.com/solo-coder/go-cron/pkg/config"
)

type Worker struct {
	cfg          config.WorkerConfig
	schedulerURL string
	client       *http.Client
	running      atomic.Int32
	sem          chan struct{}

	runningTasksMu sync.Mutex
	runningTasks   map[string]struct{}
}

type pollResponse struct {
	Task *model.Task `json:"task"`
	Job  *model.Job  `json:"job"`
}

func New(cfg config.WorkerConfig) *Worker {
	return &Worker{
		cfg:          cfg,
		schedulerURL: cfg.SchedulerURL,
		client: &http.Client{
			Timeout: time.Duration(cfg.PollTimeoutSec+5) * time.Second,
		},
		sem:          make(chan struct{}, cfg.Concurrency),
		runningTasks: make(map[string]struct{}),
	}
}

func (w *Worker) Start(ctx context.Context) error {
	log.Info().
		Str("worker_id", w.cfg.WorkerID).
		Str("scheduler", w.schedulerURL).
		Int("concurrency", w.cfg.Concurrency).
		Msg("worker starting")

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		w.heartbeatLoop(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		w.leaseRenewLoop(ctx)
	}()

	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("worker shutting down")
			wg.Wait()
			return nil
		default:
		}
		w.sem <- struct{}{}
		resp, err := w.pollForTask(ctx)
		if err != nil {
			log.Error().Err(err).Msg("poll failed, retrying")
			<-w.sem
			time.Sleep(3 * time.Second)
			continue
		}
		if resp == nil || resp.Task == nil || resp.Job == nil {
			<-w.sem
			continue
		}
		w.running.Add(1)
		w.registerRunningTask(resp.Task.ID)
		go func(task *model.Task, job *model.Job) {
			defer func() {
				w.unregisterRunningTask(task.ID)
				w.running.Add(-1)
				<-w.sem
			}()
			w.executeTask(ctx, task, job)
		}(resp.Task, resp.Job)
	}
}

func (w *Worker) registerRunningTask(taskID string) {
	w.runningTasksMu.Lock()
	defer w.runningTasksMu.Unlock()
	w.runningTasks[taskID] = struct{}{}
}

func (w *Worker) unregisterRunningTask(taskID string) {
	w.runningTasksMu.Lock()
	defer w.runningTasksMu.Unlock()
	delete(w.runningTasks, taskID)
}

func (w *Worker) snapshotRunningTasks() []string {
	w.runningTasksMu.Lock()
	defer w.runningTasksMu.Unlock()
	ids := make([]string, 0, len(w.runningTasks))
	for id := range w.runningTasks {
		ids = append(ids, id)
	}
	return ids
}

func (w *Worker) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(w.cfg.HeartbeatIntervalSec) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.sendHeartbeat(); err != nil {
				log.Error().Err(err).Msg("heartbeat failed")
			}
		}
	}
}

func (w *Worker) leaseRenewLoop(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ids := w.snapshotRunningTasks()
			if len(ids) == 0 {
				continue
			}
			for _, id := range ids {
				if err := w.renewLease(id); err != nil {
					log.Error().Err(err).Str("task_id", id).Msg("renew lease failed")
				}
			}
		}
	}
}

func (w *Worker) sendHeartbeat() error {
	hostname, _ := os.Hostname()
	ip := getOutboundIP()
	payload := map[string]interface{}{
		"worker_id":    w.cfg.WorkerID,
		"hostname":     hostname,
		"ip":           ip,
		"concurrency":  w.cfg.Concurrency,
		"running_task": int(w.running.Load()),
	}
	body, _ := json.Marshal(payload)
	req, err := http.NewRequest("POST", w.schedulerURL+"/worker/heartbeat", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := w.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("heartbeat returned status %d", resp.StatusCode)
	}
	return nil
}

func (w *Worker) renewLease(taskID string) error {
	payload := map[string]string{
		"task_id":   taskID,
		"worker_id": w.cfg.WorkerID,
	}
	body, _ := json.Marshal(payload)
	req, err := http.NewRequest("POST", w.schedulerURL+"/worker/tasks/"+taskID+"/renew", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := w.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("renew lease returned status %d: %s", resp.StatusCode, string(b))
	}
	return nil
}

func getOutboundIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return ""
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP.String()
}

func (w *Worker) pollForTask(ctx context.Context) (*pollResponse, error) {
	payload := map[string]string{"worker_id": w.cfg.WorkerID}
	body, _ := json.Marshal(payload)

	reqCtx, cancel := context.WithTimeout(ctx, time.Duration(w.cfg.PollTimeoutSec)*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, "POST", w.schedulerURL+"/worker/poll", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := w.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("poll returned status %d", resp.StatusCode)
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var result pollResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (w *Worker) executeTask(ctx context.Context, task *model.Task, job *model.Job) {
	log.Info().Str("task_id", task.ID).Str("job_id", job.ID).Str("job_type", string(job.Type)).Msg("executing task")

	taskCtx, cancel := context.WithTimeout(ctx, time.Duration(job.TimeoutSec)*time.Second)
	defer cancel()

	startTime := time.Now()
	var stdout, stderr string
	var exitCode int
	var success bool

	if job.Type == model.JobTypeShell {
		stdout, stderr, exitCode = w.runShellCommand(taskCtx, job.Command)
		success = exitCode == 0
	} else if job.Type == model.JobTypeHTTP {
		stdout, stderr, exitCode = w.runHTTPRequest(taskCtx, job)
		success = exitCode == 0
	}

	if taskCtx.Err() == context.DeadlineExceeded {
		stderr = stderr + "\n[timeout exceeded]"
		success = false
		if exitCode == 0 {
			exitCode = -1
		}
	}

	duration := time.Since(startTime).Seconds()

	stdout = truncateString(stdout, w.cfg.MaxLogBytes)
	stderr = truncateString(stderr, w.cfg.MaxLogBytes)

	status := model.TaskStatusSuccess
	if !success {
		status = model.TaskStatusFailed
	}

	metrics.RecordTaskDuration(job.ID, status, duration)
	metrics.IncTaskExecution(job.ID, status)

	if err := w.reportResult(task.ID, status, stdout, stderr, exitCode); err != nil {
		log.Error().Err(err).Str("task_id", task.ID).Msg("failed to report task result")
	}

	log.Info().
		Str("task_id", task.ID).
		Str("job_id", job.ID).
		Bool("success", success).
		Int("exit_code", exitCode).
		Float64("duration_sec", duration).
		Msg("task finished")
}

func (w *Worker) runShellCommand(ctx context.Context, command string) (string, string, int) {
	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		cmd = exec.CommandContext(ctx, "cmd.exe", "/C", command)
	} else {
		cmd = exec.CommandContext(ctx, "sh", "-c", command)
	}

	var stdoutBuf, stderrBuf bytes.Buffer
	cmd.Stdout = &stdoutBuf
	cmd.Stderr = &stderrBuf

	err := cmd.Run()
	exitCode := 0
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else {
			exitCode = -1
		}
	}
	return stdoutBuf.String(), stderrBuf.String(), exitCode
}

func (w *Worker) runHTTPRequest(ctx context.Context, job *model.Job) (string, string, int) {
	method := job.Method
	if method == "" {
		method = "GET"
	}

	var bodyReader io.Reader
	if job.Body != "" {
		bodyReader = bytes.NewReader([]byte(job.Body))
	}

	req, err := http.NewRequestWithContext(ctx, method, job.URL, bodyReader)
	if err != nil {
		return "", err.Error(), 1
	}

	if job.Headers != "" {
		var headers map[string]string
		if err := json.Unmarshal([]byte(job.Headers), &headers); err == nil {
			for k, v := range headers {
				req.Header.Set(k, v)
			}
		}
	}
	if bodyReader != nil && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}

	client := &http.Client{Timeout: time.Duration(job.TimeoutSec) * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", err.Error(), 1
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err.Error(), 1
	}

	respStr := fmt.Sprintf("HTTP %d\n%s", resp.StatusCode, string(respBody))
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return respStr, "", 0
	}
	return respStr, fmt.Sprintf("HTTP status %d", resp.StatusCode), resp.StatusCode
}

func (w *Worker) reportResult(taskID string, status model.TaskStatus, stdout, stderr string, exitCode int) error {
	payload := map[string]interface{}{
		"task_id":   taskID,
		"worker_id": w.cfg.WorkerID,
		"status":    string(status),
		"stdout":    stdout,
		"stderr":    stderr,
		"exit_code": exitCode,
	}
	body, _ := json.Marshal(payload)
	req, err := http.NewRequest("POST", w.schedulerURL+"/worker/tasks/"+taskID+"/report", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := w.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("report failed: status %d, body: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

func truncateString(s string, maxBytes int) string {
	if maxBytes <= 0 || len(s) <= maxBytes {
		return s
	}
	return s[len(s)-maxBytes:]
}
