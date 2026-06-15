package scheduler

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
	"github.com/rs/zerolog/log"

	"github.com/solo-coder/go-cron/internal/metrics"
	"github.com/solo-coder/go-cron/internal/model"
	"github.com/solo-coder/go-cron/internal/storage"
	"github.com/solo-coder/go-cron/pkg/config"
)

type Scheduler struct {
	store       *storage.Store
	cfg         config.SchedulerConfig
	schedulerID string
	cron        *cron.Cron
	jobEntries  map[string]cron.EntryID
	mu          sync.Mutex
	pendingTasks chan string
}

func New(store *storage.Store, cfg config.SchedulerConfig) *Scheduler {
	return &Scheduler{
		store:       store,
		cfg:         cfg,
		schedulerID: uuid.NewString(),
		cron:        cron.New(cron.WithSeconds()),
		jobEntries:  make(map[string]cron.EntryID),
		pendingTasks: make(chan string, 1000),
	}
}

func (s *Scheduler) Start(ctx context.Context) error {
	if err := s.loadAllJobs(); err != nil {
		log.Error().Err(err).Msg("failed to load initial jobs")
	}
	s.cron.Start()

	go s.leaseReaperLoop(ctx)
	go s.workerReaperLoop(ctx)
	go s.deadLetterCleanerLoop(ctx)
	go s.refreshMetricsLoop(ctx)

	log.Info().Str("scheduler_id", s.schedulerID).Msg("scheduler started")
	<-ctx.Done()
	s.cron.Stop()
	log.Info().Msg("scheduler stopped")
	return nil
}

func (s *Scheduler) SchedulerID() string { return s.schedulerID }

func (s *Scheduler) loadAllJobs() error {
	jobs, err := s.store.ListEnabledJobs()
	if err != nil {
		return err
	}
	for _, j := range jobs {
		if err := s.registerJob(&j); err != nil {
			log.Error().Err(err).Str("job_id", j.ID).Msg("failed to register job")
		}
	}
	return nil
}

func (s *Scheduler) registerJob(job *model.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if entryID, exists := s.jobEntries[job.ID]; exists {
		s.cron.Remove(entryID)
		delete(s.jobEntries, job.ID)
	}

	if !job.Enabled {
		return nil
	}

	entryID, err := s.cron.AddFunc(job.CronExpr, func() {
		s.dispatchJob(job.ID, "cron", "")
	})
	if err != nil {
		return err
	}
	s.jobEntries[job.ID] = entryID
	log.Info().Str("job_id", job.ID).Str("name", job.Name).Str("cron", job.CronExpr).Msg("job registered")
	return nil
}

func (s *Scheduler) ReloadJob(jobID string) error {
	job, err := s.store.GetJob(jobID)
	if err != nil {
		return err
	}
	return s.registerJob(job)
}

func (s *Scheduler) dispatchJob(jobID, triggerType, idempotencyKey string) {
	job, err := s.store.GetJob(jobID)
	if err != nil {
		log.Error().Err(err).Str("job_id", jobID).Msg("dispatchJob: job not found")
		return
	}

	if idempotencyKey != "" {
		if existing, _ := s.store.GetTaskByIdempotencyKey(idempotencyKey); existing != nil {
			log.Info().Str("idempotency_key", idempotencyKey).Msg("task already exists, skipping")
			return
		}
	}

	ok, err := s.checkDependencies(job)
	if err != nil {
		log.Error().Err(err).Str("job_id", jobID).Msg("failed to check dependencies")
		return
	}
	if !ok {
		log.Info().Str("job_id", jobID).Msg("dependencies not satisfied, skipping dispatch")
		return
	}

	now := time.Now()
	task := &model.Task{
		ID:             uuid.NewString(),
		JobID:          job.ID,
		Status:         model.TaskStatusPending,
		RetryCount:     0,
		TriggerType:    triggerType,
		IdempotencyKey: idempotencyKey,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if idempotencyKey == "" {
		task.IdempotencyKey = job.ID + ":" + now.Format("20060102150405")
	}

	if err := s.store.CreateTask(task); err != nil {
		log.Error().Err(err).Str("job_id", jobID).Msg("failed to create task")
		return
	}
	log.Info().Str("job_id", jobID).Str("task_id", task.ID).Msg("task dispatched")
}

func (s *Scheduler) TriggerJob(jobID, idempotencyKey string) error {
	s.dispatchJob(jobID, "manual", idempotencyKey)
	return nil
}

func (s *Scheduler) checkDependencies(job *model.Job) (bool, error) {
	if job.Dependencies == "" {
		return true, nil
	}
	var depIDs []string
	if err := json.Unmarshal([]byte(job.Dependencies), &depIDs); err != nil {
		return false, nil
	}
	for _, depID := range depIDs {
		depJob, err := s.store.GetJob(depID)
		if err != nil {
			log.Warn().Err(err).Str("dependency", depID).Msg("dependency job not found")
			return false, nil
		}
		tasks, _, err := s.store.ListTasks(depJob.ID, "", 1, 0)
		if err != nil || len(tasks) == 0 {
			log.Info().Str("job_id", job.ID).Str("dependency", depID).Msg("dependency has no tasks yet")
			return false, nil
		}
		latest := tasks[0]
		if latest.Status != model.TaskStatusSuccess {
			log.Info().Str("job_id", job.ID).Str("dependency", depID).Str("dep_status", string(latest.Status)).Msg("dependency not successful")
			return false, nil
		}
	}
	return true, nil
}

func (s *Scheduler) leaseReaperLoop(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := s.store.ReclaimExpiredLeases(s.schedulerID, s.cfg.LeaseTTLSeconds)
			if err != nil {
				log.Error().Err(err).Msg("lease reaper failed")
			} else if n > 0 {
				log.Info().Int64("reclaimed", n).Msg("reclaimed expired task leases")
			}
		}
	}
}

func (s *Scheduler) workerReaperLoop(ctx context.Context) {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			dead, err := s.store.ListDeadWorkers(s.cfg.HeartbeatTimeoutSec)
			if err != nil {
				log.Error().Err(err).Msg("worker reaper failed")
				continue
			}
			for _, w := range dead {
				log.Warn().Str("worker_id", w.ID).Msg("worker timed out, reclaiming its tasks")
			}
		}
	}
}

func (s *Scheduler) deadLetterCleanerLoop(ctx context.Context) {
	ticker := time.NewTicker(6 * time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := s.store.CleanOldDeadLetters(s.cfg.DeadLetterDays)
			if err != nil {
				log.Error().Err(err).Msg("dead letter cleaner failed")
			} else if n > 0 {
				log.Info().Int64("cleaned", n).Int("days", s.cfg.DeadLetterDays).Msg("cleaned old dead letters")
			}
		}
	}
}

func (s *Scheduler) refreshMetricsLoop(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.refreshMetrics()
		}
	}
}

func (s *Scheduler) refreshMetrics() {
	_, total, err := s.store.ListJobs(1, 0)
	if err == nil {
		metrics.JobTotal.Set(float64(total))
	}
	alive, err := s.store.ListAliveWorkers(s.cfg.HeartbeatTimeoutSec)
	if err == nil {
		metrics.WorkerCount.Set(float64(len(alive)))
	}
	statuses := []model.TaskStatus{
		model.TaskStatusPending,
		model.TaskStatusRunning,
		model.TaskStatusSuccess,
		model.TaskStatusFailed,
		model.TaskStatusRetrying,
		model.TaskStatusDeadLetter,
	}
	for _, st := range statuses {
		_, n, err := s.store.ListTasks("", st, 1, 0)
		if err == nil {
			metrics.TaskCount.WithLabelValues(string(st)).Set(float64(n))
		}
	}
}

func (s *Scheduler) PollForTask(ctx context.Context, workerID string) (*model.Task, *model.Job, error) {
	task, err := s.store.FindNextTaskForWorker(workerID)
	if err != nil {
		if strings.Contains(err.Error(), "record not found") {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	ok, err := s.store.LeaseTask(task.ID, workerID, s.cfg.LeaseTTLSeconds)
	if err != nil {
		return nil, nil, err
	}
	if !ok {
		return nil, nil, nil
	}
	job, err := s.store.GetJob(task.JobID)
	if err != nil {
		return nil, nil, err
	}
	return task, job, nil
}
