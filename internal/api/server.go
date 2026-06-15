package api

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"

	"github.com/solo-coder/go-cron/internal/auth"
	"github.com/solo-coder/go-cron/internal/deadletter"
	"github.com/solo-coder/go-cron/internal/dag"
	"github.com/solo-coder/go-cron/internal/model"
	"github.com/solo-coder/go-cron/internal/scheduler"
	"github.com/solo-coder/go-cron/internal/storage"
	"github.com/solo-coder/go-cron/pkg/config"
)

type Server struct {
	router    *gin.Engine
	store     *storage.Store
	authSvc   *auth.Service
	sched     *scheduler.Scheduler
	dlMgr     *deadletter.Manager
	cfg       config.SchedulerConfig
}

func NewServer(store *storage.Store, sched *scheduler.Scheduler, authSvc *auth.Service, cfg config.SchedulerConfig) *Server {
	s := &Server{
		store:   store,
		authSvc: authSvc,
		sched:   sched,
		dlMgr:   deadletter.New(store),
		cfg:     cfg,
	}
	s.setupRoutes()
	return s
}

func (s *Server) setupRoutes() {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())

	r.GET("/health", s.handleHealth)
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	r.POST("/api/login", s.handleLogin)

	api := r.Group("/api")
	api.Use(s.authSvc.AuthMiddleware())
	{
		api.GET("/jobs", s.handleListJobs)
		api.GET("/jobs/:id", s.handleGetJob)
		api.GET("/jobs/:id/tasks", s.handleListJobTasks)

		api.GET("/tasks", s.handleListTasks)
		api.GET("/tasks/:id", s.handleGetTask)
		api.GET("/tasks/:id/logs", s.handleGetTaskLogs)

		api.GET("/workers", s.handleListWorkers)
		api.GET("/dead-letters", s.handleListDeadLetters)

		admin := api.Group("")
		admin.Use(s.authSvc.RequireRole(model.RoleAdmin))
		{
			admin.POST("/jobs", s.handleCreateJob)
			admin.PUT("/jobs/:id", s.handleUpdateJob)
			admin.DELETE("/jobs/:id", s.handleDeleteJob)
			admin.POST("/jobs/:id/trigger", s.handleTriggerJob)

			admin.POST("/dead-letters/:id/rerun", s.handleRerunDeadLetter)
			admin.POST("/dead-letters/batch-rerun", s.handleBatchRerunDeadLetter)
			admin.DELETE("/dead-letters/:id", s.handleDeleteDeadLetter)
			admin.POST("/dead-letters/batch-delete", s.handleBatchDeleteDeadLetter)
		}
	}

	worker := r.Group("/worker")
	{
		worker.POST("/heartbeat", s.handleWorkerHeartbeat)
		worker.POST("/poll", s.handleWorkerPoll)
		worker.POST("/tasks/:id/report", s.handleTaskReport)
		worker.POST("/tasks/:id/renew", s.handleTaskRenew)
	}

	s.router = r
}

func (s *Server) Handler() *gin.Engine { return s.router }

func (s *Server) handleHealth(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok", "scheduler_id": s.sched.SchedulerID()})
}

func (s *Server) handleLogin(c *gin.Context) {
	var req struct {
		Username string `json:"username" binding:"required"`
		Password string `json:"password" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	user, err := s.store.GetUserByCredentials(req.Username, req.Password)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid credentials"})
		return
	}
	token, err := s.authSvc.GenerateToken(user)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "token generation failed"})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"token": token,
		"user": gin.H{
			"id":       user.ID,
			"username": user.Username,
			"role":     user.Role,
		},
	})
}

func getPagination(c *gin.Context) (int, int) {
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))
	offset, _ := strconv.Atoi(c.DefaultQuery("offset", "0"))
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	return limit, offset
}

func (s *Server) handleListJobs(c *gin.Context) {
	limit, offset := getPagination(c)
	jobs, total, err := s.store.ListJobs(limit, offset)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": jobs, "total": total})
}

func (s *Server) handleGetJob(c *gin.Context) {
	id := c.Param("id")
	job, err := s.store.GetJob(id)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "job not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, job)
}

func (s *Server) handleCreateJob(c *gin.Context) {
	var job model.Job
	if err := c.ShouldBindJSON(&job); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := s.validateJob(&job); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := s.store.CreateJob(&job); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	_ = s.sched.ReloadJob(job.ID)
	c.JSON(http.StatusCreated, job)
}

func (s *Server) handleUpdateJob(c *gin.Context) {
	id := c.Param("id")
	existing, err := s.store.GetJob(id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "job not found"})
		return
	}
	var job model.Job
	if err := c.ShouldBindJSON(&job); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	job.ID = id
	job.Version = existing.Version
	if err := s.validateJob(&job); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := s.store.UpdateJob(&job); err != nil {
		if err.Error() == "version conflict" {
			c.JSON(http.StatusConflict, gin.H{"error": "version conflict, please refresh and try again"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	_ = s.sched.ReloadJob(job.ID)
	c.JSON(http.StatusOK, gin.H{"status": "updated"})
}

func (s *Server) handleDeleteJob(c *gin.Context) {
	id := c.Param("id")
	if err := s.store.DeleteJob(id); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	_ = s.sched.ReloadJob(id)
	c.JSON(http.StatusOK, gin.H{"status": "deleted"})
}

func (s *Server) handleTriggerJob(c *gin.Context) {
	id := c.Param("id")
	var req struct {
		IdempotencyKey string `json:"idempotency_key"`
	}
	c.ShouldBindJSON(&req)
	if _, err := s.store.GetJob(id); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "job not found"})
		return
	}
	if err := s.sched.TriggerJob(id, req.IdempotencyKey); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "triggered"})
}

func (s *Server) handleListJobTasks(c *gin.Context) {
	jobID := c.Param("id")
	limit, offset := getPagination(c)
	status := c.Query("status")
	tasks, total, err := s.store.ListTasks(jobID, model.TaskStatus(status), limit, offset)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": tasks, "total": total})
}

func (s *Server) handleListTasks(c *gin.Context) {
	limit, offset := getPagination(c)
	status := c.Query("status")
	jobID := c.Query("job_id")
	tasks, total, err := s.store.ListTasks(jobID, model.TaskStatus(status), limit, offset)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": tasks, "total": total})
}

func (s *Server) handleGetTask(c *gin.Context) {
	id := c.Param("id")
	task, err := s.store.GetTask(id)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "task not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, task)
}

func (s *Server) handleGetTaskLogs(c *gin.Context) {
	id := c.Param("id")
	task, err := s.store.GetTask(id)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "task not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"stdout": task.Stdout,
		"stderr": task.Stderr,
	})
}

func (s *Server) handleListWorkers(c *gin.Context) {
	workers, err := s.store.ListAliveWorkers(s.cfg.HeartbeatTimeoutSec)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": workers, "total": len(workers)})
}

func (s *Server) handleListDeadLetters(c *gin.Context) {
	limit, offset := getPagination(c)
	tasks, total, err := s.dlMgr.List(limit, offset)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": tasks, "total": total})
}

func (s *Server) handleRerunDeadLetter(c *gin.Context) {
	id := c.Param("id")
	if err := s.dlMgr.Rerun(id); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "rerun scheduled"})
}

func (s *Server) handleBatchRerunDeadLetter(c *gin.Context) {
	var req struct {
		IDs []string `json:"ids"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	n, err := s.dlMgr.BatchRerun(req.IDs)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"rerun_count": n})
}

func (s *Server) handleDeleteDeadLetter(c *gin.Context) {
	id := c.Param("id")
	if err := s.dlMgr.Delete(id); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "deleted"})
}

func (s *Server) handleBatchDeleteDeadLetter(c *gin.Context) {
	var req struct {
		IDs []string `json:"ids"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	n, err := s.dlMgr.BatchDelete(req.IDs)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"deleted_count": n})
}

func (s *Server) validateJob(job *model.Job) error {
	if job.Name == "" {
		return errors.New("name is required")
	}
	if job.CronExpr == "" {
		return errors.New("cron_expr is required")
	}
	if job.Type != model.JobTypeShell && job.Type != model.JobTypeHTTP {
		return errors.New("type must be shell or http")
	}
	if job.Type == model.JobTypeShell && job.Command == "" {
		return errors.New("command is required for shell type")
	}
	if job.Type == model.JobTypeHTTP && job.URL == "" {
		return errors.New("url is required for http type")
	}
	if job.TimeoutSec <= 0 {
		job.TimeoutSec = 300
	}
	if job.MaxRetries < 0 {
		job.MaxRetries = 0
	}
	if job.RetryIntervalSec <= 0 {
		job.RetryIntervalSec = 60
	}
	return nil
}

func (s *Server) handleWorkerHeartbeat(c *gin.Context) {
	var req struct {
		WorkerID    string `json:"worker_id" binding:"required"`
		Hostname    string `json:"hostname"`
		IP          string `json:"ip"`
		Concurrency int    `json:"concurrency"`
		RunningTask int    `json:"running_task"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := s.store.Heartbeat(req.WorkerID, req.Hostname, req.IP, req.Concurrency, req.RunningTask); err != nil {
		log.Error().Err(err).Str("worker_id", req.WorkerID).Msg("heartbeat failed")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok", "timestamp": time.Now().Unix()})
}

func (s *Server) handleWorkerPoll(c *gin.Context) {
	var req struct {
		WorkerID string `json:"worker_id" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	deadline := time.Now().Add(time.Duration(s.cfg.LeaseTTLSeconds) * time.Second / 2)
	for time.Now().Before(deadline) {
		task, job, err := s.sched.PollForTask(c.Request.Context(), req.WorkerID)
		if err != nil {
			log.Error().Err(err).Str("worker_id", req.WorkerID).Msg("poll error")
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if task != nil && job != nil {
			c.JSON(http.StatusOK, gin.H{"task": task, "job": job})
			return
		}
		time.Sleep(2 * time.Second)
		select {
		case <-c.Request.Context().Done():
			c.JSON(http.StatusOK, gin.H{"task": nil})
			return
		default:
		}
	}
	c.JSON(http.StatusOK, gin.H{"task": nil})
}

type taskReport struct {
	TaskID   string `json:"task_id"`
	WorkerID string `json:"worker_id"`
	Status   string `json:"status"`
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
	ExitCode *int   `json:"exit_code"`
}

func (s *Server) handleTaskReport(c *gin.Context) {
	var req taskReport
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	task, err := s.store.GetTask(req.TaskID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "task not found"})
		return
	}

	job, err := s.store.GetJob(task.JobID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	now := time.Now()
	status := model.TaskStatus(req.Status)
	if status == model.TaskStatusSuccess {
		task.Status = model.TaskStatusSuccess
		task.Stdout = req.Stdout
		task.Stderr = req.Stderr
		task.ExitCode = req.ExitCode
		task.FinishedAt = &now
		task.LeaseHolder = ""
		task.LeaseExpireAt = nil
		log.Info().Str("task_id", task.ID).Str("job_id", task.JobID).Msg("task succeeded")
	} else if status == model.TaskStatusFailed {
		task.Stdout = req.Stdout
		task.Stderr = req.Stderr
		task.ExitCode = req.ExitCode
		if task.RetryCount >= job.MaxRetries {
			task.Status = model.TaskStatusDeadLetter
			task.FinishedAt = &now
			task.LeaseHolder = ""
			task.LeaseExpireAt = nil
			log.Warn().Str("task_id", task.ID).Str("job_id", task.JobID).Msg("task moved to dead letter")
		} else {
			task.RetryCount++
			task.Status = model.TaskStatusRetrying
			interval := job.RetryIntervalSec
			if job.ExponentialBackoff {
				interval = interval * (1 << (task.RetryCount - 1))
			}
			nextRun := now.Add(time.Duration(interval) * time.Second)
			task.NextRunAt = &nextRun
			task.LeaseHolder = ""
			task.LeaseExpireAt = nil
			log.Info().Str("task_id", task.ID).Str("job_id", task.JobID).
				Int("retry_count", task.RetryCount).Time("next_run_at", nextRun).Msg("task scheduled for retry")
		}
	}
	task.UpdatedAt = now
	if err := s.store.UpdateTask(task); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "reported"})
}

func (s *Server) handleTaskRenew(c *gin.Context) {
	var req struct {
		TaskID   string `json:"task_id" binding:"required"`
		WorkerID string `json:"worker_id" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := s.store.RenewLease(req.TaskID, req.WorkerID, s.cfg.LeaseTTLSeconds); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "renewed", "timestamp": time.Now().Unix()})
}

func init() {
	_ = dag.New
}
