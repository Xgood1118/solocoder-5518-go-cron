package storage

import (
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/solo-coder/go-cron/internal/model"
)

var ErrNotFound = gorm.ErrRecordNotFound

type Store struct {
	db *gorm.DB
}

func NewStore(dbPath string) (*Store, error) {
	db, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Warn),
	})
	if err != nil {
		return nil, err
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(1)

	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		return nil, err
	}
	if err := s.seedUsers(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) DB() *gorm.DB { return s.db }

func (s *Store) migrate() error {
	return s.db.AutoMigrate(
		&model.Job{},
		&model.Task{},
		&model.Worker{},
		&model.User{},
	)
}

func (s *Store) seedUsers() error {
	var count int64
	s.db.Model(&model.User{}).Count(&count)
	if count > 0 {
		return nil
	}
	users := []model.User{
		{ID: uuid.NewString(), Username: "admin", Password: "admin123", Role: model.RoleAdmin},
		{ID: uuid.NewString(), Username: "reader", Password: "reader123", Role: model.RoleRead},
	}
	for _, u := range users {
		if err := s.db.Create(&u).Error; err != nil {
			return err
		}
		log.Info().Str("username", u.Username).Str("role", string(u.Role)).Msg("seeded user")
	}
	return nil
}

func (s *Store) GetUserByCredentials(username, password string) (*model.User, error) {
	var u model.User
	err := s.db.Where("username = ? AND password = ?", username, password).First(&u).Error
	if err != nil {
		return nil, err
	}
	return &u, nil
}

func (s *Store) CreateJob(job *model.Job) error {
	if job.ID == "" {
		job.ID = uuid.NewString()
	}
	job.CreatedAt = time.Now()
	job.UpdatedAt = job.CreatedAt
	return s.db.Create(job).Error
}

func (s *Store) UpdateJob(job *model.Job) error {
	job.UpdatedAt = time.Now()
	result := s.db.Model(&model.Job{}).
		Where("id = ? AND version = ?", job.ID, job.Version).
		Updates(map[string]interface{}{
			"name":                job.Name,
			"description":         job.Description,
			"cron_expr":           job.CronExpr,
			"type":                job.Type,
			"command":             job.Command,
			"url":                 job.URL,
			"method":              job.Method,
			"headers":             job.Headers,
			"body":                job.Body,
			"timeout_sec":         job.TimeoutSec,
			"max_retries":         job.MaxRetries,
			"retry_interval_sec":  job.RetryIntervalSec,
			"exponential_backoff": job.ExponentialBackoff,
			"dependencies":        job.Dependencies,
			"enabled":             job.Enabled,
			"version":             job.Version + 1,
			"updated_at":          time.Now(),
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return errors.New("version conflict")
	}
	return nil
}

func (s *Store) DeleteJob(id string) error {
	return s.db.Where("id = ?", id).Delete(&model.Job{}).Error
}

func (s *Store) GetJob(id string) (*model.Job, error) {
	var job model.Job
	err := s.db.Where("id = ?", id).First(&job).Error
	if err != nil {
		return nil, err
	}
	return &job, nil
}

func (s *Store) ListJobs(limit, offset int) ([]model.Job, int64, error) {
	var jobs []model.Job
	var total int64
	if err := s.db.Model(&model.Job{}).Count(&total).Error; err != nil {
		return nil, 0, err
	}
	if err := s.db.Order("created_at desc").Limit(limit).Offset(offset).Find(&jobs).Error; err != nil {
		return nil, 0, err
	}
	return jobs, total, nil
}

func (s *Store) ListEnabledJobs() ([]model.Job, error) {
	var jobs []model.Job
	err := s.db.Where("enabled = ?", true).Find(&jobs).Error
	return jobs, err
}

func (s *Store) CreateTask(task *model.Task) error {
	if task.ID == "" {
		task.ID = uuid.NewString()
	}
	task.CreatedAt = time.Now()
	task.UpdatedAt = task.CreatedAt
	return s.db.Create(task).Error
}

func (s *Store) UpdateTask(task *model.Task) error {
	task.UpdatedAt = time.Now()
	return s.db.Save(task).Error
}

func (s *Store) GetTask(id string) (*model.Task, error) {
	var task model.Task
	err := s.db.Where("id = ?", id).First(&task).Error
	if err != nil {
		return nil, err
	}
	return &task, nil
}

func (s *Store) ListTasks(jobID string, status model.TaskStatus, limit, offset int) ([]model.Task, int64, error) {
	var tasks []model.Task
	var total int64
	query := s.db.Model(&model.Task{})
	if jobID != "" {
		query = query.Where("job_id = ?", jobID)
	}
	if status != "" {
		query = query.Where("status = ?", status)
	}
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	if err := query.Order("created_at desc").Limit(limit).Offset(offset).Find(&tasks).Error; err != nil {
		return nil, 0, err
	}
	return tasks, total, nil
}

func (s *Store) ListDeadLetterTasks(limit, offset int) ([]model.Task, int64, error) {
	return s.ListTasks("", model.TaskStatusDeadLetter, limit, offset)
}

func (s *Store) GetTaskByIdempotencyKey(key string) (*model.Task, error) {
	var task model.Task
	err := s.db.Where("idempotency_key = ?", key).First(&task).Error
	if err != nil {
		return nil, err
	}
	return &task, nil
}

func (s *Store) Heartbeat(workerID, hostname, ip string, concurrency, runningTask int) error {
	now := time.Now()
	var w model.Worker
	err := s.db.Where("id = ?", workerID).First(&w).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		w = model.Worker{
			ID:            workerID,
			Hostname:      hostname,
			IP:            ip,
			Concurrency:   concurrency,
			RunningTask:   runningTask,
			LastHeartbeat: now,
			CreatedAt:     now,
			UpdatedAt:     now,
		}
		return s.db.Create(&w).Error
	}
	if err != nil {
		return err
	}
	w.Hostname = hostname
	w.IP = ip
	w.Concurrency = concurrency
	w.RunningTask = runningTask
	w.LastHeartbeat = now
	w.UpdatedAt = now
	return s.db.Save(&w).Error
}

func (s *Store) ListAliveWorkers(timeoutSec int) ([]model.Worker, error) {
	cutoff := time.Now().Add(-time.Duration(timeoutSec) * time.Second)
	var workers []model.Worker
	err := s.db.Where("last_heartbeat > ?", cutoff).Order("running_task asc").Find(&workers).Error
	return workers, err
}

func (s *Store) ListDeadWorkers(timeoutSec int) ([]model.Worker, error) {
	cutoff := time.Now().Add(-time.Duration(timeoutSec) * time.Second)
	var workers []model.Worker
	err := s.db.Where("last_heartbeat <= ?", cutoff).Find(&workers).Error
	return workers, err
}

func (s *Store) LeaseTask(taskID, workerID string, ttlSec int) (bool, error) {
	now := time.Now()
	expireAt := now.Add(time.Duration(ttlSec) * time.Second)
	result := s.db.Model(&model.Task{}).
		Where("id = ? AND (lease_expire_at IS NULL OR lease_expire_at < ?) AND status IN ?",
			taskID, now, []model.TaskStatus{model.TaskStatusPending, model.TaskStatusRetrying}).
		Updates(map[string]interface{}{
			"status":          model.TaskStatusRunning,
			"worker_id":       workerID,
			"started_at":      now,
			"lease_holder":    workerID,
			"lease_expire_at": expireAt,
			"updated_at":      now,
		})
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
}

func (s *Store) RenewLease(taskID, workerID string, ttlSec int) error {
	expireAt := time.Now().Add(time.Duration(ttlSec) * time.Second)
	return s.db.Model(&model.Task{}).
		Where("id = ? AND lease_holder = ?", taskID, workerID).
		Updates(map[string]interface{}{
			"lease_expire_at": expireAt,
			"updated_at":      time.Now(),
		}).Error
}

func (s *Store) FindNextTaskForWorker(workerID string) (*model.Task, error) {
	var task model.Task
	now := time.Now()
	err := s.db.Where("(status = ? OR (status = ? AND next_run_at <= ?)) AND (lease_expire_at IS NULL OR lease_expire_at < ?)",
		model.TaskStatusPending, model.TaskStatusRetrying, now, now).
		Order("created_at asc").
		Limit(1).
		First(&task).Error
	if err != nil {
		return nil, err
	}
	return &task, nil
}

func (s *Store) ReclaimExpiredLeases(schedulerID string, ttlSec int) (int64, error) {
	cutoff := time.Now()
	result := s.db.Model(&model.Task{}).
		Where("status = ? AND lease_expire_at < ?", model.TaskStatusRunning, cutoff).
		Updates(map[string]interface{}{
			"status":          model.TaskStatusPending,
			"worker_id":       "",
			"started_at":      nil,
			"lease_holder":    "",
			"lease_expire_at": nil,
			"updated_at":      time.Now(),
		})
	return result.RowsAffected, result.Error
}

func (s *Store) CleanOldDeadLetters(days int) (int64, error) {
	cutoff := time.Now().AddDate(0, 0, -days)
	result := s.db.Where("status = ? AND updated_at < ?", model.TaskStatusDeadLetter, cutoff).Delete(&model.Task{})
	return result.RowsAffected, result.Error
}
