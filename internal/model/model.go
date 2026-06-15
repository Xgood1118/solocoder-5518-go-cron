package model

import (
	"time"
)

type JobType string

const (
	JobTypeShell JobType = "shell"
	JobTypeHTTP  JobType = "http"
)

type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "pending"
	TaskStatusRunning   TaskStatus = "running"
	TaskStatusSuccess   TaskStatus = "success"
	TaskStatusFailed    TaskStatus = "failed"
	TaskStatusRetrying  TaskStatus = "retrying"
	TaskStatusDeadLetter TaskStatus = "dead_letter"
)

type UserRole string

const (
	RoleAdmin UserRole = "admin"
	RoleRead  UserRole = "read"
)

type Job struct {
	ID               string    `gorm:"primaryKey;type:text" json:"id"`
	Name             string    `gorm:"not null;uniqueIndex" json:"name"`
	Description      string    `gorm:"type:text" json:"description"`
	CronExpr         string    `gorm:"not null;column:cron_expr" json:"cron_expr"`
	Type             JobType   `gorm:"not null;type:text" json:"type"`
	Command          string    `gorm:"type:text" json:"command,omitempty"`
	URL              string    `gorm:"type:text;column:url" json:"url,omitempty"`
	Method           string    `gorm:"type:text;default:'GET'" json:"method,omitempty"`
	Headers          string    `gorm:"type:text" json:"headers,omitempty"`
	Body             string    `gorm:"type:text" json:"body,omitempty"`
	TimeoutSec       int       `gorm:"not null;default:300;column:timeout_sec" json:"timeout_sec"`
	MaxRetries       int       `gorm:"not null;default:3;column:max_retries" json:"max_retries"`
	RetryIntervalSec int       `gorm:"not null;default:60;column:retry_interval_sec" json:"retry_interval_sec"`
	ExponentialBackoff bool    `gorm:"not null;default:true;column:exponential_backoff" json:"exponential_backoff"`
	Dependencies     string    `gorm:"type:text" json:"dependencies,omitempty"`
	Enabled          bool      `gorm:"not null;default:true" json:"enabled"`
	Version          int64     `gorm:"not null;default:0" json:"version"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

func (Job) TableName() string { return "jobs" }

type Task struct {
	ID             string     `gorm:"primaryKey;type:text" json:"id"`
	JobID          string     `gorm:"not null;index;column:job_id" json:"job_id"`
	Status         TaskStatus `gorm:"not null;index;type:text" json:"status"`
	AssignedWorker string     `gorm:"index;column:assigned_worker" json:"assigned_worker,omitempty"`
	WorkerID       string     `gorm:"index;column:worker_id" json:"worker_id,omitempty"`
	Stdout         string     `gorm:"type:text" json:"stdout,omitempty"`
	Stderr         string     `gorm:"type:text" json:"stderr,omitempty"`
	ExitCode       *int       `gorm:"column:exit_code" json:"exit_code,omitempty"`
	RetryCount     int        `gorm:"not null;default:0;column:retry_count" json:"retry_count"`
	NextRunAt      *time.Time `gorm:"index;column:next_run_at" json:"next_run_at,omitempty"`
	StartedAt      *time.Time `gorm:"index;column:started_at" json:"started_at,omitempty"`
	FinishedAt     *time.Time `gorm:"column:finished_at" json:"finished_at,omitempty"`
	LeaseHolder    string     `gorm:"index;column:lease_holder" json:"lease_holder,omitempty"`
	LeaseExpireAt  *time.Time `gorm:"index;column:lease_expire_at" json:"lease_expire_at,omitempty"`
	IdempotencyKey string     `gorm:"uniqueIndex;column:idempotency_key" json:"idempotency_key,omitempty"`
	TriggerType    string     `gorm:"type:text;default:'cron';column:trigger_type" json:"trigger_type"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
}

func (Task) TableName() string { return "tasks" }

type Worker struct {
	ID          string    `gorm:"primaryKey;type:text" json:"id"`
	Hostname    string    `gorm:"not null" json:"hostname"`
	IP          string    `gorm:"type:text" json:"ip,omitempty"`
	Concurrency int       `gorm:"not null;default:1" json:"concurrency"`
	RunningTask int       `gorm:"not null;default:0;column:running_task" json:"running_task"`
	LastHeartbeat time.Time `gorm:"not null;index;column:last_heartbeat" json:"last_heartbeat"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

func (Worker) TableName() string { return "workers" }

type User struct {
	ID       string   `gorm:"primaryKey;type:text" json:"id"`
	Username string   `gorm:"not null;uniqueIndex" json:"username"`
	Password string   `gorm:"not null" json:"-"`
	Role     UserRole `gorm:"not null;type:text" json:"role"`
}

func (User) TableName() string { return "users" }
