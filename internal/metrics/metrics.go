package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/solo-coder/go-cron/internal/model"
)

var (
	JobTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cron_jobs_total",
		Help: "Total number of cron jobs",
	})

	TaskCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cron_tasks_current",
		Help: "Number of tasks by status",
	}, []string{"status"})

	TaskDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cron_task_duration_seconds",
		Help:    "Duration of task executions in seconds",
		Buckets: prometheus.DefBuckets,
	}, []string{"job_id", "status"})

	TaskExecutionsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cron_task_executions_total",
		Help: "Total number of task executions",
	}, []string{"job_id", "status"})

	WorkerCount = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cron_workers_alive",
		Help: "Number of alive worker nodes",
	})
)

func RecordTaskDuration(jobID string, status model.TaskStatus, seconds float64) {
	TaskDuration.WithLabelValues(jobID, string(status)).Observe(seconds)
}

func IncTaskExecution(jobID string, status model.TaskStatus) {
	TaskExecutionsTotal.WithLabelValues(jobID, string(status)).Inc()
}
