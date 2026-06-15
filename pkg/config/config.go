package config

import (
	"os"
	"strconv"
)

func GetEnv(key, defaultVal string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return defaultVal
}

func GetEnvInt(key string, defaultVal int) int {
	if v, ok := os.LookupEnv(key); ok {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return defaultVal
}

type SchedulerConfig struct {
	Port            string
	DBPath          string
	JWTSecret       string
	HeartbeatTimeoutSec int
	LeaseTTLSeconds int
	DeadLetterDays  int
}

type WorkerConfig struct {
	SchedulerURL    string
	WorkerID        string
	Concurrency     int
	HeartbeatIntervalSec int
	PollTimeoutSec  int
	MaxLogBytes     int
}

func LoadSchedulerConfig() SchedulerConfig {
	return SchedulerConfig{
		Port:               GetEnv("SCHEDULER_PORT", "8080"),
		DBPath:             GetEnv("DB_PATH", "./cron.db"),
		JWTSecret:          GetEnv("JWT_SECRET", "change-me-secret-key"),
		HeartbeatTimeoutSec: GetEnvInt("HEARTBEAT_TIMEOUT_SEC", 60),
		LeaseTTLSeconds:    GetEnvInt("LEASE_TTL_SECONDS", 120),
		DeadLetterDays:     GetEnvInt("DEAD_LETTER_DAYS", 30),
	}
}

func LoadWorkerConfig() WorkerConfig {
	hostname, _ := os.Hostname()
	return WorkerConfig{
		SchedulerURL:         GetEnv("SCHEDULER_URL", "http://localhost:8080"),
		WorkerID:             GetEnv("WORKER_ID", hostname),
		Concurrency:          GetEnvInt("WORKER_CONCURRENCY", 4),
		HeartbeatIntervalSec: GetEnvInt("HEARTBEAT_INTERVAL_SEC", 15),
		PollTimeoutSec:       GetEnvInt("POLL_TIMEOUT_SEC", 30),
		MaxLogBytes:          GetEnvInt("MAX_LOG_BYTES", 4096),
	}
}
