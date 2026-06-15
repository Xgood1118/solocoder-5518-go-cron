package deadletter

import (
	"time"

	"github.com/solo-coder/go-cron/internal/model"
	"github.com/solo-coder/go-cron/internal/storage"
)

type Manager struct {
	store *storage.Store
}

func New(store *storage.Store) *Manager {
	return &Manager{store: store}
}

func (m *Manager) List(limit, offset int) ([]model.Task, int64, error) {
	return m.store.ListDeadLetterTasks(limit, offset)
}

func (m *Manager) Rerun(taskID string) error {
	task, err := m.store.GetTask(taskID)
	if err != nil {
		return err
	}
	task.Status = model.TaskStatusPending
	task.RetryCount = 0
	task.NextRunAt = nil
	task.StartedAt = nil
	task.FinishedAt = nil
	task.LeaseHolder = ""
	task.LeaseExpireAt = nil
	task.AssignedWorker = ""
	task.WorkerID = ""
	task.Stdout = ""
	task.Stderr = ""
	task.ExitCode = nil
	return m.store.UpdateTask(task)
}

func (m *Manager) BatchRerun(taskIDs []string) (int, error) {
	count := 0
	for _, id := range taskIDs {
		if err := m.Rerun(id); err == nil {
			count++
		}
	}
	return count, nil
}

func (m *Manager) Delete(taskID string) error {
	return m.store.DB().Where("id = ?", taskID).Delete(&model.Task{}).Error
}

func (m *Manager) BatchDelete(taskIDs []string) (int, error) {
	result := m.store.DB().Where("id IN ?", taskIDs).Delete(&model.Task{})
	return int(result.RowsAffected), result.Error
}

func (m *Manager) CleanOlderThan(days int) (int64, error) {
	return m.store.CleanOldDeadLetters(days)
}

func MarkAsDeadLetter(store *storage.Store, task *model.Task) error {
	now := time.Now()
	task.Status = model.TaskStatusDeadLetter
	task.FinishedAt = &now
	return store.UpdateTask(task)
}
