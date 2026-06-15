package dag

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/solo-coder/go-cron/internal/model"
)

type DAG struct {
	nodes map[string]*model.Job
	edges map[string][]string
}

func New() *DAG {
	return &DAG{
		nodes: make(map[string]*model.Job),
		edges: make(map[string][]string),
	}
}

func (d *DAG) AddJob(job *model.Job) {
	d.nodes[job.ID] = job
	var deps []string
	if job.Dependencies != "" {
		_ = json.Unmarshal([]byte(job.Dependencies), &deps)
	}
	for _, dep := range deps {
		d.edges[dep] = append(d.edges[dep], job.ID)
	}
}

func (d *DAG) TopologicalSort() ([]string, error) {
	inDegree := make(map[string]int)
	for id := range d.nodes {
		inDegree[id] = 0
	}
	for _, children := range d.edges {
		for _, c := range children {
			inDegree[c]++
		}
	}

	queue := []string{}
	for id, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, id)
		}
	}

	result := []string{}
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		result = append(result, node)
		for _, child := range d.edges[node] {
			inDegree[child]--
			if inDegree[child] == 0 {
				queue = append(queue, child)
			}
		}
	}

	if len(result) != len(d.nodes) {
		return nil, errors.New("cycle detected in DAG")
	}
	return result, nil
}

func ValidateDependencies(job *model.Job, allJobs []*model.Job) error {
	var depIDs []string
	if job.Dependencies != "" {
		if err := json.Unmarshal([]byte(job.Dependencies), &depIDs); err != nil {
			return fmt.Errorf("invalid dependencies format: %w", err)
		}
	}
	idSet := make(map[string]bool)
	for _, j := range allJobs {
		idSet[j.ID] = true
	}
	for _, dep := range depIDs {
		if !idSet[dep] && dep != job.ID {
			continue
		}
		if dep == job.ID {
			return errors.New("job cannot depend on itself")
		}
	}
	return nil
}

func GetDownstream(jobID string, allJobs []*model.Job) []string {
	d := New()
	for _, j := range allJobs {
		d.AddJob(j)
	}
	return d.edges[jobID]
}
