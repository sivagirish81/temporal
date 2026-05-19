package matching

import (
	"sync"
	"time"

	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

type syncMatchBacklogTracker struct {
	lock     sync.Mutex
	byPri    map[int32]int64
	ageByPri map[int32]backlogAgeTracker
}

func syncMatchBacklogPriority(task *internalTask) int32 {
	if task.query != nil && task.query.request != nil {
		return task.query.request.GetPriority().GetPriorityKey()
	}
	return 0
}

func isSyncMatchBacklogTask(task *internalTask) bool {
	return task.isQuery() || task.isNexus()
}

func (t *syncMatchBacklogTracker) record(task *internalTask, delta int) {
	if !isSyncMatchBacklogTask(task) {
		return
	}

	pri := syncMatchBacklogPriority(task)
	t.lock.Lock()
	defer t.lock.Unlock()

	if t.byPri == nil {
		t.byPri = make(map[int32]int64)
	}
	if t.ageByPri == nil {
		t.ageByPri = make(map[int32]backlogAgeTracker)
	}

	count := max(0, t.byPri[pri]+int64(delta))
	if count == 0 {
		delete(t.byPri, pri)
		delete(t.ageByPri, pri)
		return
	}
	t.byPri[pri] = count

	ageTracker, ok := t.ageByPri[pri]
	if !ok {
		ageTracker = newBacklogAgeTracker()
	}
	ageTracker.record(task.getCreateTime(), delta)
	t.ageByPri[pri] = ageTracker
}

func (t *syncMatchBacklogTracker) statsByPriority() map[int32]*taskqueuepb.TaskQueueStats {
	t.lock.Lock()
	defer t.lock.Unlock()

	result := make(map[int32]*taskqueuepb.TaskQueueStats, len(t.byPri))
	for pri, count := range t.byPri {
		if count == 0 {
			continue
		}
		age := time.Duration(0)
		if oldest := t.ageByPri[pri].oldestTime(); !oldest.IsZero() {
			age = time.Since(oldest)
			if age < 0 {
				age = 0
			}
		}
		result[pri] = &taskqueuepb.TaskQueueStats{
			ApproximateBacklogCount: count,
			ApproximateBacklogAge:   durationpb.New(age),
		}
	}
	return result
}
