package matching

import (
	"container/heap"
	"context"
	"slices"
	"sync"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/softassert"
	"go.temporal.io/server/common/util"
)

const invalidHeapIndex = -13 // use unusual value to stand out in panics

// maxTokens is the maximum number of tokens we might consume at a time for simpleLimiter. This
// is used to update ready times after a rate is changed from very low (or zero) to higher: we
// may have set a ready time far in the future and need to clip it to something reasonable so
// we can dispatch again.
//
// Currently we only use 1 token at a time.
const maxTokens = 1

type pollerPQ struct {
	heap []*waitingPoller
}

// implements heap.Interface
func (p *pollerPQ) Len() int {
	return len(p.heap)
}

// implements heap.Interface, do not call directly
func (p *pollerPQ) Less(i int, j int) bool {
	a, b := p.heap[i], p.heap[j]
	if !(a.isTaskForwarder || a.isTaskValidator) && (b.isTaskForwarder || b.isTaskValidator) {
		return true
	} else if (a.isTaskForwarder || a.isTaskValidator) && !(b.isTaskForwarder || b.isTaskValidator) {
		return false
	}
	return a.startTime.Before(b.startTime)
}

func (p *pollerPQ) Add(poller *waitingPoller) {
	heap.Push(p, poller)
}

func (p *pollerPQ) Remove(poller *waitingPoller) {
	heap.Remove(p, poller.matchHeapIndex)
}

// implements heap.Interface, do not call directly
func (p *pollerPQ) Swap(i int, j int) {
	p.heap[i], p.heap[j] = p.heap[j], p.heap[i]
	p.heap[i].matchHeapIndex = i
	p.heap[j].matchHeapIndex = j
}

// implements heap.Interface, do not call directly
func (p *pollerPQ) Push(x any) {
	poller := x.(*waitingPoller) // nolint:revive
	poller.matchHeapIndex = len(p.heap)
	p.heap = append(p.heap, poller)
}

// implements heap.Interface, do not call directly
func (p *pollerPQ) Pop() any {
	last := len(p.heap) - 1
	poller := p.heap[last]
	p.heap = p.heap[:last]
	poller.matchHeapIndex = invalidHeapIndex
	return poller
}

type taskPQ struct {
	heap []*internalTask

	// ages holds task create time for tasks from merged local backlogs (not forwarded).
	// note that matcherData may get tasks from multiple versioned backlogs due to
	// versioning redirection.
	ages backlogAgeTracker
}

func (t *taskPQ) Add(task *internalTask) {
	heap.Push(t, task)
}

func (t *taskPQ) Remove(task *internalTask) {
	heap.Remove(t, task.matchHeapIndex)
}

// implements heap.Interface
func (t *taskPQ) Len() int {
	return len(t.heap)
}

// implements heap.Interface, do not call directly
func (t *taskPQ) Less(i int, j int) bool {
	// Overall priority key will eventually look something like:
	// - ready time: to sort all ready tasks ahead of others, or else find the earliest ready task
	// - isPollForwarder: forwarding polls should happen only if there are no other tasks
	// - priority key: to sort tasks by priority
	// - fairness key pass: to arrange tasks fairly by key
	// - ordering key: to sort tasks by ordering key
	// - task id: last resort comparison

	a, b := t.heap[i], t.heap[j]

	// TODO(pri): ready time is not task-specific yet, we only have whole-queue, so we don't
	// need to consider this here yet.
	// // ready time
	// aready, bready := max(t.now, t.readyTimeForTask(a)), max(t.now, t.readyTimeForTask(b))
	// if aready < bready {
	// 	return true
	// } else if aready > bready {
	// 	return false
	// }

	// poll forwarder is always last
	if !a.isPollForwarder && b.isPollForwarder {
		return true
	} else if a.isPollForwarder && !b.isPollForwarder {
		return false
	}

	// try priority
	ap, bp := a.getPriority(), b.getPriority()
	apk, bpk := ap.GetPriorityKey(), bp.GetPriorityKey()
	if apk < bpk {
		return true
	} else if apk > bpk {
		return false
	}

	// Note: sync match tasks have a fixed negative id.
	// Query tasks will get 0 here.
	var alevel, blevel fairLevel
	if a.event != nil && a.event.AllocatedTaskInfo != nil {
		alevel = fairLevelFromAllocatedTask(a.event.AllocatedTaskInfo)
	}
	if b.event != nil && b.event.AllocatedTaskInfo != nil {
		blevel = fairLevelFromAllocatedTask(b.event.AllocatedTaskInfo)
	}
	return alevel.less(blevel)
}

// implements heap.Interface, do not call directly
func (t *taskPQ) Swap(i int, j int) {
	t.heap[i], t.heap[j] = t.heap[j], t.heap[i]
	t.heap[i].matchHeapIndex = i
	t.heap[j].matchHeapIndex = j
}

// implements heap.Interface, do not call directly
func (t *taskPQ) Push(x any) {
	task := x.(*internalTask) // nolint:revive
	task.matchHeapIndex = len(t.heap)
	t.heap = append(t.heap, task)

	if task.source == enumsspb.TASK_SOURCE_DB_BACKLOG && task.forwardInfo == nil {
		t.ages.record(task.event.Data.CreateTime, 1)
	}
}

// implements heap.Interface, do not call directly
func (t *taskPQ) Pop() any {
	last := len(t.heap) - 1
	task := t.heap[last]
	t.heap = t.heap[:last]
	task.matchHeapIndex = invalidHeapIndex

	if task.source == enumsspb.TASK_SOURCE_DB_BACKLOG && task.forwardInfo == nil {
		t.ages.record(task.event.Data.CreateTime, -1)
	}

	return task
}

// Calls pred on each task. If it returns true, call post on the task and remove it
// from the queue, otherwise keep it.
// pred and post must not make any other calls on taskPQ until ForEachTask returns!
func (t *taskPQ) ForEachTask(pred func(*internalTask) bool, post func(*internalTask)) {
	t.heap = slices.DeleteFunc(t.heap, func(task *internalTask) bool {
		if task.isPollForwarder || !pred(task) {
			return false
		}
		task.matchHeapIndex = invalidHeapIndex - 1 // maintain heap/index invariant
		if task.source == enumsspb.TASK_SOURCE_DB_BACKLOG && task.forwardInfo == nil {
			t.ages.record(task.event.Data.CreateTime, -1)
		}
		post(task)
		return true
	})
	// re-establish heap
	for i, task := range t.heap {
		task.matchHeapIndex = i
	}
	heap.Init(t)
}

type matcherData struct {
	config     *taskQueueConfig
	logger     log.Logger
	timeSource clock.TimeSource
	canForward bool

	lock sync.Mutex // covers everything below, and all fields in any waitableMatchResult

	rateLimitTimer         resettableTimer
	rateLimitManager       *rateLimitManager
	reconsiderForwardTimer resettableTimer

	// waiting pollers and tasks
	// invariant: all pollers and tasks in these data structures have matchResult == nil
	pollers pollerPQ
	tasks   taskPQ

	lastPoller time.Time // most recent poll start time
}

func newMatcherData(config *taskQueueConfig, logger log.Logger, timeSource clock.TimeSource, canForward bool, rateLimitManager *rateLimitManager) matcherData {
	return matcherData{
		config:     config,
		logger:     logger,
		timeSource: timeSource,
		canForward: canForward,
		tasks: taskPQ{
			ages: newBacklogAgeTracker(),
		},
		rateLimitManager: rateLimitManager,
	}
}

func (d *matcherData) EnqueueTaskNoWait(task *internalTask) {
	d.lock.Lock()
	task.initMatch(d)
	d.tasks.Add(task)
	d.findAndWakeMatches()
	d.lock.Unlock()
}

func (d *matcherData) RemoveTask(task *internalTask) {
	d.lock.Lock()
	defer d.lock.Unlock()

	if task.matchHeapIndex >= 0 {
		d.tasks.Remove(task)
	}
}

func (d *matcherData) EnqueueTaskAndWait(ctxs []context.Context, task *internalTask) *matchResult {
	d.lock.Lock()
	// add and look for match
	task.initMatch(d)
	d.tasks.Add(task)
	d.findAndWakeMatches()
	// if already matched, return
	if task.matchResult != nil {
		d.lock.Unlock()
		return task.matchResult
	}

	// arrange to wake up on context close
	for i, ctx := range ctxs {
		stop := context.AfterFunc(ctx, func() {
			d.lock.Lock()
			defer d.lock.Unlock()

			if task.matchResult == nil {
				d.tasks.Remove(task)
				task.wake(d.logger, &matchResult{ctxErr: ctx.Err(), ctxErrIdx: i})
			}
		})
		defer stop() // nolint:revive // there's only ever a small number of contexts
	}

	// Release the lock and wait for match using a channel-based approach
	d.lock.Unlock()

	// Wait for match result using a channel
	matchChan := make(chan *matchResult, 1)
	go func() {
		d.lock.Lock()
		defer d.lock.Unlock()
		result := task.waitForMatch()
		matchChan <- result
	}()

	return <-matchChan
}

func (d *matcherData) ReenqueuePollerIfNotMatched(poller *waitingPoller) {
	if poller.matchResult == nil {
		d.lock.Lock()
		d.pollers.Add(poller)
		d.findAndWakeMatches()
		d.lock.Unlock()
	}
}

func (d *matcherData) EnqueuePollerAndWait(ctxs []context.Context, poller *waitingPoller) *matchResult {
	d.lock.Lock()

	// update this for timeSinceLastPoll
	d.lastPoller = util.MaxTime(d.lastPoller, poller.startTime)

	// add and look for match
	poller.initMatch(d)
	d.pollers.Add(poller)
	d.findAndWakeMatches()
	// if already matched, return
	if poller.matchResult != nil {
		d.lock.Unlock()
		return poller.matchResult
	}

	// arrange to wake up on context close
	for i, ctx := range ctxs {
		stop := context.AfterFunc(ctx, func() {
			d.lock.Lock()
			defer d.lock.Unlock()

			if poller.matchResult == nil {
				// if poll was being forwarded, it would be absent from heap even though
				// matchResult == nil
				if poller.matchHeapIndex >= 0 {
					d.pollers.Remove(poller)
				}
				poller.wake(d.logger, &matchResult{ctxErr: ctx.Err(), ctxErrIdx: i})
			}
		})
		defer stop() // nolint:revive // there's only ever a small number of contexts
	}

	// Release the lock and wait for match using a channel-based approach
	d.lock.Unlock()

	// Wait for match result using a channel
	matchChan := make(chan *matchResult, 1)
	go func() {
		d.lock.Lock()
		defer d.lock.Unlock()
		result := poller.waitForMatch()
		matchChan <- result
	}()

	return <-matchChan
}

func (d *matcherData) MatchTaskImmediately(task *internalTask) (canSyncMatch, gotSyncMatch bool) {
	d.lock.Lock()

	if !d.isBacklogNegligible() {
		// To ensure better dispatch ordering, we block sync match when a significant backlog is present.
		// Note that this check does not make a noticeable difference for history tasks, as they do not wait for a
		// poller to become available. In presence of a backlog the chance of a poller being available when sync match
		// request comes is almost zero.
		// This check is mostly effective for the sync match requests that come from child partitions for spooled tasks.
		return false, false
	}

	task.initMatch(d)
	d.tasks.Add(task)
	d.findAndWakeMatches()
	// don't wait, check if match() picked this one already
	if task.matchResult != nil {
		d.lock.Unlock()
		return true, true
	}
	d.tasks.Remove(task)
	d.lock.Unlock()
	return true, false
}

func (d *matcherData) ReprocessTasks(pred func(*internalTask) bool) []*internalTask {
	d.lock.Lock()
	defer d.lock.Unlock()

	reprocess := make([]*internalTask, 0, len(d.tasks.heap))
	d.tasks.ForEachTask(
		pred,
		func(task *internalTask) {
			// for sync tasks: wake up waiters with a fake context error
			// for backlog tasks: the caller should call finish()
			task.wake(d.logger, &matchResult{ctxErr: errReprocessTask, ctxErrIdx: -1})
			reprocess = append(reprocess, task)
		},
	)
	return reprocess
}

// findMatch should return the highest priority task+poller match even if the per-task rate
// limit doesn't allow the task to be matched yet.
// call with lock held
// nolint:revive // will improve later
func (d *matcherData) findMatch(allowForwarding bool) (*internalTask, *waitingPoller) {
	// TODO(pri): optimize so it's not O(d*n) worst case
	// TODO(pri): this iterates over heap as slice, which isn't quite correct, but okay for now
	for _, task := range d.tasks.heap {
		if !allowForwarding && task.isPollForwarder {
			continue
		}

		for _, poller := range d.pollers.heap {
			// can't match cases:
			if poller.queryOnly && !(task.isQuery() || task.isPollForwarder) {
				continue
			} else if task.isPollForwarder && poller.forwardCtx == nil {
				continue
			} else if poller.isTaskForwarder && !allowForwarding {
				continue
			} else if poller.isTaskValidator && task.forwardCtx != nil {
				continue
			}

			return task, poller
		}
	}
	return nil, nil
}

// call with lock held
func (d *matcherData) allowForwarding() (allowForwarding bool) {
	// If there is a non-negligible backlog, we pause forwarding to make sure
	// root and leaf partitions are treated equally and can process their
	// backlog at the same rate. Stopping task forwarding, prevent poll
	// forwarding as well (in presence of a backlog). This ensures all partitions
	// receive polls and tasks at the same rate.
	//
	// Exception: we allow forward if this partition has not got any polls
	// recently. This is helpful when there are very few pollers and they
	// and they are all stuck in the wrong (root) partition. (Note that since
	// frontend balanced the number of pending pollers per partition this only
	// becomes an issue when the pollers are fewer than the partitions)
	//
	// If allowForwarding was false and changes to true due solely to the passage
	// of time, then we should ensure that match() is called again so that
	// pending tasks/polls can now be forwarded. When does that happen? if
	// isBacklogNegligible changes from false to true, or if we no longer have
	// recent polls.
	//
	// With time, backlog age gets larger, so isBacklogNegligible can go from
	// true to false and not the other way, so that's safe. But it is possible
	// that we no longer have recent polls. So we need to ensure that match() is
	// called again in that case, using reconsiderForwardTimer.
	if d.isBacklogNegligible() {
		d.reconsiderForwardTimer.unset()
		return true
	}
	delayToForwardingAllowed := d.config.MaxWaitForPollerBeforeFwd() - time.Since(d.lastPoller)
	d.reconsiderForwardTimer.set(d.timeSource, d.rematchAfterTimer, delayToForwardingAllowed)
	return delayToForwardingAllowed <= 0
}

// findAndWakeMatches finds and wakes up matching tasks and pollers
// This method must be called with d.lock held
func (d *matcherData) findAndWakeMatches() {
	allowForwarding := d.canForward && d.allowForwarding()

	r := d.rateLimitManager
	// Get the current time and rate limit state
	now := r.timeSource.Now().UnixNano()

	// Since we already hold d.lock, we can safely access rate limit manager
	// without additional locking to avoid deadlocks
	wholeQueueReady := r.wholeQueueReady
	perKeyLimit := r.perKeyLimit
	perKeyOverrides := r.perKeyOverrides

	// TODO(pri): for task-specific ready time, we need to do a full/partial re-heapify here

	for {
		// search for highest priority match
		task, poller := d.findMatch(allowForwarding)
		if task == nil || poller == nil {
			// no more current matches, stop rate limit timer if was running
			d.rateLimitTimer.unset()
			return
		}

		// check ready time using the cached rate limit state
		delay := d.readyTimeForTaskWithState(task, now, wholeQueueReady, perKeyLimit, perKeyOverrides).delay(now)
		d.rateLimitTimer.set(d.timeSource, d.rematchAfterTimer, delay)
		if delay > 0 {
			return // not ready yet, timer will call match later
		}

		// ready to signal match
		d.tasks.Remove(task)
		d.pollers.Remove(poller)

		// Consume tokens directly since we already hold the lock
		d.consumeTokensDirect(now, task, 1)
		task.recycleToken = d.recycleToken

		// Update cached rate limit state after consuming tokens
		wholeQueueReady = r.wholeQueueReady

		res := &matchResult{task: task, poller: poller}
		task.wake(d.logger, res)
		// for poll forwarder: skip waking poller, forwarder will call finishMatchAfterPollForward
		if !task.isPollForwarder {
			poller.wake(d.logger, res)
		}
		// TODO(pri): consider having task forwarding work the same way, with a half-match,
		// instead of full match and then pass forward result on response channel?
		// TODO(pri): maybe consider leaving tasks and polls in the heap while forwarding and
		// allow them to be matched locally while forwarded (and then cancel the forward)?
	}
}

// readyTimeForTaskWithState calculates the ready time for a task using provided rate limit state
// to avoid deadlocks when called from findAndWakeMatches
func (d *matcherData) readyTimeForTaskWithState(task *internalTask, now int64, wholeQueueReady simpleLimiter, perKeyLimit simpleLimiterParams, perKeyOverrides fairnessWeightOverrides) simpleLimiter {
	// TODO(pri): after we have task-specific ready time, we can re-enable this
	// if task.isForwarded() {
	// 	// don't count any rate limit for forwarded tasks, it was counted on the child
	// 	return 0
	// }
	ready := wholeQueueReady

	if perKeyLimit.limited() {
		key := task.getPriority().GetFairnessKey()
		// Get per-key ready time directly since we already hold the lock
		r := d.rateLimitManager
		var perKeyReady simpleLimiter
		if v := r.perKeyReady.Get(key); v != nil {
			perKeyReady = v.(simpleLimiter)
		}

		// Always apply per-key rate limiting, even for new keys
		// For new keys, perKeyReady is 0, which means they can start immediately
		// but subsequent tasks will be rate limited
		ready = max(ready, perKeyReady)
	}

	return ready
}

// consumeTokensAtomic consumes tokens atomically to avoid deadlocks
func (d *matcherData) consumeTokensAtomic(now int64, task *internalTask, tokens int64) {
	if task.isForwarded() {
		// don't count any rate limit for forwarded tasks, it was counted on the child
		return
	}

	r := d.rateLimitManager
	r.mu.Lock()
	defer r.mu.Unlock()

	r.wholeQueueReady = r.wholeQueueReady.consume(r.wholeQueueLimit, now, tokens)

	if r.perKeyLimit.limited() {
		pri := task.getPriority()
		key := pri.GetFairnessKey()
		weight := getEffectiveWeight(r.perKeyOverrides, pri)
		p := r.perKeyLimit
		p.interval = time.Duration(float32(p.interval) / weight) // scale by weight
		var sl simpleLimiter
		if v := r.perKeyReady.Get(key); v != nil {
			sl = v.(simpleLimiter) // nolint:revive
		}
		r.perKeyReady.Put(key, sl.consume(p, now, tokens))
	}
}

// consumeTokensDirect consumes tokens directly without additional locking
// This method must be called with d.lock held
func (d *matcherData) consumeTokensDirect(now int64, task *internalTask, tokens int64) {
	if task.isForwarded() {
		// don't count any rate limit for forwarded tasks, it was counted on the child
		return
	}

	r := d.rateLimitManager
	// Since we already hold d.lock, we can safely access rate limit manager fields
	r.wholeQueueReady = r.wholeQueueReady.consume(r.wholeQueueLimit, now, tokens)

	if r.perKeyLimit.limited() {
		pri := task.getPriority()
		key := pri.GetFairnessKey()
		weight := getEffectiveWeight(r.perKeyOverrides, pri)
		p := r.perKeyLimit
		p.interval = time.Duration(float32(p.interval) / weight) // scale by weight
		var sl simpleLimiter
		if v := r.perKeyReady.Get(key); v != nil {
			sl = v.(simpleLimiter) // nolint:revive
		}
		r.perKeyReady.Put(key, sl.consume(p, now, tokens))
	}
}

func (d *matcherData) recycleToken(task *internalTask) {
	// Use a goroutine to avoid deadlocks when recycleToken is called from finish()
	// which may already hold locks
	go func() {
		r := d.rateLimitManager
		r.mu.Lock()
		now := r.timeSource.Now().UnixNano()
		r.consumeTokens(now, task, -1)
		r.mu.Unlock()

		// Schedule a match attempt without holding any locks
		// This avoids the deadlock scenario where recycleToken is called
		// while holding rateLimitManager.mu and findAndWakeMatches tries
		// to acquire matcherData.lock
		d.scheduleMatchAttempt()
	}()
}

// scheduleMatchAttempt schedules a match attempt without holding any locks
func (d *matcherData) scheduleMatchAttempt() {
	// Use a timer to schedule the match attempt to avoid deadlocks
	d.rateLimitTimer.set(d.timeSource, d.rematchAfterTimer, 0)
}

// called from timer
func (d *matcherData) rematchAfterTimer() {
	// Timer callbacks should not hold any locks, so we need to acquire the lock here
	d.lock.Lock()
	defer d.lock.Unlock()
	d.findAndWakeMatches()
}

func (d *matcherData) FinishMatchAfterPollForward(poller *waitingPoller, task *internalTask) {
	d.lock.Lock()
	defer d.lock.Unlock()

	if poller.matchResult == nil {
		poller.wake(d.logger, &matchResult{task: task, poller: poller})
	}
}

// isBacklogNegligible returns true if the age of the task backlog is less than the threshold.
// call with lock held.
func (d *matcherData) isBacklogNegligible() bool {
	t := d.tasks.ages.oldestTime()
	return t.IsZero() || time.Since(t) < d.config.BacklogNegligibleAge()
}

func (d *matcherData) TimeSinceLastPoll() time.Duration {
	d.lock.Lock()
	defer d.lock.Unlock()
	return time.Since(d.lastPoller)
}

// waitable match result:

type waitableMatchResult struct {
	// these fields are under matcherData.lock even though they're embedded in other structs
	matchCond      sync.Cond
	matchResult    *matchResult
	matchHeapIndex int // current heap index for easy removal
}

func (w *waitableMatchResult) initMatch(d *matcherData) {
	w.matchCond.L = &d.lock
	w.matchResult = nil
}

// call with matcherData.lock held.
// w.matchResult must be nil (can't call wake twice).
// w must not be in queues anymore.
func (w *waitableMatchResult) wake(logger log.Logger, res *matchResult) {
	softassert.That(logger, w.matchResult == nil, "wake called twice")
	softassert.That(logger, w.matchHeapIndex < 0, "wake called but still in heap")
	w.matchResult = res
	w.matchCond.Signal()
}

// waitForMatch waits for a match result to be set
// This method must be called with matcherData.lock held
func (w *waitableMatchResult) waitForMatch() *matchResult {
	for w.matchResult == nil {
		w.matchCond.Wait()
	}
	return w.matchResult
}

// resettable timer:

type resettableTimer struct {
	timer clock.Timer // AfterFunc timer
}

// set sets rt to call f after delay. set to <= 0 stops the timer.
func (rt *resettableTimer) set(ts clock.TimeSource, f func(), delay time.Duration) {
	if delay <= 0 {
		rt.unset()
	} else if rt.timer == nil {
		rt.timer = ts.AfterFunc(delay, f)
	} else {
		rt.timer.Reset(delay)
	}
}

// unset stops the timer.
func (rt *resettableTimer) unset() {
	if rt.timer != nil {
		rt.timer.Stop()
		rt.timer = nil
	}
}

// simple limiter

// simpleLimiter and simpleLimiterParams implement a "GCRA" limiter.
// A simpleLimiter is "ready" if its value is <= now (as unix nanos).
type simpleLimiter int64 // ready time as unix nanos

type simpleLimiterParams struct {
	interval time.Duration // ideal task spacing interval, or 0 for no limit (infinite), or -1 for zero limit
	burst    time.Duration // burst duration
}

const maxBurst = time.Minute
const simpleLimiterNever = simpleLimiter(7 << 60) // this is in the year 2225

func makeSimpleLimiterParams(rate float64, burstDuration time.Duration) simpleLimiterParams {
	// 1e-9 would make interval overflow int64
	if rate <= 1e-9 {
		return simpleLimiterParams{
			interval: time.Duration(-1),
		}
	}
	return simpleLimiterParams{
		interval: time.Duration(float64(time.Second) / rate),
		burst:    min(burstDuration, maxBurst),
	}
}

func (p simpleLimiterParams) never() bool   { return p.interval < 0 }
func (p simpleLimiterParams) limited() bool { return p.interval > 0 }

// delay returns the time until the limiter is ready.
// If the return value is <= 0 then the limiter can go now.
func (ready simpleLimiter) delay(now int64) time.Duration {
	return time.Duration(int64(ready) - now)
}

// consume updates ready based on the current time and number of new tokens consumed.
func (ready simpleLimiter) consume(p simpleLimiterParams, now int64, tokens int64) simpleLimiter {
	// This is a slight variation of the normal GCRA: instead of tracking the end of the
	// allowed interval (the theoretical arrival time), ready tracks the beginning of it, and
	// the end is ready + burst. To find the next ready time:
	// - Add ready+burst to find the next theoretical arrival time.
	// - If that's in the past, clip it at the current time.
	// - Subtract burst to turn it back into a ready time.
	// - Finally add the tokens we used.
	//
	// For intuition, consider that if if now is > ready by only a tiny amount, i.e. we're
	// bursting, then the max takes ready+burst and we push up the ready time by the full
	// interval. We can do this burst/interval times before it catches up and we're no longer
	// ready.
	//
	// Alternatively, if now is > ready by more than burst, then we end up subtracting the full
	// burst from now and adding one interval.
	if p.never() {
		return simpleLimiterNever
	}
	clippedReady := max(now, int64(ready)+p.burst.Nanoseconds()) - p.burst.Nanoseconds()
	return simpleLimiter(clippedReady + tokens*p.interval.Nanoseconds())
}

// clip updates ready to an allowable range based on the given parameters.
func (ready simpleLimiter) clip(p simpleLimiterParams, now int64, maxTokens int64) simpleLimiter {
	if p.never() {
		return simpleLimiterNever
	}
	// If ready was set very far in the future (e.g. because the rate was zero), then we can
	// clip it back to now + maxTokens*interval + burst.
	maxDelay := maxTokens*p.interval.Nanoseconds() + p.burst.Nanoseconds()
	return min(ready, simpleLimiter(now+maxDelay))
}
