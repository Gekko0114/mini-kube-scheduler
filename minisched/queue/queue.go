package queue

import (
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

type SchedulingQueue struct {
	lock            sync.RWMutex
	activeQ         []*framework.QueuedPodInfo
	podBackoffQ     []*framework.QueuedPodInfo
	unschedulableQ  map[string]*framework.QueuedPodInfo
	clusterEventMap map[framework.ClusterEvent]sets.String
}

func New() *SchedulingQueue {
	return &SchedulingQueue{
		activeQ:        []*framework.QueuedPodInfo{},
		podBackoffQ:    []*framework.QueuedPodInfo{},
		unschedulableQ: map[string]*framework.QueuedPodInfo{},
	}
}

func (s *SchedulingQueue) Add(pod *v1.Pod) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	podInfo := s.newQueuedPodInfo(pod)

	s.activeQ = append(s.activeQ, podInfo)
	return nil
}

type preEnqueueCheck func(pod *v1.Pod) bool

func (s *SchedulingQueue) MoveAllToActiveOrBackoffQueue(event framework.ClusterEvent) {
	s.lock.Lock()
	defer s.lock.Unlock()
	unschedulablePods := make([]*framework.QueuedPodInfo, 0, len(s.unschedulableQ))
	for _, pInfo := range s.unschedulableQ {
		unschedulablePods = append(unschedulablePods, pInfo)
	}
	s.movePodsToActiveOrBackoffQueue(unschedulablePods, event)
}

func (s *SchedulingQueue) movePodsToActiveOrBackoffQueue(podInfoList []*framework.QueuedPodInfo, event framework.ClusterEvent) {
	for _, pInfo := range podInfoList {
		if len(pInfo.UnschedulablePlugins) != 0 && !s.podMatchesEvent(pInfo, event) {
			continue
		}

		if isPodBackingoff(pInfo) {
			s.podBackoffQ = append(s.podBackoffQ, pInfo)
		} else {
			s.activeQ = append(s.activeQ, pInfo)
		}
		delete(s.unschedulableQ, keyFunc(pInfo))
	}
}

func (s *SchedulingQueue) NextPod() *v1.Pod {
	// wait
	for len(s.activeQ) == 0 {
	}

	p := s.activeQ[0]
	s.activeQ = s.activeQ[1:]
	return p.Pod
}

func (s *SchedulingQueue) AddUnschedulable(pInfo *framework.QueuedPodInfo) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	pInfo.Timestamp = time.Now()
	s.unschedulableQ[keyFunc(pInfo)] = pInfo

	klog.Info("queue: pod added to unschedulableQ: "+pInfo.Pod.Name+". This pod is unscheduled by ", pInfo.UnschedulablePlugins)
	return nil
}

func (s *SchedulingQueue) Update(oldPod, newPod *v1.Pod) error {
	panic("not implemented")
	return nil
}

func (s *SchedulingQueue) Delete(pod *v1.Pod) error {
	panic("not implemented")
	return nil
}

func (s *SchedulingQueue) AssignedPodAdded(pod *v1.Pod) {
	panic("not implemented")
}

func (s *SchedulingQueue) AssignedPodUpdated(pod *v1.Pod) {
	panic("not implemented")
}

func (s *SchedulingQueue) flushBackoffQCompleted() {
	panic("not implemented")
}

func (s *SchedulingQueue) flushUnschedulableQLeftover() {
	panic("not implemented")
}

func keyFunc(pInfo *framework.QueuedPodInfo) string {
	return pInfo.Pod.Name + "_" + pInfo.Pod.Namespace
}

func (s *SchedulingQueue) newQueuedPodInfo(pod *v1.Pod, unschedulableplugins ...string) *framework.QueuedPodInfo {
	now := time.Now()
	return &framework.QueuedPodInfo{
		PodInfo:                 framework.NewPodInfo(pod),
		Timestamp:               now,
		InitialAttemptTimestamp: now,
		UnschedulablePlugins:    sets.NewString(unschedulableplugins...),
	}
}

func (s *SchedulingQueue) podMatchesEvent(podInfo *framework.QueuedPodInfo, clusterEvent framework.ClusterEvent) bool {
	if clusterEvent.IsWildCard() {
		return true
	}

	for evt, nameSet := range s.clusterEventMap {
		evtMatch := evt.IsWildCard() || (evt.Resource == clusterEvent.Resource && evt.ActionType&clusterEvent.ActionType != 0)

		if evtMatch && intersect(nameSet, podInfo.UnschedulablePlugins) {
			return true
		}
	}
	return false
}

func intersect(x, y sets.String) bool {
	if len(x) > len(y) {
		x, y = y, x
	}
	for v := range x {
		if y.Has(v) {
			return true
		}
	}
	return false
}

func isPodBackingoff(podInfo *framework.QueuedPodInfo) bool {
	boTime := getBackoffTime(podInfo)
	return boTime.After(time.Now())
}

func getBackoffTime(podInfo *framework.QueuedPodInfo) time.Time {
	duration := calculateBackoffDuration(podInfo)
	backoffTime := podInfo.Timestamp.Add(duration)
	return backoffTime
}

const (
	podInitialBackoffDuration = 1 * time.Second
	podMaxBackoffDuration     = 10 * time.Second
)

func calculateBackoffDuration(podInfo *framework.QueuedPodInfo) time.Duration {
	duration := podInitialBackoffDuration
	for i := 1; i < podInfo.Attempts; i++ {
		if duration > podMaxBackoffDuration-duration {
			return podMaxBackoffDuration
		}
		duration += duration
	}
	return duration
}
