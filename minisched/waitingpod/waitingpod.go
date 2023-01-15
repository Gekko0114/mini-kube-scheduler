package waitingpod

import (
	"fmt"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

type Handle interface {
	GetWaitingPod(uid types.UID) *WaitingPod
}

type WaitingPod struct {
	pod            *v1.Pod
	pendingPlugins map[string]*time.Timer
	s              chan *framework.Status
	mu             sync.RWMutex
}

func NewWaitingPod(pod *v1.Pod, pluginsMaxWaitTime map[string]time.Duration) *WaitingPod {
	wp := &WaitingPod{
		pod: pod,
		s:   make(chan *framework.Status, 1),
	}

	wp.pendingPlugins = make(map[string]*time.Timer, len(pluginsMaxWaitTime))
	wp.mu.Lock()
	defer wp.mu.Unlock()
	for k, v := range pluginsMaxWaitTime {
		plugin, waitTime := k, v
		wp.pendingPlugins[plugin] = time.AfterFunc(waitTime, func() {
			msg := fmt.Sprintf("rejected due to timeout after waiting %v at plugin %v", waitTime, plugin)
			wp.Reject(plugin, msg)
		})
	}
	return wp
}

func (w *WaitingPod) GetPod() *v1.Pod {
	return w.pod
}

func (w *WaitingPod) GetSignal() *framework.Status {
	return <-w.s
}

func (w *WaitingPod) GetPendingPlugins() []string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	plugins := make([]string, 0, len(w.pendingPlugins))
	for p := range w.pendingPlugins {
		plugins = append(plugins, p)
	}
	return plugins
}

func (w *WaitingPod) Allow(pluginName string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if timer, exist := w.pendingPlugins[pluginName]; exist {
		timer.Stop()
		delete(w.pendingPlugins, pluginName)
	}

	if len(w.pendingPlugins) != 0 {
		return
	}

	select {
	case w.s <- framework.NewStatus(framework.Success, ""):
	default:
	}
}

func (w *WaitingPod) Reject(pluginName, msg string) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	for _, timer := range w.pendingPlugins {
		timer.Stop()
	}

	select {
	case w.s <- framework.NewStatus(framework.Unschedulable, msg).WithFailedPlugin(pluginName):
	default:
	}
}