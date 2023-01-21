package minisched

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// 特定のevent(node関連の変化など)が発生したら、特定の原因でUnschedulableになったpodを復活させる
func addAllEventHandlers(
	sched *Scheduler,
	informerFactory informers.SharedInformerFactory,
	gvkMap map[framework.GVK]framework.ActionType,
) {
	// unscheduled pod
	informerFactory.Core().V1().Pods().Informer().AddEventHandler(
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				switch t := obj.(type) {
				case *v1.Pod:
					return !assignedPod(t)
				default:
					return false
				}
			},
			Handler: cache.ResourceEventHandlerFuncs{
				// Consider only adding.
				AddFunc: sched.addPodToSchedulingQueue,
			},
		},
	)

	buildEvtResHandler := func(at framework.ActionType, gvk framework.GVK, shortGVK string) cache.ResourceEventHandlerFuncs {
		funcs := cache.ResourceEventHandlerFuncs{}
		if at&framework.Add != 0 {
			evt := framework.ClusterEvent{Resource: gvk, ActionType: framework.Add, Label: fmt.Sprintf("%vAdd", shortGVK)}
			klog.Info("buildEvtResHandler")
			klog.Info(evt)
			funcs.AddFunc = func(_ interface{}) {
				sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(evt)
			}
		}
		if at&framework.Update != 0 {
			evt := framework.ClusterEvent{Resource: gvk, ActionType: framework.Update, Label: fmt.Sprintf("%vUpdate", shortGVK)}
			funcs.UpdateFunc = func(_, _ interface{}) {
				sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(evt)
			}
		}
		if at&framework.Delete != 0 {
			evt := framework.ClusterEvent{Resource: gvk, ActionType: framework.Delete, Label: fmt.Sprintf("%vDelete", shortGVK)}
			funcs.DeleteFunc = func(_ interface{}) {
				sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(evt)
			}
		}
		return funcs
	}

	for gvk, at := range gvkMap {
		switch gvk {
		case framework.Node:
			// Node関連のeventが発生した場合に、buildEvtResHandlerが呼ばれる?
			informerFactory.Core().V1().Nodes().Informer().AddEventHandler(
				buildEvtResHandler(at, framework.Node, "Node"),
			)
		default:
			klog.Info("We don't create informer for " + gvk)
		}
	}
}

// assignedPod selects pods that are assigned (scheduled and running).
func assignedPod(pod *v1.Pod) bool {
	return len(pod.Spec.NodeName) != 0
}

func (sched *Scheduler) addPodToSchedulingQueue(obj interface{}) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		return
	}
	sched.SchedulingQueue.Add(pod)
}
