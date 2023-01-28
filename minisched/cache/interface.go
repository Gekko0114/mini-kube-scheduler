package cache

import (
	"github.com/Gekko0114/mini-kube-scheduler/minisched/waitingpod"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
)

type Cache interface {
	NodeCount() int
	PodCount() (int, error)
	AssumePod(pod *v1.Pod) error
	FinishBinding(pod *v1.Pod) error
	ForgetPod(pod *v1.Pod) error
	AddPod(pod *v1.Pod) error
	UpdatePod(oldPod, newPod *v1.Pod) error
	RemovePod(pod *v1.Pod) error
	GetPod(pod *v1.Pod) (*v1.Pod, error)
	IsAssumedPod(pod *v1.Pod) (bool, error)
	AddNode(node *v1.Node) *waitingpod.NodeInfo
	UpdateNode(oldNode, newNode *v1.Node) *waitingpod.NodeInfo
	RemoveNode(node *v1.Node) error
	UpdateSnapshot(nodeSnapshot *Snapshot) error
	Dump() *Dump
}

type Dump struct {
	AssumedPods sets.String
	Nodes       map[string]*waitingpod.NodeInfo
}
