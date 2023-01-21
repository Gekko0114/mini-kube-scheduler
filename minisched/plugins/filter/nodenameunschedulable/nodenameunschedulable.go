package nodenameunschedulable

import (
	"context"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

type NodeNameUnschedulable struct {
}

var _ framework.Plugin = &NodeNameUnschedulable{}

const Name = "NodeNameUnschedulable"

const (
	ErrReasonUnknownCondition = "node(s) had unknown conditions"
	ErrReasonUnschedulable    = "node(s) were unschedulable"
)

func (pl *NodeNameUnschedulable) Name() string {
	return Name
}

func (pl *NodeNameUnschedulable) EventsToRegister() []framework.ClusterEvent {
	return []framework.ClusterEvent{
		{Resource: framework.Node, ActionType: framework.Add},
	}
}

func (pl *NodeNameUnschedulable) Filter(ctx context.Context, _ *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	node := nodeInfo.Node()
	if node == nil {
		return framework.NewStatus(framework.UnschedulableAndUnresolvable, ErrReasonUnknownCondition)
	}

	name := node.Name
	nodeUnschedulable := strings.Contains(name, "unschedulable")
	if nodeUnschedulable {
		return framework.NewStatus(framework.UnschedulableAndUnresolvable, ErrReasonUnschedulable)
	}
	return nil
}

func New(_ runtime.Object, _ framework.Handle) (framework.Plugin, error) {
	return &NodeNameUnschedulable{}, nil
}
