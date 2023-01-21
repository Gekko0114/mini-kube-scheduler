package nodenumber

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/sanposhiho/mini-kube-scheduler/minisched/waitingpod"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

type NodeNumber struct {
	h waitingpod.Handle
}

var _ framework.ScorePlugin = &NodeNumber{}
var _ framework.PreScorePlugin = &NodeNumber{}
var _ framework.PermitPlugin = &NodeNumber{}

const Name = "NodeNumber"
const preScoreStateKey = "PreScore" + Name

func (pl *NodeNumber) Name() string {
	return Name
}

type preScoreState struct {
	podSuffixNumber int
}

func (s *preScoreState) Clone() framework.StateData {
	return s
}

func (pl *NodeNumber) PreScore(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodes []*v1.Node) *framework.Status {
	podNameLastChar := pod.Name[len(pod.Name)-1:]
	podnum, err := strconv.Atoi(podNameLastChar)
	if err != nil {
		return nil
	}
	s := &preScoreState{
		podSuffixNumber: podnum,
	}
	state.Write(preScoreStateKey, s)
	return nil
}

func (pl *NodeNumber) EventsToRegister() []framework.ClusterEvent {
	return []framework.ClusterEvent{
		{Resource: framework.Node, ActionType: framework.Add},
	}
}

func (pl *NodeNumber) Score(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
	data, err := state.Read(preScoreStateKey)
	if err != nil {
		return 0, framework.AsStatus(err)
	}

	s, ok := data.(*preScoreState)
	if !ok {
		return 0, framework.AsStatus(errors.New("failed to convert pre score state"))
	}

	nodeNameLastChar := nodeName[len(nodeName)-1:]
	nodenum, err := strconv.Atoi(nodeNameLastChar)
	if err != nil {
		return 0, nil
	}

	if s.podSuffixNumber == nodenum {
		return 10, nil
	}
	return 0, nil
}

func (pl *NodeNumber) ScoreExtensions() framework.ScoreExtensions {
	return nil
}

func (pl *NodeNumber) Permit(ctx context.Context, state *framework.CycleState, p *v1.Pod, nodeName string) (*framework.Status, time.Duration) {
	nodeNameLastChar := nodeName[len(nodeName)-1:]
	nodenum, err := strconv.Atoi(nodeNameLastChar)
	if err != nil {
		return nil, 0
	}
	time.AfterFunc(time.Duration(nodenum)*time.Second, func() {
		wp := pl.h.GetWaitingPod(p.GetUID())
		wp.Allow(pl.Name())
	})

	timeout := time.Duration(10) * time.Second
	return framework.NewStatus(framework.Wait, ""), timeout
}

func New(_ runtime.Object, h waitingpod.Handle) (framework.Plugin, error) {
	return &NodeNumber{h: h}, nil
}
