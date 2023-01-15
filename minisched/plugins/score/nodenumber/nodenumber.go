package nodenumber

import (
	"context"
	"errors"
	"strconv"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

type NodeNumber struct{}

var _ framework.ScorePlugin = &NodeNumber{}

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

func New(_ runtime.Object, _ framework.Handle) (framework.Plugin, error) {
	return &NodeNumber{}, nil
}
