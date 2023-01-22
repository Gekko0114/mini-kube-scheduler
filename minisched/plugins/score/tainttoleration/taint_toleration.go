package tainttoleration

import (
	"context"

	"github.com/sanposhiho/mini-kube-scheduler/minisched/waitingpod"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

type TaintToleration struct {
	h waitingpod.Handle
}

var _ framework.ScorePlugin = &TaintToleration{}
var _ framework.PreScorePlugin = &TaintToleration{}

const Name = "TaintToleration"
const preScoreStateKey = "PreScore" + Name

func (pl *TaintToleration) Name() string {
	return Name
}

type preScoreState struct {
	toleration []v1.Toleration
}

func (s *preScoreState) Clone() framework.StateData {
	return s
}

func (pl *TaintToleration) PreScore(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodes []*v1.Node) *framework.Status {
	if len(nodes) == 0 {
		return nil
	}
	toleration := pod.Spec.Tolerations
	s := &preScoreState{
		toleration: toleration,
	}
	state.Write(preScoreStateKey, s)
	return nil
}

func (pl *TaintToleration) Score(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
	klog.Info("Calculate traint_toleration score")

	for _, toleration := range pod.Spec.Tolerations {
		klog.Info("Key: " + toleration.Key)
		klog.Info("Operator: " + toleration.Operator)
		klog.Info("Effect: " + toleration.Effect)
	}

	return 100000000, nil
}

func (pl *TaintToleration) ScoreExtensions() framework.ScoreExtensions {
	return nil
}

func New(_ runtime.Object, h waitingpod.Handle) (framework.Plugin, error) {
	return &TaintToleration{h: h}, nil
}
