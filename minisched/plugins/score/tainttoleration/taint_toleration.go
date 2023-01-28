package tainttoleration

import (
	"context"
	"fmt"

	"github.com/Gekko0114/mini-kube-scheduler/minisched/waitingpod"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	v1helper "k8s.io/component-helpers/scheduling/corev1"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

type TaintToleration struct {
	handle waitingpod.Handle
}

var _ framework.FilterPlugin = &TaintToleration{}
var _ framework.ScorePlugin = &TaintToleration{}
var _ framework.PreScorePlugin = &TaintToleration{}

const Name = "TaintToleration"
const preScoreStateKey = "PreScore" + Name

func (pl *TaintToleration) Name() string {
	return Name
}

func (pl *TaintToleration) Filter(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	node := nodeInfo.Node()
	if node == nil {
		return framework.AsStatus(fmt.Errorf("invalid nodeInfo"))
	}

	taint, isUntolerated := v1helper.FindMatchingUntoleratedTaint(node.Spec.Taints, pod.Spec.Tolerations, DoNotScheduleTaintsFilterFunc())
	if !isUntolerated {
		return nil
	}

	errReason := fmt.Sprintf("node(s) had untolerated taint {%s: %s}", taint.Key, taint.Value)
	return framework.NewStatus(framework.UnschedulableAndUnresolvable, errReason)
}

func DoNotScheduleTaintsFilterFunc() func(t *v1.Taint) bool {
	return func(t *v1.Taint) bool {
		return t.Effect == v1.TaintEffectNoSchedule || t.Effect == v1.TaintEffectNoExecute
	}
}

type preScoreState struct {
	tolerationsPreferNoSchedule []v1.Toleration
}

func (s *preScoreState) Clone() framework.StateData {
	return s
}

func getAllTolerationPreferNoSchedule(tolerations []v1.Toleration) (tolerationList []v1.Toleration) {
	for _, toleration := range tolerations {
		if len(toleration.Effect) == 0 || toleration.Effect == v1.TaintEffectPreferNoSchedule {
			tolerationList = append(tolerationList, toleration)
		}
	}
	return
}

func (pl *TaintToleration) PreScore(ctx context.Context, cyclestate *framework.CycleState, pod *v1.Pod, nodes []*v1.Node) *framework.Status {
	if len(nodes) == 0 {
		return nil
	}
	tolerationsPreferNoSchedule := getAllTolerationPreferNoSchedule(pod.Spec.Tolerations)
	state := &preScoreState{
		tolerationsPreferNoSchedule: tolerationsPreferNoSchedule,
	}
	cyclestate.Write(preScoreStateKey, state)
	return nil
}

func getPreScoreState(cycleState *framework.CycleState) (*preScoreState, error) {
	c, err := cycleState.Read(preScoreStateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to read %q from cycleState: %w", preScoreStateKey, err)
	}

	s, ok := c.(*preScoreState)
	if !ok {
		return nil, fmt.Errorf("%+v convert to tainttoleration.preScoreState error", c)
	}
	return s, nil
}

func countIntolerableTaintsPreferNoSchedule(taints []v1.Taint, tolerations []v1.Toleration) (intolerableTaints int) {
	for _, taint := range taints {
		if taint.Effect != v1.TaintEffectPreferNoSchedule {
			continue
		}

		if !v1helper.TolerationsTolerateTaint(tolerations, &taint) {
			intolerableTaints++
		}
	}
	return
}

func (pl *TaintToleration) Score(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
	nodeInfo, err := pl.handle.SnapshotSharedLister().NodeInfos().Get(nodeName)
	if err != nil {
		return 0, framework.AsStatus(fmt.Errorf("getting node %q from Snapshot: %w", nodeName, err))
	}
	node := nodeInfo.Node()

	s, err := getPreScoreState(state)
	if err != nil {
		return 0, framework.AsStatus(err)
	}

	score := int64(countIntolerableTaintsPreferNoSchedule(node.Spec.Taints, s.tolerationsPreferNoSchedule))
	return score, nil
}

func (pl *TaintToleration) ScoreExtensions() framework.ScoreExtensions {
	return nil
}

func New(_ runtime.Object, h waitingpod.Handle) (framework.Plugin, error) {
	return &TaintToleration{handle: h}, nil
}
