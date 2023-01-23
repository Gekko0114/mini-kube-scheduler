package minisched

import (
	"fmt"

	"github.com/Gekko0114/mini-kube-scheduler/minisched/plugins/filter/nodenameunschedulable"
	"github.com/Gekko0114/mini-kube-scheduler/minisched/plugins/score/nodenumber"
	"github.com/Gekko0114/mini-kube-scheduler/minisched/plugins/score/tainttoleration"
	"github.com/Gekko0114/mini-kube-scheduler/minisched/queue"
	"github.com/Gekko0114/mini-kube-scheduler/minisched/waitingpod"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodeunschedulable"
)

type Scheduler struct {
	SchedulingQueue *queue.SchedulingQueue

	client clientset.Interface

	waitingPods map[types.UID]*waitingpod.WaitingPod

	filterPlugins   []framework.FilterPlugin
	preScorePlugins []framework.PreScorePlugin
	scorePlugins    []framework.ScorePlugin
	permitPlugins   []framework.PermitPlugin
}

func New(
	client clientset.Interface,
	informerFactory informers.SharedInformerFactory,
) (*Scheduler, error) {
	sched := &Scheduler{
		client:      client,
		waitingPods: map[types.UID]*waitingpod.WaitingPod{},
	}

	filterP, err := createFilterPlugins(sched)
	if err != nil {
		return nil, fmt.Errorf("create filter plugins: %w", err)
	}
	sched.filterPlugins = filterP

	preScoreP, err := createPreScorePlugins(sched)
	if err != nil {
		return nil, fmt.Errorf("create pre score plugins: %w", err)
	}
	sched.preScorePlugins = preScoreP

	scoreP, err := createScorePlugins(sched)
	if err != nil {
		return nil, fmt.Errorf("create score plugins: %w", err)
	}
	sched.scorePlugins = scoreP

	permitP, err := createPermitPlugins(sched)
	if err != nil {
		return nil, fmt.Errorf("create permit plugins: %w", err)
	}
	sched.permitPlugins = permitP

	// eventsには、各event(node add等)をkeyにして、Plugin名のsetが入っている。
	events, err := eventsToRegister(sched)
	if err != nil {
		return nil, fmt.Errorf("create gvks: %w", err)
	}

	sched.SchedulingQueue = queue.New(events)

	addAllEventHandlers(sched, informerFactory, unionedGVK(events))

	return sched, nil
}

func createFilterPlugins(h waitingpod.Handle) ([]framework.FilterPlugin, error) {
	nodeunschedulableplugin, err := createNodeUnschedulablePlugin()
	if err != nil {
		return nil, fmt.Errorf("create nodeunschedulable plugin: %w", err)
	}

	filterPlugins := []framework.FilterPlugin{
		nodeunschedulableplugin.(framework.FilterPlugin),
	}

	nodenameunschedulableplugin, err := createNodeNameUnschedulablePlugin()
	if err != nil {
		return nil, fmt.Errorf("create nodenameunschedulableplugin: %w", err)
	}

	filterPlugins = append(filterPlugins, nodenameunschedulableplugin.(framework.FilterPlugin))

	return filterPlugins, nil
}

func createPreScorePlugins(h waitingpod.Handle) ([]framework.PreScorePlugin, error) {
	nodenumberplugin, err := createNodeNumberPlugin(h)
	if err != nil {
		return nil, fmt.Errorf("create nodenumber plugin: %w", err)
	}

	preScorePlugins := []framework.PreScorePlugin{
		nodenumberplugin.(framework.PreScorePlugin),
	}

	tainttolerationplugin, err := createTaintTolerationPlugin(h)
	if err != nil {
		return nil, fmt.Errorf("create tainttoleration plugin: %w", err)
	}
	preScorePlugins = append(preScorePlugins, tainttolerationplugin.(framework.PreScorePlugin))

	return preScorePlugins, nil
}

func createScorePlugins(h waitingpod.Handle) ([]framework.ScorePlugin, error) {
	nodenumberplugin, err := createNodeNumberPlugin(h)
	if err != nil {
		return nil, fmt.Errorf("create nodenumber plugin: %w", err)
	}

	scorePlugins := []framework.ScorePlugin{
		nodenumberplugin.(framework.ScorePlugin),
	}

	tainttolerationplugin, err := createTaintTolerationPlugin(h)
	if err != nil {
		return nil, fmt.Errorf("create tainttoleration plugin: %w", err)
	}
	scorePlugins = append(scorePlugins, tainttolerationplugin.(framework.ScorePlugin))

	return scorePlugins, nil
}

func createPermitPlugins(h waitingpod.Handle) ([]framework.PermitPlugin, error) {
	nodenumberplugin, err := createNodeNumberPlugin(h)
	if err != nil {
		return nil, fmt.Errorf("create nodenumber plugin: %w", err)
	}

	permitPlugins := []framework.PermitPlugin{
		nodenumberplugin.(framework.PermitPlugin),
	}
	return permitPlugins, nil
}

func eventsToRegister(h waitingpod.Handle) (map[framework.ClusterEvent]sets.String, error) {
	nUnschedulablePlugin, err := createNodeUnschedulablePlugin()
	if err != nil {
		return nil, fmt.Errorf("create node unschedulable plugin: %w", err)
	}
	nNumberPlugin, err := createNodeNumberPlugin(h)
	if err != nil {
		return nil, fmt.Errorf("create node number plugin: %w", err)
	}
	nNodeNameUnschedulablePlugin, err := createNodeNameUnschedulablePlugin()
	if err != nil {
		return nil, fmt.Errorf("create node name unschedulable plugin: %w", err)
	}

	clusterEventMap := make(map[framework.ClusterEvent]sets.String)

	// 各pluginに対応するeventを取得して、それらをclusterEventMapに登録する。その繰り返し

	// node Add / node taint update
	nUnschedulablePluginEvents := nUnschedulablePlugin.(framework.EnqueueExtensions).EventsToRegister()
	registerClusterEvents(nUnschedulablePlugin.Name(), clusterEventMap, nUnschedulablePluginEvents)

	// node Add
	nNumberPluginEvents := nNumberPlugin.(framework.EnqueueExtensions).EventsToRegister()
	registerClusterEvents(nNumberPlugin.Name(), clusterEventMap, nNumberPluginEvents)

	// node Add
	nNodeNameUnschedulablePluginEvents := nNodeNameUnschedulablePlugin.(framework.EnqueueExtensions).EventsToRegister()
	registerClusterEvents(nNodeNameUnschedulablePlugin.Name(), clusterEventMap, nNodeNameUnschedulablePluginEvents)

	return clusterEventMap, nil
}

func registerClusterEvents(name string, eventToPlugins map[framework.ClusterEvent]sets.String, evts []framework.ClusterEvent) {
	// 各event(node add等)をkeyにして、Plugin名のsetが作られる。eventが発生したら、どのpluginを再スケジュールするかを判断する。
	for _, evt := range evts {
		if eventToPlugins[evt] == nil {
			eventToPlugins[evt] = sets.NewString(name)
		} else {
			eventToPlugins[evt].Insert(name)
		}
	}
}

func unionedGVK(m map[framework.ClusterEvent]sets.String) map[framework.GVK]framework.ActionType {
	gvkMap := make(map[framework.GVK]framework.ActionType)
	for evt := range m {
		if _, ok := gvkMap[evt.Resource]; ok {
			gvkMap[evt.Resource] |= evt.ActionType
		} else {
			gvkMap[evt.Resource] = evt.ActionType
		}
	}
	return gvkMap
}

var (
	nodeunschedulableplugin     framework.Plugin
	nodenumberplugin            framework.Plugin
	nodenameunschedulableplugin framework.Plugin
	tainttolerationplugin       framework.Plugin
)

func createNodeUnschedulablePlugin() (framework.Plugin, error) {
	if nodeunschedulableplugin != nil {
		return nodeunschedulableplugin, nil
	}

	p, err := nodeunschedulable.New(nil, nil)
	nodeunschedulableplugin = p
	return p, err
}

func createNodeNameUnschedulablePlugin() (framework.Plugin, error) {
	if nodenameunschedulableplugin != nil {
		return nodenameunschedulableplugin, nil
	}
	p, err := nodenameunschedulable.New(nil, nil)
	nodenameunschedulableplugin = p
	return p, err
}

func createTaintTolerationPlugin(h waitingpod.Handle) (framework.Plugin, error) {
	if tainttolerationplugin != nil {
		return tainttolerationplugin, nil
	}

	p, err := tainttoleration.New(nil, h)
	tainttolerationplugin = p
	return p, err
}

func createNodeNumberPlugin(h waitingpod.Handle) (framework.Plugin, error) {
	if nodenumberplugin != nil {
		return nodenumberplugin, nil
	}

	p, err := nodenumber.New(nil, h)
	nodenumberplugin = p
	return p, err
}
