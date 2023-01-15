package minisched

import (
	"fmt"

	"github.com/sanposhiho/mini-kube-scheduler/minisched/plugins/score/nodenumber"
	"github.com/sanposhiho/mini-kube-scheduler/minisched/queue"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodeunschedulable"
)

type Scheduler struct {
	SchedulingQueue *queue.SchedulingQueue

	client clientset.Interface

	filterPlugins   []framework.FilterPlugin
	preScorePlugins []framework.PreScorePlugin
	scorePlugins    []framework.ScorePlugin
}

func New(
	client clientset.Interface,
	informerFactory informers.SharedInformerFactory,
) (*Scheduler, error) {
	sched := &Scheduler{
		SchedulingQueue: queue.New(),
		client:          client,
	}

	filterP, err := createFilterPlugins()
	if err != nil {
		return nil, fmt.Errorf("create filter plugins: %w", err)
	}
	sched.filterPlugins = filterP

	preScoreP, err := createPreScorePlugins()
	if err != nil {
		return nil, fmt.Errorf("create pre score plugins: %w", err)
	}
	sched.preScorePlugins = preScoreP

	scoreP, err := createScorePlugins()
	if err != nil {
		return nil, fmt.Errorf("create score plugins: %w", err)
	}
	sched.scorePlugins = scoreP

	addAllEventHandlers(sched, informerFactory)

	return sched, nil
}

func createFilterPlugins() ([]framework.FilterPlugin, error) {
	nodeunschedulableplugin, err := createNodeUnschedulablePlugin()
	if err != nil {
		return nil, fmt.Errorf("create nodeunschedulable plugin: %w", err)
	}

	filterPlugins := []framework.FilterPlugin{
		nodeunschedulableplugin.(framework.FilterPlugin),
	}

	return filterPlugins, nil
}

func createPreScorePlugins() ([]framework.PreScorePlugin, error) {
	nodenumberplugin, err := createNodeNumberPlugin()
	if err != nil {
		return nil, fmt.Errorf("create nodenumber plugin: %w", err)
	}

	preScorePlugins := []framework.PreScorePlugin{
		nodenumberplugin.(framework.PreScorePlugin),
	}

	return preScorePlugins, nil
}

func createScorePlugins() ([]framework.ScorePlugin, error) {
	nodenumberplugin, err := createNodeNumberPlugin()
	if err != nil {
		return nil, fmt.Errorf("create nodenumber plugin: %w", err)
	}

	scorePlugins := []framework.ScorePlugin{
		nodenumberplugin.(framework.ScorePlugin),
	}

	return scorePlugins, nil
}

var (
	nodeunschedulableplugin framework.Plugin
	nodenumberplugin        framework.Plugin
)

func createNodeUnschedulablePlugin() (framework.Plugin, error) {
	if nodeunschedulableplugin != nil {
		return nodeunschedulableplugin, nil
	}

	p, err := nodeunschedulable.New(nil, nil)
	nodeunschedulableplugin = p
	return p, err
}

func createNodeNumberPlugin() (framework.Plugin, error) {
	if nodenumberplugin != nil {
		return nodenumberplugin, nil
	}

	p, err := nodenumber.New(nil, nil)
	nodenumberplugin = p
	return p, err
}
