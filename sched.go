package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/Gekko0114/mini-kube-scheduler/scheduler"
	"golang.org/x/xerrors"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"github.com/Gekko0114/mini-kube-scheduler/config"
	"github.com/Gekko0114/mini-kube-scheduler/k8sapiserver"
	"github.com/Gekko0114/mini-kube-scheduler/pvcontroller"
	"github.com/Gekko0114/mini-kube-scheduler/scheduler/defaultconfig"
)

// entry point.
func main() {
	if err := start(); err != nil {
		klog.Fatalf("failed with error on running scheduler: %+v", err)
	}
}

// start starts scheduler and needed k8s components.
func start() error {
	cfg, err := config.NewConfig()
	if err != nil {
		return xerrors.Errorf("get config: %w", err)
	}

	restclientCfg, apiShutdown, err := k8sapiserver.StartAPIServer(cfg.EtcdURL)
	if err != nil {
		return xerrors.Errorf("start API server: %w", err)
	}
	defer apiShutdown()

	client := clientset.NewForConfigOrDie(restclientCfg)

	pvshutdown, err := pvcontroller.StartPersistentVolumeController(client)
	if err != nil {
		return xerrors.Errorf("start pv controller: %w", err)
	}
	defer pvshutdown()

	sched := scheduler.NewSchedulerService(client, restclientCfg)

	sc, err := defaultconfig.DefaultSchedulerConfig()
	if err != nil {
		return xerrors.Errorf("create scheduler config")
	}

	if err := sched.StartScheduler(sc); err != nil {
		return xerrors.Errorf("start scheduler: %w", err)
	}
	defer sched.ShutdownScheduler()

	err = scenario(client)
	if err != nil {
		return xerrors.Errorf("start scenario: %w", err)
	}

	return nil
}

func scenario(client clientset.Interface) error {
	ctx := context.Background()

	// create node0 ~ node9, all nodes are unschedulable
	for i := 0; i < 9; i++ {
		suffix := strconv.Itoa(i)
		_, err := client.CoreV1().Nodes().Create(ctx, &v1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: "node" + suffix,
			},
			Spec: v1.NodeSpec{
				Unschedulable: true,
			},
		}, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("create node: %w", err)
		}
	}

	klog.Info("scenario: all nodes created")

	_, err := client.CoreV1().Pods("default").Create(ctx, &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pod1"},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:  "container1",
					Image: "k8s.gcr.io/pause:3.5",
				},
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create pod: %w", err)
	}
	klog.Info("schenario: pod1 created")

	// wait to schedule
	time.Sleep(3 * time.Second)
	err = checkPodBound(client, ctx, "pod1")
	if err != nil {
		return err
	}

	_, err = client.CoreV1().Nodes().Update(ctx, &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node5",
		},
		Spec: v1.NodeSpec{
			Unschedulable: false,
		},
	}, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("update node: %w", err)
	}
	klog.Info("schenario: node5 updated")

	// wait to schedule
	time.Sleep(10 * time.Second)
	err = checkPodBound(client, ctx, "pod1")
	if err != nil {
		return err
	}
	return nil
}

func checkPodBound(client clientset.Interface, ctx context.Context, podName string) error {
	pod, err := client.CoreV1().Pods("default").Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get pod: %w", err)
	}
	if len(pod.Spec.NodeName) != 0 {
		klog.Info(podName + " is bound to " + pod.Spec.NodeName)
	} else {
		klog.Info(podName + " has not been bound yet")
	}
	return nil
}
