# mini-kube-scheduler: Scheduler for learning Kubernetes Scheduler

**This scheduler is not for production**

[日本語版はこちら(Japanese ver)](/README.ja.md)

Hello world. 

This is mini-kube-scheduler -- the scheduler for learning Kubernetes Scheduler.

And this repository also has scenario system. You can write scenario like this and check the scheduler's behaviours.

```go
func scenario(client clientset.Interface) error {
	ctx := context.Background()

	// create node0 ~ node9, but all nodes are unschedulable
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

	klog.Info("scenario: pod1 created")

	// wait to schedule
	time.Sleep(3 * time.Second)

	pod1, err := client.CoreV1().Pods("default").Get(ctx, "pod1", metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get pod: %w", err)
	}

    klog.Info("scenario: pod1 is bound to " + pod1.Spec.NodeName)
	
    return nil
}
```

## The evolution of Scheduler

The scheduler evolves step by step, branch by branch.
You can use them to learn the scheduler step by step.

TODO: I'm planning to add docs to describe what is new feature for each branchs.

1. [initial scheduler](/tree/01/init-scheduler)

This scheduler selects node for pod randomly.

2. [filter plugins](/tree/02/filter-plugin)

This scheduler selects node for pod with only filter plugin. Only unschedulable node plugin is enabled as filter plugin.

3. [score plugins](/tree/03/score-plugin)

This scheduler selects node for pod with filter and score plugin. Only nodenumber plugin(implemented as custom plugin) is enabled as score plugin.

4. [prescore plugins](/tree/04/prescore-plugins)

This scheduler supports pre-score plugins. The nodenumber plugin is improved so that it is also used as prescore plugins. 

5. [permit plugins](/tree/05/permit-plugins)

This scheduler supports permit plugins. The nodenumber plugin is improved so that it is also used as permit plugins.
And binding cycle is now goroutined(work in parallel).

6. [scheduling queue](/tree/06/scheduling-queue)

This branch implements Scheduling Queue. It also supports putting the Pod back into the Queue as unschedulable when the schedule fails.

7. [eventhandler](/tree/07/event-handler)

This branch has support for updating Queues using EventHandler. It supports re-scheduling of pods that fail to schedule.

## custom main logic of this scheduler

Most of the codes for this scheduler is placed under [/minisched](./minisched). 
You can change this scheduler to what you want.

And this scheduler is started on [here](/scheduler/scheduler.go#L50-L80)

If you want to change how to start the scheduler, you can change here.

## custom scenario

You can write scenario [here](/sched.go#L70) and check this scheduler's behaviour.

## How to start this scheduler and scenario

To run this scheduler and start scenario, you have to install Go and etcd.
You can install etcd with [kubernetes/kubernetes/hack/install-etcd.sh](https://github.com/kubernetes/kubernetes/blob/master/hack/install-etcd.sh).

And, `make start` starts your scenario.

## Note

This mini-kube-scheduler starts scheduler, etcd, api-server and pv-controller.

The whole mechanism is based on [kubernetes-sigs/kube-scheduler-simulator](https://github.com/kubernetes-sigs/kube-scheduler-simulator)

