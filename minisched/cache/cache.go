package cache

import (
	"fmt"
	"sync"
	"time"

	"github.com/Gekko0114/mini-kube-scheduler/minisched/waitingpod"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
)

var (
	cleanAssumedPeriod = 1 * time.Second
)

func New(ttl time.Duration, stop <-chan struct{}) Cache {
	cache := newCache(ttl, cleanAssumedPeriod, stop)
	cache.run()
	return cache
}

type nodeInfoListItem struct {
	info *waitingpod.NodeInfo
	next *nodeInfoListItem
	prev *nodeInfoListItem
}

type cacheImpl struct {
	stop        <-chan struct{}
	ttl         time.Duration
	period      time.Duration
	mu          sync.RWMutex
	assumedPods sets.String
	podStates   map[string]*podState
	nodes       map[string]*nodeInfoListItem
	headNode    *nodeInfoListItem
	nodeTree    *nodeTree
}

type podState struct {
	pod             *v1.Pod
	deadline        *time.Time
	bindingFinished bool
}

func newCache(ttl, period time.Duration, stop <-chan struct{}) *cacheImpl {
	return &cacheImpl{
		ttl:         ttl,
		period:      period,
		stop:        stop,
		nodes:       make(map[string]*nodeInfoListItem),
		nodeTree:    newNodeTree(nil),
		assumedPods: make(sets.String),
		podStates:   make(map[string]*podState),
	}
}

func newNodeInfoListItem(ni *waitingpod.NodeInfo) *nodeInfoListItem {
	return &nodeInfoListItem{
		info: ni,
	}
}

func (cache *cacheImpl) moveNodeInfoToHead(name string) {
	ni, ok := cache.nodes[name]
	if !ok {
		klog.ErrorS(nil, "No node info with given name found in the cache", "node", klog.KRef("", name))
		return
	}

	if ni == cache.headNode {
		return
	}

	if ni.prev != nil {
		ni.prev.next = ni.next
	}
	if ni.next != nil {
		ni.next.prev = ni.prev
	}
	if cache.headNode != nil {
		cache.headNode.prev = nil
	}
	ni.next = cache.headNode
	ni.prev = nil
	cache.headNode = ni
}

func (cache *cacheImpl) removeNodeInfoFromList(name string) {
	ni, ok := cache.nodes[name]
	if !ok {
		klog.ErrorS(nil, "No node info with given name found in the cache", "node", klog.KRef("", name))
		return
	}

	if ni.prev != nil {
		ni.prev.next = ni.next
	}
	if ni.next != nil {
		ni.next.prev = ni.prev
	}
	if ni == cache.headNode {
		cache.headNode = ni.next
	}
	delete(cache.nodes, name)
}

func (cache *cacheImpl) Dump() *Dump {
	cache.mu.RLock()
	defer cache.mu.RUnlock()

	nodes := make(map[string]*waitingpod.NodeInfo, len(cache.nodes))
	for k, v := range cache.nodes {
		nodes[k] = v.info.Clone()
	}

	return &Dump{
		Nodes:       nodes,
		AssumedPods: cache.assumedPods.Union(nil),
	}
}

func (cache *cacheImpl) UpdateSnapshot(nodeSnapshot *Snapshot) error {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	snapshotGeneration := nodeSnapshot.generation
	updateAllLists := false
	updateNodesHavePodsWithAffinity := false
	udpateNodesHavePodsWithRequiredAntiAffinity := false
	updateUsedPVCSet := false
	//	klog.Info("updatesnapshot running")

	for node := cache.headNode; node != nil; node = node.next {
		//		klog.Info("updatesnapshot running.")
		if node.info.Generation <= snapshotGeneration {
			break
		}
		if np := node.info.Node(); np != nil {
			//			klog.Info("updatesnapshot: " + np.Name)
			existing, ok := nodeSnapshot.nodeInfoMap[np.Name]
			if !ok {
				updateAllLists = true
				existing = &waitingpod.NodeInfo{}
				nodeSnapshot.nodeInfoMap[np.Name] = existing
			}
			clone := node.info.Clone()

			if (len(existing.PodsWithAffinity) > 0) != (len(clone.PodsWithAffinity) > 0) {
				updateNodesHavePodsWithAffinity = true
			}
			if (len(existing.PodsWithRequiredAntiAffinity) > 0) != (len(clone.PodsWithRequiredAntiAffinity) > 0) {
				udpateNodesHavePodsWithRequiredAntiAffinity = true
			}
			if !updateUsedPVCSet {
				if len(existing.PVCRefCounts) != len(clone.PVCRefCounts) {
					updateUsedPVCSet = true
				} else {
					for pvcKey := range clone.PVCRefCounts {
						if _, found := existing.PVCRefCounts[pvcKey]; !found {
							updateUsedPVCSet = true
							break
						}
					}
				}
			}
			*existing = *clone
		}
	}

	if cache.headNode != nil {
		nodeSnapshot.generation = cache.headNode.info.Generation
	}

	if len(nodeSnapshot.nodeInfoMap) > cache.nodeTree.numNodes {
		cache.removeDeletedNodesFromSnapshot(nodeSnapshot)
		updateAllLists = true
	}

	if updateAllLists || updateNodesHavePodsWithAffinity || udpateNodesHavePodsWithRequiredAntiAffinity || updateUsedPVCSet {
		cache.updateNodeInfoSnapshotList(nodeSnapshot, updateAllLists)
	}

	if len(nodeSnapshot.nodeInfoList) != cache.nodeTree.numNodes {
		errMsg := fmt.Sprintf("snapshot state is not consistent, length of NodeInfoList=%v not equal to length of nodes in tree=%v "+
			", length of NodeInfoMap=%v, length of nodes in cache=%v"+
			", trying to recover",
			len(nodeSnapshot.nodeInfoList), cache.nodeTree.numNodes,
			len(nodeSnapshot.nodeInfoMap), len(cache.nodes))
		klog.ErrorS(nil, errMsg)
		cache.updateNodeInfoSnapshotList(nodeSnapshot, true)
		return fmt.Errorf(errMsg)
	}
	return nil
}

func (cache *cacheImpl) updateNodeInfoSnapshotList(snapshot *Snapshot, updateAll bool) {
	snapshot.havePodsWithAffinityNodeInfoList = make([]*waitingpod.NodeInfo, 0, cache.nodeTree.numNodes)
	snapshot.havePodsWithRequiredAntiAffinityNodeInfoList = make([]*waitingpod.NodeInfo, 0, cache.nodeTree.numNodes)
	snapshot.usedPVCSet = sets.NewString()
	if updateAll {
		snapshot.nodeInfoList = make([]*waitingpod.NodeInfo, 0, cache.nodeTree.numNodes)
		nodesList, err := cache.nodeTree.list()
		if err != nil {
			klog.ErrorS(err, "Error occurred while retrieving the list of names of the nodes from node tree")
		}
		for _, nodeName := range nodesList {
			if nodeInfo := snapshot.nodeInfoMap[nodeName]; nodeInfo != nil {
				snapshot.nodeInfoList = append(snapshot.nodeInfoList, nodeInfo)
				if len(nodeInfo.PodsWithAffinity) > 0 {
					snapshot.havePodsWithAffinityNodeInfoList = append(snapshot.havePodsWithAffinityNodeInfoList, nodeInfo)
				}
				if len(nodeInfo.PodsWithRequiredAntiAffinity) > 0 {
					snapshot.havePodsWithRequiredAntiAffinityNodeInfoList = append(snapshot.havePodsWithRequiredAntiAffinityNodeInfoList, nodeInfo)
				}
				for key := range nodeInfo.PVCRefCounts {
					snapshot.usedPVCSet.Insert(key)
				}
			} else {
				klog.ErrorS(nil, "Node exists in nodeTree but not in NodeInfoMap, this should not happen", "node", klog.KRef("", nodeName))
			}
		}
	} else {
		for _, nodeInfo := range snapshot.nodeInfoList {
			if len(nodeInfo.PodsWithAffinity) > 0 {
				snapshot.havePodsWithAffinityNodeInfoList = append(snapshot.havePodsWithAffinityNodeInfoList, nodeInfo)
			}
			if len(nodeInfo.PodsWithRequiredAntiAffinity) > 0 {
				snapshot.havePodsWithRequiredAntiAffinityNodeInfoList = append(snapshot.havePodsWithRequiredAntiAffinityNodeInfoList, nodeInfo)
			}
			for key := range nodeInfo.PVCRefCounts {
				snapshot.usedPVCSet.Insert(key)
			}
		}
	}
}

func (cache *cacheImpl) removeDeletedNodesFromSnapshot(snapshot *Snapshot) {
	toDelete := len(snapshot.nodeInfoMap) - cache.nodeTree.numNodes
	for name := range snapshot.nodeInfoMap {
		if toDelete <= 0 {
			break
		}
		if n, ok := cache.nodes[name]; !ok || n.info.Node() == nil {
			delete(snapshot.nodeInfoMap, name)
			toDelete--
		}
	}
}

func (cache *cacheImpl) NodeCount() int {
	cache.mu.RLock()
	defer cache.mu.RUnlock()
	return len(cache.nodes)
}

func (cache *cacheImpl) PodCount() (int, error) {
	cache.mu.RLock()
	defer cache.mu.RUnlock()

	count := 0
	for _, n := range cache.nodes {
		count += len(n.info.Pods)
	}
	return count, nil
}

func (cache *cacheImpl) AssumePod(pod *v1.Pod) error {
	key, err := waitingpod.GetPodKey(pod)
	if err != nil {
		return err
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if _, ok := cache.podStates[key]; ok {
		return fmt.Errorf("pod %v(%v) is in the cache, so can't be assumed", key, klog.KObj(pod))
	}
	return cache.addPod(pod, true)
}

func (cache *cacheImpl) FinishBinding(pod *v1.Pod) error {
	return cache.finishBinding(pod, time.Now())
}

func (cache *cacheImpl) finishBinding(pod *v1.Pod, now time.Time) error {
	key, err := waitingpod.GetPodKey(pod)
	if err != nil {
		return err
	}

	cache.mu.RLock()
	defer cache.mu.RUnlock()
	klog.V(5).InfoS("Finished binding for pod, can be expired", "podKey", key, "pod", klog.KObj(pod))
	currState, ok := cache.podStates[key]
	if ok && cache.assumedPods.Has(key) {
		if cache.ttl == time.Duration(0) {
			currState.deadline = nil
		} else {
			dl := now.Add(cache.ttl)
			currState.deadline = &dl
		}
		currState.bindingFinished = true
	}
	return nil
}

func (cache *cacheImpl) ForgetPod(pod *v1.Pod) error {
	key, err := waitingpod.GetPodKey(pod)
	if err != nil {
		return err
	}

	cache.mu.Lock()
	defer cache.mu.Unlock()

	currState, ok := cache.podStates[key]
	if ok && currState.pod.Spec.NodeName != pod.Spec.NodeName {
		return fmt.Errorf("pod %v(%v) was assumed on %v but assigned to %v", key, klog.KObj(pod), pod.Spec.NodeName, currState.pod.Spec.NodeName)
	}

	if ok && cache.assumedPods.Has(key) {
		return cache.removePod(pod)
	}
	return fmt.Errorf("pod %v(%v) wasn't assumed so cannot be forgotten", key, klog.KObj(pod))
}

func (cache *cacheImpl) addPod(pod *v1.Pod, assumePod bool) error {
	key, err := waitingpod.GetPodKey(pod)
	if err != nil {
		return err
	}
	n, ok := cache.nodes[pod.Spec.NodeName]
	if !ok {
		n = newNodeInfoListItem(waitingpod.NewNodeInfo())
		cache.nodes[pod.Spec.NodeName] = n
	}
	n.info.AddPod(pod)
	cache.moveNodeInfoToHead(pod.Spec.NodeName)
	ps := &podState{
		pod: pod,
	}
	cache.podStates[key] = ps
	if assumePod {
		cache.assumedPods.Insert(key)
	}
	return nil
}

func (cache *cacheImpl) updatePod(oldPod, newPod *v1.Pod) error {
	if err := cache.removePod(oldPod); err != nil {
		return err
	}
	return cache.addPod(newPod, false)
}

func (cache *cacheImpl) removePod(pod *v1.Pod) error {
	key, err := waitingpod.GetPodKey(pod)
	if err != nil {
		return err
	}
	n, ok := cache.nodes[pod.Spec.NodeName]
	if !ok {
		klog.ErrorS(nil, "Node not found when trying to remove pod", "node", klog.KRef("", pod.Spec.NodeName), "podKey", key, "pod", klog.KObj(pod))
	} else {
		if err := n.info.RemovePod(pod); err != nil {
			return err
		}
		if len(n.info.Pods) == 0 && n.info.Node() == nil {
			cache.removeNodeInfoFromList(pod.Spec.NodeName)
		} else {
			cache.moveNodeInfoToHead(pod.Spec.NodeName)
		}
	}
	delete(cache.podStates, key)
	delete(cache.assumedPods, key)
	return nil
}

func (cache *cacheImpl) AddPod(pod *v1.Pod) error {
	key, err := waitingpod.GetPodKey(pod)
	if err != nil {
		return err
	}

	cache.mu.Lock()
	defer cache.mu.Unlock()

	currState, ok := cache.podStates[key]
	switch {
	case ok && cache.assumedPods.Has(key):
		if err = cache.updatePod(currState.pod, pod); err != nil {
			klog.ErrorS(err, "Error occurred while updating pod")
		}
		if currState.pod.Spec.NodeName != pod.Spec.NodeName {
			klog.InfoS("Pod was added to a different node than it was assumed", "podKey", key, "pod", klog.KObj(pod), "assumedNode", klog.KRef("", pod.Spec.NodeName), "currentNode", klog.KRef("", currState.pod.Spec.NodeName))
			return nil
		}
	case !ok:
		if err = cache.addPod(pod, false); err != nil {
			klog.ErrorS(err, "Error occurred while adding pod")
		}
	default:
		return fmt.Errorf("pod %v(%v) was already in added state", key, klog.KObj(pod))
	}
	return nil
}

func (cache *cacheImpl) UpdatePod(oldPod, newPod *v1.Pod) error {
	key, err := waitingpod.GetPodKey(oldPod)
	if err != nil {
		return err
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()

	currState, ok := cache.podStates[key]
	if !ok {
		return fmt.Errorf("pod %v(%v) is not added to scheduler cache, so cannot be updated", key, klog.KObj(oldPod))
	}

	if cache.assumedPods.Has(key) {
		return fmt.Errorf("assumed pod %v(%v) should not be updated", key, klog.KObj(oldPod))
	}

	if currState.pod.Spec.NodeName != newPod.Spec.NodeName {
		klog.ErrorS(nil, "Pod updated on a different node than previously added to", "podKey", key, "pod", klog.KObj(oldPod))
		klog.ErrorS(nil, "scheduler cache is corrupted and can badly affect scheduling decisions")
		//		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}
	return cache.updatePod(oldPod, newPod)
}

func (cache *cacheImpl) RemovePod(pod *v1.Pod) error {
	key, err := waitingpod.GetPodKey(pod)
	if err != nil {
		return err
	}

	cache.mu.Lock()
	defer cache.mu.Unlock()

	currState, ok := cache.podStates[key]
	if !ok {
		return fmt.Errorf("pod %v(%v) is not found in scheduler cache, so cannot be removed from it", key, klog.KObj(pod))
	}
	if currState.pod.Spec.NodeName != pod.Spec.NodeName {
		klog.ErrorS(nil, "Pod was added to a different node than it was assumed", "podKey", key, "pod", klog.KObj(pod), "assumedNode", klog.KRef("", pod.Spec.NodeName), "currentNode", klog.KRef("", currState.pod.Spec.NodeName))
		if pod.Spec.NodeName != "" {
			klog.ErrorS(nil, "scheduler cache is corrupted and can badly affect scheduling decisions")
			//			klog.FlushAndExit(klog.ExitFlushTimeout, 1)
		}
	}
	return cache.removePod(currState.pod)
}

func (cache *cacheImpl) IsAssumedPod(pod *v1.Pod) (bool, error) {
	key, err := waitingpod.GetPodKey(pod)
	if err != nil {
		return false, err
	}
	cache.mu.RLock()
	defer cache.mu.RUnlock()

	return cache.assumedPods.Has(key), nil
}

func (cache *cacheImpl) GetPod(pod *v1.Pod) (*v1.Pod, error) {
	key, err := waitingpod.GetPodKey(pod)
	if err != nil {
		return nil, err
	}
	cache.mu.RLock()
	defer cache.mu.RUnlock()

	podState, ok := cache.podStates[key]
	if !ok {
		return nil, fmt.Errorf("pod %v(%v) does not exist in scheduler cache", key, klog.KObj(pod))
	}
	return podState.pod, nil
}

func (cache *cacheImpl) AddNode(node *v1.Node) *waitingpod.NodeInfo {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	n, ok := cache.nodes[node.Name]
	if !ok {
		n = newNodeInfoListItem(waitingpod.NewNodeInfo())
		cache.nodes[node.Name] = n
	}
	cache.moveNodeInfoToHead(node.Name)

	cache.nodeTree.addNode(node)
	n.info.SetNode(node)
	return n.info.Clone()
}

func (cache *cacheImpl) UpdateNode(oldNode, newNode *v1.Node) *waitingpod.NodeInfo {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	n, ok := cache.nodes[newNode.Name]
	if !ok {
		n = newNodeInfoListItem(waitingpod.NewNodeInfo())
		cache.nodes[newNode.Name] = n
		cache.nodeTree.addNode(newNode)
	}

	cache.moveNodeInfoToHead(newNode.Name)
	cache.nodeTree.updateNode(oldNode, newNode)
	n.info.SetNode(newNode)
	return n.info.Clone()
}

func (cache *cacheImpl) RemoveNode(node *v1.Node) error {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	n, ok := cache.nodes[node.Name]
	if !ok {
		return fmt.Errorf("node %v is not found", node.Name)
	}
	n.info.RemoveNode()

	if len(n.info.Pods) == 0 {
		cache.removeNodeInfoFromList(node.Name)
	} else {
		cache.moveNodeInfoToHead(node.Name)
	}
	if err := cache.nodeTree.removeNode(node); err != nil {
		return err
	}
	return nil
}

func (cache *cacheImpl) run() {
	go wait.Until(cache.cleanupExpiredAssumedPods, cache.period, cache.stop)
}

func (cache *cacheImpl) cleanupExpiredAssumedPods() {
	cache.cleanupAssumedPods(time.Now())
}

func (cache *cacheImpl) cleanupAssumedPods(now time.Time) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	defer cache.updateMetrics()

	for key := range cache.assumedPods {
		ps, ok := cache.podStates[key]
		if !ok {
			klog.ErrorS(nil, "Key found in assumed set but not in podStates, potentially a logical error")
			//			klog.FlushAndExit(klog.ExitFlushTimeout, 1)
		}
		if !ps.bindingFinished {
			klog.V(5).InfoS("Could not expire cache for pod as binding is still in progress", "podKey", key, "pod", klog.KObj(ps.pod))
			continue
		}
		if cache.ttl != 0 && now.After(*ps.deadline) {
			klog.InfoS("Pod expired", "podKey", key, "pod", klog.KObj(ps.pod))
			if err := cache.removePod(ps.pod); err != nil {
				klog.ErrorS(err, "ExpirePod failed", "podKey", key, "pod", klog.KObj(ps.pod))
			}
		}
	}
}

func (cache *cacheImpl) updateMetrics() {
	metrics.CacheSize.WithLabelValues("assumed_pods").Set(float64(len(cache.assumedPods)))
	metrics.CacheSize.WithLabelValues("pods").Set(float64(len(cache.podStates)))
	metrics.CacheSize.WithLabelValues("nodes").Set(float64(len(cache.nodes)))
}
