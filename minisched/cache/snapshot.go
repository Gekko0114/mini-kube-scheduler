package cache

import (
	"fmt"

	"github.com/Gekko0114/mini-kube-scheduler/minisched/waitingpod"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
)

type Snapshot struct {
	nodeInfoMap                                  map[string]*waitingpod.NodeInfo
	nodeInfoList                                 []*waitingpod.NodeInfo
	havePodsWithAffinityNodeInfoList             []*waitingpod.NodeInfo
	havePodsWithRequiredAntiAffinityNodeInfoList []*waitingpod.NodeInfo
	usedPVCSet                                   sets.String
	generation                                   int64
}

var _ waitingpod.SharedLister = &Snapshot{}

func NewEmptySnapshot() *Snapshot {
	return &Snapshot{
		nodeInfoMap: make(map[string]*waitingpod.NodeInfo),
		usedPVCSet:  sets.NewString(),
	}
}

func NewSnapshot(pods []*v1.Pod, nodes []*v1.Node) *Snapshot {
	nodeInfoMap := createNodeInfoMap(pods, nodes)
	nodeInfoList := make([]*waitingpod.NodeInfo, 0, len(nodeInfoMap))
	havePodsWithAffinityNodeInfoList := make([]*waitingpod.NodeInfo, 0, len(nodeInfoMap))
	havePodsWithRequiredAntiAffinityNodeInfoList := make([]*waitingpod.NodeInfo, 0, len(nodeInfoMap))
	for _, v := range nodeInfoMap {
		nodeInfoList = append(nodeInfoList, v)
		if len(v.PodsWithAffinity) > 0 {
			havePodsWithAffinityNodeInfoList = append(havePodsWithAffinityNodeInfoList, v)
		}
		if len(v.PodsWithRequiredAntiAffinity) > 0 {
			havePodsWithRequiredAntiAffinityNodeInfoList = append(havePodsWithRequiredAntiAffinityNodeInfoList, v)
		}
	}
	s := NewEmptySnapshot()
	s.nodeInfoMap = nodeInfoMap
	s.nodeInfoList = nodeInfoList
	s.havePodsWithAffinityNodeInfoList = havePodsWithAffinityNodeInfoList
	s.havePodsWithRequiredAntiAffinityNodeInfoList = s.havePodsWithRequiredAntiAffinityNodeInfoList
	s.usedPVCSet = createUsedPVCSet(pods)

	return s
}

func createNodeInfoMap(pods []*v1.Pod, nodes []*v1.Node) map[string]*waitingpod.NodeInfo {
	nodeNameToInfo := make(map[string]*waitingpod.NodeInfo)
	for _, pod := range pods {
		nodeName := pod.Spec.NodeName
		if _, ok := nodeNameToInfo[nodeName]; !ok {
			nodeNameToInfo[nodeName] = waitingpod.NewNodeInfo()
		}
		nodeNameToInfo[nodeName].AddPod(pod)
	}

	for _, node := range nodes {
		if _, ok := nodeNameToInfo[node.Name]; !ok {
			nodeNameToInfo[node.Name] = waitingpod.NewNodeInfo()
		}
		nodeInfo := nodeNameToInfo[node.Name]
		nodeInfo.SetNode(node)
	}
	return nodeNameToInfo
}

func createUsedPVCSet(pods []*v1.Pod) sets.String {
	usedPVCSet := sets.NewString()
	for _, pod := range pods {
		if pod.Spec.NodeName == "" {
			continue
		}

		for _, v := range pod.Spec.Volumes {
			if v.PersistentVolumeClaim == nil {
				continue
			}

			key := GetNamespacedName(pod.Namespace, v.PersistentVolumeClaim.ClaimName)
			usedPVCSet.Insert(key)
		}
	}
	return usedPVCSet
}

func GetNamespacedName(namespace, name string) string {
	return fmt.Sprintf("%s/%s", namespace, name)
}

func (s *Snapshot) NodeInfos() waitingpod.NodeInfoLister {
	return s
}

func (s *Snapshot) StorageInfos() waitingpod.StorageInfoLister {
	return s
}

func (s *Snapshot) NumNodes() int {
	return len(s.nodeInfoList)
}

func (s *Snapshot) List() ([]*waitingpod.NodeInfo, error) {
	return s.nodeInfoList, nil
}

func (s *Snapshot) HavePodsWithAffinityList() ([]*waitingpod.NodeInfo, error) {
	return s.havePodsWithAffinityNodeInfoList, nil
}

func (s *Snapshot) HavePodsWithRequiredAntiAffinityList() ([]*waitingpod.NodeInfo, error) {
	return s.havePodsWithRequiredAntiAffinityNodeInfoList, nil
}

func (s *Snapshot) Get(nodeName string) (*waitingpod.NodeInfo, error) {
	if v, ok := s.nodeInfoMap[nodeName]; ok && v.Node() != nil {
		return v, nil
	}
	return nil, fmt.Errorf("nodeinfo not found for node name %q", nodeName)
}

func (s *Snapshot) IsPVCUsedByPods(key string) bool {
	return s.usedPVCSet.Has(key)
}
