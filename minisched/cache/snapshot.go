package cache

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

type Snapshot struct {
	nodeInfoMap                                  map[string]*framework.NodeInfo
	nodeInfoList                                 []*framework.NodeInfo
	havePodsWithAffinityNodeInfoList             []*framework.NodeInfo
	havePodsWithRequiredAntiAffinityNodeInfoList []*framework.NodeInfo
	usedPVCSet                                   sets.String
	generation                                   int64
}

var _ framework.SharedLister = &Snapshot{}

func NewEmptySnapshot() *Snapshot {
	return &Snapshot{
		nodeInfoMap: make(map[string]*framework.NodeInfo),
		usedPVCSet:  sets.NewString(),
	}
}

func NewSnapshot(pods []*v1.Pod, nodes []*v1.Node) *Snapshot {
	nodeInfoMap := createNodeInfoMap(pods, nodes)
	nodeInfoList := make([]*framework.NodeInfo, 0, len(nodeInfoMap))
	havePodsWithAffinityNodeInfoList := make([]*framework.NodeInfo, 0, len(nodeInfoMap))
	havePodsWithRequiredAntiAffinityNodeInfoList := make([]*framework.NodeInfo, 0, len(nodeInfoMap))
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

func createNodeInfoMap(pods []*v1.Pod, nodes []*v1.Node) map[string]*framework.NodeInfo {
	nodeNameToInfo := make(map[string]*framework.NodeInfo)
	for _, pod := range pods {
		nodeName := pod.Spec.NodeName
		if _, ok := nodeNameToInfo[nodeName]; !ok {
			nodeNameToInfo[nodeName] = framework.NewNodeInfo()
		}
		nodeNameToInfo[nodeName].AddPod(pod)
	}
	imageExistenceMap := createImageExistenceMap(nodes)

	for _, node := range nodes {
		if _, ok := nodeNameToInfo[node.Name]; !ok {
			nodeNameToInfo[node.Name] = framework.NewNodeInfo()
		}
		nodeInfo := nodeNameToInfo[node.Name]
		nodeInfo.SetNode(node)
		nodeInfo.ImageStates = getNodeImageStates(node, imageExistenceMap)
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

			key := framework.GetNamespacedName(pod.Namespace, v.PersistentVolumeClaim.ClaimName)
			usedPVCSet.Insert(key)
		}
	}
	return usedPVCSet
}

func getNodeImageStates(node *v1.Node, imageExistenceMap map[string]sets.String) map[string]*framework.ImageStateSummary {
	imageStates := make(map[string]*framework.ImageStateSummary)

	for _, image := range node.Status.Images {
		for _, name := range image.Names {
			imageStates[name] = &framework.ImageStateSummary{
				Size:     image.SizeBytes,
				NumNodes: len(imageExistenceMap[name]),
			}
		}
	}
	return imageStates
}

func createImageExistenceMap(nodes []*v1.Node) map[string]sets.String {
	imageExistenceMap := make(map[string]sets.String)
	for _, node := range nodes {
		for _, image := range node.Status.Images {
			for _, name := range image.Names {
				if _, ok := imageExistenceMap[name]; !ok {
					imageExistenceMap[name] = sets.NewString(node.Name)
				} else {
					imageExistenceMap[name].Insert(node.Name)
				}
			}
		}
	}
	return imageExistenceMap
}

func (s *Snapshot) NodeInfos() framework.NodeInfoLister {
	return s
}

func (s *Snapshot) StorageInfos() framework.StorageInfoLister {
	return s
}

func (s *Snapshot) NumNodes() int {
	return len(s.nodeInfoList)
}

func (s *Snapshot) List() ([]*framework.NodeInfo, error) {
	return s.nodeInfoList, nil
}

func (s *Snapshot) HavePodsWithAffinityList() ([]*framework.NodeInfo, error) {
	return s.havePodsWithAffinityNodeInfoList, nil
}

func (s *Snapshot) HavePodsWithRequiredAntiAffinityList() ([]*framework.NodeInfo, error) {
	return s.havePodsWithRequiredAntiAffinityNodeInfoList, nil
}

func (s *Snapshot) Get(nodeName string) (*framework.NodeInfo, error) {
	if v, ok := s.nodeInfoMap[nodeName]; ok && v.Node() != nil {
		return v, nil
	}
	return nil, fmt.Errorf("nodeinfo not found for node name %q", nodeName)
}

func (s *Snapshot) IsPVCUsedByPods(key string) bool {
	return s.usedPVCSet.Has(key)
}
