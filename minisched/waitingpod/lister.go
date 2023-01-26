package waitingpod

type SharedLister interface {
	NodeInfos() NodeInfoLister
	StorageInfos() StorageInfoLister
}

type StorageInfoLister interface {
	IsPVCUsedByPods(key string) bool
}

type NodeInfoLister interface {
	List() ([]*NodeInfo, error)
	HavePodsWithAffinityList() ([]*NodeInfo, error)
	HavePodsWithRequiredAntiAffinityList() ([]*NodeInfo, error)
	Get(nodeName string) (*NodeInfo, error)
}
