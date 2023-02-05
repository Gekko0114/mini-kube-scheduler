package waitingpod

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
)

var generation int64

type QueuedPodInfo struct {
	*PodInfo
	Timestamp               time.Time
	Attempts                int
	InitialAttemptTimestamp time.Time
	Gated                   bool
}

func (pqi *QueuedPodInfo) DeepCopy() *QueuedPodInfo {
	return &QueuedPodInfo{
		PodInfo:                 pqi.PodInfo.DeepCopy(),
		Timestamp:               pqi.Timestamp,
		Attempts:                pqi.Attempts,
		InitialAttemptTimestamp: pqi.InitialAttemptTimestamp,
		Gated:                   pqi.Gated,
	}
}

type PodInfo struct {
	Pod                        *v1.Pod
	RequiredAffinityTerms      []AffinityTerm
	RequiredAntiAffinityTerms  []AffinityTerm
	PreferredAffinityTerms     []WeightedAffinityTerm
	PreferredAntiAffinityTerms []WeightedAffinityTerm
}

func (pi *PodInfo) DeepCopy() *PodInfo {
	return &PodInfo{
		Pod:                        pi.Pod.DeepCopy(),
		RequiredAffinityTerms:      pi.RequiredAffinityTerms,
		RequiredAntiAffinityTerms:  pi.RequiredAntiAffinityTerms,
		PreferredAffinityTerms:     pi.PreferredAffinityTerms,
		PreferredAntiAffinityTerms: pi.PreferredAntiAffinityTerms,
	}
}

func (pi *PodInfo) Update(pod *v1.Pod) error {
	if pod != nil && pi.Pod != nil && pi.Pod.UID == pod.UID {
		pi.Pod = pod
		return nil
	}
	var preferredAffinityTerms []v1.WeightedPodAffinityTerm
	var preferredAntiAffinityTerms []v1.WeightedPodAffinityTerm
	if affinity := pod.Spec.Affinity; affinity != nil {
		if a := affinity.PodAffinity; a != nil {
			preferredAffinityTerms = a.PreferredDuringSchedulingIgnoredDuringExecution
		}
		if a := affinity.PodAntiAffinity; a != nil {
			preferredAntiAffinityTerms = a.PreferredDuringSchedulingIgnoredDuringExecution
		}
	}

	var parseErrs []error
	requiredAffinityTerms, err := getAffinityTerms(pod, getPodAffinityTerms(pod.Spec.Affinity))
	if err != nil {
		parseErrs = append(parseErrs, fmt.Errorf("requiredAffinityTerms: %w", err))
	}
	requiredAntiAffinityTerms, err := getAffinityTerms(pod, getPodAntiAffinityTerms(pod.Spec.Affinity))
	if err != nil {
		parseErrs = append(parseErrs, fmt.Errorf("requiredAntiAffinityTerms: %w", err))
	}
	weightedAffinityTerms, err := getWeightedAffinityTerms(pod, preferredAffinityTerms)
	if err != nil {
		parseErrs = append(parseErrs, fmt.Errorf("preferredAffinityTerms: %w", err))
	}
	weightedAntiAffinityTerms, err := getWeightedAffinityTerms(pod, preferredAntiAffinityTerms)
	if err != nil {
		parseErrs = append(parseErrs, fmt.Errorf("preferredAntiAffinityTerms: %w", err))
	}

	pi.Pod = pod
	pi.RequiredAffinityTerms = requiredAffinityTerms
	pi.RequiredAntiAffinityTerms = requiredAntiAffinityTerms
	pi.PreferredAffinityTerms = weightedAffinityTerms
	pi.PreferredAntiAffinityTerms = weightedAntiAffinityTerms
	return utilerrors.NewAggregate(parseErrs)
}

type AffinityTerm struct {
	Namespaces        sets.String
	Selector          labels.Selector
	TopologyKey       string
	NamespaceSelector labels.Selector
}

func (at *AffinityTerm) Matches(pod *v1.Pod, nsLabels labels.Set) bool {
	if at.Namespaces.Has(pod.Namespace) || at.NamespaceSelector.Matches(nsLabels) {
		return at.Selector.Matches(labels.Set(pod.Labels))
	}
	return false
}

type WeightedAffinityTerm struct {
	AffinityTerm
	Weight int32
}

type Diagnosis struct {
	NodeToStatusMap      NodeToStatusMap
	UnschedulablePlugins sets.String
	PreFilterMsg         string
	PostFilterMsg        string
}

type FitError struct {
	Pod         *v1.Pod
	NumAllNodes int
	Diagnosis   Diagnosis
}

const (
	// NoNodeAvailableMsg is used to format message when no nodes available.
	NoNodeAvailableMsg = "0/%v nodes are available"
	// SeparatorFormat is used to separate PreFilterMsg, FilterMsg and PostFilterMsg.
	SeparatorFormat = " %v."
)

func (f *FitError) Error() string {
	reasons := make(map[string]int)
	for _, status := range f.Diagnosis.NodeToStatusMap {
		for _, reason := range status.Reasons() {
			reasons[reason]++
		}
	}

	reasonMsg := fmt.Sprintf(NoNodeAvailableMsg+":", f.NumAllNodes)
	// Add the messages from PreFilter plugins to reasonMsg.
	preFilterMsg := f.Diagnosis.PreFilterMsg
	if preFilterMsg != "" {
		reasonMsg += fmt.Sprintf(SeparatorFormat, preFilterMsg)
	}
	sortReasonsHistogram := func() []string {
		var reasonStrings []string
		for k, v := range reasons {
			reasonStrings = append(reasonStrings, fmt.Sprintf("%v %v", v, k))
		}
		sort.Strings(reasonStrings)
		return reasonStrings
	}
	sortedFilterMsg := sortReasonsHistogram()
	if len(sortedFilterMsg) != 0 {
		reasonMsg += fmt.Sprintf(SeparatorFormat, strings.Join(sortedFilterMsg, ", "))
	}
	// Add the messages from PostFilter plugins to reasonMsg.
	postFilterMsg := f.Diagnosis.PostFilterMsg
	if postFilterMsg != "" {
		reasonMsg += fmt.Sprintf(SeparatorFormat, postFilterMsg)
	}
	return reasonMsg
}

func getAffinityTerms(pod *v1.Pod, v1Terms []v1.PodAffinityTerm) ([]AffinityTerm, error) {
	if v1Terms == nil {
		return nil, nil
	}

	var terms []AffinityTerm
	for i := range v1Terms {
		t, err := newAffinityTerm(pod, &v1Terms[i])
		if err != nil {
			return nil, err
		}
		terms = append(terms, *t)
	}
	return terms, nil
}

func getWeightedAffinityTerms(pod *v1.Pod, v1Terms []v1.WeightedPodAffinityTerm) ([]WeightedAffinityTerm, error) {
	if v1Terms == nil {
		return nil, nil
	}

	var terms []WeightedAffinityTerm
	for i := range v1Terms {
		t, err := newAffinityTerm(pod, &v1Terms[i].PodAffinityTerm)
		if err != nil {
			return nil, err
		}
		terms = append(terms, WeightedAffinityTerm{AffinityTerm: *t, Weight: v1Terms[i].Weight})
	}
	return terms, nil
}

func NewPodInfo(pod *v1.Pod) (*PodInfo, error) {
	pInfo := &PodInfo{}
	err := pInfo.Update(pod)
	return pInfo, err
}

func newAffinityTerm(pod *v1.Pod, term *v1.PodAffinityTerm) (*AffinityTerm, error) {
	selector, err := metav1.LabelSelectorAsSelector(term.LabelSelector)
	if err != nil {
		return nil, err
	}

	namespaces := getNamespacesFromPodAffinityTerm(pod, term)
	nsSelector, err := metav1.LabelSelectorAsSelector(term.NamespaceSelector)
	if err != nil {
		return nil, err
	}

	return &AffinityTerm{Namespaces: namespaces, Selector: selector, TopologyKey: term.TopologyKey, NamespaceSelector: nsSelector}, nil
}

func getPodAffinityTerms(affinity *v1.Affinity) (terms []v1.PodAffinityTerm) {
	if affinity != nil && affinity.PodAffinity != nil {
		if len(affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution) != 0 {
			terms = affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution
		}
	}
	return terms
}

func getPodAntiAffinityTerms(affinity *v1.Affinity) (terms []v1.PodAffinityTerm) {
	if affinity != nil && affinity.PodAntiAffinity != nil {
		if len(affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution) != 0 {
			terms = affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution
		}
	}
	return terms
}

func getNamespacesFromPodAffinityTerm(pod *v1.Pod, podAffinityTerm *v1.PodAffinityTerm) sets.String {
	names := sets.String{}
	if len(podAffinityTerm.Namespaces) == 0 && podAffinityTerm.NamespaceSelector == nil {
		names.Insert(pod.Namespace)
	} else {
		names.Insert(podAffinityTerm.Namespaces...)
	}
	return names
}

type NodeInfo struct {
	node                         *v1.Node
	Pods                         []*PodInfo
	PodsWithAffinity             []*PodInfo
	PodsWithRequiredAntiAffinity []*PodInfo
	PVCRefCounts                 map[string]int
	Generation                   int64
}

func nextGeneration() int64 {
	return atomic.AddInt64(&generation, 1)
}

func NewNodeInfo(pods ...*v1.Pod) *NodeInfo {
	ni := &NodeInfo{
		PVCRefCounts: make(map[string]int),
		Generation:   nextGeneration(),
	}
	for _, pod := range pods {
		ni.AddPod(pod)
	}
	return ni
}

func (n *NodeInfo) Node() *v1.Node {
	if n == nil {
		return nil
	}
	return n.node
}

func (n *NodeInfo) Clone() *NodeInfo {
	clone := &NodeInfo{
		node:       n.node,
		Generation: n.Generation,
	}
	if len(n.Pods) > 0 {
		clone.Pods = append([]*PodInfo(nil), n.Pods...)
	}
	if len(n.PodsWithAffinity) > 0 {
		clone.PodsWithAffinity = append([]*PodInfo(nil), n.PodsWithAffinity...)
	}
	if len(n.PodsWithRequiredAntiAffinity) > 0 {
		clone.PodsWithRequiredAntiAffinity = append([]*PodInfo(nil), n.PodsWithRequiredAntiAffinity...)
	}
	for key, value := range n.PVCRefCounts {
		clone.PVCRefCounts[key] = value
	}
	return clone
}

func (n *NodeInfo) String() string {
	podKeys := make([]string, len(n.Pods))
	for i, p := range n.Pods {
		podKeys[i] = p.Pod.Name
	}

	return fmt.Sprintf("&NodeInfo{Pods:%v}", podKeys)
}

func (n *NodeInfo) AddPodInfo(podInfo *PodInfo) {
	n.Pods = append(n.Pods, podInfo)
	if podWithAffinity(podInfo.Pod) {
		n.PodsWithAffinity = append(n.PodsWithAffinity, podInfo)
	}
	if podWithRequiredAntiAffinity(podInfo.Pod) {
		n.PodsWithRequiredAntiAffinity = append(n.PodsWithRequiredAntiAffinity, podInfo)
	}
	n.update(podInfo.Pod, 1)
}

func (n *NodeInfo) AddPod(pod *v1.Pod) {
	podInfo, _ := NewPodInfo(pod)
	n.AddPodInfo(podInfo)
}

func podWithAffinity(p *v1.Pod) bool {
	affinity := p.Spec.Affinity
	return affinity != nil && (affinity.PodAffinity != nil || affinity.PodAntiAffinity != nil)
}

func podWithRequiredAntiAffinity(p *v1.Pod) bool {
	affinity := p.Spec.Affinity
	return affinity != nil && affinity.PodAntiAffinity != nil &&
		len(affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution) != 0
}

func removeFromSlice(s []*PodInfo, k string) []*PodInfo {
	for i := range s {
		k2, err := GetPodKey(s[i].Pod)
		if err != nil {
			klog.ErrorS(err, "Cannot get pod key", "pod", klog.KObj(s[i].Pod))
			continue
		}
		if k == k2 {
			s[i] = s[len(s)-1]
			s = s[:len(s)-1]
			break
		}
	}
	return s
}

func (n *NodeInfo) RemovePod(pod *v1.Pod) error {
	k, err := GetPodKey(pod)
	if err != nil {
		return err
	}
	if podWithAffinity(pod) {
		n.PodsWithAffinity = removeFromSlice(n.PodsWithAffinity, k)
	}
	if podWithRequiredAntiAffinity(pod) {
		n.PodsWithRequiredAntiAffinity = removeFromSlice(n.PodsWithRequiredAntiAffinity, k)
	}

	for i := range n.Pods {
		k2, err := GetPodKey(n.Pods[i].Pod)
		if err != nil {
			klog.ErrorS(err, "Cannot get pod key", "pod", klog.KObj(n.Pods[i].Pod))
			continue
		}
		if k == k2 {
			n.Pods[i] = n.Pods[len(n.Pods)-1]
			n.Pods = n.Pods[:len(n.Pods)-1]
			n.update(pod, -1)
			n.resetSlicesIfEmpty()
			return nil
		}
	}
	return fmt.Errorf("no corresponding pod %s in pods of node %s", pod.Name, n.node.Name)
}

func (n *NodeInfo) update(pod *v1.Pod, sign int64) {
	n.Generation = nextGeneration()
}

func (n *NodeInfo) resetSlicesIfEmpty() {
	if len(n.PodsWithAffinity) == 0 {
		n.PodsWithAffinity = nil
	}
	if len(n.PodsWithRequiredAntiAffinity) == 0 {
		n.PodsWithRequiredAntiAffinity = nil
	}
	if len(n.Pods) == 0 {
		n.Pods = nil
	}
}

func max(a, b int64) int64 {
	if a >= b {
		return a
	}
	return b
}

func (n *NodeInfo) SetNode(node *v1.Node) {
	n.node = node
	n.Generation = nextGeneration()
}

func (n *NodeInfo) RemoveNode() {
	n.node = nil
	n.Generation = nextGeneration()
}

func GetPodKey(pod *v1.Pod) (string, error) {
	uid := string(pod.UID)
	if len(uid) == 0 {
		return "", errors.New("cannot get cache key for pod with empty UID")
	}
	return uid, nil
}

func GetNamespacedName(namespace, name string) string {
	return fmt.Sprintf("%s/%s", namespace, name)
}
