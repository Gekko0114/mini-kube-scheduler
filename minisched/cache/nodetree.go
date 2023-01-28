package cache

import (
	"errors"
	"fmt"

	v1 "k8s.io/api/core/v1"
	utilnode "k8s.io/component-helpers/node/topology"
	"k8s.io/klog/v2"
)

type nodeTree struct {
	tree     map[string][]string
	zones    []string
	numNodes int
}

func newNodeTree(nodes []*v1.Node) *nodeTree {
	nt := &nodeTree{
		tree: make(map[string][]string, len(nodes)),
	}

	for _, n := range nodes {
		nt.addNode(n)
	}
	return nt
}

func (nt *nodeTree) addNode(n *v1.Node) {
	zone := utilnode.GetZoneKey(n)
	if na, ok := nt.tree[zone]; ok {
		for _, nodeName := range na {
			if nodeName == n.Name {
				klog.InfoS("Node already exists in the NodeTree", "node", klog.KObj(n))
				return
			}
		}
		nt.tree[zone] = append(na, n.Name)
	} else {
		nt.zones = append(nt.zones, zone)
		nt.tree[zone] = []string{n.Name}
	}
	klog.V(2).InfoS("Added node in listed group to NodeTree", "node", klog.KObj(n), "zone", zone)
	nt.numNodes++
}

func (nt *nodeTree) removeNode(n *v1.Node) error {
	zone := utilnode.GetZoneKey(n)
	if na, ok := nt.tree[zone]; ok {
		for i, nodeName := range na {
			if nodeName == n.Name {
				nt.tree[zone] = append(na[:i], na[i+1:]...)
				if len(nt.tree[zone]) == 0 {
					nt.removeZone(zone)
				}
				klog.V(2).InfoS("Removed node in listed group from NodeTree", "node", klog.KObj(n), "zone", zone)
				nt.numNodes--
				return nil
			}
		}
	}
	klog.ErrorS(nil, "Node in listed group was not found", "node", klog.KObj(n), "zone", zone)
	return fmt.Errorf("node %q in group %q was not found", n.Name, zone)
}

func (nt *nodeTree) removeZone(zone string) {
	delete(nt.tree, zone)
	for i, z := range nt.zones {
		if z == zone {
			nt.zones = append(nt.zones[:i], nt.zones[i+1:]...)
			return
		}
	}
}

func (nt *nodeTree) updateNode(old, new *v1.Node) {
	var oldZone string
	if old != nil {
		oldZone = utilnode.GetZoneKey(old)
	}
	newZone := utilnode.GetZoneKey(new)
	if oldZone == newZone {
		return
	}
	nt.removeNode(old)
	nt.addNode(new)
}

func (nt *nodeTree) list() ([]string, error) {
	if len(nt.zones) == 0 {
		return nil, nil
	}
	nodesList := make([]string, 0, nt.numNodes)
	numExhaustedZones := 0
	nodeIndex := 0
	for len(nodesList) < nt.numNodes {
		if numExhaustedZones >= len(nt.zones) {
			return nodesList, errors.New("all zones exhausted before reaching count of nodes expected")
		}
		for zoneIndex := 0; zoneIndex < len(nt.zones); zoneIndex++ {
			na := nt.tree[nt.zones[zoneIndex]]
			if nodeIndex >= len(na) {
				if nodeIndex == len(na) {
					numExhaustedZones++
				}
				continue
			}
			nodesList = append(nodesList, na[nodeIndex])
		}
		nodeIndex++
	}
	return nodesList, nil
}
