package v1alpha1

import (
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/utils/ptr"
)

// MultiCluster is the multi-cluster topology configuration.
type MultiCluster struct {
	// MultiClusterSpec is the desired multi-cluster topology specification.
	// +operator-sdk:csv:customresourcedefinitions:type=spec
	MultiClusterSpec `json:",inline"`
	// Enabled is a flag to enable the multi-cluster topology.
	// +optional
	// +operator-sdk:csv:customresourcedefinitions:type=spec,xDescriptors={"urn:alm:descriptor:com.tectonic.ui:booleanSwitch"}
	Enabled bool `json:"enabled,omitempty"`
}

// MultiClusterSpec is the specification for the multi-cluster topology.
type MultiClusterSpec struct {
	// Primary is the name of the primary cluster. It refers to a member in the 'members' field, containing its full specification.
	// +optional
	// +operator-sdk:csv:customresourcedefinitions:type=spec,xDescriptors={"urn:alm:descriptor:com.tectonic.ui:advanced"}
	Primary string `json:"primary,omitempty"`
	// Members is the specification of each member of the multi-cluster topology.
	// +optional
	// +operator-sdk:csv:customresourcedefinitions:type=spec,xDescriptors={"urn:alm:descriptor:com.tectonic.ui:advanced"}
	Members []MultiClusterMember `json:"members,omitempty"`
}

// MultiClusterMember defines the configuration for a multi-cluster topology member.
type MultiClusterMember struct {
	// Name is the identifier of the member.
	// +operator-sdk:csv:customresourcedefinitions:type=spec,xDescriptors={"urn:alm:descriptor:com.tectonic.ui:advanced"}
	Name string `json:"name"`
	// ExternalMariaDBRef holds a reference to an ExternalMariaDB with connection details to form the multi-cluster topology.
	// These connection details are utilized to setup remote replicas.
	// +operator-sdk:csv:customresourcedefinitions:type=spec,xDescriptors={"urn:alm:descriptor:com.tectonic.ui:advanced"}
	ExternalMariaDBRef ObjectReference `json:"externalMariaDbRef,omitempty"`
}

// GetExternalMariaDBRefForMember allows for easy access to the ExternalMariaDBRef defined in the members list
func (c *MultiCluster) GetExternalMariaDBRefForMember(memberName string) (*ObjectReference, error) {
	members := c.Members
	for _, member := range members {
		if member.Name == memberName {
			return &member.ExternalMariaDBRef, nil
		}
	}
	return nil, fmt.Errorf("no externalMariaDBRef found for member %s", memberName)
}

// IsMultiClusterEnabled indicates whether the multi-cluster topology is enabled.
func (m *MariaDB) IsMultiClusterEnabled() bool {
	return ptr.Deref(m.Spec.MultiCluster, MultiCluster{}).Enabled
}

// currentMultiClusterPrimary is the cluster member currently acting as the multi-cluster
// primary. Roles are gated on status.currentMultiClusterPrimary, which the multi-cluster
// reconciliation phase only advances after fencing, connection teardown and GTID
// reconfiguration: deriving roles from the spec directly would make a promoted cluster
// writable while it is still applying the outgoing primary's stream, and start a demoted
// cluster's replication before its GTID position is set (dual-writer window).
// During provisioning, before the status is first populated, the spec is the only source.
func (m *MariaDB) currentMultiClusterPrimary() string {
	if current := ptr.Deref(m.Status.CurrentMultiClusterPrimary, ""); current != "" {
		return current
	}
	return ptr.Deref(m.Spec.MultiCluster, MultiCluster{}).Primary
}

// IsMultiClusterPrimary indicates whether the current cluster acts as a primary cluster in a
// multi-cluster topology. See currentMultiClusterPrimary for the status-gating rationale; the
// spec's designation is available via IsMultiClusterDesiredPrimary.
func (m *MariaDB) IsMultiClusterPrimary() bool {
	return m.IsMultiClusterEnabled() && m.currentMultiClusterPrimary() == m.Name
}

// IsMultiClusterDesiredPrimary indicates whether the spec designates the current cluster as
// the primary cluster of a multi-cluster topology. It differs from IsMultiClusterPrimary
// while a cluster-level switchover is pending or in progress.
func (m *MariaDB) IsMultiClusterDesiredPrimary() bool {
	return m.IsMultiClusterEnabled() && ptr.Deref(m.Spec.MultiCluster, MultiCluster{}).Primary == m.Name
}

// GetMultiClusterPrimary obtains the member name of the cluster currently acting as primary.
func (m *MariaDB) GetMultiClusterPrimary() *string {
	if !m.IsMultiClusterEnabled() {
		return nil
	}
	return ptr.To(m.currentMultiClusterPrimary())
}

// IsMultiClusterReplica indicates whether the current cluster acts as a replica cluster in a
// multi-cluster topology. See currentMultiClusterPrimary for the status-gating rationale.
func (m *MariaDB) IsMultiClusterReplica() bool {
	return m.IsMultiClusterEnabled() && m.currentMultiClusterPrimary() != m.Name
}

// IsSwitchingMultiClusterPrimary indicates whether a cluster-level switchover is in progress.
func (m *MariaDB) IsSwitchingMultiClusterPrimary() bool {
	return meta.IsStatusConditionFalse(m.Status.Conditions, ConditionTypeMultiClusterPrimarySwitched)
}

// IsMultiClusterPrimaryReplica determines whether a given Pod index is a primary Pod in a replica cluster.
func (m *MariaDB) IsMultiClusterPrimaryReplica(podIndex int) bool {
	return m.IsMultiClusterReplica() && m.Status.CurrentPrimaryPodIndex != nil && *m.Status.CurrentPrimaryPodIndex == podIndex
}
