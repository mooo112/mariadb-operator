package conditions

import (
	"fmt"

	mariadbv1alpha1 "github.com/mariadb-operator/mariadb-operator/v26/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SetMultiClusterPrimarySwitching marks a cluster-level switchover as in progress. Unlike
// SetPrimarySwitching it does not flip Ready: the cluster keeps serving while the switchover
// is fenced and reconfigured.
func SetMultiClusterPrimarySwitching(c Conditioner, newPrimary string) {
	c.SetCondition(metav1.Condition{
		Type:    mariadbv1alpha1.ConditionTypeMultiClusterPrimarySwitched,
		Status:  metav1.ConditionFalse,
		Reason:  mariadbv1alpha1.ConditionReasonSwitchMultiClusterPrimary,
		Message: fmt.Sprintf("Switching multi-cluster primary to '%s'", newPrimary),
	})
}

func SetMultiClusterPrimarySwitched(c Conditioner) {
	c.SetCondition(metav1.Condition{
		Type:    mariadbv1alpha1.ConditionTypeMultiClusterPrimarySwitched,
		Status:  metav1.ConditionTrue,
		Reason:  mariadbv1alpha1.ConditionReasonSwitchMultiClusterPrimary,
		Message: "Cluster switchover complete",
	})
}
