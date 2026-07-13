package maintenance

import (
	"testing"

	mariadbv1alpha1 "github.com/mariadb-operator/mariadb-operator/v26/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

func TestGetReadOnlyDesiredPodState(t *testing.T) {
	tests := []struct {
		name          string
		mariadb       *mariadbv1alpha1.MariaDB
		expectedState map[int]bool
	}{
		{
			name: "replication without maintenance",
			mariadb: &mariadbv1alpha1.MariaDB{
				Spec: mariadbv1alpha1.MariaDBSpec{
					Replicas: 3,
					Replication: &mariadbv1alpha1.Replication{
						Enabled: true,
					},
				},
				Status: mariadbv1alpha1.MariaDBStatus{
					CurrentPrimaryPodIndex: ptr.To(0),
				},
			},
			expectedState: map[int]bool{
				0: false,
				1: true,
				2: true,
			},
		},
		{
			name: "replication without maintenance - primary at index 1",
			mariadb: &mariadbv1alpha1.MariaDB{
				Spec: mariadbv1alpha1.MariaDBSpec{
					Replicas: 3,
					Replication: &mariadbv1alpha1.Replication{
						Enabled: true,
					},
				},
				Status: mariadbv1alpha1.MariaDBStatus{
					CurrentPrimaryPodIndex: ptr.To(1),
				},
			},
			expectedState: map[int]bool{
				0: true,
				1: false,
				2: true,
			},
		},
		{
			name: "replication with maintenance and readOnly",
			mariadb: &mariadbv1alpha1.MariaDB{
				Spec: mariadbv1alpha1.MariaDBSpec{
					Replicas: 3,
					Replication: &mariadbv1alpha1.Replication{
						Enabled: true,
					},
					Maintenance: ptr.To(mariadbv1alpha1.MariaDBMaintenance{
						Enabled:  true,
						ReadOnly: true,
					}),
				},
				Status: mariadbv1alpha1.MariaDBStatus{
					CurrentPrimaryPodIndex: ptr.To(0),
				},
			},
			expectedState: map[int]bool{
				0: true,
				1: true,
				2: true,
			},
		},
		{
			name: "replication with maintenance and readOnly - primary at index 1",
			mariadb: &mariadbv1alpha1.MariaDB{
				Spec: mariadbv1alpha1.MariaDBSpec{
					Replicas: 3,
					Replication: &mariadbv1alpha1.Replication{
						Enabled: true,
					},
					Maintenance: ptr.To(mariadbv1alpha1.MariaDBMaintenance{
						Enabled:  true,
						ReadOnly: true,
					}),
				},
				Status: mariadbv1alpha1.MariaDBStatus{
					CurrentPrimaryPodIndex: ptr.To(1),
				},
			},
			expectedState: map[int]bool{
				0: true,
				1: true,
				2: true,
			},
		},
		{
			name: "replication with maintenance without readOnly",
			mariadb: &mariadbv1alpha1.MariaDB{
				Spec: mariadbv1alpha1.MariaDBSpec{
					Replicas: 3,
					Replication: &mariadbv1alpha1.Replication{
						Enabled: true,
					},
					Maintenance: ptr.To(mariadbv1alpha1.MariaDBMaintenance{
						Enabled:  true,
						ReadOnly: false,
					}),
				},
				Status: mariadbv1alpha1.MariaDBStatus{
					CurrentPrimaryPodIndex: ptr.To(0),
				},
			},
			expectedState: map[int]bool{
				0: false,
				1: true,
				2: true,
			},
		},
		{
			name: "replication with maintenance without readOnly - primary at index 1",
			mariadb: &mariadbv1alpha1.MariaDB{
				Spec: mariadbv1alpha1.MariaDBSpec{
					Replicas: 3,
					Replication: &mariadbv1alpha1.Replication{
						Enabled: true,
					},
					Maintenance: ptr.To(mariadbv1alpha1.MariaDBMaintenance{
						Enabled:  true,
						ReadOnly: false,
					}),
				},
				Status: mariadbv1alpha1.MariaDBStatus{
					CurrentPrimaryPodIndex: ptr.To(1),
				},
			},
			expectedState: map[int]bool{
				0: true,
				1: false,
				2: true,
			},
		},
		{
			name: "standalone without maintenance",
			mariadb: &mariadbv1alpha1.MariaDB{
				Spec: mariadbv1alpha1.MariaDBSpec{
					Replicas: 1,
				},
			},
			expectedState: map[int]bool{
				0: false,
			},
		},
		{
			name: "standalone with maintenance and readOnly",
			mariadb: &mariadbv1alpha1.MariaDB{
				Spec: mariadbv1alpha1.MariaDBSpec{
					Replicas: 1,
					Maintenance: ptr.To(mariadbv1alpha1.MariaDBMaintenance{
						Enabled:  true,
						ReadOnly: true,
					}),
				},
			},
			expectedState: map[int]bool{
				0: true,
			},
		},
		{
			name: "standalone with maintenance without readOnly",
			mariadb: &mariadbv1alpha1.MariaDB{
				Spec: mariadbv1alpha1.MariaDBSpec{
					Replicas: 1,
					Maintenance: ptr.To(mariadbv1alpha1.MariaDBMaintenance{
						Enabled:  true,
						ReadOnly: false,
					}),
				},
			},
			expectedState: map[int]bool{
				0: false,
			},
		},
		{
			name: "galera without maintenance",
			mariadb: &mariadbv1alpha1.MariaDB{
				Spec: mariadbv1alpha1.MariaDBSpec{
					Replicas: 3,
					Galera: ptr.To(mariadbv1alpha1.Galera{
						Enabled: true,
					}),
				},
				Status: mariadbv1alpha1.MariaDBStatus{
					CurrentPrimaryPodIndex: ptr.To(0),
				},
			},
			expectedState: map[int]bool{
				0: false,
				1: false,
				2: false,
			},
		},
		{
			name: "galera without maintenance - primary at index 1",
			mariadb: &mariadbv1alpha1.MariaDB{
				Spec: mariadbv1alpha1.MariaDBSpec{
					Replicas: 3,
					Galera: ptr.To(mariadbv1alpha1.Galera{
						Enabled: true,
					}),
				},
				Status: mariadbv1alpha1.MariaDBStatus{
					CurrentPrimaryPodIndex: ptr.To(1),
				},
			},
			expectedState: map[int]bool{
				0: false,
				1: false,
				2: false,
			},
		},
		{
			name: "galera with maintenance and readOnly",
			mariadb: &mariadbv1alpha1.MariaDB{
				Spec: mariadbv1alpha1.MariaDBSpec{
					Replicas: 3,
					Galera: ptr.To(mariadbv1alpha1.Galera{
						Enabled: true,
					}),
					Maintenance: ptr.To(mariadbv1alpha1.MariaDBMaintenance{
						Enabled:  true,
						ReadOnly: true,
					}),
				},
				Status: mariadbv1alpha1.MariaDBStatus{
					CurrentPrimaryPodIndex: ptr.To(0),
				},
			},
			expectedState: map[int]bool{
				0: true,
				1: true,
				2: true,
			},
		},
		{
			name: "galera with maintenance and readOnly - primary at index 1",
			mariadb: &mariadbv1alpha1.MariaDB{
				Spec: mariadbv1alpha1.MariaDBSpec{
					Replicas: 3,
					Galera: ptr.To(mariadbv1alpha1.Galera{
						Enabled: true,
					}),
					Maintenance: ptr.To(mariadbv1alpha1.MariaDBMaintenance{
						Enabled:  true,
						ReadOnly: true,
					}),
				},
				Status: mariadbv1alpha1.MariaDBStatus{
					CurrentPrimaryPodIndex: ptr.To(1),
				},
			},
			expectedState: map[int]bool{
				0: true,
				1: true,
				2: true,
			},
		},
		{
			name: "galera with maintenance without readOnly",
			mariadb: &mariadbv1alpha1.MariaDB{
				Spec: mariadbv1alpha1.MariaDBSpec{
					Replicas: 3,
					Galera: ptr.To(mariadbv1alpha1.Galera{
						Enabled: true,
					}),
					Maintenance: ptr.To(mariadbv1alpha1.MariaDBMaintenance{
						Enabled:  true,
						ReadOnly: false,
					}),
				},
				Status: mariadbv1alpha1.MariaDBStatus{
					CurrentPrimaryPodIndex: ptr.To(0),
				},
			},
			expectedState: map[int]bool{
				0: false,
				1: false,
				2: false,
			},
		},
		{
			name: "galera with maintenance without readOnly - primary at index 1",
			mariadb: &mariadbv1alpha1.MariaDB{
				Spec: mariadbv1alpha1.MariaDBSpec{
					Replicas: 3,
					Galera: ptr.To(mariadbv1alpha1.Galera{
						Enabled: true,
					}),
					Maintenance: ptr.To(mariadbv1alpha1.MariaDBMaintenance{
						Enabled:  true,
						ReadOnly: false,
					}),
				},
				Status: mariadbv1alpha1.MariaDBStatus{
					CurrentPrimaryPodIndex: ptr.To(1),
				},
			},
			expectedState: map[int]bool{
				0: false,
				1: false,
				2: false,
			},
		},
		{
			name: "multi-cluster replica cluster",
			mariadb: &mariadbv1alpha1.MariaDB{
				ObjectMeta: metav1.ObjectMeta{
					Name: "mariadb-eu-central",
				},
				Spec: mariadbv1alpha1.MariaDBSpec{
					Replicas: 3,
					Replication: &mariadbv1alpha1.Replication{
						Enabled: true,
					},
					MultiCluster: &mariadbv1alpha1.MultiCluster{
						Enabled: true,
						MultiClusterSpec: mariadbv1alpha1.MultiClusterSpec{
							Primary: "mariadb-eu-south",
						},
					},
				},
				Status: mariadbv1alpha1.MariaDBStatus{
					CurrentPrimaryPodIndex: ptr.To(0),
				},
			},
			expectedState: map[int]bool{
				0: true,
				1: true,
				2: true,
			},
		},
		{
			name: "multi-cluster replica cluster - primary at index 1",
			mariadb: &mariadbv1alpha1.MariaDB{
				ObjectMeta: metav1.ObjectMeta{
					Name: "mariadb-eu-central",
				},
				Spec: mariadbv1alpha1.MariaDBSpec{
					Replicas: 3,
					Replication: &mariadbv1alpha1.Replication{
						Enabled: true,
					},
					MultiCluster: &mariadbv1alpha1.MultiCluster{
						Enabled: true,
						MultiClusterSpec: mariadbv1alpha1.MultiClusterSpec{
							Primary: "mariadb-eu-south",
						},
					},
				},
				Status: mariadbv1alpha1.MariaDBStatus{
					CurrentPrimaryPodIndex: ptr.To(1),
				},
			},
			expectedState: map[int]bool{
				0: true,
				1: true,
				2: true,
			},
		},
		{
			name: "multi-cluster primary cluster",
			mariadb: &mariadbv1alpha1.MariaDB{
				ObjectMeta: metav1.ObjectMeta{
					Name: "mariadb-eu-south",
				},
				Spec: mariadbv1alpha1.MariaDBSpec{
					Replicas: 3,
					Replication: &mariadbv1alpha1.Replication{
						Enabled: true,
					},
					MultiCluster: &mariadbv1alpha1.MultiCluster{
						Enabled: true,
						MultiClusterSpec: mariadbv1alpha1.MultiClusterSpec{
							Primary: "mariadb-eu-south",
						},
					},
				},
				Status: mariadbv1alpha1.MariaDBStatus{
					CurrentPrimaryPodIndex: ptr.To(0),
				},
			},
			expectedState: map[int]bool{
				0: false,
				1: true,
				2: true,
			},
		},
	}

	r := &MaintenanceReconciler{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := r.getReadOnlyDesiredPodState(tt.mariadb)
			assert.Equal(t, tt.expectedState, result, "ReadOnly state mismatch")
		})
	}
}
