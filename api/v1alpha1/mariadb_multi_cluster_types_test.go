package v1alpha1

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

func TestMariaDB_MultiClusterRoles(t *testing.T) {
	buildMariaDB := func(enabled bool, specPrimary string, currentPrimary *string) *MariaDB {
		return &MariaDB{
			ObjectMeta: metav1.ObjectMeta{
				Name: "site1",
			},
			Spec: MariaDBSpec{
				MultiCluster: &MultiCluster{
					Enabled: enabled,
					MultiClusterSpec: MultiClusterSpec{
						Primary: specPrimary,
					},
				},
			},
			Status: MariaDBStatus{
				CurrentMultiClusterPrimary: currentPrimary,
			},
		}
	}
	tests := []struct {
		name               string
		mdb                *MariaDB
		wantPrimary        bool
		wantReplica        bool
		wantDesiredPrimary bool
		wantGetPrimary     *string
	}{
		{
			name:               "multi-cluster disabled",
			mdb:                buildMariaDB(false, "site1", nil),
			wantPrimary:        false,
			wantReplica:        false,
			wantDesiredPrimary: false,
			wantGetPrimary:     nil,
		},
		{
			// provisioning: status not yet populated, the spec is the only source
			name:               "status empty falls back to spec",
			mdb:                buildMariaDB(true, "site1", nil),
			wantPrimary:        true,
			wantReplica:        false,
			wantDesiredPrimary: true,
			wantGetPrimary:     ptr.To("site1"),
		},
		{
			name:               "status empty falls back to spec as replica",
			mdb:                buildMariaDB(true, "site2", nil),
			wantPrimary:        false,
			wantReplica:        true,
			wantDesiredPrimary: false,
			wantGetPrimary:     ptr.To("site2"),
		},
		{
			name:               "converged primary",
			mdb:                buildMariaDB(true, "site1", ptr.To("site1")),
			wantPrimary:        true,
			wantReplica:        false,
			wantDesiredPrimary: true,
			wantGetPrimary:     ptr.To("site1"),
		},
		{
			// pending promotion: the spec designates this cluster, but the role must keep
			// following the status until the switchover phase completed fencing and GTID work
			name:               "pending promotion keeps replica role",
			mdb:                buildMariaDB(true, "site1", ptr.To("site2")),
			wantPrimary:        false,
			wantReplica:        true,
			wantDesiredPrimary: true,
			wantGetPrimary:     ptr.To("site2"),
		},
		{
			// pending demotion: the cluster keeps acting as primary until the switchover
			// phase set read_only and positioned gtid_slave_pos
			name:               "pending demotion keeps primary role",
			mdb:                buildMariaDB(true, "site2", ptr.To("site1")),
			wantPrimary:        true,
			wantReplica:        false,
			wantDesiredPrimary: false,
			wantGetPrimary:     ptr.To("site1"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.mdb.IsMultiClusterPrimary(); got != tt.wantPrimary {
				t.Errorf("IsMultiClusterPrimary() = %v, want %v", got, tt.wantPrimary)
			}
			if got := tt.mdb.IsMultiClusterReplica(); got != tt.wantReplica {
				t.Errorf("IsMultiClusterReplica() = %v, want %v", got, tt.wantReplica)
			}
			if got := tt.mdb.IsMultiClusterDesiredPrimary(); got != tt.wantDesiredPrimary {
				t.Errorf("IsMultiClusterDesiredPrimary() = %v, want %v", got, tt.wantDesiredPrimary)
			}
			got := tt.mdb.GetMultiClusterPrimary()
			if (got == nil) != (tt.wantGetPrimary == nil) || (got != nil && *got != *tt.wantGetPrimary) {
				t.Errorf("GetMultiClusterPrimary() = %v, want %v", got, tt.wantGetPrimary)
			}
		})
	}
}
