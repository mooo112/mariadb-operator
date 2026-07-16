package controller

import (
	"context"
	"testing"
	"time"

	mariadbv1alpha1 "github.com/mariadb-operator/mariadb-operator/v26/api/v1alpha1"
	"github.com/stretchr/testify/assert"
)

func TestRequeueResult(t *testing.T) {
	tests := []struct {
		name        string
		mdb         *mariadbv1alpha1.MariaDB
		wantRequeue time.Duration
	}{
		{
			name: "maintenance mode wins",
			mdb: &mariadbv1alpha1.MariaDB{
				Spec: mariadbv1alpha1.MariaDBSpec{
					Maintenance: &mariadbv1alpha1.MariaDBMaintenance{Enabled: true},
					Replication: &mariadbv1alpha1.Replication{Enabled: true},
				},
			},
			wantRequeue: 10 * time.Second,
		},
		{
			// a stopped/broken replication connection emits no Kubernetes event: without a
			// periodic requeue it stays undetected and unrepaired, with stale thread states
			// in status.replication
			name: "replication requeues to observe health",
			mdb: &mariadbv1alpha1.MariaDB{
				Spec: mariadbv1alpha1.MariaDBSpec{
					Replication: &mariadbv1alpha1.Replication{Enabled: true},
				},
			},
			wantRequeue: 1 * time.Minute,
		},
		{
			// a Galera replica cluster runs the 'multi-cluster' replication connection on its
			// primary replica: its health needs the same periodic observation even though
			// spec.replication is not enabled
			name: "multi-cluster without replication requeues to observe health",
			mdb: &mariadbv1alpha1.MariaDB{
				Spec: mariadbv1alpha1.MariaDBSpec{
					MultiCluster: &mariadbv1alpha1.MultiCluster{Enabled: true},
				},
			},
			wantRequeue: 1 * time.Minute,
		},
		{
			name: "replication takes precedence over TLS",
			mdb: &mariadbv1alpha1.MariaDB{
				Spec: mariadbv1alpha1.MariaDBSpec{
					Replication: &mariadbv1alpha1.Replication{Enabled: true},
					TLS:         &mariadbv1alpha1.TLS{Enabled: true},
				},
			},
			wantRequeue: 1 * time.Minute,
		},
		{
			name: "TLS requeues for cert renewal",
			mdb: &mariadbv1alpha1.MariaDB{
				Spec: mariadbv1alpha1.MariaDBSpec{
					TLS: &mariadbv1alpha1.TLS{Enabled: true},
				},
			},
			wantRequeue: 5 * time.Minute,
		},
		{
			name:        "no requeue otherwise",
			mdb:         &mariadbv1alpha1.MariaDB{},
			wantRequeue: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := requeueResult(context.Background(), tt.mdb)
			assert.NoError(t, err)
			assert.Equal(t, tt.wantRequeue, result.RequeueAfter)
		})
	}
}
