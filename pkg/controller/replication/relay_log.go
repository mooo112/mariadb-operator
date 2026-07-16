package replication

import (
	"errors"
	"fmt"

	"github.com/go-logr/logr"
	mariadbv1alpha1 "github.com/mariadb-operator/mariadb-operator/v26/api/v1alpha1"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/replication"
)

// HasRelayLogEvents indicates that there are events in the IO thread to be applied by the SQL
// thread. Every domain present in the IO position is checked: with multiple GTID domains
// (e.g. multi-cluster), pending events live in foreign domains that a local-domain-only
// comparison would miss, reporting a replica as drained while it still has a relay backlog.
func HasRelayLogEvents(status *mariadbv1alpha1.ReplicaStatusVars, logger logr.Logger) (bool, error) {
	if status.GtidIOPos == nil {
		return false, errors.New("GTID IO position must be set")
	}
	if status.GtidCurrentPos == nil {
		return false, errors.New("GTID SQL position must be set")
	}

	gtidIOPos, err := replication.ParseGtidSet(*status.GtidIOPos)
	if err != nil {
		return false, fmt.Errorf("error parsing GTID IO position: %v", err)
	}
	gtidCurrentPos, err := replication.ParseGtidSet(*status.GtidCurrentPos)
	if err != nil {
		return false, fmt.Errorf("error parsing GTID SQL position: %v", err)
	}

	if !gtidCurrentPos.AheadOrEqual(gtidIOPos) {
		logger.Info(
			"Detected events in relay log. Skipping...",
			"gtid-io-pos", gtidIOPos.String(),
			"gtid-current-pos", gtidCurrentPos.String(),
		)
		return true, nil
	}
	return false, nil
}
