package replication

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/go-logr/logr"
	mariadbv1alpha1 "github.com/mariadb-operator/mariadb-operator/v26/api/v1alpha1"
	mdbpod "github.com/mariadb-operator/mariadb-operator/v26/pkg/pod"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/refresolver"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/replication"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/sql"
	mdbsts "github.com/mariadb-operator/mariadb-operator/v26/pkg/statefulset"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type FailoverHandler struct {
	client      client.Client
	refResolver *refresolver.RefResolver
	mariadb     *mariadbv1alpha1.MariaDB
	logger      logr.Logger
}

func NewFailoverHandler(client client.Client, mariadb *mariadbv1alpha1.MariaDB,
	logger logr.Logger) *FailoverHandler {
	return &FailoverHandler{
		client:      client,
		refResolver: refresolver.New(client),
		mariadb:     mariadb,
		logger:      logger,
	}
}

// FurthestAdvancedReplica finds a candidate to be promoted as primary, taking into account replica status.
func (f *FailoverHandler) FurthestAdvancedReplica(ctx context.Context) (string, error) {
	pods, err := mdbpod.ListMariaDBSecondaryPods(ctx, f.client, f.mariadb)
	if err != nil {
		return "", fmt.Errorf("error listing secondary Pods: %v", err)
	}
	f.logger.Info("Finding candidates to be promoted to primary")

	candidates := f.findCandidates(ctx, pods)
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].name < candidates[j].name
	})
	if len(candidates) == 0 {
		return "", errors.New("no promotion candidates were found")
	}
	f.logger.Info("Found promotion candidates", "candidates", getCandidateNames(candidates))

	furthestAdvanced := f.furthestAdvancedCandidate(candidates)
	if furthestAdvanced == nil {
		return "", errors.New("no furthest advanced candidate was found")
	}
	return furthestAdvanced.name, nil
}

type promotionCandidate struct {
	name           string
	gtidCurrentPos replication.GtidSet
}

func (f *FailoverHandler) findCandidates(ctx context.Context, pods []corev1.Pod) []promotionCandidate {
	candidates := make([]promotionCandidate, 0, len(pods))
	for _, pod := range pods {
		podLogger := f.logger.WithValues("name", pod.Name)

		if !mdbpod.PodReady(&pod) {
			podLogger.Info("Pod not ready. Skipping...")
			continue
		}
		podIndex, err := mdbsts.PodIndex(pod.Name)
		if err != nil {
			podLogger.Info("Invalid Pod name. Skipping...", "err", err)
			continue
		}

		sqlClient, err := sql.NewInternalClientWithPodIndex(ctx, f.mariadb, f.refResolver, *podIndex, sql.WithTimeout(3*time.Second))
		if err != nil {
			podLogger.Info("Unable to create SQL connection. Skipping...", "err", err)
			continue
		}
		defer sqlClient.Close()

		status, err := sqlClient.ReplicaStatus(ctx, podLogger)
		if err != nil {
			podLogger.Info("Unable to get replica status Skipping...", "err", err)
			continue
		}

		slaveIORunning := ptr.Deref(status.SlaveIORunning, false)
		if !slaveIORunning {
			podLogger.Info("IO thread not running. Skipping...")
			continue
		}
		slaveSQLRunning := ptr.Deref(status.SlaveSQLRunning, false)
		if !slaveSQLRunning {
			podLogger.Info("SQL thread not running. Skipping...")
			continue
		}

		hasRelayLogEvents, err := HasRelayLogEvents(status, podLogger)
		if err != nil {
			podLogger.Info("Error checking relay log events. Skipping...", "err", err)
			continue
		}
		if hasRelayLogEvents {
			podLogger.Info("Detected events in relay log. Skipping...")
			continue
		}

		if status.GtidCurrentPos == nil {
			podLogger.Info("GTID current position not set. Skipping...")
			continue
		}
		// Positions are compared across ALL domains: with multiple GTID domains (e.g.
		// multi-cluster), progress lives in foreign domains, and a candidate whose position
		// lacks the local domain (e.g. bootstrapped from another cluster's backup) is still
		// a valid candidate.
		gtidCurrentPos, err := replication.ParseGtidSet(*status.GtidCurrentPos)
		if err != nil {
			podLogger.Info("Error parsing GTID current position. Skipping...", "err", err)
			continue
		}

		candidates = append(candidates, promotionCandidate{
			name:           pod.Name,
			gtidCurrentPos: gtidCurrentPos,
		})
	}
	return candidates
}

// furthestAdvancedCandidate picks the candidate whose position is ahead of (or equal to)
// every other candidate's, across all GTID domains. Domain-wise comparison is a partial
// order: candidates ahead in different domains have diverged, which within a single cluster
// should not happen (replicas apply a single stream). Divergence is logged loudly and
// resolved deterministically by keeping the earlier candidate (candidates are sorted by
// name) — promoting either side of a divergence loses the other side's writes regardless.
func (f *FailoverHandler) furthestAdvancedCandidate(candidates []promotionCandidate) *promotionCandidate {
	var furthestAdvanced *promotionCandidate
	for i := range candidates {
		c := &candidates[i]

		if furthestAdvanced == nil {
			furthestAdvanced = c
			continue
		}
		if c.gtidCurrentPos.AheadOrEqual(furthestAdvanced.gtidCurrentPos) {
			furthestAdvanced = c
		} else if !furthestAdvanced.gtidCurrentPos.AheadOrEqual(c.gtidCurrentPos) {
			f.logger.Info(
				"GTID positions of promotion candidates have diverged, keeping the first candidate",
				"candidate", furthestAdvanced.name, "candidate-gtid", furthestAdvanced.gtidCurrentPos.String(),
				"diverged", c.name, "diverged-gtid", c.gtidCurrentPos.String(),
			)
		}
	}
	return furthestAdvanced
}

func getCandidateNames(candidates []promotionCandidate) []string {
	names := make([]string, len(candidates))
	for i, c := range candidates {
		names[i] = c.name
	}
	return names
}
