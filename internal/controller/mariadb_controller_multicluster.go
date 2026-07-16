package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	mariadbv1alpha1 "github.com/mariadb-operator/mariadb-operator/v26/api/v1alpha1"
	condition "github.com/mariadb-operator/mariadb-operator/v26/pkg/condition"
	replicationctrl "github.com/mariadb-operator/mariadb-operator/v26/pkg/controller/replication"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/metadata"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/replication"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/sql"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	// multiClusterCatchUpTimeout bounds the in-reconcile wait for the to-be-promoted cluster to
	// apply the outgoing primary's binlog; when exceeded, the promotion is requeued.
	multiClusterCatchUpTimeout = 30 * time.Second
	// multiClusterCatchUpRequeue is the requeue interval while the promotion is fenced.
	multiClusterCatchUpRequeue = 10 * time.Second
)

func (r *MariaDBReconciler) reconcileMultiCluster(ctx context.Context, mdb *mariadbv1alpha1.MariaDB) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName("multi-cluster")

	shouldReconcile, err := r.shouldReconcileMultiCluster(ctx, mdb, logger)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("error determining whether multi-cluster should be reconciled: %v", err)
	}
	if !shouldReconcile {
		return ctrl.Result{}, nil
	}
	multiCluster := ptr.Deref(mdb.Spec.MultiCluster, mariadbv1alpha1.MultiCluster{})
	primary := multiCluster.Primary
	currentPrimary := ptr.Deref(mdb.Status.CurrentMultiClusterPrimary, "")

	// during provisioning, cluster-level switchover reconciliation is not performed
	if currentPrimary == "" {
		return ctrl.Result{}, r.patchStatus(ctx, mdb, func(status *mariadbv1alpha1.MariaDBStatus) error {
			status.CurrentMultiClusterPrimary = &primary
			condition.SetMultiClusterPrimarySwitched(status)
			return nil
		})
	}
	if primary == currentPrimary {
		// consume stale force-promote annotations so a leftover cannot silently
		// forfeit the fence of a future promotion
		return ctrl.Result{}, r.clearForcePromote(ctx, mdb, logger)
	}

	// mark the switchover in progress so it is distinguishable from a converged (or stuck)
	// state; roles keep following status.currentMultiClusterPrimary until it completes
	if !mdb.IsSwitchingMultiClusterPrimary() {
		if err := r.patchStatus(ctx, mdb, func(status *mariadbv1alpha1.MariaDBStatus) error {
			condition.SetMultiClusterPrimarySwitching(status, primary)
			return nil
		}); err != nil {
			return ctrl.Result{}, err
		}
	}

	// promote/demote is decided by the spec (the desired role); every other role consumer is
	// gated on status.currentMultiClusterPrimary so that the promoted cluster only becomes
	// writable — and the demoted cluster only starts replicating — after the steps below
	if mdb.IsMultiClusterDesiredPrimary() {
		forced := isPromotionForced(mdb)
		if forced {
			r.Recorder.Eventf(mdb, nil, corev1.EventTypeWarning, mariadbv1alpha1.ReasonMultiClusterPromotionForced,
				mariadbv1alpha1.ActionReconciling,
				"Promotion forced via the %s annotation: catch-up fence skipped. Writes not yet replicated from '%s' are lost on this cluster",
				metadata.ForcePromoteAnnotation, currentPrimary)
			logger.Info("Promotion forced, skipping catch-up fence", "member", currentPrimary)
		} else {
			caughtUp, err := r.waitForSwitchoverCatchUp(ctx, mdb, currentPrimary, logger)
			if err != nil {
				return ctrl.Result{}, fmt.Errorf("error waiting for switchover catch-up: %v", err)
			}
			if !caughtUp {
				return ctrl.Result{RequeueAfter: multiClusterCatchUpRequeue}, nil
			}
		}
		if err := r.resetPrimaryReplicaConnection(ctx, mdb, logger); err != nil {
			return ctrl.Result{}, fmt.Errorf("error resetting primary replica connection: %v", err)
		}
	} else {
		if err := r.reconfigureReplicaClusterGtids(ctx, mdb, logger); err != nil {
			return ctrl.Result{}, fmt.Errorf("error reconciling replica cluster GTIDs: %v", err)
		}
	}

	if err := r.patchStatus(ctx, mdb, func(status *mariadbv1alpha1.MariaDBStatus) error {
		status.CurrentMultiClusterPrimary = &primary
		condition.SetMultiClusterPrimarySwitched(status)
		return nil
	}); err != nil {
		return ctrl.Result{}, err
	}
	r.Recorder.Eventf(mdb, nil, corev1.EventTypeNormal, mariadbv1alpha1.ReasonMultiClusterPrimarySwitched,
		mariadbv1alpha1.ActionReconciling,
		"Multi-cluster primary switched from '%s' to '%s'", currentPrimary, primary)
	return ctrl.Result{}, r.clearForcePromote(ctx, mdb, logger)
}

// isPromotionForced indicates whether the user explicitly requested to skip the promotion
// catch-up fence (unplanned failover / disaster recovery).
func isPromotionForced(mdb *mariadbv1alpha1.MariaDB) bool {
	return mdb.Annotations[metadata.ForcePromoteAnnotation] == "true"
}

// clearForcePromote consumes the force-promote annotation. Forcing is a one-shot, per-switchover
// decision: leaving the annotation behind would silently disable the fence for future promotions.
func (r *MariaDBReconciler) clearForcePromote(ctx context.Context, mdb *mariadbv1alpha1.MariaDB,
	logger logr.Logger) error {
	if _, ok := mdb.Annotations[metadata.ForcePromoteAnnotation]; !ok {
		return nil
	}
	logger.Info("Removing force-promote annotation", "annotation", metadata.ForcePromoteAnnotation)
	return r.patch(ctx, mdb, func(mdb *mariadbv1alpha1.MariaDB) error {
		delete(mdb.Annotations, metadata.ForcePromoteAnnotation)
		return nil
	})
}

func (r *MariaDBReconciler) resetPrimaryReplicaConnection(ctx context.Context, mdb *mariadbv1alpha1.MariaDB,
	logger logr.Logger) error {
	logger.Info("Resetting primary replica connection")

	clientSet := sql.NewClientSet(mdb, r.RefResolver)
	defer clientSet.Close()

	for i := 0; i < int(mdb.Spec.Replicas); i++ {
		client, err := clientSet.ClientForIndex(ctx, i)
		if err != nil {
			return fmt.Errorf("error getting client for Pod index %d: %v", i, err)
		}
		if err := client.StopSlave(
			ctx,
			sql.WithConnectionName(replicationctrl.MultiClusterReplicaConnectionName),
		); err != nil && !sql.IsConnectionNotExists(err) {
			return fmt.Errorf("error stopping primary replica connection in Pod index %d: %v", i, err)
		}
		if err := client.ResetSlave(
			ctx,
			sql.WithConnectionName(replicationctrl.MultiClusterReplicaConnectionName),
		); err != nil && !sql.IsConnectionNotExists(err) {
			return fmt.Errorf("error resetting primary replica connection in Pod index %d: %v", i, err)
		}
	}
	return nil
}

// reconfigureReplicaClusterGtids restarts the primary replica from its own gtid_current_pos.
// gtid_current_pos reflects, per domain, everything this server has applied or written. Resuming from
// exactly there is safe because a promoted primary keeps its full multi-domain gtid_binlog_state, so
// every domain that ever transited a primary is servable. A domain the new primary genuinely lacks
// (true divergence, e.g. writes that never replicated before an unplanned failover) fails the
// connection loudly with error 1236 instead of being silently discarded or replayed.
func (r *MariaDBReconciler) reconfigureReplicaClusterGtids(ctx context.Context, mdb *mariadbv1alpha1.MariaDB, logger logr.Logger) error {
	if !mdb.IsReplicationEnabled() {
		return nil
	}
	logger.Info("Reconfiguring replica GTIDs")

	primaryClient, err := sql.NewInternalClientWithPodIndex(ctx, mdb, r.RefResolver, *mdb.Status.CurrentPrimaryPodIndex)
	if err != nil {
		return fmt.Errorf("error getting primary client: %v", err)
	}
	defer primaryClient.Close()

	// Stop taking writes before snapshotting the position: gtid_current_pos of a still-writable
	// primary is a moving target, and the promoting cluster's fence waits for this cluster to
	// report read_only=1 before trusting its gtid_binlog_pos.
	if err := primaryClient.EnableReadOnly(ctx); err != nil {
		return fmt.Errorf("error enabling read_only in primary: %v", err)
	}
	// The 'multi-cluster' connection does not exist yet on a demoting primary: it is created by
	// the Replication phase once the demotion completes and the role follows the patched status.
	if err := primaryClient.StopSlave(
		ctx,
		sql.WithConnectionName(replicationctrl.MultiClusterReplicaConnectionName),
	); err != nil && !sql.IsConnectionNotExists(err) {
		return fmt.Errorf("error stopping primary replica: %v", err)
	}
	currentPos, err := primaryClient.GtidCurrentPos(ctx)
	if err != nil {
		return fmt.Errorf("error getting gtid_current_pos: %v", err)
	}
	if currentPos != "" {
		// gtid_current_pos covers every domain present in the local binlog by construction,
		// so this cannot fail with error 1948 (missing domain in gtid_slave_pos).
		if err := primaryClient.SetGtidSlavePos(ctx, currentPos); err != nil {
			return fmt.Errorf("error setting gtid_slave_pos %s in primary replica: %v", currentPos, err)
		}
	}
	if err := primaryClient.StartSlave(
		ctx,
		sql.WithConnectionName(replicationctrl.MultiClusterReplicaConnectionName),
	); err != nil && !sql.IsConnectionNotExists(err) {
		return fmt.Errorf("error starting primary replica: %v", err)
	}
	return nil
}

// waitForSwitchoverCatchUp fences a cluster promotion: before the to-be-promoted cluster stops
// replicating from the outgoing primary, it must have applied everything the outgoing primary has
// binlogged. Promoting with an un-applied tail permanently loses those writes on the promoted
// cluster and diverges the datasets (e.g. duplicate auto-increment keys once writes resume).
// The fence fails closed: if the outgoing primary cannot be verified (unreachable, probe errors),
// the promotion stays fenced rather than silently degrading a planned switchover into
// unplanned-failover semantics. Unplanned failover is an explicit, human decision: setting the
// force-promote annotation skips the fence and accepts the loss of the un-replicated tail.
func (r *MariaDBReconciler) waitForSwitchoverCatchUp(ctx context.Context, mdb *mariadbv1alpha1.MariaDB,
	outgoingPrimary string, logger logr.Logger) (bool, error) {
	if !mdb.IsReplicationEnabled() {
		return true, nil
	}
	externalClient, err := r.getExternalMemberClient(ctx, mdb, outgoingPrimary)
	if err != nil {
		r.eventPromotionFencedUnverifiable(mdb, outgoingPrimary)
		logger.Info(
			"Promotion fenced: outgoing multi-cluster primary is not reachable, requeuing...",
			"member", outgoingPrimary, "error", err.Error(),
		)
		return false, nil
	}
	defer externalClient.Close()

	// The outgoing primary's position is only trustworthy once it stopped taking writes: its own
	// demotion (a separate CR, reconciled by its own operator) sets read_only. Snapshotting the
	// position while it is still writable races with in-flight writes — anything committed between
	// snapshot and read_only would be silently lost on this cluster after promotion. Fence until
	// the outgoing primary is read_only, then its gtid_binlog_pos is frozen and the wait is exact.
	readOnly, err := externalClient.GetReadOnly(ctx)
	if err != nil {
		r.eventPromotionFencedUnverifiable(mdb, outgoingPrimary)
		logger.Info(
			"Promotion fenced: unable to get read_only from outgoing multi-cluster primary, requeuing...",
			"member", outgoingPrimary, "error", err.Error(),
		)
		return false, nil
	}
	if !readOnly {
		r.Recorder.Eventf(mdb, nil, corev1.EventTypeNormal, mariadbv1alpha1.ReasonMultiClusterPromotionFenced,
			mariadbv1alpha1.ActionReconciling,
			"Promotion fenced: outgoing primary '%s' is still writable (read_only=0)", outgoingPrimary)
		logger.Info(
			"Promotion fenced: outgoing multi-cluster primary is still writable (read_only=0), requeuing...",
			"member", outgoingPrimary,
		)
		return false, nil
	}

	outgoingPos, err := externalClient.GtidBinlogPos(ctx)
	if err != nil {
		r.eventPromotionFencedUnverifiable(mdb, outgoingPrimary)
		logger.Info(
			"Promotion fenced: unable to get gtid_binlog_pos from outgoing multi-cluster primary, requeuing...",
			"member", outgoingPrimary, "error", err.Error(),
		)
		return false, nil
	}
	if outgoingPos == "" {
		return true, nil
	}

	primaryClient, err := sql.NewInternalClientWithPodIndex(ctx, mdb, r.RefResolver, *mdb.Status.CurrentPrimaryPodIndex)
	if err != nil {
		return false, fmt.Errorf("error getting primary client: %v", err)
	}
	defer primaryClient.Close()

	// Wait only on domains FOREIGN to this cluster. The outgoing primary's gtid_binlog_pos also
	// contains this cluster's own domain (its writes, relayed back), but gtid_slave_pos does not
	// advance for own-server-id events (the SQL thread skips them) — including the own domain
	// would make MASTER_GTID_WAIT unsatisfiable and fence the promotion forever. Our own domain's
	// writes are local by definition; the tail we must not lose lives in the other domains.
	localDomainId, err := primaryClient.GtidDomainId(ctx)
	if err != nil {
		return false, fmt.Errorf("error getting gtid_domain_id: %v", err)
	}
	waitPos, err := filterOutDomain(outgoingPos, *localDomainId)
	if err != nil {
		return false, fmt.Errorf("error filtering outgoing gtid_binlog_pos %s: %v", outgoingPos, err)
	}
	if waitPos == "" {
		return true, nil
	}

	err = primaryClient.WaitForReplicaGtid(ctx, waitPos, multiClusterCatchUpTimeout)
	if errors.Is(err, sql.ErrWaitReplicaTimeout) {
		r.Recorder.Eventf(mdb, nil, corev1.EventTypeNormal, mariadbv1alpha1.ReasonMultiClusterPromotionFenced,
			mariadbv1alpha1.ActionReconciling,
			"Promotion fenced: not yet caught up with outgoing primary '%s'", outgoingPrimary)
		logger.Info(
			"Promotion fenced: not yet caught up with outgoing multi-cluster primary, requeuing...",
			"member", outgoingPrimary, "gtid", waitPos,
		)
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("error waiting for GTID %s from outgoing primary: %v", waitPos, err)
	}
	logger.Info(
		"Caught up with outgoing multi-cluster primary, proceeding with promotion",
		"member", outgoingPrimary, "gtid", waitPos,
	)
	return true, nil
}

// eventPromotionFencedUnverifiable records that a promotion is fenced because the outgoing
// primary cannot be verified. Warning severity: this needs a human — either the outgoing site
// recovers (planned switchover resumes) or the user forces the promotion (unplanned failover).
// The event message is kept stable (no error string) so Kubernetes aggregates repeats.
func (r *MariaDBReconciler) eventPromotionFencedUnverifiable(mdb *mariadbv1alpha1.MariaDB, outgoingPrimary string) {
	r.Recorder.Eventf(mdb, nil, corev1.EventTypeWarning, mariadbv1alpha1.ReasonMultiClusterPromotionFenced,
		mariadbv1alpha1.ActionReconciling,
		"Promotion fenced: outgoing primary '%s' cannot be verified. "+
			"If it is permanently gone (unplanned failover), annotate this MariaDB with %s=\"true\" to force the promotion, "+
			"losing writes not yet replicated from it",
		outgoingPrimary, metadata.ForcePromoteAnnotation)
}

// filterOutDomain drops the GTIDs of the given replication domain from a GTID position.
func filterOutDomain(rawPos string, domainId uint32) (string, error) {
	gtids, err := replication.ParseAllGtids(rawPos)
	if err != nil {
		return "", err
	}
	filtered := make([]replication.Gtid, 0, len(gtids))
	for _, gtid := range gtids {
		if gtid.DomainID != domainId {
			filtered = append(filtered, gtid)
		}
	}
	return replication.GtidsToString(filtered...), nil
}

func (r *MariaDBReconciler) getExternalMemberClient(ctx context.Context, mdb *mariadbv1alpha1.MariaDB,
	member string) (*sql.Client, error) {
	externalMariaDBRef, err := mdb.Spec.MultiCluster.GetExternalMariaDBRefForMember(member)
	if err != nil {
		return nil, fmt.Errorf("error finding externalMariaDBRef for member %s: %v", member, err)
	}
	externalMariaDB, err := r.RefResolver.ExternalMariaDB(ctx, externalMariaDBRef, mdb.Namespace)
	if err != nil {
		return nil, fmt.Errorf("error getting ExternalMariaDB for member %s: %v", member, err)
	}
	externalClient, err := sql.NewClientWithMariaDB(ctx, externalMariaDB, r.RefResolver)
	if err != nil {
		return nil, fmt.Errorf("error creating client for member %s: %v", member, err)
	}
	return externalClient, nil
}

func (r *MariaDBReconciler) shouldReconcileMultiCluster(ctx context.Context, mdb *mariadbv1alpha1.MariaDB,
	logger logr.Logger) (bool, error) {
	if !mdb.IsMultiClusterEnabled() {
		return false, nil
	}
	if mdb.Status.CurrentPrimary == nil || mdb.Status.CurrentPrimaryPodIndex == nil {
		logger.V(1).Info("Current MariaDB primary not set, skipping multi-cluster reconciliation...")
		return false, nil
	}
	if mdb.HasPendingHATopologyConfiguration() ||
		mdb.IsSwitchingPrimary() || mdb.IsReplicationSwitchoverRequired() ||
		mdb.HasGaleraNotReadyCondition() ||
		mdb.IsInitializing() || mdb.IsScalingOut() || mdb.IsRestoringBackup() || mdb.IsResizingStorage() || mdb.IsUpdating() ||
		mdb.HasPendingBinlogReplay() {
		logger.V(1).Info("Ongoing MariaDB operation detected, skipping multi-cluster reconciliation...")
		return false, nil
	}
	if mdb.IsMaxScaleEnabled() {
		mxs, err := r.RefResolver.MaxScale(ctx, mdb.Spec.MaxScaleRef, mdb.Namespace)
		if err != nil {
			return false, fmt.Errorf("error getting MaxScale: %v", err)
		}
		if mxs.IsSwitchingPrimary() {
			logger.V(1).Info("Ongoing MaxScale switchover detected, skipping multi-cluster reconciliation...")
			return false, nil
		}
	}
	return true, nil
}
