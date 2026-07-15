package metadata

var (
	WatchLabel              = "k8s.mariadb.com/watch"
	PhysicalBackupNameLabel = "physicalbackup.k8s.mariadb.com/name"

	KubernetesServiceLabel                = "kubernetes.io/service-name"
	KubernetesEndpointSliceManagedByLabel = "endpointslice.kubernetes.io/managed-by"
	KubernetesEndpointSliceManagedByValue = "mariadb-operator.k8s.mariadb.com"
	KubernetesHostnameLabel               = "kubernetes.io/hostname"

	ReplicationAnnotation = "k8s.mariadb.com/replication"
	GtidAnnotation        = "k8s.mariadb.com/gtid"
	LastGtidAnnotation    = "k8s.mariadb.com/last-gtid"
	GaleraAnnotation      = "k8s.mariadb.com/galera"
	MariadbAnnotation     = "k8s.mariadb.com/mariadb"

	ConfigAnnotation       = "k8s.mariadb.com/config"
	ConfigTLSAnnotation    = "k8s.mariadb.com/config-tls"
	ConfigGaleraAnnotation = "k8s.mariadb.com/config-galera"

	TLSCAAnnotation           = "k8s.mariadb.com/ca"
	TLSServerCertAnnotation   = "k8s.mariadb.com/server-cert"
	TLSClientCertAnnotation   = "k8s.mariadb.com/client-cert"
	TLSAdminCertAnnotation    = "k8s.mariadb.com/admin-cert"
	TLSListenerCertAnnotation = "k8s.mariadb.com/listener-cert"

	WebhookConfigAnnotation = "k8s.mariadb.com/webhook"

	// ForcePromoteAnnotation, when set to "true" on a MariaDB, skips the multi-cluster promotion
	// catch-up fence: the cluster is promoted without verifying it applied everything the outgoing
	// primary binlogged. Intended for unplanned failover (outgoing primary unreachable or its
	// operator down); writes not yet replicated from the outgoing primary are lost on the promoted
	// cluster. The annotation is consumed (removed) once the promotion completes.
	ForcePromoteAnnotation = "k8s.mariadb.com/force-promote"

	MetaCtrlFieldPath = ".metadata.controller"
)
