package binlog

import (
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/datastructures"
	mariadbrepl "github.com/mariadb-operator/mariadb-operator/v26/pkg/replication"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	BinlogIndexV1 = "v1"
	ErrNoBinlogs  = errors.New("no binlogs available")
)

type BinlogIndex struct {
	APIVersion string `json:"apiVersion"`
	// Binlogs indexed by server ID
	Binlogs map[string][]BinlogMetadata `json:"binlogs"`
}

func (b *BinlogIndex) Exists(serverId uint32, binlog string) bool {
	binlogs, ok := b.Binlogs[serverKey(serverId)]
	if !ok {
		return false
	}
	return datastructures.Any(binlogs, func(meta BinlogMetadata) bool {
		return meta.BinlogFilename == binlog
	})
}

func NewBinlogIndex() *BinlogIndex {
	return &BinlogIndex{
		APIVersion: BinlogIndexV1,
	}
}

func (b *BinlogIndex) Add(serverId uint32, meta BinlogMetadata) {
	if b.Binlogs == nil {
		b.Binlogs = make(map[string][]BinlogMetadata)
	}
	b.Binlogs[serverKey(serverId)] = append(b.Binlogs[serverKey(serverId)], meta)
}

func (b *BinlogIndex) BuildTimeline(startGtid mariadbrepl.GtidSet, targetTime time.Time, strictMode bool,
	logger logr.Logger) ([]BinlogMetadata, error) {
	currentServerKey, err := b.startServerKey(startGtid)
	if err != nil {
		return nil, err
	}
	return b.buildTimelineWithBinlogs(nil, currentServerKey, startGtid, targetTime, strictMode, logger)
}

// startServerKey selects the server bucket to start the timeline from. A GTID position
// references one server per domain: the bucket of any of them continues the position (relayed
// foreign-domain servers are not archived here, so typically exactly one bucket exists).
// Domains are tried in ascending order for determinism.
func (b *BinlogIndex) startServerKey(startGtid mariadbrepl.GtidSet) (string, error) {
	domains := make([]uint32, 0, len(startGtid))
	for domain := range startGtid {
		domains = append(domains, domain)
	}
	sort.Slice(domains, func(i, j int) bool { return domains[i] < domains[j] })

	for _, domain := range domains {
		gtid := startGtid[domain]
		key := serverKey(gtid.ServerID)
		if _, ok := b.Binlogs[key]; ok {
			return key, nil
		}
	}
	return "", fmt.Errorf("binlogs for servers in start position %q not found: %w", startGtid.String(), ErrNoBinlogs)
}

func (b *BinlogIndex) buildTimelineWithBinlogs(binlogs []BinlogMetadata, currentServerKey string, startGtid mariadbrepl.GtidSet,
	targetTime time.Time, strictMode bool, binlogLogger logr.Logger) ([]BinlogMetadata, error) {
	logger := binlogLogger.WithValues(
		"num-binlogs", len(binlogs),
		"start-gtid", startGtid.String(),
		"target-time", targetTime.Format(time.RFC3339),
		"strict-mode", strictMode,
		"server", currentServerKey,
	)
	logger.Info("Building binlog timeline")

	binlogsToProcess, ok := b.Binlogs[currentServerKey]
	if !ok {
		return nil, fmt.Errorf("binlogs for server %s not found: %w", currentServerKey, ErrNoBinlogs)
	}
	hasReachedTargetTime := false
	var currentTime metav1.Time

	for _, binlog := range binlogsToProcess {
		// next binlog is out of time range, done!
		if binlog.FirstTime.After(targetTime) {
			logger.V(1).Info(
				"Next binlog is out of time range. Done.",
				"binlog", binlog.BinlogFilename,
				"time", binlog.FirstTime.Format(time.RFC3339),
			)
			hasReachedTargetTime = true
			break
		}

		shouldFilter, err := shouldFilterBinlog(&binlog, startGtid, targetTime, logger)
		if err != nil {
			return nil, fmt.Errorf("error determining whether binlog %s should be filtered: %v", binlog.BinlogFilename, err)
		}
		if shouldFilter {
			continue
		}

		if len(binlogs) > 0 {
			lastBinlog := binlogs[len(binlogs)-1]
			// there is a GTID gap, for example: 0-10-7 ... 0-10-11
			// we should continue in another server, for example: 0-11-8
			gtidGap, err := hasGtidGap(&lastBinlog, &binlog)
			if err != nil {
				return nil, fmt.Errorf("error determining GTID gap: %v", err)
			}
			if gtidGap {
				logger.Info(
					"GTID gap detected. Attempting to find next GTID in another server...",
					"processed-binlog", lastBinlog.BinlogFilename,
					"processed-gtid", lastBinlog.LastGtidSet().String(),
					"binlog", binlog.BinlogFilename,
					"gtid", binlog.FirstGtidSet().String(),
				)
				nextServerKey, err := b.findNextServer(&lastBinlog, currentServerKey, targetTime, logger.WithName("gtid-gap"))
				if err != nil {
					return nil, fmt.Errorf("unable to find next server: %v", err)
				}
				if nextServerKey == "" {
					break // stop processing binlogs when a gap is detected
				}
				return b.buildTimelineWithBinlogs(binlogs, nextServerKey, lastBinlog.LastGtidSet(), targetTime, strictMode, logger)
			}
		}

		binlogs = append(binlogs, binlog)
		currentTime = binlog.LastTime

		if binlog.LastTime.Time.Equal(targetTime) {
			logger.V(1).Info(
				"Found binlog with exact target time. Done.",
				"binlog", binlog.BinlogFilename,
				"time", binlog.LastTime.Format(time.RFC3339),
			)
			hasReachedTargetTime = true
			break
		}
	}
	if len(binlogs) == 0 {
		return nil, ErrNoBinlogs
	}
	if !hasReachedTargetTime {
		if strictMode {
			return nil, fmt.Errorf(
				"timeline did not reach target time: %s, last recoverable time: %s",
				targetTime.Format(time.RFC3339),
				currentTime.Format(time.RFC3339),
			)
		}
		logger.Info(
			"Timeline did not reach target time.",
			"target-time", targetTime.Format(time.RFC3339),
			"last-recoverable-time", currentTime.Format(time.RFC3339),
		)
	}
	return binlogs, nil
}

// findNextServer looks for a server bucket, other than the current one, containing a binlog
// that continues the last processed binlog without a GTID gap. Buckets are iterated in sorted
// order for determinism. An empty key is returned when no continuation exists.
func (b *BinlogIndex) findNextServer(lastBinlog *BinlogMetadata, currentServer string, untilTime time.Time,
	logger logr.Logger) (string, error) {
	if lastBinlog == nil {
		return "", errors.New("last processed binlog must be set")
	}
	if lastBinlog.LastGtid == nil {
		logger.Info("Last processed binlog must have last GTID set. Skipping...", "binlog", lastBinlog.BinlogFilename)
		return "", nil
	}
	if !lastBinlog.StopEvent {
		logger.Info("Last processed binlog must have a stop event. Skipping...", "binlog", lastBinlog.BinlogFilename)
		return "", nil
	}
	serverKeys := make([]string, 0, len(b.Binlogs))
	for key := range b.Binlogs {
		serverKeys = append(serverKeys, key)
	}
	sort.Strings(serverKeys)

	for _, serverKey := range serverKeys {
		if serverKey == currentServer {
			continue
		}
		for _, binlog := range b.Binlogs[serverKey] {
			shouldFilter, err := shouldFilterBinlog(&binlog, lastBinlog.LastGtidSet(), untilTime, logger)
			if err != nil {
				return "", fmt.Errorf("error determining whether binlog %s should be filtered: %v", binlog.BinlogFilename, err)
			}
			if shouldFilter {
				continue
			}

			gtidGap, err := hasGtidGap(lastBinlog, &binlog)
			if err != nil {
				return "", fmt.Errorf("error determining GTID gap: %v", err)
			}
			if !gtidGap {
				return serverKey, nil
			}
		}
	}
	return "", nil
}

func serverKey(serverId uint32) string {
	return fmt.Sprintf("server-%d", serverId)
}

func shouldFilterBinlog(binlog *BinlogMetadata, fromGtid mariadbrepl.GtidSet, untilTime time.Time, binlogLogger logr.Logger) (bool, error) {
	logger := binlogLogger.WithValues(
		"binlog", binlog.BinlogFilename,
		"time", binlog.FirstTime.Format(time.RFC3339),
		"start-gtid", fromGtid.String(),
		"target-time", untilTime.Format(time.RFC3339),
	).V(1)
	logger.Info("Processing binlog")

	// only binlogs with GTID events are considered
	if binlog.FirstGtid == nil || binlog.LastGtid == nil {
		logger.Info("Skipping binlog, as it does not have GTID events")
		return true, nil
	}
	logger = logger.WithValues(
		"gtid", binlog.LastGtidSet().String(),
	)

	// the binlog is skippable when the start position already covers its newest event in
	// every domain: replay would contribute nothing
	if fromGtid.AheadOrEqual(binlog.LastGtidSet()) {
		logger.Info("Skipping binlog, as it has older GTID events")
		return true, nil
	}

	if binlog.FirstTime.After(untilTime) {
		logger.Info("Skipping binlog, as it is out of time range")
		return true, nil
	}
	return false, nil
}

// hasGtidGap detects missing events between two consecutive binlogs: within a domain,
// sequence numbers of consecutive events differ by 1, so a larger step means a lost binlog.
// Domains only present in the next binlog carry no continuity evidence (a domain can
// legitimately be absent from a binlog) and are not treated as gaps.
func hasGtidGap(lastBinlog, nextBinlog *BinlogMetadata) (bool, error) {
	if lastBinlog == nil || lastBinlog.LastGtid == nil {
		return false, errors.New("last processed binlog must have last GTID set")
	}
	if nextBinlog == nil || nextBinlog.FirstGtid == nil {
		return false, errors.New("next binlog must have first GTID set")
	}
	lastGtids := lastBinlog.LastGtidSet()
	for domain, nextGtid := range nextBinlog.FirstGtidSet() {
		lastGtid, ok := lastGtids[domain]
		if !ok {
			continue
		}
		if nextGtid.SequenceID > lastGtid.SequenceID+1 {
			return true, nil
		}
	}
	return false, nil
}

type BinlogNum struct {
	filename string
	num      int
}

func ParseBinlogNum(filename string) (*BinlogNum, error) {
	p := strings.LastIndexAny(filename, ".")
	if p < 0 {
		return nil, fmt.Errorf("unexpected binlog name: %v", filename)
	}
	num, err := strconv.Atoi(filename[p+1:])
	if err != nil {
		return nil, fmt.Errorf("unexpected binlog name: %v", filename)
	}
	return &BinlogNum{filename: filename, num: num}, nil
}

func (b *BinlogNum) String() string {
	return fmt.Sprintf("BinlogNum{filename: %s, num: %d}", b.filename, b.num)
}

func (b *BinlogNum) LessThan(other *BinlogNum) bool {
	return b.num < other.num
}

func (b *BinlogNum) Equal(other *BinlogNum) bool {
	return b.num == other.num
}

type BinlogMetadata struct {
	ServerId       uint32              `json:"serverId"`
	ServerVersion  string              `json:"serverVersion"`
	BinlogVersion  uint16              `json:"binlogVersion"`
	BinlogFilename string              `json:"binlogFilename"`
	LogPosition    uint32              `json:"logPosition"`
	FirstTime      metav1.Time         `json:"firstTime"`
	LastTime       metav1.Time         `json:"lastTime"`
	PreviousGtids  []*mariadbrepl.Gtid `json:"previousGtids,omitempty"`
	FirstGtid      *mariadbrepl.Gtid   `json:"firstGtid,omitempty"`
	LastGtid       *mariadbrepl.Gtid   `json:"lastGtid,omitempty"`
	// FirstGtids and LastGtids track the first and last GTID event per replication domain:
	// with multiple domains (e.g. multi-cluster with log_slave_updates) events of several
	// domains interleave within a binlog, and the overall first/last event alone cannot
	// answer ordering or continuity questions for the other domains.
	FirstGtids  []*mariadbrepl.Gtid `json:"firstGtids,omitempty"`
	LastGtids   []*mariadbrepl.Gtid `json:"lastGtids,omitempty"`
	RotateEvent bool                `json:"rotateEvent"`
	StopEvent   bool                `json:"stopEvent"`
}

// FirstGtidSet returns the first GTID per domain. Indexes written before multi-domain
// support only carry the overall first/last GTID event: fall back to a single-domain set.
func (b *BinlogMetadata) FirstGtidSet() mariadbrepl.GtidSet {
	return gtidSetOf(b.FirstGtids, b.FirstGtid)
}

// LastGtidSet returns the last GTID per domain, falling back like FirstGtidSet.
func (b *BinlogMetadata) LastGtidSet() mariadbrepl.GtidSet {
	return gtidSetOf(b.LastGtids, b.LastGtid)
}

func gtidSetOf(gtids []*mariadbrepl.Gtid, fallback *mariadbrepl.Gtid) mariadbrepl.GtidSet {
	set := make(mariadbrepl.GtidSet, len(gtids))
	for _, gtid := range gtids {
		if gtid != nil {
			set[gtid.DomainID] = *gtid
		}
	}
	if len(set) == 0 && fallback != nil {
		set[fallback.DomainID] = *fallback
	}
	return set
}

func (b *BinlogMetadata) ObjectStoragePath() string {
	return fmt.Sprintf("%s/%s", serverKey(b.ServerId), b.BinlogFilename)
}

func GetBinlogMetadata(binlogPath string, logger logr.Logger) (*BinlogMetadata, error) {
	parser := replication.NewBinlogParser()
	parser.SetFlavor(mysql.MariaDBFlavor)
	parser.SetVerifyChecksum(false)
	parser.SetRawMode(true)

	meta := BinlogMetadata{
		BinlogFilename: filepath.Base(binlogPath),
	}
	var (
		rawFormatDescriptionEvent []byte
		rawGtidListEvent          []byte
		firstGtidByDomain         = make(map[uint32]*mariadbrepl.Gtid)
		lastGtidByDomain          = make(map[uint32]*mariadbrepl.Gtid)
	)

	if err := parser.ParseFile(binlogPath, 0, func(e *replication.BinlogEvent) error {
		// The first event (format description) is written by the server owning the binlog.
		// Later events carry the ORIGINATING server's ID: with log_slave_updates, relayed
		// events from other servers would otherwise make the metadata's server flap.
		if meta.ServerId == 0 {
			meta.ServerId = e.Header.ServerID
		}
		meta.LogPosition = e.Header.LogPos

		// See: https://mariadb.com/docs/server/reference/clientserver-protocol/replication-protocol
		switch e.Header.EventType {
		case replication.FORMAT_DESCRIPTION_EVENT:
			rawFormatDescriptionEvent = e.RawData
		case replication.MARIADB_GTID_LIST_EVENT:
			rawGtidListEvent = e.RawData
		case replication.MARIADB_GTID_EVENT:
			gtidEvent, err := decodeGTIDEvent(e.RawData, e.Header.ServerID)
			if err != nil {
				return fmt.Errorf("error decoding GTID event: %v", err)
			}
			gtid, err := toMariadbGtid(&gtidEvent.GTID)
			if err != nil {
				return err
			}
			if meta.FirstGtid == nil {
				meta.FirstGtid = gtid
			}
			meta.LastGtid = gtid
			if _, ok := firstGtidByDomain[gtid.DomainID]; !ok {
				firstGtidByDomain[gtid.DomainID] = gtid
			}
			lastGtidByDomain[gtid.DomainID] = gtid
		case replication.ROTATE_EVENT:
			meta.RotateEvent = true
		case replication.STOP_EVENT:
			meta.StopEvent = true
		}
		if meta.FirstTime == (metav1.Time{}) {
			meta.FirstTime = metav1.NewTime(time.Unix(int64(e.Header.Timestamp), 0))
		}
		meta.LastTime = metav1.NewTime(time.Unix(int64(e.Header.Timestamp), 0))

		return nil
	}); err != nil {
		return nil, fmt.Errorf("error getting binlog metadata: %v", err)
	}
	meta.FirstGtids = sortedGtidsByDomain(firstGtidByDomain)
	meta.LastGtids = sortedGtidsByDomain(lastGtidByDomain)

	if rawFormatDescriptionEvent != nil {
		formatDescription := &replication.FormatDescriptionEvent{}
		if err := formatDescription.Decode(rawFormatDescriptionEvent[replication.EventHeaderSize:]); err != nil {
			return nil, fmt.Errorf("error decoding format description event: %v", err)
		}
		meta.ServerVersion = formatDescription.ServerVersion
		meta.BinlogVersion = formatDescription.Version
	}

	if rawGtidListEvent != nil {
		listEvent := &replication.MariadbGTIDListEvent{}
		if err := listEvent.Decode(rawGtidListEvent[replication.EventHeaderSize:]); err != nil {
			return nil, fmt.Errorf("error decoding GTID list event: %v", err)
		}
		prevGtids := make([]*mariadbrepl.Gtid, len(listEvent.GTIDs))
		for i, gtid := range listEvent.GTIDs {
			gtid, err := toMariadbGtid(&gtid)
			if err != nil {
				return nil, err
			}
			prevGtids[i] = gtid
		}
		meta.PreviousGtids = prevGtids
	}

	return &meta, nil
}

func sortedGtidsByDomain(gtidsByDomain map[uint32]*mariadbrepl.Gtid) []*mariadbrepl.Gtid {
	if len(gtidsByDomain) == 0 {
		return nil
	}
	domains := make([]uint32, 0, len(gtidsByDomain))
	for domain := range gtidsByDomain {
		domains = append(domains, domain)
	}
	sort.Slice(domains, func(i, j int) bool { return domains[i] < domains[j] })

	gtids := make([]*mariadbrepl.Gtid, len(domains))
	for i, domain := range domains {
		gtids[i] = gtidsByDomain[domain]
	}
	return gtids
}

func decodeGTIDEvent(rawEvent []byte, serverId uint32) (*replication.MariadbGTIDEvent, error) {
	gtidEvent := &replication.MariadbGTIDEvent{}
	// See:
	// https://github.com/go-mysql-org/go-mysql/blob/a07c974ef5a34a8d0d7dfb543652c4ba2dec90cf/replication/parser.go#L149
	// https://github.com/wal-g/wal-g/blob/c98a8ea2d4afcb639e112164b7ce30316c4fbdb0/internal/databases/mysql/mysql_binlog.go#L76
	if err := gtidEvent.Decode(rawEvent[replication.EventHeaderSize:]); err != nil {
		return nil, err
	}
	// See:
	// https://github.com/go-mysql-org/go-mysql/blob/a07c974ef5a34a8d0d7dfb543652c4ba2dec90cf/replication/parser.go#L315
	// https://github.com/go-mysql-org/go-mysql/blob/a07c974ef5a34a8d0d7dfb543652c4ba2dec90cf/replication/event.go#L876
	gtidEvent.GTID.ServerID = serverId
	return gtidEvent, nil
}

func toMariadbGtid(mysqlGtid *mysql.MariadbGTID) (*mariadbrepl.Gtid, error) {
	rawGtid := mysqlGtid.String()
	gtid, err := mariadbrepl.ParseGtid(rawGtid)
	if err != nil {
		return nil, fmt.Errorf("error parsing GTID %s: %v", rawGtid, err)
	}
	return gtid, nil
}
