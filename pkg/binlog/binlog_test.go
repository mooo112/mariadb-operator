package binlog

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/google/go-cmp/cmp"
	mariadbrepl "github.com/mariadb-operator/mariadb-operator/v26/pkg/replication"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

func TestBuildTimeline(t *testing.T) {
	tests := []struct {
		name       string
		indexFile  *BinlogIndex
		startGtid  mariadbrepl.GtidSet
		targetTime time.Time
		strictMode bool
		wantPath   []string
		wantErr    bool
	}{
		{
			name:       "single binlog",
			indexFile:  mustParseTestFile(t, "single-binlog.yaml"),
			startGtid:  mustParseGtidSet(t, "0-10-1"),
			targetTime: time.Now(),
			strictMode: false,
			wantPath: []string{
				"server-10/mariadb-repl-bin.000002",
			},
			wantErr: false,
		},
		{
			name:       "single binlog - strict",
			indexFile:  mustParseTestFile(t, "single-binlog.yaml"),
			startGtid:  mustParseGtidSet(t, "0-10-1"),
			targetTime: time.Now(),
			strictMode: true,
			wantPath:   nil,
			wantErr:    true,
		},
		{
			name:       "multiple binlogs",
			indexFile:  mustParseTestFile(t, "multiple-binlogs.yaml"),
			startGtid:  mustParseGtidSet(t, "0-10-1"),
			targetTime: mustParseDate(t, "2026-01-20T11:11:26Z"),
			strictMode: false,
			wantPath: []string{
				"server-10/mariadb-repl-bin.000002",
				"server-10/mariadb-repl-bin.000003",
				"server-10/mariadb-repl-bin.000004",
				"server-10/mariadb-repl-bin.000005",
				"server-10/mariadb-repl-bin.000006",
				"server-10/mariadb-repl-bin.000007",
				"server-10/mariadb-repl-bin.000008",
				"server-10/mariadb-repl-bin.000009",
				"server-10/mariadb-repl-bin.000010",
				"server-10/mariadb-repl-bin.000011",
			},
			wantErr: false,
		},
		{
			name:       "multiple binlogs - strict",
			indexFile:  mustParseTestFile(t, "multiple-binlogs.yaml"),
			startGtid:  mustParseGtidSet(t, "0-10-1"),
			targetTime: time.Now(),
			strictMode: true,
			wantPath:   nil,
			wantErr:    true,
		},
		{
			name:       "filter by server-10 gtid and date",
			indexFile:  mustParseTestFile(t, "failover-1205-1208.yaml"),
			startGtid:  mustParseGtidSet(t, "0-10-40"),
			targetTime: mustParseDate(t, "2026-02-04T12:05:00Z"),
			strictMode: true,
			wantPath: []string{
				"server-10/mariadb-repl-bin.000004",
				"server-10/mariadb-repl-bin.000005",
				"server-10/mariadb-repl-bin.000006",
			},
			wantErr: false,
		},
		{
			name:       "filter by server-11 gtid and date",
			indexFile:  mustParseTestFile(t, "failover-1205-1208.yaml"),
			startGtid:  mustParseGtidSet(t, "0-11-100"),
			targetTime: mustParseDate(t, "2026-02-04T12:06:56Z"),
			strictMode: true,
			wantPath: []string{
				"server-11/mariadb-repl-bin.000002",
				"server-11/mariadb-repl-bin.000003",
				"server-11/mariadb-repl-bin.000004",
				"server-11/mariadb-repl-bin.000005",
				"server-11/mariadb-repl-bin.000006",
			},
			wantErr: false,
		},
		{
			name:       "failover",
			indexFile:  mustParseTestFile(t, "failover.yaml"),
			startGtid:  mustParseGtidSet(t, "0-10-1"),
			targetTime: time.Now(),
			strictMode: false,
			wantPath: []string{
				"server-10/mariadb-repl-bin.000002",
				"server-10/mariadb-repl-bin.000003",
				"server-10/mariadb-repl-bin.000004",
				"server-10/mariadb-repl-bin.000005",
			},
			wantErr: false,
		},
		{
			name:       "failover - strict",
			indexFile:  mustParseTestFile(t, "failover.yaml"),
			startGtid:  mustParseGtidSet(t, "0-10-1"),
			targetTime: time.Now(),
			strictMode: true,
			wantPath:   nil,
			wantErr:    true,
		},
		{
			name:       "failover no stop event",
			indexFile:  mustParseTestFile(t, "failover-no-stop-event.yaml"),
			startGtid:  mustParseGtidSet(t, "0-10-1"),
			targetTime: time.Now(),
			strictMode: false,
			wantPath: []string{
				"server-10/mariadb-repl-bin.000002",
				"server-10/mariadb-repl-bin.000003",
				"server-10/mariadb-repl-bin.000004",
				"server-10/mariadb-repl-bin.000005",
				"server-10/mariadb-repl-bin.000006",
			},
			wantErr: false,
		},
		{
			name:       "failover no stop event - strict",
			indexFile:  mustParseTestFile(t, "failover-no-stop-event.yaml"),
			startGtid:  mustParseGtidSet(t, "0-10-1"),
			targetTime: time.Now(),
			strictMode: true,
			wantPath:   nil,
			wantErr:    true,
		},
		{
			name:       "failover at 12:05",
			indexFile:  mustParseTestFile(t, "failover-1205-1208.yaml"),
			startGtid:  mustParseGtidSet(t, "0-10-1"),
			targetTime: mustParseDate(t, "2026-02-04T12:06:39Z"),
			strictMode: false,
			wantPath: []string{
				"server-10/mariadb-repl-bin.000002",
				"server-10/mariadb-repl-bin.000003",
				"server-10/mariadb-repl-bin.000004",
				"server-10/mariadb-repl-bin.000005",
				"server-10/mariadb-repl-bin.000006",
				// FAILOVER to server-11
				"server-11/mariadb-repl-bin.000001",
				"server-11/mariadb-repl-bin.000002",
				"server-11/mariadb-repl-bin.000003",
				"server-11/mariadb-repl-bin.000004",
				"server-11/mariadb-repl-bin.000005",
			},
			wantErr: false,
		},
		{
			name:       "failover at 12:05 - strict",
			indexFile:  mustParseTestFile(t, "failover-1205-1208.yaml"),
			startGtid:  mustParseGtidSet(t, "0-10-1"),
			targetTime: mustParseDate(t, "2026-02-04T12:06:39Z"),
			strictMode: true,
			wantPath: []string{
				"server-10/mariadb-repl-bin.000002",
				"server-10/mariadb-repl-bin.000003",
				"server-10/mariadb-repl-bin.000004",
				"server-10/mariadb-repl-bin.000005",
				"server-10/mariadb-repl-bin.000006",
				// FAILOVER to server-11
				"server-11/mariadb-repl-bin.000001",
				"server-11/mariadb-repl-bin.000002",
				"server-11/mariadb-repl-bin.000003",
				"server-11/mariadb-repl-bin.000004",
				"server-11/mariadb-repl-bin.000005",
				// FAILOVER to server-10
			},
			wantErr: false,
		},
		{
			name:       "failover at 12:05 and 12:08",
			indexFile:  mustParseTestFile(t, "failover-1205-1208.yaml"),
			startGtid:  mustParseGtidSet(t, "0-10-1"),
			targetTime: mustParseDate(t, "2026-02-04T12:08:32Z"), // server-10/mariadb-repl-bin.000008
			strictMode: false,
			wantPath: []string{
				"server-10/mariadb-repl-bin.000002",
				"server-10/mariadb-repl-bin.000003",
				"server-10/mariadb-repl-bin.000004",
				"server-10/mariadb-repl-bin.000005",
				"server-10/mariadb-repl-bin.000006",
				// FAILOVER to server-11
				"server-11/mariadb-repl-bin.000001",
				"server-11/mariadb-repl-bin.000002",
				"server-11/mariadb-repl-bin.000003",
				"server-11/mariadb-repl-bin.000004",
				"server-11/mariadb-repl-bin.000005",
				"server-11/mariadb-repl-bin.000006",
				"server-11/mariadb-repl-bin.000007",
				"server-11/mariadb-repl-bin.000008",
				"server-11/mariadb-repl-bin.000009",
				// FAILOVER to server-10
			},
			wantErr: false,
		},
		{
			name:       "failover at 12:05 and 12:08 - strict",
			indexFile:  mustParseTestFile(t, "failover-1205-1208.yaml"),
			startGtid:  mustParseGtidSet(t, "0-10-1"),
			targetTime: mustParseDate(t, "2026-02-04T12:08:32Z"), // server-10/mariadb-repl-bin.000008
			strictMode: true,
			wantPath:   nil,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			binlogMetas, err := tt.indexFile.BuildTimeline(tt.startGtid, tt.targetTime, tt.strictMode, logr.Discard())
			if tt.wantErr {
				assert.Error(t, err, "expected error")
			} else {
				assert.NoError(t, err, "unexpected error")
				if diff := cmp.Diff(getBinlogTimeline(binlogMetas), tt.wantPath); diff != "" {
					t.Errorf("unexpected binlog timeline (-want +got):\n%s", diff)
				}
			}
		})
	}
}

func TestErrNoBinlogs(t *testing.T) {
	binlogIndex := &BinlogIndex{
		APIVersion: BinlogIndexV1,
		Binlogs:    make(map[string][]BinlogMetadata),
	}
	startGtid := mariadbrepl.GtidSet{
		1: {DomainID: 1, ServerID: 1, SequenceID: 1},
	}
	targetTime := time.Now()

	result, err := binlogIndex.BuildTimeline(startGtid, targetTime, false, logr.Discard())

	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrNoBinlogs))
	assert.Nil(t, result)
}

func TestHasGtidGapMultiDomain(t *testing.T) {
	meta := func(firstGtids, lastGtids string) *BinlogMetadata {
		t.Helper()
		first := mustParseGtidSet(t, firstGtids)
		last := mustParseGtidSet(t, lastGtids)
		m := &BinlogMetadata{}
		for _, g := range first {
			g := g
			m.FirstGtids = append(m.FirstGtids, &g)
			m.FirstGtid = &g
		}
		for _, g := range last {
			g := g
			m.LastGtids = append(m.LastGtids, &g)
			m.LastGtid = &g
		}
		return m
	}
	tests := []struct {
		name     string
		last     *BinlogMetadata
		next     *BinlogMetadata
		expected bool
	}{
		{
			name:     "contiguous single domain",
			last:     meta("0-10-5", "0-10-9"),
			next:     meta("0-10-10", "0-10-15"),
			expected: false,
		},
		{
			name:     "gap in single domain",
			last:     meta("0-10-5", "0-10-9"),
			next:     meta("0-10-12", "0-10-15"),
			expected: true,
		},
		{
			// the case a single first/last event pair misses: continuity in the domain of the
			// boundary events, but a hole in the other domain's stream
			name:     "gap in foreign domain only",
			last:     meta("0-10-5,2-30-3", "0-10-9,2-30-4"),
			next:     meta("0-10-10,2-30-8", "0-10-15,2-30-9"),
			expected: true,
		},
		{
			name:     "contiguous multi domain",
			last:     meta("0-10-5,2-30-3", "0-10-9,2-30-4"),
			next:     meta("0-10-10,2-30-5", "0-10-15,2-30-9"),
			expected: false,
		},
		{
			// a domain appearing for the first time carries no continuity evidence
			name:     "new domain is not a gap",
			last:     meta("0-10-5", "0-10-9"),
			next:     meta("0-10-10,2-30-7", "0-10-15,2-30-9"),
			expected: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gap, err := hasGtidGap(tt.last, tt.next)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, gap)
		})
	}
}

func TestShouldFilterBinlogMultiDomain(t *testing.T) {
	meta := func(lastGtids string) *BinlogMetadata {
		t.Helper()
		last := mustParseGtidSet(t, lastGtids)
		m := &BinlogMetadata{FirstTime: metav1.NewTime(time.Now().Add(-time.Hour))}
		for _, g := range last {
			g := g
			m.LastGtids = append(m.LastGtids, &g)
			m.FirstGtid = &g
			m.LastGtid = &g
		}
		return m
	}
	tests := []struct {
		name     string
		binlog   *BinlogMetadata
		fromGtid string
		expected bool
	}{
		{
			name:     "start covers the binlog",
			binlog:   meta("0-10-9"),
			fromGtid: "0-10-12",
			expected: true,
		},
		{
			name:     "binlog has newer events",
			binlog:   meta("0-10-9"),
			fromGtid: "0-10-5",
			expected: false,
		},
		{
			// covered in the boundary-event domain but not in the foreign one: the binlog
			// still contributes foreign-domain events and must be replayed
			name:     "newer events in foreign domain only",
			binlog:   meta("0-10-9,2-30-9"),
			fromGtid: "0-10-12,2-30-5",
			expected: false,
		},
		{
			name:     "start covers every domain",
			binlog:   meta("0-10-9,2-30-9"),
			fromGtid: "0-10-12,2-30-12",
			expected: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := shouldFilterBinlog(tt.binlog, mustParseGtidSet(t, tt.fromGtid), time.Now(), logr.Discard())
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, filter)
		})
	}
}

func TestGtidSetFallback(t *testing.T) {
	// indexes written before multi-domain support only carry the single first/last GTID
	legacy := &BinlogMetadata{
		FirstGtid: &mariadbrepl.Gtid{DomainID: 0, ServerID: 10, SequenceID: 5},
		LastGtid:  &mariadbrepl.Gtid{DomainID: 0, ServerID: 10, SequenceID: 9},
	}
	assert.Equal(t, "0-10-5", legacy.FirstGtidSet().String())
	assert.Equal(t, "0-10-9", legacy.LastGtidSet().String())

	empty := &BinlogMetadata{}
	assert.Empty(t, empty.FirstGtidSet())
	assert.Empty(t, empty.LastGtidSet())
}

func mustParseTestFile(t *testing.T, file string) *BinlogIndex {
	t.Helper()
	testFile := filepath.Join("test", file)
	bytes, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("failed to parse test file %s: %v", testFile, err)
	}
	var bi BinlogIndex
	if err := yaml.Unmarshal(bytes, &bi); err != nil {
		t.Fatalf("failed to unmarshal test file %s: %v", testFile, err)
	}
	return &bi
}

func mustParseGtidSet(t *testing.T, s string) mariadbrepl.GtidSet {
	t.Helper()
	g, err := mariadbrepl.ParseGtidSet(s)
	if err != nil {
		t.Fatalf("failed to parse gtid %s: %v", s, err)
	}
	return g
}

func mustParseDate(t *testing.T, s string) time.Time {
	t.Helper()
	d, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatalf("failed to parse date %s: %v", s, err)
	}
	return d
}

func getBinlogTimeline(binlogMetas []BinlogMetadata) []string {
	path := make([]string, len(binlogMetas))
	for i, binlogMeta := range binlogMetas {
		path[i] = binlogMeta.ObjectStoragePath()
	}
	return path
}
