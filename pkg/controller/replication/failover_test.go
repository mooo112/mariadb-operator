package replication

import (
	"testing"

	"github.com/go-logr/logr"
	mariadbv1alpha1 "github.com/mariadb-operator/mariadb-operator/v26/api/v1alpha1"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/replication"
	"k8s.io/utils/ptr"
)

func mustGtidSet(t *testing.T, raw string) replication.GtidSet {
	t.Helper()
	set, err := replication.ParseGtidSet(raw)
	if err != nil {
		t.Fatalf("parsing %q: %v", raw, err)
	}
	return set
}

func TestFurthestAdvancedCandidate(t *testing.T) {
	tests := []struct {
		name       string
		candidates []struct{ name, pos string }
		expected   string
	}{
		{
			name: "single domain",
			candidates: []struct{ name, pos string }{
				{"mariadb-1", "0-10-95"},
				{"mariadb-2", "0-10-99"},
				{"mariadb-3", "0-10-97"},
			},
			expected: "mariadb-2",
		},
		{
			// the multi-cluster replica cluster case: all candidates equal in the local
			// domain (1), progress lives in the foreign domain (0)
			name: "foreign domain decides",
			candidates: []struct{ name, pos string }{
				{"mariadb-1", "1-20-5,0-10-95"},
				{"mariadb-2", "1-20-5,0-10-99"},
			},
			expected: "mariadb-2",
		},
		{
			// a candidate without the local domain (bootstrapped from the other cluster's
			// backup) must not be disqualified
			name: "candidate missing local domain still ranked",
			candidates: []struct{ name, pos string }{
				{"mariadb-1", "0-10-95"},
				{"mariadb-2", "0-10-99"},
			},
			expected: "mariadb-2",
		},
		{
			// diverged candidates: deterministic, keep the first (name-sorted) one
			name: "diverged positions keep first candidate",
			candidates: []struct{ name, pos string }{
				{"mariadb-1", "0-10-99,2-30-5"},
				{"mariadb-2", "0-10-95,2-30-9"},
			},
			expected: "mariadb-1",
		},
		{
			name: "all equal keeps a candidate",
			candidates: []struct{ name, pos string }{
				{"mariadb-1", "0-10-95"},
				{"mariadb-2", "0-10-95"},
			},
			expected: "mariadb-2",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &FailoverHandler{logger: logr.Discard()}
			candidates := make([]promotionCandidate, 0, len(tt.candidates))
			for _, c := range tt.candidates {
				candidates = append(candidates, promotionCandidate{
					name:           c.name,
					gtidCurrentPos: mustGtidSet(t, c.pos),
				})
			}
			got := f.furthestAdvancedCandidate(candidates)
			if got == nil {
				t.Fatal("expected a candidate, got nil")
			}
			if got.name != tt.expected {
				t.Errorf("furthestAdvancedCandidate() = %s, want %s", got.name, tt.expected)
			}
		})
	}
}

func TestHasRelayLogEvents(t *testing.T) {
	tests := []struct {
		name       string
		ioPos      string
		currentPos string
		expected   bool
	}{
		{
			name:       "drained single domain",
			ioPos:      "0-10-95",
			currentPos: "0-10-95",
			expected:   false,
		},
		{
			name:       "pending single domain",
			ioPos:      "0-10-99",
			currentPos: "0-10-95",
			expected:   true,
		},
		{
			// the case a local-domain-only comparison reports as drained: backlog lives in
			// the foreign domain (2)
			name:       "pending in foreign domain only",
			ioPos:      "0-10-95,2-30-9",
			currentPos: "0-10-95,2-30-5",
			expected:   true,
		},
		{
			name:       "drained multi domain",
			ioPos:      "0-10-95,2-30-9",
			currentPos: "0-10-95,2-30-9",
			expected:   false,
		},
		{
			// current position covering extra domains (e.g. own binlog domain) is fine
			name:       "extra domain in current position",
			ioPos:      "2-30-9",
			currentPos: "1-20-4,2-30-9",
			expected:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status := &mariadbv1alpha1.ReplicaStatusVars{
				GtidIOPos:      ptr.To(tt.ioPos),
				GtidCurrentPos: ptr.To(tt.currentPos),
			}
			got, err := HasRelayLogEvents(status, logr.Discard())
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.expected {
				t.Errorf("HasRelayLogEvents(io=%q, current=%q) = %v, want %v", tt.ioPos, tt.currentPos, got, tt.expected)
			}
		})
	}
}
