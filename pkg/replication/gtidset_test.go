package replication

import (
	"testing"
)

func TestParseGtidSet(t *testing.T) {
	tests := []struct {
		name     string
		raw      string
		expected string
		wantErr  bool
	}{
		{
			name:     "empty is a valid position",
			raw:      "",
			expected: "",
		},
		{
			name:     "single domain",
			raw:      "0-10-95",
			expected: "0-10-95",
		},
		{
			name:     "multi domain sorted deterministically",
			raw:      "2-30-7,0-10-95",
			expected: "0-10-95,2-30-7",
		},
		{
			name:    "duplicate domain is invalid",
			raw:     "0-10-95,0-11-7",
			wantErr: true,
		},
		{
			name:    "malformed",
			raw:     "0-10",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			set, err := ParseGtidSet(tt.raw)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got none")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got := set.String(); got != tt.expected {
				t.Errorf("String() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestGtidSetAheadOrEqual(t *testing.T) {
	mustParse := func(raw string) GtidSet {
		set, err := ParseGtidSet(raw)
		if err != nil {
			t.Fatalf("parsing %q: %v", raw, err)
		}
		return set
	}
	tests := []struct {
		name     string
		s        string
		o        string
		expected bool
	}{
		{
			name:     "equal single domain",
			s:        "0-10-95",
			o:        "0-10-95",
			expected: true,
		},
		{
			name:     "ahead in the only domain",
			s:        "0-10-96",
			o:        "0-10-95",
			expected: true,
		},
		{
			name:     "behind in the only domain",
			s:        "0-10-94",
			o:        "0-10-95",
			expected: false,
		},
		{
			// the multi-cluster case a local-domain-only comparison gets wrong: equal in the
			// local domain (0) but behind in the foreign domain (2) carrying the data
			name:     "behind in a foreign domain",
			s:        "0-10-95,2-30-5",
			o:        "0-10-95,2-30-9",
			expected: false,
		},
		{
			name:     "ahead in every domain",
			s:        "0-10-96,2-30-9",
			o:        "0-10-95,2-30-5",
			expected: true,
		},
		{
			// a replica bootstrapped from another cluster's backup has no local-domain GTID
			// yet still covers everything the other position has
			name:     "missing domain in the other position is irrelevant",
			s:        "2-30-9",
			o:        "2-30-5",
			expected: true,
		},
		{
			name:     "missing domain counts as behind",
			s:        "0-10-95",
			o:        "0-10-95,2-30-5",
			expected: false,
		},
		{
			name:     "anything is ahead of the empty position",
			s:        "",
			o:        "",
			expected: true,
		},
		{
			name:     "empty position is behind any non-empty one",
			s:        "",
			o:        "0-10-1",
			expected: false,
		},
		{
			// diverged: each side ahead in a different domain — both directions false
			name:     "diverged positions are not comparable",
			s:        "0-10-96,2-30-5",
			o:        "0-10-95,2-30-9",
			expected: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mustParse(tt.s).AheadOrEqual(mustParse(tt.o)); got != tt.expected {
				t.Errorf("AheadOrEqual(%q, %q) = %v, want %v", tt.s, tt.o, got, tt.expected)
			}
		})
	}
}
