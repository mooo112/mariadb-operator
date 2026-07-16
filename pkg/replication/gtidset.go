package replication

import (
	"fmt"
	"sort"
	"strings"
)

// GtidSet is a MariaDB GTID position: at most one GTID per replication domain.
// MariaDB positions (gtid_current_pos, gtid_binlog_pos, Gtid_IO_Pos) are comma-separated
// lists with one entry per domain. Ordering decisions must consider every domain present in
// the stream, not just the server's own gtid_domain_id: with multiple domains (e.g. one per
// cluster in a multi-cluster topology), data progress lives in the foreign domains, and a
// server whose GTID position lacks the local domain entirely (e.g. bootstrapped from another
// cluster's backup) is still a perfectly valid replica.
type GtidSet map[uint32]Gtid

// ParseGtidSet parses a raw MariaDB GTID position. An empty string yields an empty set:
// a freshly provisioned server legitimately has no position yet.
func ParseGtidSet(raw string) (GtidSet, error) {
	set := make(GtidSet)
	if strings.TrimSpace(raw) == "" {
		return set, nil
	}
	gtids, err := ParseAllGtids(raw)
	if err != nil {
		return nil, err
	}
	for _, gtid := range gtids {
		if _, ok := set[gtid.DomainID]; ok {
			return nil, fmt.Errorf("invalid GTID position %q: duplicate domain %d", raw, gtid.DomainID)
		}
		set[gtid.DomainID] = gtid
	}
	return set, nil
}

// String formats the set as a MariaDB GTID position, ordered by domain for determinism.
func (s GtidSet) String() string {
	domains := make([]uint32, 0, len(s))
	for domain := range s {
		domains = append(domains, domain)
	}
	sort.Slice(domains, func(i, j int) bool { return domains[i] < domains[j] })

	parts := make([]string, len(domains))
	for i, domain := range domains {
		gtid := s[domain]
		parts[i] = gtid.String()
	}
	return strings.Join(parts, ",")
}

// AheadOrEqual reports whether s has reached everything o has, in every domain present in o.
// A domain missing from s counts as behind. Note that this is a partial order: two sets can
// each be ahead in different domains (diverged), in which case both directions return false.
func (s GtidSet) AheadOrEqual(o GtidSet) bool {
	for domain, other := range o {
		gtid, ok := s[domain]
		if !ok || gtid.SequenceID < other.SequenceID {
			return false
		}
	}
	return true
}
