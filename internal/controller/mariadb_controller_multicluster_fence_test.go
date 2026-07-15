package controller

import (
	"testing"

	mariadbv1alpha1 "github.com/mariadb-operator/mariadb-operator/v26/api/v1alpha1"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/metadata"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestIsPromotionForced(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		expected    bool
	}{
		{
			name:        "no annotations",
			annotations: nil,
			expected:    false,
		},
		{
			name:        "annotation set",
			annotations: map[string]string{metadata.ForcePromoteAnnotation: "true"},
			expected:    true,
		},
		{
			// only an explicit "true" skips the fence: an empty or arbitrary value must not
			// accidentally forfeit it
			name:        "annotation with non-true value",
			annotations: map[string]string{metadata.ForcePromoteAnnotation: "yes"},
			expected:    false,
		},
		{
			name:        "annotation with empty value",
			annotations: map[string]string{metadata.ForcePromoteAnnotation: ""},
			expected:    false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mdb := &mariadbv1alpha1.MariaDB{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: tt.annotations,
				},
			}
			assert.Equal(t, tt.expected, isPromotionForced(mdb))
		})
	}
}

func TestFilterOutDomain(t *testing.T) {
	tests := []struct {
		name     string
		pos      string
		domainId uint32
		expected string
		wantErr  bool
	}{
		{
			// The fence case: don't wait on the promoted cluster's own domain (1),
			// only on the outgoing primary's foreign domains.
			name:     "drops own domain",
			pos:      "0-10-95,1-20-488",
			domainId: 1,
			expected: "0-10-95",
		},
		{
			name:     "keeps all when domain absent",
			pos:      "0-10-95,2-30-7",
			domainId: 1,
			expected: "0-10-95,2-30-7",
		},
		{
			name:     "single own domain yields empty",
			pos:      "1-20-488",
			domainId: 1,
			expected: "",
		},
		{
			name:     "invalid position",
			pos:      "1-20-488-extra",
			domainId: 1,
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := filterOutDomain(tt.pos, tt.domainId)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}
