package nntpDirectSearch

import (
	"testing"
	"time"
)

func TestWithinBoundaryTolerance(t *testing.T) {
	base := time.Date(2025, time.January, 16, 11, 0, 0, 0, time.FixedZone("CET", 3600))

	tests := []struct {
		name     string
		forFirst bool
		target   time.Time
		mean     time.Time
		tol      uint
		want     bool
	}{
		{
			name:     "first search accepts exact equality",
			forFirst: true,
			target:   base,
			mean:     base,
			tol:      60,
			want:     true,
		},
		{
			name:     "last search accepts exact equality",
			forFirst: false,
			target:   base,
			mean:     base,
			tol:      60,
			want:     true,
		},
		{
			name:     "first search accepts target shortly before mean",
			forFirst: true,
			target:   base,
			mean:     base.Add(30 * time.Second),
			tol:      60,
			want:     true,
		},
		{
			name:     "last search accepts target shortly after mean",
			forFirst: false,
			target:   base,
			mean:     base.Add(-30 * time.Second),
			tol:      60,
			want:     true,
		},
		{
			name:     "wrong side of mean is rejected",
			forFirst: false,
			target:   base,
			mean:     base.Add(30 * time.Second),
			tol:      60,
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := withinBoundaryTolerance(tt.forFirst, tt.target, tt.mean, tt.tol)
			if got != tt.want {
				t.Fatalf("withinBoundaryTolerance(%v, %v, %v, %d) = %v, want %v", tt.forFirst, tt.target, tt.mean, tt.tol, got, tt.want)
			}
		})
	}
}
