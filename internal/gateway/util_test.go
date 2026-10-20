//go:build !integration
// +build !integration

package gateway

import (
	"testing"
	"time"
)

func TestRotateByTimes(t *testing.T) {
	now := time.Now()

	newTimes := func() []time.Time {
		return []time.Time{
			now.Add(-time.Hour),
			now.Add(-time.Minute),
			now.Add(-time.Second),
			now,
		}
	}

	testCases := []struct {
		name  string
		limit time.Time
		want  int
	}{
		{"drops-older-than-limit", now.Add(-5 * time.Minute), 3},
		{"keeps-everything", now.Add(-2 * time.Hour), 4},
		{"drops-everything", now.Add(time.Hour), 0},
		{"limit-equal-to-entry-is-kept", now.Add(-time.Minute), 3},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			times := newTimes()
			rotated := rotateByTime(times, tc.limit)
			if len(rotated) != tc.want {
				t.Fatalf("got %d entries, wants %d", len(rotated), tc.want)
			}
			for i := range rotated {
				if rotated[i].Before(tc.limit) {
					t.Errorf("entry %d (%s) is before the limit", i, rotated[i])
				}
			}
			if tc.want > 0 && !rotated[len(rotated)-1].Equal(now) {
				t.Errorf("the newest entry must be kept last")
			}
		})
	}
}
