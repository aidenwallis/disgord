//go:build !integration
// +build !integration

package gateway

import (
	"testing"
	"time"
)

func TestShardMetric_ReconnectsSince(t *testing.T) {
	now := time.Now()

	// oldest first, all in the past - identifies can only be recorded after they happened
	times := []time.Time{
		now.Add(-time.Hour),
		now.Add(-time.Minute),
		now.Add(-time.Second),
		now.Add(-time.Millisecond),
	}

	metric := &IdentifyMetric{Reconnects: append([]time.Time(nil), times...)}

	if since := metric.ReconnectsSince(2 * time.Millisecond); since != 1 {
		t.Errorf("expected to count one entry. Got %d, wants %d", since, 1)
	}
	if since := metric.ReconnectsSince(2 * time.Second); since != 2 {
		t.Errorf("expected to count two entries. Got %d, wants %d", since, 2)
	}
	if since := metric.ReconnectsSince(2 * time.Hour); int(since) != len(times) {
		t.Errorf("expected to count every entry. Got %d, wants %d", since, len(times))
	}
}

func TestShardMetric_Prune(t *testing.T) {
	now := time.Now()
	metric := &IdentifyMetric{Reconnects: []time.Time{
		now.Add(-72 * time.Hour),
		now.Add(-49 * time.Hour),
		now.Add(-47 * time.Hour),
		now.Add(-time.Minute),
	}}

	if since := metric.ReconnectsSince(24 * time.Hour); since != 1 {
		t.Errorf("expected one entry within 24h, got %d", since)
	}
	if len(metric.Reconnects) != 2 {
		t.Errorf("entries older than %s should have been pruned, got %d entries left", MetricReconnectPeriod, len(metric.Reconnects))
	}
}

func TestShardMetric_RollingWait(t *testing.T) {
	now := time.Now()
	metric := &IdentifyMetric{}
	if wait := metric.rollingWait(now, 24*time.Hour, 2); wait != 0 {
		t.Fatalf("empty metric should not wait, got %s", wait)
	}

	metric.add(now.Add(-23 * time.Hour))
	if wait := metric.rollingWait(now, 24*time.Hour, 2); wait != 0 {
		t.Fatalf("one of two identifies used should not wait, got %s", wait)
	}

	metric.add(now.Add(-time.Hour))
	wait := metric.rollingWait(now, 24*time.Hour, 2)
	if wait != time.Hour {
		t.Fatalf("expected to wait until the oldest entry leaves the window (1h), got %s", wait)
	}

	// an entry older than the window never causes a wait, and waits are never negative
	metric = &IdentifyMetric{Reconnects: []time.Time{now.Add(-25 * time.Hour), now.Add(-24*time.Hour - time.Second)}}
	if wait := metric.rollingWait(now, 24*time.Hour, 1); wait != 0 {
		t.Fatalf("expected no wait for entries outside the window, got %s", wait)
	}
}
