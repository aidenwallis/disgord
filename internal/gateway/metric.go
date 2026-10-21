package gateway

import (
	"sync"
	"time"
)

const MetricReconnectPeriod = time.Hour * 48

// IdentifyMetric records when IDENTIFY payloads were charged against the identify budget.
// Entries are kept oldest first and pruned after MetricReconnectPeriod.
// TODO: make it specific to individual shards for more insight
type IdentifyMetric struct {
	sync.Mutex
	Reconnects []time.Time
}

func (s *IdentifyMetric) cleanup(now time.Time) {
	s.Reconnects = rotateByTime(s.Reconnects, now.Add(-MetricReconnectPeriod))
}

// ReconnectsSince counts the identifies within the last d, where d can be no more than MetricReconnectPeriod.
func (s *IdentifyMetric) ReconnectsSince(d time.Duration) (counter uint) {
	s.Lock()
	defer s.Unlock()
	return s.countSince(time.Now(), d)
}

func (s *IdentifyMetric) countSince(now time.Time, d time.Duration) (counter uint) {
	limit := now.Add(-d)
	for i := len(s.Reconnects) - 1; i >= 0; i-- {
		if !s.Reconnects[i].After(limit) {
			break
		}
		counter++
	}
	s.cleanup(now)
	return counter
}

func (s *IdentifyMetric) add(now time.Time) {
	s.Lock()
	defer s.Unlock()
	s.Reconnects = append(s.Reconnects, now)
	s.cleanup(now)
}

// rollingWait returns how long to wait until fewer than limit entries fall within the last window.
func (s *IdentifyMetric) rollingWait(now time.Time, window time.Duration, limit uint) time.Duration {
	s.Lock()
	defer s.Unlock()
	if limit == 0 || s.countSince(now, window) < limit {
		return 0
	}

	// the window holds at least limit entries; the oldest one that must age out is limit entries from the end
	oldest := s.Reconnects[len(s.Reconnects)-int(limit)]
	if wait := oldest.Add(window).Sub(now); wait > 0 {
		return wait
	}
	return 0
}
