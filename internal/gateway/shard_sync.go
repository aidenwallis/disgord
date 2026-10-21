package gateway

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aidenwallis/disgord/internal/logger"
)

const DefaultIdentifyRateLimit = 1000

const identifyWindow = 24 * time.Hour

// errIdentifyNotSent is returned from an identify turn (the connect queue callback) when no IDENTIFY was
// written. Such turns are not charged against the identify budgets and are not followed by spacing.
var errIdentifyNotSent = errors.New("identify was not sent")

var errShardSyncShutdown = errors.New("identify queue is shutting down")

// sessionStartLimit is Discord's session_start_limit as reported by GET /gateway/bot.
type sessionStartLimit struct {
	total      uint
	remaining  uint
	resetAfter time.Duration
	observedAt time.Time
}

func (l sessionStartLimit) known() bool {
	return l.total > 0
}

// serverBudget tracks Discord's session start allowance. It is tracked separately from the local rolling
// window since Discord's budget is shared with every other process using the same bot token.
type serverBudget struct {
	known     bool
	total     uint
	remaining uint
	resetAt   time.Time
}

// advance applies every window reset that has elapsed by now.
func (b *serverBudget) advance(now time.Time) {
	if !b.known {
		return
	}
	for !now.Before(b.resetAt) {
		b.remaining = b.total
		b.resetAt = b.resetAt.Add(identifyWindow)
	}
}

func newShardSync(conf *ShardConfig, l logger.Logger, lPrefix string, shutdownChan chan interface{}) *shardSync {
	s := &shardSync{
		identifiesPer24H: conf.IdentifiesPer24H,
		timeout:          conf.ShardRateLimit,
		pollInterval:     time.Second,
		queue:            make(chan *shardSyncQueueItem, 100), // it's just pointers anyways
		logger:           l,
		lpre:             lPrefix,
		shutdownChan:     shutdownChan,
		metric:           &IdentifyMetric{},
	}
	if s.identifiesPer24H == 0 {
		s.identifiesPer24H = DefaultIdentifyRateLimit
	}
	if s.timeout <= 0 {
		s.timeout = defaultShardRateLimit
	}

	if ssl := conf.sessionStart; ssl.known() {
		s.server = serverBudget{
			known:     true,
			total:     ssl.total,
			remaining: ssl.remaining,
			resetAt:   ssl.observedAt.Add(ssl.resetAfter),
		}
		if ssl.remaining == 0 {
			s.logger.Error(s.lpre, fmt.Sprintf(
				"Discord reports 0 of %d remaining session starts; no shard can identify until %s (connecting will block until then)",
				ssl.total, s.server.resetAt.Format(time.RFC3339)))
		}
	}
	return s
}

type shardSyncQueueItem struct {
	ShardID   uint
	run       func() error
	abandoned func() bool // optional: true when nobody wants this identify anymore
	errChan   chan error  // buffered(1) and never closed
}

func (i *shardSyncQueueItem) isAbandoned() bool {
	return i.abandoned != nil && i.abandoned()
}

// shardSync is the built-in identify limiter (default ShardConfig.ConnectQueue). Every turn goes through
// admission (both identify budgets allow one more IDENTIFY), run, and then charge + spacing for turns that
// (possibly) sent an IDENTIFY.
type shardSync struct {
	mu     sync.Mutex // guards server
	server serverBudget

	identifiesPer24H uint
	timeout          time.Duration // spacing between charged identifies
	pollInterval     time.Duration // re-check abandonment while waiting for admission
	queue            chan *shardSyncQueueItem
	logger           logger.Logger
	lpre             string
	shutdownChan     chan interface{}
	metric           *IdentifyMetric
}

// queueShard implements the connectQueue signature.
func (s *shardSync) queueShard(shardID uint, cb func() error) (err error) {
	return s.queueIdentify(shardID, cb, nil)
}

func (s *shardSync) queueIdentify(shardID uint, cb func() error, abandoned func() bool) (err error) {
	item := &shardSyncQueueItem{
		ShardID:   shardID,
		run:       cb,
		abandoned: abandoned,
		errChan:   make(chan error, 1),
	}

	start := time.Now()
	s.logger.Debug(s.lpre, "shard", shardID, "is waiting to identify")
	select {
	case s.queue <- item:
	case <-s.shutdownChan:
		return errShardSyncShutdown
	}

	select {
	case <-s.shutdownChan:
		return errShardSyncShutdown
	case err = <-item.errChan:
	}
	s.logger.Debug(s.lpre, "shard", shardID, "waited and finished execution after", time.Since(start))
	return err
}

// admissionWait returns how long to wait, as of now, before one more IDENTIFY is allowed by both budgets.
func (s *shardSync) admissionWait(now time.Time) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()

	var wait time.Duration
	if s.server.known {
		s.server.advance(now)
		if s.server.remaining == 0 {
			wait = s.server.resetAt.Sub(now)
		}
	}
	if local := s.metric.rollingWait(now, identifyWindow, s.identifiesPer24H); local > wait {
		wait = local
	}
	if wait < 0 {
		wait = 0
	}
	return wait
}

// admissionETA is how long a new identify would currently have to wait for admission, ignoring queue position.
func (s *shardSync) admissionETA() time.Duration {
	return s.admissionWait(time.Now())
}

// charge records a completed or ambiguous IDENTIFY write against the window that is current at completion.
func (s *shardSync) charge(now time.Time) {
	s.mu.Lock()
	if s.server.known {
		s.server.advance(now)
		if s.server.remaining > 0 {
			s.server.remaining--
		}
	}
	s.mu.Unlock()
	s.metric.add(now)
}

func (s *shardSync) logAdmissionWait(shardID uint, wait time.Duration, now time.Time) {
	s.mu.Lock()
	server := s.server
	s.mu.Unlock()

	msg := fmt.Sprintf("shard %d must wait %s (until %s) before it may identify: local identifies in the last 24h=%d (limit %d)",
		shardID, wait.Round(time.Millisecond), now.Add(wait).Format(time.RFC3339), s.metric.ReconnectsSince(identifyWindow), s.identifiesPer24H)
	if server.known {
		msg += fmt.Sprintf(", Discord session starts remaining=%d/%d (resets at %s)",
			server.remaining, server.total, server.resetAt.Format(time.RFC3339))
	}
	if wait >= time.Minute {
		s.logger.Error(s.lpre, msg)
	} else {
		s.logger.Info(s.lpre, msg)
	}
}

// awaitAdmission blocks until the item may run. Abandoned items are resolved with errIdentifyNotSent without
// consuming budget or spacing.
func (s *shardSync) awaitAdmission(item *shardSyncQueueItem) (admitted, shutdown bool) {
	logged := false
	for {
		if item.isAbandoned() {
			s.logger.Debug(s.lpre, "shard", item.ShardID, "identify request was abandoned, skipping it")
			item.errChan <- errIdentifyNotSent
			return false, false
		}

		now := time.Now()
		wait := s.admissionWait(now)
		if wait <= 0 {
			return true, false
		}
		if !logged {
			s.logAdmissionWait(item.ShardID, wait, now)
			logged = true
		}

		if wait > s.pollInterval {
			wait = s.pollInterval
		}
		timer := time.NewTimer(wait)
		select {
		case <-s.shutdownChan:
			timer.Stop()
			return false, true
		case <-timer.C:
		}
	}
}

func (s *shardSync) process() {
	for {
		var item *shardSyncQueueItem
		var open bool

		select {
		case <-s.shutdownChan:
			s.logger.Debug(s.lpre, "shard identify-rate-limiter got shutdown signal")
			return
		case item, open = <-s.queue:
			if !open {
				s.logger.Error(s.lpre, "queue unexpectly closed - shards can no longer identify")
				return
			}
		}
		if item == nil {
			continue
		}

		admitted, shutdown := s.awaitAdmission(item)
		if shutdown {
			item.errChan <- errShardSyncShutdown
			s.logger.Debug(s.lpre, "shard identify-rate-limiter got shutdown signal")
			return
		}
		if !admitted {
			continue
		}
		if item.isAbandoned() {
			item.errChan <- errIdentifyNotSent
			continue
		}

		err := item.run()
		item.errChan <- err
		if errors.Is(err, errIdentifyNotSent) {
			continue
		}

		s.charge(time.Now())

		timer := time.NewTimer(s.timeout)
		select {
		case <-s.shutdownChan:
			timer.Stop()
			s.logger.Debug(s.lpre, "shard identify-rate-limiter got shutdown signal")
			return
		case <-timer.C:
		}
	}
}
