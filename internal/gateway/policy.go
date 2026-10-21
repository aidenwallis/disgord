package gateway

import (
	"fmt"
	"time"
)

type failKind int

const (
	// failTransport covers EOF, network and write errors, dial/HELLO failures and connections that ended without a
	// usable close frame.
	failTransport failKind = iota
	failRemoteClose
	failReconnect      // op 7
	failInvalidSession // op 9
	failZombie
	failReadyTimeout
	failStop // Disconnect or system shutdown
)

var failKindNames = map[failKind]string{
	failTransport:      "transport",
	failRemoteClose:    "remote close",
	failReconnect:      "reconnect requested (op 7)",
	failInvalidSession: "invalid session (op 9)",
	failZombie:         "zombie connection (no heartbeat ACK)",
	failReadyTimeout:   "no READY/RESUMED progress",
	failStop:           "stopped",
}

// failReason describes why a session ended.
type failReason struct {
	kind      failKind
	code      int  // close code, failRemoteClose only
	resumable bool // op 9 "d", failInvalidSession only
	err       error
}

func (r failReason) String() string {
	s := failKindNames[r.kind]
	switch r.kind {
	case failRemoteClose:
		s = fmt.Sprintf("%s %d", s, r.code)
		if info, ok := closeCodeInfo[r.code]; ok {
			s += " (" + info.name + ")"
		}
	case failInvalidSession:
		s = fmt.Sprintf("%s resumable=%t", s, r.resumable)
	}
	if r.err != nil {
		s += ": " + r.err.Error()
	}
	return s
}

// rank orders failure reasons for arbitration: when several are observed for one session, the highest wins.
func (r failReason) rank() int {
	switch r.kind {
	case failStop:
		return 6
	case failRemoteClose:
		switch {
		case isFatalCloseCode(r.code):
			return 5
		case isSessionInvalidatingCloseCode(r.code):
			return 4
		case isTransportCloseCode(r.code):
			return 1
		default:
			return 2
		}
	case failInvalidSession:
		if !r.resumable {
			return 4
		}
		return 3
	case failReconnect, failZombie, failReadyTimeout:
		return 3
	default:
		return 1
	}
}

type closeCode struct {
	name   string
	remedy string
}

var closeCodeInfo = map[int]closeCode{
	4000: {name: "unknown error"},
	4001: {name: "unknown opcode"},
	4002: {name: "decode error"},
	4003: {name: "not authenticated"},
	4004: {name: "authentication failed", remedy: "the bot token is invalid; check Config.BotToken"},
	4005: {name: "already authenticated"},
	4007: {name: "invalid seq"},
	4008: {name: "rate limited"},
	4009: {name: "session timed out"},
	4010: {name: "invalid shard", remedy: "check ShardConfig.ShardIDs and ShardConfig.ShardCount"},
	4011: {name: "sharding required", remedy: "the bot is in too many guilds for its shard count; increase ShardConfig.ShardCount"},
	4012: {name: "invalid API version", remedy: "upgrade disgord"},
	4013: {name: "invalid intent(s)", remedy: "the configured intents bitfield is invalid; check Config.Intents"},
	4014: {name: "disallowed intent(s)", remedy: "enable the privileged intents in the Discord developer portal or remove them from Config.Intents"},
}

func isFatalCloseCode(code int) bool {
	switch code {
	case 4004, 4010, 4011, 4012, 4013, 4014:
		return true
	}
	return false
}

func isSessionInvalidatingCloseCode(code int) bool {
	return code == 4007 || code == 4009
}

// isTransportCloseCode reports codes that are never sent on the wire; they describe a broken connection.
func isTransportCloseCode(code int) bool {
	switch code {
	case 1005, 1006, 1015:
		return true
	}
	return false
}

// GatewayCloseError is returned when Discord closed the gateway connection with a close code that cannot be recovered
// from by reconnecting.
type GatewayCloseError struct {
	Code   int
	Reason string
	Remedy string
}

func (e *GatewayCloseError) Error() string {
	msg := fmt.Sprintf("discord closed the gateway connection with %d (%s)", e.Code, e.Reason)
	if e.Remedy != "" {
		msg += ": " + e.Remedy
	}
	return msg
}

func newCloseError(code int) *GatewayCloseError {
	info := closeCodeInfo[code]
	if info.name == "" {
		info.name = "unknown"
	}
	return &GatewayCloseError{Code: code, Reason: info.name, Remedy: info.remedy}
}

// attemptPhase is how far a connection attempt got before it ended.
type attemptPhase int

const (
	phaseDial attemptPhase = iota
	phaseHello
	phaseIdentify
	phaseResume
	phaseReady // IDENTIFY/RESUME written, waiting for READY/RESUMED
	phaseSession
)

var phaseNames = map[attemptPhase]string{
	phaseDial:     "dial",
	phaseHello:    "waiting for HELLO",
	phaseIdentify: "identifying",
	phaseResume:   "resuming",
	phaseReady:    "waiting for READY/RESUMED",
	phaseSession:  "session",
}

func (p attemptPhase) String() string {
	return phaseNames[p]
}

// outcome is the result of one connection attempt, including establish failures.
type outcome struct {
	phase        attemptPhase
	reason       failReason
	reachedReady bool
	healthyFor   time.Duration
}

type action struct {
	stop           bool // the run was cancelled; not an error
	fatal          bool
	clearSession   bool // the next connection must IDENTIFY
	invalidSession bool // wait the 1-5s invalid session delay before reconnecting
}

// policy decides what happens after every attempt outcome.
func policy(o outcome) action {
	r := o.reason
	switch r.kind {
	case failStop:
		return action{stop: true}
	case failRemoteClose:
		switch {
		case isFatalCloseCode(r.code):
			return action{fatal: true}
		case isSessionInvalidatingCloseCode(r.code):
			return action{clearSession: true, invalidSession: true}
		}
	case failInvalidSession:
		return action{clearSession: !r.resumable, invalidSession: true}
	}
	// everything else is transient and keeps the session for a RESUME
	return action{}
}
