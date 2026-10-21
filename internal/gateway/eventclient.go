package gateway

import (
	"errors"
	"fmt"
	"net/http"
	"runtime"
	"sync"
	"time"

	"github.com/aidenwallis/disgord/json"

	"go.uber.org/atomic"

	"github.com/aidenwallis/disgord/internal/gateway/cmd"
	"github.com/aidenwallis/disgord/internal/gateway/opcode"
	"github.com/aidenwallis/disgord/internal/logger"
)

// NewEventClient creates a new socket client for handling behavior and Discord events. Connections are managed
// by a supervisor goroutine started by Connect.
func NewEventClient(shardID uint, conf *EvtConfig) (client *EvtClient, err error) {
	conf.validate()

	if conf.EventChan == nil {
		return nil, errors.New("missing event channel")
	}
	if conf.Logger == nil {
		conf.Logger = logger.Empty{}
	}

	conf.Logger.Debug(fmt.Sprintf("shard{%d} intents(%d): %s", shardID, conf.Intents, conf.Intents.String()))

	to := defaultTimeouts()
	if conf.timeouts != nil {
		to = conf.timeouts.withDefaults()
	}

	queueLimit := int(conf.MessageQueueLimit)
	if queueLimit == 0 {
		queueLimit = 20
	}

	pool := conf.DiscordPktPool
	if pool == nil {
		pool = &sync.Pool{New: func() interface{} { return &DiscordPacket{} }}
	}

	client = &EvtClient{
		evtConf:      conf,
		ShardID:      shardID,
		log:          conf.Logger,
		to:           to,
		eventChan:    conf.EventChan,
		shutdown:     conf.SystemShutdown,
		connectQueue: conf.connectQueue,
		identifySync: conf.identifySync,
		ratelimit:    newRatelimiter(),
		messageQueue: newClientPktQueue(queueLimit),
		cmdSignal:    make(chan struct{}, 1),
		pktPool:      pool,
		newConn:      conf.newConn,
	}
	if client.connectQueue == nil {
		client.connectQueue = func(_ uint, cb func() error) error { return cb() }
	}
	if client.newConn == nil {
		httpClient, writeTimeout, closeTimeout := conf.HTTPClient, to.write, to.closeHandshake
		client.newConn = func() (sessionConn, error) {
			return newSessionConn(httpClient, writeTimeout, closeTimeout), nil
		}
	}

	client.identity = &evtIdentity{
		Token: conf.BotToken,
		Properties: struct {
			OS      string `json:"$os"`
			Browser string `json:"$browser"`
			Device  string `json:"$device"`
		}{runtime.GOOS, conf.Browser, conf.Device},
		LargeThreshold: conf.GuildLargeThreshold,
		Shard:          &[2]uint{client.ShardID, conf.ShardCount},
		Intents:        conf.Intents,
	}
	if conf.Presence != nil {
		if err = client.SetPresence(conf.Presence); err != nil {
			return nil, err
		}
	}

	return client, nil
}

// Event is dispatched by the socket layer after parsing and extracting Discord data from a incoming packet.
// This is the data structure used by Disgord for triggering handlers and channels with an event.
type Event struct {
	Name    string
	Data    []byte
	ShardID uint
}

// EvtConfig ws
type EvtConfig struct {
	// BotToken Discord bot token
	BotToken   string
	HTTPClient *http.Client

	// newConn creates the transport for every new connection; defaults to nhooyr over HTTPClient
	newConn func() (sessionConn, error)

	// timeouts overrides the per-client timeouts; zero fields keep their default
	timeouts *timeouts

	Intents Intent

	// EventChan can be used to inject a channel instead of letting the ws client construct one
	// useful in sharding to avoid complicated patterns to handle N channels.
	EventChan chan<- *Event

	connectQueue connectQueue
	// identifySync is set when connectQueue is the built-in shardSync, which can skip abandoned identifies
	identifySync *shardSync

	// discordErrListener is not invoked by the event client: the shard manager's autoscaling listener would
	// deadlock (see ShardConfig.OnScalingRequired).
	discordErrListener discordErrListener

	Presence *UpdateStatusPayload

	// Endpoint for establishing socket connection. Either endpoints, `Gateway` or `Gateway Bot`, is used to retrieve
	// a valid socket endpoint from Discord
	Endpoint string

	// Encoding make sure we support the correct encoding
	Encoding string

	// Version make sure we support the correct Discord version
	Version int

	// for identify packets
	Browser             string
	Device              string
	GuildLargeThreshold uint
	ShardCount          uint

	DiscordPktPool *sync.Pool

	// MessageQueueLimit number of outgoing messages that can be queued and sent correctly.
	MessageQueueLimit uint

	Logger logger.Logger

	SystemShutdown chan interface{}
}

func (conf *EvtConfig) validate() {
	if conf.BotToken == "" {
		panic("missing bot token in gateway event config")
	}
	if conf.SystemShutdown == nil {
		panic("missing conf.SystemShutdown channel in gateway event config")
	}
}

// EvtClient is one gateway shard.
//
// Lock order: shardMngr.mu -> lifecycleMu -> mu -> identifyRequest.mu. The receiver never holds mu while
// sending on the event channel.
type EvtClient struct {
	evtConf *EvtConfig
	ShardID uint
	log     logger.Logger
	to      timeouts

	eventChan    chan<- *Event
	shutdown     <-chan interface{}
	ignoreEvents []string
	pktPool      *sync.Pool
	newConn      func() (sessionConn, error)

	connectQueue connectQueue
	identifySync *shardSync

	ratelimit    *ratelimiter
	messageQueue clientPktQueue
	cmdSignal    chan struct{} // buffered(1): a command was queued

	identity *evtIdentity
	idMu     sync.RWMutex

	// lifecycle: at most one run (supervisor) exists; it is only cleared by the supervisor itself
	lifecycleMu         sync.Mutex
	run                 *run
	requestedDisconnect atomic.Bool
	haveConnectedOnce   atomic.Bool
	isConnected         atomic.Bool

	runCounter     atomic.Uint64
	sessionCounter atomic.Uint64
	logSequence    atomic.Uint32 // ARM 32bit causes panic with 64bit
	lastFrameAt    atomic.Int64

	// mu guards the shared shard state below. Sessions only mutate it while they are current.
	mu           sync.Mutex
	current      *session
	identSess    *session // current session waiting for its identify turn
	identReq     *identifyRequest
	sessionID    string
	seq          uint32
	hasSeq       bool
	latency      time.Duration
	ReadyCounter uint
}

var _ Link = (*EvtClient)(nil)

func (c *EvtClient) getLogPrefix() string {
	nr := c.logSequence.Add(1)
	return fmt.Sprintf("[ws-e,s:%d,shard:%d]", nr, c.ShardID)
}

func (c *EvtClient) SetPresence(data interface{}) (err error) {
	// marshalling is done to avoid race
	var presence json.RawMessage
	if presence, err = json.Marshal(data); err != nil {
		return err
	}
	c.idMu.Lock()
	c.identity.Presence = presence
	c.idMu.Unlock()

	return nil
}

// Emit queues a gateway command. Commands are sent once the shard is (re)connected and READY/RESUMED.
func (c *EvtClient) Emit(command string, data CmdPayload) (err error) {
	if command == cmd.UpdateStatus {
		if err = c.SetPresence(data); err != nil {
			return err
		}
	}
	return c.queueRequest(command, data)
}

func (c *EvtClient) queueRequest(command string, data CmdPayload) (err error) {
	if !c.haveConnectedOnce.Load() {
		return errors.New("race condition detected: you must Connect to the socket API/Gateway before you can send gateway commands: " + command)
	}

	op := CmdNameToOpCode(command, clientTypeEvent)
	if op == opcode.None {
		return errors.New("unsupported command: " + command)
	}

	p := &clientPacket{
		Op:      op,
		Data:    data,
		CmdName: command,
	}
	if accepted := c.ratelimit.Request(command); !accepted {
		// we might be rate limited.. but lets see if there is another
		// presence update in the queue; then it can be overwritten
		if err := c.messageQueue.AddByOverwrite(p); err != nil {
			return errors.New("rate limited")
		}
		c.signalCommands()
		return nil
	}
	if err = c.messageQueue.Add(p); err != nil {
		return err
	}
	c.signalCommands()
	return nil
}

func (c *EvtClient) signalCommands() {
	select {
	case c.cmdSignal <- struct{}{}:
	default:
	}
}

// HeartbeatLatency get the time diff between sending a heartbeat and Discord replying with a heartbeat ack
func (c *EvtClient) HeartbeatLatency() (duration time.Duration, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	duration = c.latency
	if duration == 0 {
		err = errors.New("latency not determined yet")
	}
	return
}

func (c *EvtClient) eventOfInterest(name string) bool {
	for i := range c.ignoreEvents {
		if c.ignoreEvents[i] == name {
			return false
		}
	}
	return true
}
