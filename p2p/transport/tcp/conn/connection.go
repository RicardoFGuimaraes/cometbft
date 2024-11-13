package conn

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"reflect"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cosmos/gogoproto/proto"

	tmp2p "github.com/cometbft/cometbft/api/cometbft/p2p/v1"
	"github.com/cometbft/cometbft/config"
	flow "github.com/cometbft/cometbft/internal/flowrate"
	"github.com/cometbft/cometbft/internal/timer"
	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/libs/protoio"
	"github.com/cometbft/cometbft/libs/service"
	cmtsync "github.com/cometbft/cometbft/libs/sync"
	"github.com/cometbft/cometbft/p2p/abstract"
)

const (
	defaultMaxPacketMsgPayloadSize = 1024

	numBatchPacketMsgs = 10
	minReadBufferSize  = 1024
	minWriteBufferSize = 65536
	updateStats        = 2 * time.Second

	// some of these defaults are written in the user config
	// flushThrottle, sendRate, recvRate
	// TODO: remove values present in config.
	defaultFlushThrottle = 10 * time.Millisecond

	defaultSendQueueCapacity = 1
	defaultSendRate          = int64(512000) // 500KB/s
	defaultRecvRate          = int64(512000) // 500KB/s
	defaultPingInterval      = 60 * time.Second
	defaultPongTimeout       = 45 * time.Second

	// Capacity of the receive channel for each stream.
	maxRecvChanCap = 100
)

// MConnection is a multiplexed connection.
//
// __multiplex__ *noun* a system or signal involving simultaneous transmission
// of several messages along a single channel of communication.
//
// Each connection handles message transmission on multiple abstract
// communication streams. Each stream has a globally unique byte id. The byte
// id and the relative priorities of each stream are configured upon
// initialization of the connection.
//
// To open a stream, call OpenStream with the stream id. Remember that the
// stream id must be globally unique.
//
// Connection errors are communicated through the ErrorCh channel.
//
// Connection can be closed either by calling Close or FlushAndClose. The
// latter will flush all pending messages before closing the connection.
type MConnection struct {
	service.BaseService

	conn          net.Conn
	bufConnReader *bufio.Reader
	bufConnWriter *bufio.Writer
	sendMonitor   *flow.Monitor
	recvMonitor   *flow.Monitor
	send          chan struct{}
	pong          chan struct{}
	errorCh       chan error
	config        MConnConfig

	// Closing quitSendRoutine will cause the sendRoutine to eventually quit.
	// doneSendRoutine is closed when the sendRoutine actually quits.
	quitSendRoutine chan struct{}
	doneSendRoutine chan struct{}

	// Closing quitRecvRouting will cause the recvRouting to eventually quit.
	quitRecvRoutine chan struct{}

	// used to ensure FlushAndClose and OnStop
	// are safe to call concurrently.
	stopMtx cmtsync.Mutex

	flushTimer *timer.ThrottleTimer // flush writes as necessary but throttled.
	pingTimer  *time.Ticker         // send pings periodically

	// close conn if pong is not received in pongTimeout
	pongTimer     *time.Timer
	pongTimeoutCh chan bool // true - timeout, false - peer sent pong

	chStatsTimer *time.Ticker // update channel stats periodically

	created time.Time // time of creation

	_maxPacketMsgSize int

	mtx sync.RWMutex
	// streamID -> list of incoming messages
	recvMsgsByStreamID map[byte]chan []byte
	// streamID -> channel
	channelsIdx map[byte]*Channel
}

var _ abstract.Connection = (*MConnection)(nil)

// MConnConfig is a MConnection configuration.
type MConnConfig struct {
	SendRate int64 `mapstructure:"send_rate"`
	RecvRate int64 `mapstructure:"recv_rate"`

	// Maximum payload size
	MaxPacketMsgPayloadSize int `mapstructure:"max_packet_msg_payload_size"`

	// Interval to flush writes (throttled)
	FlushThrottle time.Duration `mapstructure:"flush_throttle"`

	// Interval to send pings
	PingInterval time.Duration `mapstructure:"ping_interval"`

	// Maximum wait time for pongs
	PongTimeout time.Duration `mapstructure:"pong_timeout"`

	// Fuzz connection
	TestFuzz       bool                   `mapstructure:"test_fuzz"`
	TestFuzzConfig *config.FuzzConnConfig `mapstructure:"test_fuzz_config"`
}

// DefaultMConnConfig returns the default config.
func DefaultMConnConfig() MConnConfig {
	return MConnConfig{
		SendRate:                defaultSendRate,
		RecvRate:                defaultRecvRate,
		MaxPacketMsgPayloadSize: defaultMaxPacketMsgPayloadSize,
		FlushThrottle:           defaultFlushThrottle,
		PingInterval:            defaultPingInterval,
		PongTimeout:             defaultPongTimeout,
	}
}

// NewMConnection wraps net.Conn and creates multiplex connection.
func NewMConnection(conn net.Conn, config MConnConfig) *MConnection {
	if config.PongTimeout >= config.PingInterval {
		panic("pongTimeout must be less than pingInterval (otherwise, next ping will reset pong timer)")
	}

	mconn := &MConnection{
		conn:               conn,
		bufConnReader:      bufio.NewReaderSize(conn, minReadBufferSize),
		bufConnWriter:      bufio.NewWriterSize(conn, minWriteBufferSize),
		sendMonitor:        flow.New(0, 0),
		recvMonitor:        flow.New(0, 0),
		send:               make(chan struct{}, 1),
		pong:               make(chan struct{}, 1),
		errorCh:            make(chan error, 1),
		config:             config,
		created:            time.Now(),
		recvMsgsByStreamID: make(map[byte]chan []byte),
		channelsIdx:        make(map[byte]*Channel),
	}

	mconn.BaseService = *service.NewBaseService(nil, "MConnection", mconn)

	// maxPacketMsgSize() is a bit heavy, so call just once
	mconn._maxPacketMsgSize = mconn.maxPacketMsgSize()

	return mconn
}

func (c *MConnection) SetLogger(l log.Logger) {
	c.BaseService.SetLogger(l)
	for _, ch := range c.channelsIdx {
		ch.SetLogger(l.With("streamID", ch.desc.ID))
	}
}

// OnStart implements BaseService.
func (c *MConnection) OnStart() error {
	if err := c.BaseService.OnStart(); err != nil {
		return err
	}
	c.flushTimer = timer.NewThrottleTimer("flush", c.config.FlushThrottle)
	c.pingTimer = time.NewTicker(c.config.PingInterval)
	c.pongTimeoutCh = make(chan bool, 1)
	c.chStatsTimer = time.NewTicker(updateStats)
	c.quitSendRoutine = make(chan struct{})
	c.doneSendRoutine = make(chan struct{})
	c.quitRecvRoutine = make(chan struct{})
	go c.sendRoutine()
	go c.recvRoutine()
	return nil
}

func (c *MConnection) Conn() net.Conn {
	return c.conn
}

// stopServices stops the BaseService and timers and closes the quitSendRoutine.
// if the quitSendRoutine was already closed, it returns true, otherwise it returns false.
// It uses the stopMtx to ensure only one of FlushAndClose and OnStop can do this at a time.
func (c *MConnection) stopServices() (alreadyStopped bool) {
	c.stopMtx.Lock()
	defer c.stopMtx.Unlock()

	select {
	case <-c.quitSendRoutine:
		// already quit
		return true
	default:
	}

	select {
	case <-c.quitRecvRoutine:
		// already quit
		return true
	default:
	}

	c.flushTimer.Stop()
	c.pingTimer.Stop()
	c.chStatsTimer.Stop()

	// inform the recvRouting that we are shutting down
	close(c.quitRecvRoutine)
	close(c.quitSendRoutine)
	return false
}

// ErrorCh returns a channel that will receive errors from the connection.
func (c *MConnection) ErrorCh() <-chan error {
	return c.errorCh
}

// OnStop implements BaseService.
func (c *MConnection) OnStop() {
	if c.stopServices() {
		return
	}

	c.conn.Close()

	// We can't close pong safely here because
	// recvRoutine may write to it after we've stopped.
	// Though it doesn't need to get closed at all,
	// we close it @ recvRoutine.
}

func (c *MConnection) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

func (c *MConnection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *MConnection) OpenStream(streamID byte, desc any) (abstract.Stream, error) {
	c.mtx.Lock()
	if _, ok := c.channelsIdx[streamID]; ok {
		c.mtx.Unlock()
		return nil, fmt.Errorf("stream %X already exists", streamID)
	}

	d := ChannelDescriptor{
		ID:       streamID,
		Priority: 1,
	}
	if desc, ok := desc.(ChannelDescriptor); ok {
		d = desc
	}
	c.channelsIdx[streamID] = newChannel(c, d)
	c.channelsIdx[streamID].SetLogger(c.Logger.With("streamID", streamID))

	c.recvMsgsByStreamID[streamID] = make(chan []byte, maxRecvChanCap)
	c.mtx.Unlock()

	return &MConnectionStream{conn: c, streadID: streamID}, nil
}

func (c *MConnection) Close(reason string) error {
	// inform the error channel that we are shutting down.
	select {
	case c.errorCh <- fmt.Errorf("Close called (reason: %s)", reason):
	default:
	}

	if c.stopServices() {
		return nil
	}

	// We can't close pong safely here because
	// recvRoutine may write to it after we've stopped.
	// Though it doesn't need to get closed at all,
	// we close it @ recvRoutine.

	// c.Stop()

	return c.conn.Close()
}

// FlushAndClose replicates the logic of OnStop. It additionally ensures that
// all successful writes will get flushed before closing the connection.
func (c *MConnection) FlushAndClose(reason string) error {
	// inform the error channel that we are shutting down.
	select {
	case c.errorCh <- fmt.Errorf("FlushAndClose called (reason: %s)", reason):
	default:
	}

	if c.stopServices() {
		return nil
	}

	// this block is unique to FlushAndClose
	{
		// wait until the sendRoutine exits
		// so we dont race on calling sendSomePacketMsgs
		<-c.doneSendRoutine

		// Send and flush all pending msgs.
		// Since sendRoutine has exited, we can call this
		// safely
		w := protoio.NewDelimitedWriter(c.bufConnWriter)
		eof := c.sendSomePacketMsgs(w)
		for !eof {
			eof = c.sendSomePacketMsgs(w)
		}
		c.flush()

		// Now we can close the connection
	}

	// We can't close pong safely here because
	// recvRoutine may write to it after we've stopped.
	// Though it doesn't need to get closed at all,
	// we close it @ recvRoutine.

	// c.Stop()

	return c.conn.Close()
}

func (c *MConnection) ConnectionState() any {
	var status ConnectionStatus
	status.Duration = time.Since(c.created)
	status.SendMonitor = c.sendMonitor.Status()
	status.RecvMonitor = c.recvMonitor.Status()
	c.mtx.RLock()
	status.Channels = make([]ChannelStatus, len(c.channelsIdx))
	i := 0
	for _, channel := range c.channelsIdx {
		status.Channels[i] = ChannelStatus{
			ID:                channel.desc.ID,
			SendQueueCapacity: cap(channel.sendQueue),
			SendQueueSize:     int(atomic.LoadInt32(&channel.sendQueueSize)),
			Priority:          channel.desc.Priority,
			RecentlySent:      atomic.LoadInt64(&channel.recentlySent),
		}
		i++
	}
	c.mtx.RUnlock()
	return status
}

func (c *MConnection) String() string {
	return fmt.Sprintf("MConn{%v}", c.conn.RemoteAddr())
}

func (c *MConnection) flush() {
	c.Logger.Debug("Flush", "conn", c)
	err := c.bufConnWriter.Flush()
	if err != nil {
		c.Logger.Debug("MConnection flush failed", "err", err)
	}
}

// Catch panics, usually caused by remote disconnects.
func (c *MConnection) _recover() {
	if r := recover(); r != nil {
		c.Logger.Error("MConnection panicked", "err", r, "stack", string(debug.Stack()))
		c.stopForError(fmt.Errorf("recovered from panic: %v", r))
	}
}

func (c *MConnection) stopForError(r error) {
	select {
	case c.errorCh <- r:
	default:
	}

	if err := c.Stop(); err != nil {
		c.Logger.Error("Error stopping connection", "err", err)
	}
}

// thread-safe.
func (c *MConnection) sendBytes(chID byte, msgBytes []byte, timeout time.Duration) error {
	if !c.IsRunning() {
		return ErrNotRunning
	}

	c.Logger.Debug("Send",
		"streamID", chID,
		"msgBytes", log.NewLazySprintf("%X", msgBytes),
		"timeout", timeout)

	c.mtx.RLock()
	channel, ok := c.channelsIdx[chID]
	c.mtx.RUnlock()
	if !ok {
		panic(fmt.Sprintf("Unknown channel %X. Forgot to register?", chID))
	}
	if err := channel.sendBytes(msgBytes, timeout); err != nil {
		c.Logger.Error("Send failed", "err", err)
		return err
	}

	// Wake up sendRoutine if necessary
	select {
	case c.send <- struct{}{}:
	default:
	}
	return nil
}

// CanSend returns true if you can send more data onto the chID, false
// otherwise. Use only as a heuristic.
//
// thread-safe.
func (c *MConnection) CanSend(chID byte) bool {
	if !c.IsRunning() {
		return false
	}

	c.mtx.RLock()
	channel, ok := c.channelsIdx[chID]
	c.mtx.RUnlock()
	if !ok {
		c.Logger.Error(fmt.Sprintf("Unknown channel %X", chID))
		return false
	}
	return channel.canSend()
}

// sendRoutine polls for packets to send from channels.
func (c *MConnection) sendRoutine() {
	defer c._recover()

	protoWriter := protoio.NewDelimitedWriter(c.bufConnWriter)

FOR_LOOP:
	for {
		var _n int
		var err error
	SELECTION:
		select {
		case <-c.flushTimer.Ch:
			// NOTE: flushTimer.Set() must be called every time
			// something is written to .bufConnWriter.
			c.flush()
		case <-c.chStatsTimer.C:
			c.mtx.RLock()
			for _, channel := range c.channelsIdx {
				channel.updateStats()
			}
			c.mtx.RUnlock()
		case <-c.pingTimer.C:
			c.Logger.Debug("Send Ping")
			_n, err = protoWriter.WriteMsg(mustWrapPacket(&tmp2p.PacketPing{}))
			if err != nil {
				c.Logger.Error("Failed to send PacketPing", "err", err)
				break SELECTION
			}
			c.sendMonitor.Update(_n)
			c.Logger.Debug("Starting pong timer", "dur", c.config.PongTimeout)
			c.pongTimer = time.AfterFunc(c.config.PongTimeout, func() {
				select {
				case c.pongTimeoutCh <- true:
				default:
				}
			})
			c.flush()
		case timeout := <-c.pongTimeoutCh:
			if timeout {
				c.Logger.Debug("Pong timeout")
				err = errors.New("pong timeout")
			} else {
				c.stopPongTimer()
			}
		case <-c.pong:
			c.Logger.Debug("Send Pong")
			_n, err = protoWriter.WriteMsg(mustWrapPacket(&tmp2p.PacketPong{}))
			if err != nil {
				c.Logger.Error("Failed to send PacketPong", "err", err)
				break SELECTION
			}
			c.sendMonitor.Update(_n)
			c.flush()
		case <-c.quitSendRoutine:
			break FOR_LOOP
		case <-c.send:
			// Send some PacketMsgs
			eof := c.sendSomePacketMsgs(protoWriter)
			if !eof {
				// Keep sendRoutine awake.
				select {
				case c.send <- struct{}{}:
				default:
				}
			}
		}

		if !c.IsRunning() {
			break FOR_LOOP
		}
		if err != nil {
			c.Logger.Error("Connection failed @ sendRoutine", "err", err)
			c.stopForError(err)
			break FOR_LOOP
		}
	}

	// Cleanup
	c.stopPongTimer()
	close(c.doneSendRoutine)
}

// Returns true if messages from channels were exhausted.
// Blocks in accordance to .sendMonitor throttling.
func (c *MConnection) sendSomePacketMsgs(w protoio.Writer) bool {
	// Block until .sendMonitor says we can write.
	// Once we're ready we send more than we asked for,
	// but amortized it should even out.
	c.sendMonitor.Limit(c._maxPacketMsgSize, c.config.SendRate, true)

	// Now send some PacketMsgs.
	return c.sendBatchPacketMsgs(w, numBatchPacketMsgs)
}

// Returns true if messages from channels were exhausted.
func (c *MConnection) sendBatchPacketMsgs(w protoio.Writer, batchSize int) bool {
	// Send a batch of PacketMsgs.
	totalBytesWritten := 0
	defer func() {
		if totalBytesWritten > 0 {
			c.sendMonitor.Update(totalBytesWritten)
		}
	}()
	for i := 0; i < batchSize; i++ {
		channel := c.selectChannelToGossipOn()
		// nothing to send across any channel.
		if channel == nil {
			return true
		}
		bytesWritten, err := c.sendPacketMsgOnChannel(w, channel)
		if err {
			return true
		}
		totalBytesWritten += bytesWritten
	}
	return false
}

// selects a channel to gossip our next message on.
// TODO: Make "batchChannelToGossipOn", so we can do our proto marshaling overheads in parallel,
// and we can avoid re-checking for `isSendPending`.
// We can easily mock the recentlySent differences for the batch choosing.
func (c *MConnection) selectChannelToGossipOn() *Channel {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	// Choose a channel to create a PacketMsg from.
	// The chosen channel will be the one whose recentlySent/priority is the least.
	var leastRatio float32 = math.MaxFloat32
	var leastChannel *Channel
	for _, channel := range c.channelsIdx {
		// If nothing to send, skip this channel
		// TODO: Skip continually looking for isSendPending on channels we've already skipped in this batch-send.
		if !channel.isSendPending() {
			continue
		}
		// Get ratio, and keep track of lowest ratio.
		// TODO: RecentlySent right now is bytes. This should be refactored to num messages to fix
		// gossip prioritization bugs.
		ratio := float32(channel.recentlySent) / float32(channel.desc.Priority)
		if ratio < leastRatio {
			leastRatio = ratio
			leastChannel = channel
		}
	}
	return leastChannel
}

// returns (num_bytes_written, error_occurred).
func (c *MConnection) sendPacketMsgOnChannel(w protoio.Writer, sendChannel *Channel) (int, bool) {
	// Make & send a PacketMsg from this channel
	n, err := sendChannel.writePacketMsgTo(w)
	if err != nil {
		c.Logger.Error("Failed to write PacketMsg", "err", err)
		c.stopForError(err)
		return n, true
	}
	// TODO: Change this to only add flush signals at the start and end of the batch.
	c.flushTimer.Set()
	return n, false
}

// recvRoutine reads PacketMsgs and reconstructs the message using the
// channels' "recving" buffer. After a whole message has been assembled, it's
// pushed to an internal queue, which is accessible via Read. Blocks depending
// on how the connection is throttled. Otherwise, it never blocks.
func (c *MConnection) recvRoutine() {
	defer c._recover()

	protoReader := protoio.NewDelimitedReader(c.bufConnReader, c._maxPacketMsgSize)

FOR_LOOP:
	for {
		// Block until .recvMonitor says we can read.
		c.recvMonitor.Limit(c._maxPacketMsgSize, atomic.LoadInt64(&c.config.RecvRate), true)

		// Peek into bufConnReader for debugging
		/*
			if numBytes := c.bufConnReader.Buffered(); numBytes > 0 {
				bz, err := c.bufConnReader.Peek(cmtmath.MinInt(numBytes, 100))
				if err == nil {
					// return
				} else {
					c.Logger.Debug("Error peeking connection buffer", "err", err)
					// return nil
				}
				c.Logger.Info("Peek connection buffer", "numBytes", numBytes, "bz", bz)
			}
		*/

		// Read packet type
		var packet tmp2p.Packet

		_n, err := protoReader.ReadMsg(&packet)
		c.recvMonitor.Update(_n)
		if err != nil {
			// stopServices was invoked and we are shutting down
			// receiving is expected to fail since we will close the connection
			select {
			case <-c.quitRecvRoutine:
				break FOR_LOOP
			default:
			}

			if c.IsRunning() {
				if errors.Is(err, io.EOF) {
					c.Logger.Info("Connection is closed @ recvRoutine (likely by the other side)")
				} else {
					c.Logger.Debug("Connection failed @ recvRoutine (reading byte)", "err", err)
				}
				c.stopForError(err)
			}
			break FOR_LOOP
		}

		// Read more depending on packet type.
		switch pkt := packet.Sum.(type) {
		case *tmp2p.Packet_PacketPing:
			// TODO: prevent abuse, as they cause flush()'s.
			// https://github.com/tendermint/tendermint/issues/1190
			c.Logger.Debug("Receive Ping")
			select {
			case c.pong <- struct{}{}:
			default:
				// never block
			}
		case *tmp2p.Packet_PacketPong:
			c.Logger.Debug("Receive Pong")
			select {
			case c.pongTimeoutCh <- false:
			default:
				// never block
			}
		case *tmp2p.Packet_PacketMsg:
			channelID := byte(pkt.PacketMsg.ChannelID)
			c.mtx.RLock()
			channel, ok := c.channelsIdx[channelID]
			c.mtx.RUnlock()
			if pkt.PacketMsg.ChannelID < 0 || pkt.PacketMsg.ChannelID > math.MaxUint8 || !ok || channel == nil {
				err := fmt.Errorf("unknown channel %X", pkt.PacketMsg.ChannelID)
				c.Logger.Debug("Connection failed @ recvRoutine", "err", err)
				c.stopForError(err)
				break FOR_LOOP
			}

			msgBytes, err := channel.recvPacketMsg(*pkt.PacketMsg)
			if err != nil {
				if c.IsRunning() {
					c.Logger.Debug("Connection failed @ recvRoutine", "err", err)
					c.stopForError(err)
				}
				break FOR_LOOP
			}
			if msgBytes != nil {
				c.Logger.Debug("Received", "streamID", channelID, "msgBytes", log.NewLazySprintf("%X", msgBytes))
				if err := c.pushRecvMsg(channelID, msgBytes); err != nil {
					c.Logger.Error("Failed to store msgBytes", "streamID", channelID,
						"msgBytes", log.NewLazySprintf("%X", msgBytes), "err", err)
				}
			}
		default:
			err := fmt.Errorf("unknown message type %v", reflect.TypeOf(packet))
			c.Logger.Error("Connection failed @ recvRoutine", "err", err)
			c.stopForError(err)
			break FOR_LOOP
		}
	}

	// Cleanup
	close(c.pong)
}

func (c *MConnection) pushRecvMsg(streamID byte, msgBytes []byte) error {
	c.mtx.RLock()
	ch, ok := c.recvMsgsByStreamID[streamID]
	c.mtx.RUnlock()

	// Init.
	if !ok {
		return fmt.Errorf("unknown stream %X", streamID)
	}

	// Push the message.
	select {
	case ch <- msgBytes:
		return nil
	default: // Drop the message if the buffer is full.
		return fmt.Errorf("receive buffer is full for stream %X", streamID)
	}
}

// not goroutine-safe.
func (c *MConnection) stopPongTimer() {
	if c.pongTimer != nil {
		_ = c.pongTimer.Stop()
		c.pongTimer = nil
	}
}

// maxPacketMsgSize returns a maximum size of PacketMsg.
func (c *MConnection) maxPacketMsgSize() int {
	bz, err := proto.Marshal(mustWrapPacket(&tmp2p.PacketMsg{
		ChannelID: 0x01,
		EOF:       true,
		Data:      make([]byte, c.config.MaxPacketMsgPayloadSize),
	}))
	if err != nil {
		panic(err)
	}
	return len(bz)
}

// -----------------------------------------------------------------------------

// TODO: lowercase.
// NOTE: not goroutine-safe.
type Channel struct {
	conn          *MConnection
	desc          ChannelDescriptor
	sendQueue     chan []byte
	sendQueueSize int32 // atomic.
	recving       []byte
	sending       []byte
	recentlySent  int64 // exponential moving average

	nextPacketMsg           *tmp2p.PacketMsg
	nextP2pWrapperPacketMsg *tmp2p.Packet_PacketMsg
	nextPacket              *tmp2p.Packet

	maxPacketMsgPayloadSize int

	Logger log.Logger
}

func newChannel(conn *MConnection, desc ChannelDescriptor) *Channel {
	desc = desc.FillDefaults()
	if desc.Priority <= 0 {
		panic("Channel default priority must be a positive integer")
	}
	return &Channel{
		conn:                    conn,
		desc:                    desc,
		sendQueue:               make(chan []byte, desc.SendQueueCapacity),
		recving:                 make([]byte, 0, desc.RecvBufferCapacity),
		nextPacketMsg:           &tmp2p.PacketMsg{ChannelID: int32(desc.ID)},
		nextP2pWrapperPacketMsg: &tmp2p.Packet_PacketMsg{},
		nextPacket:              &tmp2p.Packet{},
		maxPacketMsgPayloadSize: conn.config.MaxPacketMsgPayloadSize,
	}
}

func (ch *Channel) SetLogger(l log.Logger) {
	ch.Logger = l
}

// Queues message to send to this channel.
// Goroutine-safe
// It returns ErrTimeout if bytes were not queued after timeout.
// If timeout is zero, it will wait forever.
func (ch *Channel) sendBytes(bytes []byte, timeout time.Duration) error {
	if timeout == 0 {
		select {
		case ch.sendQueue <- bytes:
			atomic.AddInt32(&ch.sendQueueSize, 1)
			return nil
		case <-ch.conn.Quit():
			return ErrNotRunning
		}
	}

	select {
	case ch.sendQueue <- bytes:
		atomic.AddInt32(&ch.sendQueueSize, 1)
		return nil
	case <-ch.conn.Quit():
		return ErrNotRunning
	case <-time.After(timeout):
		return ErrTimeout
	}
}

// Goroutine-safe.
func (ch *Channel) loadSendQueueSize() (size int) {
	return int(atomic.LoadInt32(&ch.sendQueueSize))
}

// Goroutine-safe
// Use only as a heuristic.
func (ch *Channel) canSend() bool {
	return ch.loadSendQueueSize() < defaultSendQueueCapacity
}

// Returns true if any PacketMsgs are pending to be sent.
// Call before calling updateNextPacket
// Goroutine-safe.
func (ch *Channel) isSendPending() bool {
	if len(ch.sending) == 0 {
		if len(ch.sendQueue) == 0 {
			return false
		}
		ch.sending = <-ch.sendQueue
	}
	return true
}

// Updates the nextPacket proto message for us to send.
// Not goroutine-safe.
func (ch *Channel) updateNextPacket() {
	maxSize := ch.maxPacketMsgPayloadSize
	if len(ch.sending) <= maxSize {
		ch.nextPacketMsg.Data = ch.sending
		ch.nextPacketMsg.EOF = true
		ch.sending = nil
		atomic.AddInt32(&ch.sendQueueSize, -1) // decrement sendQueueSize
	} else {
		ch.nextPacketMsg.Data = ch.sending[:maxSize]
		ch.nextPacketMsg.EOF = false
		ch.sending = ch.sending[maxSize:]
	}

	ch.nextP2pWrapperPacketMsg.PacketMsg = ch.nextPacketMsg
	ch.nextPacket.Sum = ch.nextP2pWrapperPacketMsg
}

// Writes next PacketMsg to w and updates c.recentlySent.
// Not goroutine-safe.
func (ch *Channel) writePacketMsgTo(w protoio.Writer) (n int, err error) {
	ch.updateNextPacket()
	n, err = w.WriteMsg(ch.nextPacket)
	if err != nil {
		err = ErrPacketWrite{Source: err}
	}

	atomic.AddInt64(&ch.recentlySent, int64(n))
	return n, err
}

// Handles incoming PacketMsgs. It returns a message bytes if message is
// complete. NOTE message bytes may change on next call to recvPacketMsg.
// Not goroutine-safe.
func (ch *Channel) recvPacketMsg(packet tmp2p.PacketMsg) ([]byte, error) {
	recvCap, recvReceived := ch.desc.RecvMessageCapacity, len(ch.recving)+len(packet.Data)
	if recvCap < recvReceived {
		return nil, ErrPacketTooBig{Max: recvCap, Received: recvReceived}
	}

	ch.recving = append(ch.recving, packet.Data...)
	if packet.EOF {
		msgBytes := ch.recving

		// clear the slice without re-allocating.
		// http://stackoverflow.com/questions/16971741/how-do-you-clear-a-slice-in-go
		//   suggests this could be a memory leak, but we might as well keep the memory for the channel until it closes,
		//	at which point the recving slice stops being used and should be garbage collected
		ch.recving = ch.recving[:0] // make([]byte, 0, ch.desc.RecvBufferCapacity)
		return msgBytes, nil
	}
	return nil, nil
}

// Call this periodically to update stats for throttling purposes.
// Not goroutine-safe.
func (ch *Channel) updateStats() {
	// Exponential decay of stats.
	// TODO: optimize.
	atomic.StoreInt64(&ch.recentlySent, int64(float64(atomic.LoadInt64(&ch.recentlySent))*0.8))
}

// ----------------------------------------
// Packet

// mustWrapPacket takes a packet kind (oneof) and wraps it in a tmp2p.Packet message.
func mustWrapPacket(pb proto.Message) *tmp2p.Packet {
	msg := &tmp2p.Packet{}
	mustWrapPacketInto(pb, msg)
	return msg
}

func mustWrapPacketInto(pb proto.Message, dst *tmp2p.Packet) {
	switch pb := pb.(type) {
	case *tmp2p.PacketPing:
		dst.Sum = &tmp2p.Packet_PacketPing{
			PacketPing: pb,
		}
	case *tmp2p.PacketPong:
		dst.Sum = &tmp2p.Packet_PacketPong{
			PacketPong: pb,
		}
	case *tmp2p.PacketMsg:
		dst.Sum = &tmp2p.Packet_PacketMsg{
			PacketMsg: pb,
		}
	default:
		panic(fmt.Errorf("unknown packet type %T", pb))
	}
}