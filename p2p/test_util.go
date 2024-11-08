package p2p

import (
	"fmt"
	"net"
	"time"

	"github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/crypto/ed25519"
	cmtnet "github.com/cometbft/cometbft/internal/net"
	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/p2p/abstract"
	na "github.com/cometbft/cometbft/p2p/netaddr"
	ni "github.com/cometbft/cometbft/p2p/nodeinfo"
	"github.com/cometbft/cometbft/p2p/nodekey"
	"github.com/cometbft/cometbft/p2p/transport/tcp/conn"
)

const testCh = 0x01

// ------------------------------------------------

func AddPeerToSwitchPeerSet(sw *Switch, peer Peer) {
	sw.peers.Add(peer) //nolint:errcheck // ignore error
}

func CreateRandomPeer(outbound bool) Peer {
	addr, netAddr := na.CreateRoutableAddr()
	p := &peer{
		peerConn: peerConn{
			outbound: outbound,
			// Connection: &conn.MConnection{},
			socketAddr: netAddr,
		},
		nodeInfo: mockNodeInfo{netAddr},
		metrics:  NopMetrics(),
	}
	p.SetLogger(log.TestingLogger().With("peer", addr))
	return p
}

// ------------------------------------------------------------------
// Connects switches via arbitrary net.Conn. Used for testing.

const TestHost = "localhost"

// MakeConnectedSwitches returns n switches, initialized according to the
// initSwitch function, and connected according to the connect function.
func MakeConnectedSwitches(cfg *config.P2PConfig,
	n int,
	initSwitch func(int, *Switch) *Switch,
	connect func([]*Switch, int, int),
) []*Switch {
	switches := MakeSwitches(cfg, n, initSwitch)
	return StartAndConnectSwitches(switches, connect)
}

// MakeSwitches returns n switches.
// initSwitch defines how the i'th switch should be initialized (ie. with what reactors).
func MakeSwitches(
	cfg *config.P2PConfig,
	n int,
	initSwitch func(int, *Switch) *Switch,
) []*Switch {
	switches := make([]*Switch, n)
	for i := 0; i < n; i++ {
		switches[i] = MakeSwitch(cfg, i, initSwitch)
	}
	return switches
}

// StartAndConnectSwitches connects the switches according to the connect function.
// If connect==Connect2Switches, the switches will be fully connected.
// NOTE: panics if any switch fails to start.
func StartAndConnectSwitches(
	switches []*Switch,
	connect func([]*Switch, int, int),
) []*Switch {
	if err := StartSwitches(switches); err != nil {
		panic(err)
	}

	for i := 0; i < len(switches); i++ {
		for j := i + 1; j < len(switches); j++ {
			connect(switches, i, j)
		}
	}

	return switches
}

// Connect2Switches will connect switches i and j via net.Pipe().
// Blocks until a connection is established.
// NOTE: caller ensures i and j are within bounds.
func Connect2Switches(switches []*Switch, i, j int) {
	switchI := switches[i]
	switchJ := switches[j]

	c1, c2 := conn.NetPipe()

	doneCh := make(chan struct{})
	go func() {
		err := switchI.addPeerWithConnection(mockConnection{c1})
		if err != nil {
			panic(err)
		}
		doneCh <- struct{}{}
	}()
	go func() {
		err := switchJ.addPeerWithConnection(mockConnection{c2})
		if err != nil {
			panic(err)
		}
		doneCh <- struct{}{}
	}()
	<-doneCh
	<-doneCh
}

// ConnectStarSwitches will connect switches c and j via net.Pipe().
func ConnectStarSwitches(c int) func([]*Switch, int, int) {
	// Blocks until a connection is established.
	// NOTE: caller ensures i and j is within bounds.
	return func(switches []*Switch, i, j int) {
		if i != c {
			return
		}

		switchI := switches[i]
		switchJ := switches[j]

		c1, c2 := conn.NetPipe()

		doneCh := make(chan struct{})
		go func() {
			err := switchI.addPeerWithConnection(mockConnection{c1})
			if err != nil {
				panic(err)
			}
			doneCh <- struct{}{}
		}()
		go func() {
			err := switchJ.addPeerWithConnection(mockConnection{c2})
			if err != nil {
				panic(err)
			}
			doneCh <- struct{}{}
		}()
		<-doneCh
		<-doneCh
	}
}

func (sw *Switch) addPeerWithConnection(conn abstract.Connection) error {
	closeConn := func(err error) {
		if cErr := conn.Close(err.Error()); cErr != nil {
			sw.Logger.Error("Error closing connection", "err", cErr)
		}
	}

	pc, err := testInboundPeerConn(conn, sw.config)
	if err != nil {
		closeConn(err)
		return err
	}

	stream, err := conn.OpenStream(testCh)
	if err != nil {
		closeConn(err)
		return err
	}
	defer stream.Close()

	ni, err := handshake(sw.nodeInfo, stream, time.Second)
	if err != nil {
		closeConn(err)
		return err
	}

	p, err := newPeer(
		pc,
		ni,
		sw.streamInfoByStreamID,
		sw.StopPeerForError,
	)
	if err != nil {
		closeConn(err)
		return err
	}

	if err = sw.addPeer(p); err != nil {
		closeConn(err)
		return err
	}

	return nil
}

// StartSwitches calls sw.Start() for each given switch.
// It returns the first encountered error.
func StartSwitches(switches []*Switch) error {
	for _, s := range switches {
		err := s.Start() // start switch and reactors
		if err != nil {
			return err
		}
	}
	return nil
}

func MakeSwitch(
	cfg *config.P2PConfig,
	i int,
	initSwitch func(int, *Switch) *Switch,
	opts ...SwitchOption,
) *Switch {
	nk := nodekey.NodeKey{
		PrivKey: ed25519.GenPrivKey(),
	}
	nodeInfo := testNodeInfo(nk.ID(), fmt.Sprintf("node%d", i))
	addr, err := na.NewFromString(
		na.IDAddrString(nk.ID(), nodeInfo.ListenAddr),
	)
	if err != nil {
		panic(err)
	}

	t := &mockTransport{}

	if err := t.Listen(*addr); err != nil {
		panic(err)
	}

	// TODO: let the config be passed in?
	sw := initSwitch(i, NewSwitch(cfg, t, opts...))
	sw.SetLogger(log.TestingLogger().With("switch", i))
	sw.SetNodeKey(&nk)

	// reset channels
	for ch := range sw.streamInfoByStreamID {
		if ch != testCh {
			nodeInfo.Channels = append(nodeInfo.Channels, ch)
		}
	}

	sw.SetNodeInfo(nodeInfo)

	return sw
}

func testInboundPeerConn(
	conn abstract.Connection,
	config *config.P2PConfig,
	// ourNodePrivKey crypto.PrivKey,
) (peerConn, error) {
	return testPeerConn(conn, config, false, false, nil)
}

func testPeerConn(
	rawConn abstract.Connection,
	cfg *config.P2PConfig,
	outbound, persistent bool,
	// _ourNodePrivKey crypto.PrivKey,
	socketAddr *na.NetAddr,
) (pc peerConn, err error) {
	conn := rawConn

	// Fuzz connection
	// TODO: uncomment
	if cfg.TestFuzz {
		fmt.Println("Fuzzing connection")
		// 	// so we have time to do peer handshakes and get set up
		// 	conn = fuzz.ConnAfterFromConfig(conn, 10*time.Second, cfg.TestFuzzConfig)
	}

	// Only the information we already have
	return newPeerConn(outbound, persistent, conn, socketAddr), nil
}

// ----------------------------------------------------------------
// rand node info

type AddrBookMock struct {
	Addrs        map[string]struct{}
	OurAddrs     map[string]struct{}
	PrivateAddrs map[string]struct{}
}

var _ AddrBook = (*AddrBookMock)(nil)

func (book *AddrBookMock) AddAddress(addr *na.NetAddr, _ *na.NetAddr) error {
	book.Addrs[addr.String()] = struct{}{}
	return nil
}

func (book *AddrBookMock) AddOurAddress(addr *na.NetAddr) {
	book.OurAddrs[addr.String()] = struct{}{}
}

func (book *AddrBookMock) OurAddress(addr *na.NetAddr) bool {
	_, ok := book.OurAddrs[addr.String()]
	return ok
}
func (*AddrBookMock) MarkGood(nodekey.ID) {}
func (book *AddrBookMock) HasAddress(addr *na.NetAddr) bool {
	_, ok := book.Addrs[addr.String()]
	return ok
}

func (book *AddrBookMock) RemoveAddress(addr *na.NetAddr) {
	delete(book.Addrs, addr.String())
}
func (*AddrBookMock) Save() {}
func (book *AddrBookMock) AddPrivateIDs(addrs []string) {
	for _, addr := range addrs {
		book.PrivateAddrs[addr] = struct{}{}
	}
}

type mockNodeInfo struct {
	addr *na.NetAddr
}

func (ni mockNodeInfo) ID() nodekey.ID                                      { return ni.addr.ID }
func (ni mockNodeInfo) NetAddr() (*na.NetAddr, error)                       { return ni.addr, nil }
func (mockNodeInfo) Validate() error                                        { return nil }
func (mockNodeInfo) CompatibleWith(ni.NodeInfo) error                       { return nil }
func (mockNodeInfo) Handshake(net.Conn, time.Duration) (ni.NodeInfo, error) { return nil, nil }

func testNodeInfo(id nodekey.ID, name string) ni.Default {
	return ni.Default{
		ProtocolVersion: ni.NewProtocolVersion(0, 0, 0),
		DefaultNodeID:   id,
		ListenAddr:      fmt.Sprintf("127.0.0.1:%d", getFreePort()),
		Network:         "testing",
		Version:         "1.2.3-rc0-deadbeef",
		Channels:        []byte{testCh},
		Moniker:         name,
		Other: ni.DefaultOther{
			TxIndex:    "on",
			RPCAddress: fmt.Sprintf("127.0.0.1:%d", getFreePort()),
		},
	}
}

func getFreePort() int {
	port, err := cmtnet.GetFreePort()
	if err != nil {
		panic(err)
	}
	return port
}
