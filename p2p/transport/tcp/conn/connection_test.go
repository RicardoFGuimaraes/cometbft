package conn

import (
	"encoding/hex"
	"net"
	"testing"
	"time"

	"github.com/cosmos/gogoproto/proto"
	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	tmp2p "github.com/cometbft/cometbft/api/cometbft/p2p/v1"
	pbtypes "github.com/cometbft/cometbft/api/cometbft/types/v1"
	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/libs/protoio"
)

const (
	maxPingPongPacketSize = 1024 // bytes
	testStreamID          = 0x01
)

func createMConnectionWithSingleStream(t *testing.T, conn net.Conn) (*MConnection, *MConnectionStream) {
	t.Helper()

	cfg := DefaultMConnConfig()
	cfg.PingInterval = 90 * time.Millisecond
	cfg.PongTimeout = 45 * time.Millisecond
	chDescs := []*ChannelDescriptor{{ID: testStreamID, Priority: 1, SendQueueCapacity: 1}}
	c := NewMConnectionWithConfig(conn, chDescs, cfg)
	c.SetLogger(log.TestingLogger())

	stream, err := c.OpenStream(testStreamID)
	require.NoError(t, err)

	return c, stream.(*MConnectionStream)
}

func TestMConnection_FlushStop(t *testing.T) {
	server, client := NetPipe()
	defer server.Close()
	defer client.Close()

	clientConn, clientStream := createMConnectionWithSingleStream(t, client)
	err := clientConn.Start()
	require.NoError(t, err)
	defer clientConn.Stop() //nolint:errcheck // ignore for tests

	msg := []byte("abc")
	_, err = clientStream.Write(msg)
	assert.NoError(t, err)

	msgLength := 14

	// start the reader in a new routine, so we can flush
	errCh := make(chan error)
	go func() {
		msgB := make([]byte, msgLength)
		_, err := server.Read(msgB)
		if err != nil {
			t.Error(err)
			return
		}
		errCh <- err
	}()

	// stop the conn - it should flush all conns
	clientConn.FlushStop()

	timer := time.NewTimer(3 * time.Second)
	select {
	case <-errCh:
	case <-timer.C:
		t.Error("timed out waiting for msgs to be read")
	}
}

func TestMConnection_StreamWrite(t *testing.T) {
	server, client := NetPipe()
	defer server.Close()
	defer client.Close()

	mconn, clientStream := createMConnectionWithSingleStream(t, client)
	err := mconn.Start()
	require.NoError(t, err)
	defer mconn.Stop() //nolint:errcheck // ignore for tests

	msg := []byte("Ant-Man")
	_, err = clientStream.Write(msg)
	require.NoError(t, err)
	// NOTE: subsequent writes could pass because we are reading from
	// the send queue in a separate goroutine.
	_, err = server.Read(make([]byte, len(msg)))
	require.NoError(t, err)
	assert.True(t, mconn.CanSend(testStreamID))

	msg = []byte("Spider-Man")
	err = clientStream.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
	require.NoError(t, err)
	_, err = clientStream.Write(msg)
	require.NoError(t, err)
	_, err = server.Read(make([]byte, len(msg)))
	require.NoError(t, err)
}

func TestMConnection_StreamReadWrite(t *testing.T) {
	server, client := NetPipe()
	defer server.Close()
	defer client.Close()

	mconn1, stream1 := createMConnectionWithSingleStream(t, client)
	err := mconn1.Start()
	require.NoError(t, err)
	defer mconn1.Stop() //nolint:errcheck // ignore for tests

	mconn2, stream2 := createMConnectionWithSingleStream(t, server)
	err = mconn2.Start()
	require.NoError(t, err)
	defer mconn2.Stop() //nolint:errcheck // ignore for tests

	// => write
	msg := []byte("Cyclops")
	_, err = stream1.Write(msg)
	require.NoError(t, err)

	// => read
	buf := make([]byte, len(msg))
	n, err := stream2.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, len(msg), n)
	assert.Equal(t, msg, buf)
}

func TestMConnectionStatus(t *testing.T) {
	server, client := NetPipe()
	defer server.Close()
	defer client.Close()

	mconn, _ := createMConnectionWithSingleStream(t, client)
	err := mconn.Start()
	require.NoError(t, err)
	defer mconn.Stop() //nolint:errcheck // ignore for tests

	status := mconn.Status()
	assert.NotNil(t, status)
	assert.Zero(t, status.Channels[0].SendQueueSize)
}

func TestMConnection_PongTimeoutResultsInError(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	mconn, _ := createMConnectionWithSingleStream(t, client)
	err := mconn.Start()
	require.NoError(t, err)
	defer mconn.Stop() //nolint:errcheck // ignore for tests

	serverGotPing := make(chan struct{})
	go func() {
		// read ping
		var pkt tmp2p.Packet
		_, err := protoio.NewDelimitedReader(server, maxPingPongPacketSize).ReadMsg(&pkt)
		require.NoError(t, err)
		serverGotPing <- struct{}{}
	}()
	<-serverGotPing

	pongTimerExpired := mconn.config.PongTimeout + 200*time.Millisecond
	select {
	case err := <-mconn.ErrorCh():
		assert.NotNil(t, err)
	case <-time.After(pongTimerExpired):
		t.Fatalf("Expected to receive error after %v", pongTimerExpired)
	}
}

func TestMConnection_MultiplePongsInTheBeginning(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	mconn, stream := createMConnectionWithSingleStream(t, client)
	err := mconn.Start()
	require.NoError(t, err)
	defer mconn.Stop() //nolint:errcheck // ignore for tests

	// sending 3 pongs in a row (abuse)
	protoWriter := protoio.NewDelimitedWriter(server)

	_, err = protoWriter.WriteMsg(mustWrapPacket(&tmp2p.PacketPong{}))
	require.NoError(t, err)

	_, err = protoWriter.WriteMsg(mustWrapPacket(&tmp2p.PacketPong{}))
	require.NoError(t, err)

	_, err = protoWriter.WriteMsg(mustWrapPacket(&tmp2p.PacketPong{}))
	require.NoError(t, err)

	serverGotPing := make(chan struct{})
	go func() {
		// read ping (one byte)
		var packet tmp2p.Packet
		_, err := protoio.NewDelimitedReader(server, maxPingPongPacketSize).ReadMsg(&packet)
		require.NoError(t, err)
		serverGotPing <- struct{}{}

		// respond with pong
		_, err = protoWriter.WriteMsg(mustWrapPacket(&tmp2p.PacketPong{}))
		require.NoError(t, err)
	}()
	<-serverGotPing

	pongTimerExpired := mconn.config.PongTimeout + 20*time.Millisecond
	select {
	case err := <-mconn.ErrorCh():
		t.Fatalf("Expected no error, but got %v", err)
	case <-time.After(pongTimerExpired):
		assert.True(t, mconn.IsRunning())

		// check there's no data
		n, err := stream.Read(make([]byte, 1))
		assert.NoError(t, err)
		assert.Zero(t, n)
	}
}

func TestMConnection_MultiplePings(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	mconn, _ := createMConnectionWithSingleStream(t, client)
	err := mconn.Start()
	require.NoError(t, err)
	defer mconn.Stop() //nolint:errcheck // ignore for tests

	// sending 3 pings in a row (abuse)
	// see https://github.com/tendermint/tendermint/issues/1190
	protoReader := protoio.NewDelimitedReader(server, maxPingPongPacketSize)
	protoWriter := protoio.NewDelimitedWriter(server)
	var pkt tmp2p.Packet

	_, err = protoWriter.WriteMsg(mustWrapPacket(&tmp2p.PacketPing{}))
	require.NoError(t, err)

	_, err = protoReader.ReadMsg(&pkt)
	require.NoError(t, err)

	_, err = protoWriter.WriteMsg(mustWrapPacket(&tmp2p.PacketPing{}))
	require.NoError(t, err)

	_, err = protoReader.ReadMsg(&pkt)
	require.NoError(t, err)

	_, err = protoWriter.WriteMsg(mustWrapPacket(&tmp2p.PacketPing{}))
	require.NoError(t, err)

	_, err = protoReader.ReadMsg(&pkt)
	require.NoError(t, err)

	assert.True(t, mconn.IsRunning())
}

func TestMConnection_PingPongs(t *testing.T) {
	// check that we are not leaking any go-routines
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	server, client := net.Pipe()

	defer server.Close()
	defer client.Close()

	mconn, stream := createMConnectionWithSingleStream(t, client)
	err := mconn.Start()
	require.NoError(t, err)
	defer mconn.Stop() //nolint:errcheck // ignore for tests

	serverGotPing := make(chan struct{})
	go func() {
		protoReader := protoio.NewDelimitedReader(server, maxPingPongPacketSize)
		protoWriter := protoio.NewDelimitedWriter(server)
		var pkt tmp2p.PacketPing

		// read ping
		_, err = protoReader.ReadMsg(&pkt)
		require.NoError(t, err)
		serverGotPing <- struct{}{}

		// respond with pong
		_, err = protoWriter.WriteMsg(mustWrapPacket(&tmp2p.PacketPong{}))
		require.NoError(t, err)

		time.Sleep(mconn.config.PingInterval)

		// read ping
		_, err = protoReader.ReadMsg(&pkt)
		require.NoError(t, err)
		serverGotPing <- struct{}{}

		// respond with pong
		_, err = protoWriter.WriteMsg(mustWrapPacket(&tmp2p.PacketPong{}))
		require.NoError(t, err)
	}()
	<-serverGotPing
	<-serverGotPing

	pongTimerExpired := (mconn.config.PongTimeout + 20*time.Millisecond) * 2
	select {
	case err := <-mconn.ErrorCh():
		t.Fatalf("Expected no error, but got %v", err)
	case <-time.After(2 * pongTimerExpired):
		assert.True(t, mconn.IsRunning())

		// check there's no data
		n, err := stream.Read(make([]byte, 1))
		assert.NoError(t, err)
		assert.Zero(t, n)
	}
}

func TestMConnectionStopsAndReturnsError(t *testing.T) {
	server, client := NetPipe()
	defer server.Close()
	defer client.Close()

	mconn, _ := createMConnectionWithSingleStream(t, client)
	err := mconn.Start()
	require.NoError(t, err)
	defer mconn.Stop() //nolint:errcheck // ignore for tests

	if err := client.Close(); err != nil {
		t.Error(err)
	}

	select {
	case err := <-mconn.ErrorCh():
		assert.NotNil(t, err)
		assert.False(t, mconn.IsRunning())

		// check there's no data
		n, err := stream.Read(make([]byte, 1))
		assert.NoError(t, err)
		assert.Zero(t, n)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Did not receive error in 500ms")
	}
}

func newClientAndServerConnsForReadErrors(t *testing.T) (*MConnection, *MConnectionStream, *MConnection, *MConnectionStream) {
	t.Helper()
	server, client := NetPipe()

	// create client conn with two channels
	chDescs := []*ChannelDescriptor{
		{ID: testStreamID, Priority: 1, SendQueueCapacity: 1},
		{ID: 0x02, Priority: 1, SendQueueCapacity: 1},
	}
	cfg := DefaultMConnConfig()
	cfg.PingInterval = 90 * time.Millisecond
	cfg.PongTimeout = 45 * time.Millisecond
	mconnClient := NewMConnectionWithConfig(client, chDescs, cfg)
	clientStream, err := mconnClient.OpenStream(testStreamID)
	require.NoError(t, err)
	mconnClient.SetLogger(log.TestingLogger().With("module", "client"))
	err = mconnClient.Start()
	require.NoError(t, err)

	// create server conn with 1 channel
	// it fires on chOnErr when there's an error
	serverLogger := log.TestingLogger().With("module", "server")
	mconnServer, serverStream := createMConnectionWithSingleStream(t, server)
	mconnServer.SetLogger(serverLogger)
	err = mconnServer.Start()
	require.NoError(t, err)

	return mconnClient, clientStream.(*MConnectionStream), mconnServer, serverStream
}

func assertGotBytes(t *testing.T, s *MConnectionStream, want []byte) {
	t.Helper()

	err := s.SetReadDeadline(time.Now().Add(5 * time.Second))
	require.NoError(t, err)
	buf := make([]byte, len(want))
	n, err := s.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, len(want), n)
	assert.Equal(t, want, buf)
}

func expectSend(ch <-chan error) bool {
	after := time.After(time.Second * 5)
	select {
	case <-ch:
		return true
	case <-after:
		return false
	}
}

func TestMConnection_ReadErrorBadEncoding(t *testing.T) {
	mconnClient, mconnServer := newClientAndServerConnsForReadErrors(t)
	defer mconnClient.Stop() //nolint:errcheck // ignore for tests
	defer mconnServer.Stop() //nolint:errcheck // ignore for tests

	client := mconnClient.conn

	// Write it.
	_, err := client.Write([]byte{1, 2, 3, 4, 5})
	require.NoError(t, err)
	assert.True(t, expectSend(mconnServer.ErrorCh()), "badly encoded msgPacket")
}

func TestMConnection_ReadErrorUnknownChannel(t *testing.T) {
	mconnClient, mconnServer := newClientAndServerConnsForReadErrors(t)
	defer mconnClient.Stop() //nolint:errcheck // ignore for tests
	defer mconnServer.Stop() //nolint:errcheck // ignore for tests

	msg := []byte("Ant-Man")

	// fail to send msg on channel unknown by client
	assert.NoError(t, mconnClient.Send(0x03, msg, 0))

	// send msg on channel unknown by the server.
	// should cause an error
	assert.Error(t, mconnClient.Send(0x02, msg, 0))
	assert.True(t, expectSend(mconnServer.ErrorCh()), "unknown channel")
}

func TestMConnection_ReadErrorLongMessage(t *testing.T) {
	mconnClient, mconnServer := newClientAndServerConnsForReadErrors(t)
	defer mconnClient.Stop() //nolint:errcheck // ignore for tests
	defer mconnServer.Stop() //nolint:errcheck // ignore for tests

	client := mconnClient.conn
	protoWriter := protoio.NewDelimitedWriter(client)

	// send msg that's just right
	packet := tmp2p.PacketMsg{
		ChannelID: 0x01,
		EOF:       true,
		Data:      make([]byte, mconnClient.config.MaxPacketMsgPayloadSize),
	}

	_, err := protoWriter.WriteMsg(mustWrapPacket(&packet))
	require.NoError(t, err)
	assert.True(t, expectBytes(mconnServer.recvMsgsByStreamID[0x01]), "msg just right")

	// send msg that's too long
	packet = tmp2p.PacketMsg{
		ChannelID: 0x01,
		EOF:       true,
		Data:      make([]byte, mconnClient.config.MaxPacketMsgPayloadSize+100),
	}

	_, err = protoWriter.WriteMsg(mustWrapPacket(&packet))
	require.Error(t, err)
	assert.True(t, expectSend(chOnErr), "msg too long")
}

func TestMConnection_ReadErrorUnknownMsgType(t *testing.T) {
	mconnClient, _, mconnServer, _ := newClientAndServerConnsForReadErrors(t)
	defer mconnClient.Stop() //nolint:errcheck // ignore for tests
	defer mconnServer.Stop() //nolint:errcheck // ignore for tests

	// send msg with unknown msg type
	_, err := protoio.NewDelimitedWriter(mconnClient.conn).WriteMsg(&pbtypes.Header{ChainID: "x"})
	require.NoError(t, err)
	assert.True(t, expectSend(mconnServer.ErrorCh()), "unknown msg type")
}

//nolint:lll //ignore line length for tests
func TestConnVectors(t *testing.T) {
	testCases := []struct {
		testName string
		msg      proto.Message
		expBytes string
	}{
		{"PacketPing", &tmp2p.PacketPing{}, "0a00"},
		{"PacketPong", &tmp2p.PacketPong{}, "1200"},
		{"PacketMsg", &tmp2p.PacketMsg{ChannelID: 1, EOF: false, Data: []byte("data transmitted over the wire")}, "1a2208011a1e64617461207472616e736d6974746564206f766572207468652077697265"},
	}

	for _, tc := range testCases {
		pm := mustWrapPacket(tc.msg)
		bz, err := pm.Marshal()
		require.NoError(t, err, tc.testName)

		require.Equal(t, tc.expBytes, hex.EncodeToString(bz), tc.testName)
	}
}

func TestMConnection_ChannelOverflow(t *testing.T) {
	mconnClient, _, mconnServer, serverStream := newClientAndServerConnsForReadErrors(t)
	defer mconnClient.Stop() //nolint:errcheck // ignore for tests
	defer mconnServer.Stop() //nolint:errcheck // ignore for tests

	client := mconnClient.conn
	protoWriter := protoio.NewDelimitedWriter(client)

	// send msg that's just right
	packet := tmp2p.PacketMsg{
		ChannelID: 0x01,
		EOF:       true,
		Data:      []byte(`42`),
	}
	_, err := protoWriter.WriteMsg(mustWrapPacket(&packet))
	require.NoError(t, err)
	assertGotBytes(t, serverStream, []byte(`42`))

	// channel ID that's too large
	packet.ChannelID = int32(1025)
	_, err = protoWriter.WriteMsg(mustWrapPacket(&packet))
	require.NoError(t, err)
	// assert.False(t, expectBytes(mconnServer.recvMsgsByStreamID[1025]))
}
