package p2p

import (
	"net"
	"time"

	"github.com/cometbft/cometbft/p2p/abstract"
	na "github.com/cometbft/cometbft/p2p/netaddr"
)

type mockStream struct {
	net.Conn
}

func (s mockStream) Read(b []byte) (n int, err error) {
	return s.Conn.Read(b)
}

func (s mockStream) Write(b []byte) (n int, err error) {
	return s.Conn.Write(b)
}

func (mockStream) Close() error {
	return nil
}
func (s mockStream) SetDeadline(t time.Time) error      { return s.Conn.SetReadDeadline(t) }
func (s mockStream) SetReadDeadline(t time.Time) error  { return s.Conn.SetReadDeadline(t) }
func (s mockStream) SetWriteDeadline(t time.Time) error { return s.Conn.SetWriteDeadline(t) }

type mockConnection struct {
	net.Conn
	connectedAt time.Time
}

func newMockConnection(c net.Conn) *mockConnection {
	return &mockConnection{
		Conn:        c,
		connectedAt: time.Now(),
	}
}

func (c mockConnection) OpenStream(byte) (abstract.Stream, error) {
	return &mockStream{
		Conn: c.Conn,
	}, nil
}

func (c mockConnection) LocalAddr() net.Addr {
	return c.Conn.LocalAddr()
}

func (c mockConnection) RemoteAddr() net.Addr {
	return c.Conn.RemoteAddr()
}
func (c mockConnection) Close(string) error         { return c.Conn.Close() }
func (c mockConnection) FlushAndClose(string) error { return c.Conn.Close() }

type mockStatus struct {
	connectedFor time.Duration
}

func (s mockStatus) ConnectedFor() time.Duration { return s.connectedFor }
func (c mockConnection) ConnectionState() any {
	return &mockStatus{
		connectedFor: time.Since(c.connectedAt),
	}
}

var _ abstract.Transport = (*mockTransport)(nil)

type mockTransport struct {
	ln   net.Listener
	addr na.NetAddr
}

func (t *mockTransport) Listen(addr na.NetAddr) error {
	ln, err := net.Listen("tcp", addr.DialString())
	if err != nil {
		return err
	}
	t.addr = addr
	t.ln = ln
	return nil
}

func (t *mockTransport) NetAddr() na.NetAddr {
	return t.addr
}

func (t *mockTransport) Accept() (abstract.Connection, *na.NetAddr, error) {
	c, err := t.ln.Accept()
	return newMockConnection(c), nil, err
}

func (*mockTransport) Dial(addr na.NetAddr) (abstract.Connection, error) {
	c, err := addr.DialTimeout(time.Second)
	return newMockConnection(c), err
}

func (*mockTransport) Cleanup(abstract.Connection) error {
	return nil
}

func (*mockTransport) UpdateStreamDescriptors([]abstract.StreamDescriptor) {}
