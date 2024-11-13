package abstract

import (
	"github.com/cosmos/gogoproto/proto"

	na "github.com/cometbft/cometbft/p2p/netaddr"
)

const (
	// HandshakeStreamID is the stream ID for the handshake stream.
	// No reactor can use the same stream ID to avoid conflicts.
	HandshakeStreamID byte = 0x00
)

// Transport connects the local node to the rest of the network.
type Transport interface {
	// NetAddr returns the network address of the local node.
	NetAddr() na.NetAddr

	// Accept waits for and returns the next connection to the local node.
	Accept() (Connection, *na.NetAddr, error)

	// Dial dials the given address and returns a connection.
	Dial(addr na.NetAddr) (Connection, error)

	// Cleanup any resources associated with the given connection.
	//
	// Must be run when the peer is dropped for any reason.
	Cleanup(conn Connection) error
}

// StreamDescriptor describes a data stream. This could be a substream within a
// multiplexed TCP connection, QUIC stream, etc.
type StreamDescriptor interface {
	// StreamID returns the ID of the stream.
	StreamID() byte
	// MessageType returns the type of the message sent/received on this stream.
	MessageType() proto.Message
}
