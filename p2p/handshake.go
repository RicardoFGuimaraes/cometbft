package p2p

import (
	"fmt"
	"io"
	"time"

	tmp2p "github.com/cometbft/cometbft/api/cometbft/p2p/v1"
	"github.com/cometbft/cometbft/libs/protoio"
	ni "github.com/cometbft/cometbft/p2p/nodeinfo"
	"github.com/cometbft/cometbft/p2p/nodekey"
)

const (
	// HandshakeStreamID is the stream ID for the handshake stream.
	// This stream can be reused by any reactor.
	HandshakeStreamID byte = 0x00
)

type HandshakeStream interface {
	SetDeadline(t time.Time) error
	io.ReadWriter
}

// ErrRejected indicates that a Peer was rejected carrying additional
// information as to the reason.
type ErrRejected struct {
	conn              HandshakeStream
	err               error
	id                nodekey.ID
	isAuthFailure     bool
	isDuplicate       bool
	isFiltered        bool
	isIncompatible    bool
	isNodeInfoInvalid bool
	isSelf            bool
}

func (e ErrRejected) Error() string {
	if e.isAuthFailure {
		return fmt.Sprintf("auth failure: %s", e.err)
	}

	if e.isDuplicate {
		if e.conn != nil {
			return "duplicate CONN"
		}
		if e.id != "" {
			return fmt.Sprintf("duplicate ID<%v>", e.id)
		}
	}

	if e.isFiltered {
		if e.conn != nil {
			return fmt.Sprintf("filtered CONN: %s", e.err)
		}

		if e.id != "" {
			return fmt.Sprintf("filtered ID<%v>: %s", e.id, e.err)
		}
	}

	if e.isIncompatible {
		return fmt.Sprintf("incompatible: %s", e.err)
	}

	if e.isNodeInfoInvalid {
		return fmt.Sprintf("invalid NodeInfo: %s", e.err)
	}

	if e.isSelf {
		return fmt.Sprintf("self ID<%v>", e.id)
	}

	return e.err.Error()
}

// IsAuthFailure when Peer authentication was unsuccessful.
func (e ErrRejected) IsAuthFailure() bool { return e.isAuthFailure }

// IsDuplicate when Peer ID or IP are present already.
func (e ErrRejected) IsDuplicate() bool { return e.isDuplicate }

// IsFiltered when Peer ID or IP was filtered.
func (e ErrRejected) IsFiltered() bool { return e.isFiltered }

// IsIncompatible when Peer NodeInfo is not compatible with our own.
func (e ErrRejected) IsIncompatible() bool { return e.isIncompatible }

// IsNodeInfoInvalid when the sent NodeInfo is not valid.
func (e ErrRejected) IsNodeInfoInvalid() bool { return e.isNodeInfoInvalid }

// IsSelf when Peer is our own node.
func (e ErrRejected) IsSelf() bool { return e.isSelf }

// Do a handshake and verify the node info.
func handshake(ourNodeInfo ni.NodeInfo, s HandshakeStream, handshakeTimeout time.Duration) (ni.NodeInfo, error) {
	nodeInfo, err := exchangeNodeInfo(ourNodeInfo, s, handshakeTimeout)
	if err != nil {
		return nil, ErrRejected{
			conn:          s,
			err:           fmt.Errorf("handshake failed: %w", err),
			isAuthFailure: true,
		}
	}

	if err := nodeInfo.Validate(); err != nil {
		return nil, ErrRejected{
			conn:              s,
			err:               err,
			isNodeInfoInvalid: true,
		}
	}

	// TODO
	// Ensure connection key matches self reported key.
	//
	// Transport ensures that connID == addr.ID.
	// Assert that addr.ID == nodeInfo.ID.
	// if remoteNodeID != nodeInfo.ID() {
	// 	return nil, ErrRejected{
	// 		conn: c,
	// 		id:   remoteNodeID,
	// 		err: fmt.Errorf(
	// 			"addr.ID (%v) NodeInfo.ID (%v) mismatch",
	// 			remoteNodeID,
	// 			nodeInfo.ID(),
	// 		),
	// 		isAuthFailure: true,
	// 	}
	// }

	// Reject self.
	if ourNodeInfo.ID() == nodeInfo.ID() {
		return nil, ErrRejected{
			conn:   s,
			id:     nodeInfo.ID(),
			isSelf: true,
		}
	}

	if err := ourNodeInfo.CompatibleWith(nodeInfo); err != nil {
		return nil, ErrRejected{
			conn:           s,
			err:            err,
			id:             nodeInfo.ID(),
			isIncompatible: true,
		}
	}

	return nodeInfo, nil
}

func exchangeNodeInfo(ourNodeInfo ni.NodeInfo, s HandshakeStream, timeout time.Duration) (peerNodeInfo ni.NodeInfo, err error) {
	if err = s.SetDeadline(time.Now().Add(timeout)); err != nil {
		return nil, err
	}

	var (
		errc           = make(chan error, 2)
		pbpeerNodeInfo tmp2p.DefaultNodeInfo
	)

	go func(errc chan<- error, s HandshakeStream) {
		ourNodeInfoProto := ourNodeInfo.(ni.Default).ToProto()
		_, err := protoio.NewDelimitedWriter(s).WriteMsg(ourNodeInfoProto)
		errc <- err
	}(errc, s)
	go func(errc chan<- error, s HandshakeStream) {
		protoReader := protoio.NewDelimitedReader(s, ni.MaxSize())
		_, err := protoReader.ReadMsg(&pbpeerNodeInfo)
		errc <- err
	}(errc, s)

	for i := 0; i < cap(errc); i++ {
		err := <-errc
		if err != nil {
			return nil, err
		}
	}

	peerNodeInfo, err = ni.DefaultFromToProto(&pbpeerNodeInfo)
	if err != nil {
		return nil, err
	}

	return peerNodeInfo, s.SetDeadline(time.Time{})
}