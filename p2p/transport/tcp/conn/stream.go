package conn

import (
	"io"
	"time"
)

// MCConnectionStream is just a wrapper around the original net.Conn.
type MConnectionStream struct {
	conn     *MConnection
	streadID byte

	deadline      time.Time
	readDeadline  time.Time
	writeDeadline time.Time
}

// Read reads whole messages from the internal map, which is populated by the
// global read loop.
func (s MConnectionStream) Read(b []byte) (n int, err error) {
	// If there are messages to read, read them.
	if _, ok := s.conn.recvMsgsByStreamID[s.streadID]; ok {
		readTimeout := s.readTimeout()
		if readTimeout > 0 { // read with timeout
			select {
			case msgBytes := <-s.conn.recvMsgsByStreamID[s.streadID]:
				if len(b) < len(msgBytes) {
					return len(msgBytes), io.ErrShortBuffer
				}
				n = copy(b, msgBytes)
				return n, nil
			case <-time.After(readTimeout):
				return 0, ErrTimeout
			}
		}

		// read without timeout
		msgBytes := <-s.conn.recvMsgsByStreamID[s.streadID]
		if len(b) < len(msgBytes) {
			return len(msgBytes), io.ErrShortBuffer
		}
		n = copy(b, msgBytes)
		return n, nil
	}

	// No messages to read.
	return 0, nil
}

// Write queues bytes to be sent onto the internal write queue. It returns
// len(b), but it doesn't guarantee that the Write actually succeeds.
func (s MConnectionStream) Write(b []byte) (n int, err error) {
	if err := s.conn.sendBytes(s.streadID, b, s.writeTimeout()); err != nil {
		return 0, err
	}
	return len(b), nil
}

// Close does nothing.
func (MConnectionStream) Close() error {
	return nil
}

// SetDeadline sets both the read and write deadlines for this stream. It does not set the
// read nor write deadline on the underlying TCP connection! A zero value for t means
// Conn.Read and Conn.Write will not time out.
//
// Only applies to new reads and writes.
func (s *MConnectionStream) SetDeadline(t time.Time) error { s.deadline = t; return nil }

// SetReadDeadline sets the read deadline for this stream. It does not set the
// read deadline on the underlying TCP connection! A zero value for t means
// Conn.Read will not time out.
//
// Only applies to new reads.
func (s *MConnectionStream) SetReadDeadline(t time.Time) error { s.readDeadline = t; return nil }

// SetWriteDeadline sets the write deadline for this stream. It does not set the
// write deadline on the underlying TCP connection! A zero value for t means
// Conn.Write will not time out.
//
// Only applies to new writes.
func (s *MConnectionStream) SetWriteDeadline(t time.Time) error { s.writeDeadline = t; return nil }

func (s MConnectionStream) readTimeout() time.Duration {
	now := time.Now()
	switch {
	case s.readDeadline.IsZero() && s.deadline.IsZero():
		return 0
	case s.readDeadline.After(now):
		return s.readDeadline.Sub(now)
	case s.deadline.After(now):
		return s.deadline.Sub(now)
	}
	return 0
}

func (s MConnectionStream) writeTimeout() time.Duration {
	now := time.Now()
	switch {
	case s.writeDeadline.IsZero() && s.deadline.IsZero():
		return 0
	case s.writeDeadline.After(now):
		return s.writeDeadline.Sub(now)
	case s.deadline.After(now):
		return s.deadline.Sub(now)
	}
	return 0
}
