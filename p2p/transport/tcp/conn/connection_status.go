package conn

import (
	"time"

	flow "github.com/cometbft/cometbft/internal/flowrate"
)

type ConnectionStatus struct {
	Duration    time.Duration
	SendMonitor flow.Status
	RecvMonitor flow.Status
	Channels    []ChannelStatus
}

func (cs ConnectionStatus) ConnectedFor() time.Duration {
	return cs.Duration
}

type ChannelStatus struct {
	ID                byte
	SendQueueCapacity int
	SendQueueSize     int
	Priority          int
	RecentlySent      int64
}
