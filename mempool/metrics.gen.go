// Code generated by metricsgen. DO NOT EDIT.

package mempool

import (
	"github.com/cometbft/cometbft/libs/metrics/discard"
	prometheus "github.com/cometbft/cometbft/libs/metrics/prometheus"
	stdprometheus "github.com/prometheus/client_golang/prometheus"
)

func PrometheusMetrics(namespace string, labelsAndValues ...string) *Metrics {
	labels := []string{}
	for i := 0; i < len(labelsAndValues); i += 2 {
		labels = append(labels, labelsAndValues[i])
	}
	return &Metrics{
		Size: prometheus.NewGaugeFrom(stdprometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: MetricsSubsystem,
			Name:      "size",
			Help:      "Number of uncommitted transactions in the mempool.  Deprecated: this value can be obtained as the sum of LaneSize.",
		}, labels).With(labelsAndValues...),
		SizeBytes: prometheus.NewGaugeFrom(stdprometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: MetricsSubsystem,
			Name:      "size_bytes",
			Help:      "Total size of the mempool in bytes.  Deprecated: this value can be obtained as the sum of LaneBytes.",
		}, labels).With(labelsAndValues...),
		LaneSize: prometheus.NewGaugeFrom(stdprometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: MetricsSubsystem,
			Name:      "lane_size",
			Help:      "Number of uncommitted transactions per lane.",
		}, append(labels, "lane")).With(labelsAndValues...),
		LaneBytes: prometheus.NewGaugeFrom(stdprometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: MetricsSubsystem,
			Name:      "lane_bytes",
			Help:      "Number of used bytes per lane.",
		}, append(labels, "lane")).With(labelsAndValues...),
		TxLifeSpan: prometheus.NewHistogramFrom(stdprometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: MetricsSubsystem,
			Name:      "tx_life_span",
			Help:      "Duration in ms of a transaction in the mempool.",

			Buckets: []float64{50, 100, 200, 500, 1000},
		}, append(labels, "lane")).With(labelsAndValues...),
		TxSizeBytes: prometheus.NewHistogramFrom(stdprometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: MetricsSubsystem,
			Name:      "tx_size_bytes",
			Help:      "Histogram of transaction sizes in bytes.",

			Buckets: stdprometheus.ExponentialBuckets(1, 3, 7),
		}, labels).With(labelsAndValues...),
		FailedTxs: prometheus.NewCounterFrom(stdprometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: MetricsSubsystem,
			Name:      "failed_txs",
			Help:      "Number of failed transactions.",
		}, labels).With(labelsAndValues...),
		RejectedTxs: prometheus.NewCounterFrom(stdprometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: MetricsSubsystem,
			Name:      "rejected_txs",
			Help:      "Number of rejected transactions.",
		}, labels).With(labelsAndValues...),
		EvictedTxs: prometheus.NewCounterFrom(stdprometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: MetricsSubsystem,
			Name:      "evicted_txs",
			Help:      "Number of evicted transactions.",
		}, labels).With(labelsAndValues...),
		RecheckTimes: prometheus.NewCounterFrom(stdprometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: MetricsSubsystem,
			Name:      "recheck_times",
			Help:      "Number of times transactions are rechecked in the mempool.",
		}, labels).With(labelsAndValues...),
		AlreadyReceivedTxs: prometheus.NewCounterFrom(stdprometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: MetricsSubsystem,
			Name:      "already_received_txs",
			Help:      "Number of duplicate transaction reception.",
		}, labels).With(labelsAndValues...),
		ActiveOutboundConnections: prometheus.NewGaugeFrom(stdprometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: MetricsSubsystem,
			Name:      "active_outbound_connections",
			Help:      "Number of connections being actively used for gossiping transactions (experimental feature).",
		}, labels).With(labelsAndValues...),
		HaveTxMsgsReceived: prometheus.NewCounterFrom(stdprometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: MetricsSubsystem,
			Name:      "have_tx_msgs_received",
			Help:      "Number of HaveTx messages received (cumulative).",
		}, append(labels, "from")).With(labelsAndValues...),
		ResetMsgsSent: prometheus.NewCounterFrom(stdprometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: MetricsSubsystem,
			Name:      "reset_msgs_sent",
			Help:      "Number of Reset messages sent (cumulative).",
		}, labels).With(labelsAndValues...),
		DisabledRoutes: prometheus.NewGaugeFrom(stdprometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: MetricsSubsystem,
			Name:      "disabled_routes",
			Help:      "Number of disabled routes.",
		}, labels).With(labelsAndValues...),
		RecheckDurationSeconds: prometheus.NewGaugeFrom(stdprometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: MetricsSubsystem,
			Name:      "recheck_duration_seconds",
			Help:      "Cumulative time spent rechecking transactions",
		}, labels).With(labelsAndValues...),
	}
}

func NopMetrics() *Metrics {
	return &Metrics{
		Size:                      discard.NewGauge(),
		SizeBytes:                 discard.NewGauge(),
		LaneSize:                  discard.NewGauge(),
		LaneBytes:                 discard.NewGauge(),
		TxLifeSpan:                discard.NewHistogram(),
		TxSizeBytes:               discard.NewHistogram(),
		FailedTxs:                 discard.NewCounter(),
		RejectedTxs:               discard.NewCounter(),
		EvictedTxs:                discard.NewCounter(),
		RecheckTimes:              discard.NewCounter(),
		AlreadyReceivedTxs:        discard.NewCounter(),
		ActiveOutboundConnections: discard.NewGauge(),
		HaveTxMsgsReceived:        discard.NewCounter(),
		ResetMsgsSent:             discard.NewCounter(),
		DisabledRoutes:            discard.NewGauge(),
		RecheckDurationSeconds:    discard.NewGauge(),
	}
}
