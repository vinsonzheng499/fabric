/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package bdls

import (
	"sync"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/metrics"
)

// BDLSMetrics provides comprehensive performance monitoring for BDLS consensus
type BDLSMetrics struct {
	// Consensus performance metrics
	ConsensusLatency         metrics.Histogram
	MessageProcessingTime    metrics.Histogram
	NetworkLatency          metrics.Histogram
	
	// Throughput metrics
	TotalTransactions       metrics.Counter
	TransactionsPerSecond   metrics.Gauge
	
	// Error and retry metrics
	ConsensusErrors         metrics.Counter
	NetworkErrors           metrics.Counter
	RetryAttempts          metrics.Counter
	
	// Resource utilization
	MemoryUsage            metrics.Gauge
	CPUUsage               metrics.Gauge
	NetworkBandwidth       metrics.Gauge
	
	// BDLS-specific metrics
	ViewChanges            metrics.Counter
	HeartbeatLatency       metrics.Histogram
	BatchSize              metrics.Histogram
	
	// **PHASE 1.3**: Security and validation metrics
	SecurityEvents         metrics.Counter
	ValidatedMessages      metrics.Counter
	
	// Operational metrics
	ActiveConnections      metrics.Gauge
	QueueDepth            metrics.Gauge
	
	mu                    sync.RWMutex
	lastTxCount          int64
	lastTxTime           time.Time
}

// NewBDLSMetrics creates a new metrics instance for BDLS consensus monitoring
func NewBDLSMetrics(provider metrics.Provider) *BDLSMetrics {
	return &BDLSMetrics{
		ConsensusLatency: provider.NewHistogram(metrics.HistogramOpts{
			Namespace:    "consensus",
			Subsystem:    "bdls",
			Name:         "consensus_latency",
			Help:         "Time taken to reach consensus on a proposal",
			LabelNames:   []string{"channel"},
			StatsdFormat: "%{#fqname}.%{channel}",
		}),
		MessageProcessingTime: provider.NewHistogram(metrics.HistogramOpts{
			Namespace:    "consensus",
			Subsystem:    "bdls",
			Name:         "message_processing_time",
			Help:         "Time taken to process consensus messages",
			LabelNames:   []string{"channel", "message_type"},
			StatsdFormat: "%{#fqname}.%{channel}.%{message_type}",
		}),
		NetworkLatency: provider.NewHistogram(metrics.HistogramOpts{
			Namespace:    "consensus",
			Subsystem:    "bdls",
			Name:         "network_latency",
			Help:         "Network latency for consensus messages",
			LabelNames:   []string{"channel", "peer"},
			StatsdFormat: "%{#fqname}.%{channel}.%{peer}",
		}),
		TotalTransactions: provider.NewCounter(metrics.CounterOpts{
			Namespace:    "consensus",
			Subsystem:    "bdls",
			Name:         "total_transactions",
			Help:         "Total number of transactions processed",
			LabelNames:   []string{"channel"},
			StatsdFormat: "%{#fqname}.%{channel}",
		}),
		TransactionsPerSecond: provider.NewGauge(metrics.GaugeOpts{
			Namespace:    "consensus",
			Subsystem:    "bdls",
			Name:         "transactions_per_second",
			Help:         "Current transactions per second throughput",
			LabelNames:   []string{"channel"},
			StatsdFormat: "%{#fqname}.%{channel}",
		}),
		ConsensusErrors: provider.NewCounter(metrics.CounterOpts{
			Namespace:    "consensus",
			Subsystem:    "bdls",
			Name:         "consensus_errors",
			Help:         "Number of consensus errors encountered",
			LabelNames:   []string{"channel", "error_type"},
			StatsdFormat: "%{#fqname}.%{channel}.%{error_type}",
		}),
		NetworkErrors: provider.NewCounter(metrics.CounterOpts{
			Namespace:    "consensus",
			Subsystem:    "bdls",
			Name:         "network_errors",
			Help:         "Number of network errors in consensus",
			LabelNames:   []string{"channel", "peer"},
			StatsdFormat: "%{#fqname}.%{channel}.%{peer}",
		}),
		RetryAttempts: provider.NewCounter(metrics.CounterOpts{
			Namespace:    "consensus",
			Subsystem:    "bdls",
			Name:         "retry_attempts",
			Help:         "Number of retry attempts for failed operations",
			LabelNames:   []string{"channel", "operation"},
			StatsdFormat: "%{#fqname}.%{channel}.%{operation}",
		}),
		MemoryUsage: provider.NewGauge(metrics.GaugeOpts{
			Namespace:    "consensus",
			Subsystem:    "bdls",
			Name:         "memory_usage_bytes",
			Help:         "Current memory usage of BDLS consensus",
			LabelNames:   []string{"channel"},
			StatsdFormat: "%{#fqname}.%{channel}",
		}),
		ViewChanges: provider.NewCounter(metrics.CounterOpts{
			Namespace:    "consensus",
			Subsystem:    "bdls",
			Name:         "view_changes",
			Help:         "Number of view changes in BDLS consensus",
			LabelNames:   []string{"channel"},
			StatsdFormat: "%{#fqname}.%{channel}",
		}),
		HeartbeatLatency: provider.NewHistogram(metrics.HistogramOpts{
			Namespace:    "consensus",
			Subsystem:    "bdls",
			Name:         "heartbeat_latency",
			Help:         "Latency of heartbeat messages in BDLS",
			LabelNames:   []string{"channel"},
			StatsdFormat: "%{#fqname}.%{channel}",
		}),
		BatchSize: provider.NewHistogram(metrics.HistogramOpts{
			Namespace:    "consensus",
			Subsystem:    "bdls",
			Name:         "batch_size",
			Help:         "Size of transaction batches in BDLS",
			LabelNames:   []string{"channel"},
			StatsdFormat: "%{#fqname}.%{channel}",
		}),
		
		// **PHASE 1.3**: Security and validation metrics
		SecurityEvents: provider.NewCounter(metrics.CounterOpts{
			Namespace:    "consensus",
			Subsystem:    "bdls",
			Name:         "security_events_total",
			Help:         "Total number of security events detected",
			LabelNames:   []string{"channel", "event"},
			StatsdFormat: "%{#fqname}.%{channel}.%{event}",
		}),
		ValidatedMessages: provider.NewCounter(metrics.CounterOpts{
			Namespace:    "consensus",
			Subsystem:    "bdls",
			Name:         "validated_messages_total", 
			Help:         "Total number of validated consensus messages",
			LabelNames:   []string{"channel", "type"},
			StatsdFormat: "%{#fqname}.%{channel}.%{type}",
		}),
		
		ActiveConnections: provider.NewGauge(metrics.GaugeOpts{
			Namespace:    "consensus",
			Subsystem:    "bdls",
			Name:         "active_connections",
			Help:         "Number of active network connections",
			LabelNames:   []string{"channel"},
			StatsdFormat: "%{#fqname}.%{channel}",
		}),
		QueueDepth: provider.NewGauge(metrics.GaugeOpts{
			Namespace:    "consensus",
			Subsystem:    "bdls",
			Name:         "queue_depth",
			Help:         "Current depth of consensus message queue",
			LabelNames:   []string{"channel"},
			StatsdFormat: "%{#fqname}.%{channel}",
		}),
		lastTxTime: time.Now(),
	}
}

// RecordConsensusLatency records the time taken to reach consensus
func (m *BDLSMetrics) RecordConsensusLatency(channel string, duration time.Duration) {
	m.ConsensusLatency.With("channel", channel).Observe(duration.Seconds())
}

// RecordMessageProcessing records message processing time
func (m *BDLSMetrics) RecordMessageProcessing(channel, messageType string, duration time.Duration) {
	m.MessageProcessingTime.With("channel", channel, "message_type", messageType).Observe(duration.Seconds())
}

// RecordTransaction records a processed transaction and updates TPS
func (m *BDLSMetrics) RecordTransaction(channel string) {
	m.TotalTransactions.With("channel", channel).Add(1)
	m.updateTPS(channel)
}

// RecordError records various types of errors
func (m *BDLSMetrics) RecordError(channel, errorType string) {
	m.ConsensusErrors.With("channel", channel, "error_type", errorType).Add(1)
}

// RecordNetworkError records network-related errors
func (m *BDLSMetrics) RecordNetworkError(channel, peer string) {
	m.NetworkErrors.With("channel", channel, "peer", peer).Add(1)
}

// RecordViewChange records a view change event
func (m *BDLSMetrics) RecordViewChange(channel string) {
	m.ViewChanges.With("channel", channel).Add(1)
}

// RecordBatchSize records the size of a consensus batch
func (m *BDLSMetrics) RecordBatchSize(channel string, size int) {
	m.BatchSize.With("channel", channel).Observe(float64(size))
}

// UpdateActiveConnections updates the number of active connections
func (m *BDLSMetrics) UpdateActiveConnections(channel string, count int) {
	m.ActiveConnections.With("channel", channel).Set(float64(count))
}

// UpdateQueueDepth updates the message queue depth
func (m *BDLSMetrics) UpdateQueueDepth(channel string, depth int) {
	m.QueueDepth.With("channel", channel).Set(float64(depth))
}

// updateTPS calculates and updates transactions per second
func (m *BDLSMetrics) updateTPS(channel string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	now := time.Now()
	elapsed := now.Sub(m.lastTxTime)
	
	// Update TPS every second
	if elapsed >= time.Second {
		currentTxCount := m.lastTxCount + 1
		tps := float64(currentTxCount) / elapsed.Seconds()
		m.TransactionsPerSecond.With("channel", channel).Set(tps)
		
		m.lastTxCount = currentTxCount
		m.lastTxTime = now
	}
}

// PerformanceBenchmark provides benchmarking utilities for BDLS consensus
type PerformanceBenchmark struct {
	metrics       *BDLSMetrics
	startTime     time.Time
	totalMessages int64
	totalErrors   int64
}

// NewPerformanceBenchmark creates a new benchmark instance
func NewPerformanceBenchmark(metrics *BDLSMetrics) *PerformanceBenchmark {
	return &PerformanceBenchmark{
		metrics:   metrics,
		startTime: time.Now(),
	}
}

// GetBenchmarkResults returns current benchmark statistics
func (pb *PerformanceBenchmark) GetBenchmarkResults() map[string]interface{} {
	elapsed := time.Since(pb.startTime)
	
	return map[string]interface{}{
		"total_runtime_seconds":    elapsed.Seconds(),
		"total_messages_processed": pb.totalMessages,
		"total_errors":            pb.totalErrors,
		"messages_per_second":     float64(pb.totalMessages) / elapsed.Seconds(),
		"error_rate_percent":      float64(pb.totalErrors) / float64(pb.totalMessages) * 100,
		"average_latency_ms":      elapsed.Milliseconds() / pb.totalMessages,
	}
} 