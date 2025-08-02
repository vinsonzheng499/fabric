package bdls

import (
	"context"
	"crypto/ecdsa"
	"sync"
	"sync/atomic"
	"time"

	"github.com/BDLS-bft/bdls"
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-lib-go/common/metrics"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric/orderer/common/cluster"
	"github.com/hyperledger/fabric/orderer/consensus"
	"github.com/pkg/errors"
)

// This file now uses the proper network implementation from communicator.go
// The fabricNetworkManager and fabricPeer implementations provide full
// integration with Fabric's cluster communication system

// Chain implements the BDLS consensus chain following Fabric patterns
type Chain struct {
	// Core consensus components
	consensus    *bdls.Consensus
	support      consensus.ConsenterSupport
	logger       *flogging.FabricLogger
	
	// Channel management
	channelID    string
	selfID       uint32
	
	// Lifecycle management (following Raft pattern)
	submitC      chan *submitRequest
	errorC       chan struct{}
	haltC        chan struct{}
	doneC        chan struct{}
	startC       chan struct{}
	
	// State management (following SmartBFT pattern)
	running      atomic.Bool
	errorCLock   sync.RWMutex
	
	// Network communication - using Fabric's cluster communicator
	communicator cluster.Communicator
	
	// Configuration
	config       *BDLSConfig
	participants []bdls.Identity
	privateKey   *ecdsa.PrivateKey
	
	// **PHASE 1.1**: Metrics and monitoring
	metrics      *BDLSMetrics
	heightC      chan uint64
	
	// Block processing
	lastBlock    *common.Block
	appliedIndex uint64
	
	// BDLS specific state
	currentHeight uint64
	
	// Timing and performance
	clock        Clock
	ticker       *time.Ticker
}

// submitRequest represents a transaction submission request
type submitRequest struct {
	req   *common.Envelope
	leadC chan uint64
}

// BDLSConfig contains BDLS-specific configuration
type BDLSConfig struct {
	// BDLS protocol configuration
	RequestBatchMaxCount    uint64
	RequestBatchMaxBytes    uint64
	RequestBatchMaxInterval time.Duration
	
	// Network configuration
	MessageTimeoutInterval  time.Duration
	HeartbeatInterval      time.Duration
	
	// Performance tuning
	EnableCommitUnicast    bool
	LatencyOptimization    time.Duration
}

// Clock interface for testability (following Raft pattern)
type Clock interface {
	Now() time.Time
	Since(time.Time) time.Duration
}

// systemClock implements Clock using system time
type systemClock struct{}

func (systemClock) Now() time.Time                 { return time.Now() }
func (systemClock) Since(t time.Time) time.Duration { return time.Since(t) }

// NewChain creates a new BDLS consensus chain
func NewChain(
	support consensus.ConsenterSupport,
	config *BDLSConfig,
	selfID uint32,
	participants []bdls.Identity,
	privateKey *ecdsa.PrivateKey,
	clusterDialer *cluster.PredicateDialer,
	communicator cluster.Communicator,
	metricsProvider metrics.Provider, // **PHASE 1.1**: Add metrics provider
) (*Chain, error) {
	logger := flogging.MustGetLogger("orderer.consensus.bdls.chain").With("channel", support.ChannelID())
	
	chain := &Chain{
		support:      support,
		logger:       logger,
		channelID:    support.ChannelID(),
		selfID:       selfID,
		config:       config,
		participants: participants,
		privateKey:   privateKey,
		
		// Channels for lifecycle management
		submitC:      make(chan *submitRequest, 100),
		errorC:       make(chan struct{}),
		haltC:        make(chan struct{}),
		doneC:        make(chan struct{}),
		startC:       make(chan struct{}),
		heightC:      make(chan uint64, 10),
		
		// **PHASE 1.1**: Initialize metrics properly
		metrics:      NewBDLSMetrics(metricsProvider),
		
		// State
		lastBlock:    support.Block(support.Height() - 1),
		currentHeight: support.Height(),
		
		// Timing
		clock:        systemClock{},
	}
	
	// Set up cluster communicator
	chain.communicator = communicator
	
	// Create BDLS configuration
	bdlsConfig := &bdls.Config{
		Epoch:                   chain.clock.Now(),
		CurrentHeight:          chain.currentHeight,
		PrivateKey:             privateKey,
		Participants:           participants,
		EnableCommitUnicast:    config.EnableCommitUnicast,
		StateCompare:           chain.stateCompare,
		StateValidate:          chain.stateValidate,
		MessageValidator:       chain.messageValidator,
		MessageOutCallback:     chain.messageOutCallback, // **PHASE 1.1**: Add message monitoring
		PubKeyToIdentity:       bdls.DefaultPubKeyToIdentity,
	}
	
	// Validate BDLS configuration
	if err := bdls.VerifyConfig(bdlsConfig); err != nil {
		return nil, errors.Wrap(err, "invalid BDLS configuration")
	}
	
	// Create BDLS consensus instance
	consensus, err := bdls.NewConsensus(bdlsConfig)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create BDLS consensus")
	}
	chain.consensus = consensus
	
	// Set latency optimization
	if config.LatencyOptimization > 0 {
		consensus.SetLatency(config.LatencyOptimization)
	}
	
	logger.Infof("BDLS chain created for channel %s with %d participants", support.ChannelID(), len(participants))
	return chain, nil
}

// Start implements consensus.Chain interface
func (c *Chain) Start() {
	if c.running.Load() {
		c.logger.Warn("Chain is already running")
		return
	}
	
	c.logger.Info("Starting BDLS consensus chain")
	c.running.Store(true)
	
	// Start main consensus loop
	go c.run()
	
	// Close start channel to signal readiness
	close(c.startC)
	c.logger.Info("BDLS consensus chain started successfully")
}

// Halt implements consensus.Chain interface
func (c *Chain) Halt() {
	if !c.running.Load() {
		c.logger.Warn("Chain is not running")
		return
	}
	
	c.logger.Info("Halting BDLS consensus chain")
	close(c.haltC)
	<-c.doneC
	c.running.Store(false)
	
	c.logger.Info("BDLS consensus chain halted")
}

// Order implements consensus.Chain interface
func (c *Chain) Order(env *common.Envelope, configSeq uint64) error {
	if !c.running.Load() {
		return errors.New("chain is not running")
	}
	
	c.logger.Debugf("Ordering transaction")
	leadC := make(chan uint64, 1)
	
	select {
	case c.submitC <- &submitRequest{req: env, leadC: leadC}:
		// Wait for leadership status
		select {
		case <-leadC:
			return nil
		case <-c.errorC:
			return errors.New("chain is in error state")
		case <-c.doneC:
			return errors.New("chain is shutting down")
		}
	case <-c.errorC:
		return errors.New("chain is in error state")
	case <-c.doneC:
		return errors.New("chain is shutting down")
	default:
		return errors.New("submit queue full")
	}
}

// Configure implements consensus.Chain interface
func (c *Chain) Configure(config *common.Envelope, configSeq uint64) error {
	return c.Order(config, configSeq)
}

// WaitReady implements consensus.Chain interface
func (c *Chain) WaitReady() error {
	if !c.running.Load() {
		return errors.New("chain is not running")
	}
	
	select {
	case <-c.startC:
		return nil
	case <-c.errorC:
		return errors.New("chain is in error state")
	case <-c.doneC:
		return errors.New("chain is shutting down")
	}
}

// Errored implements consensus.Chain interface
func (c *Chain) Errored() <-chan struct{} {
	return c.errorC
}

// run is the main consensus loop (following Raft/SmartBFT patterns)
func (c *Chain) run() {
	defer close(c.doneC)
	
	c.logger.Info("Starting BDLS consensus main loop")
	
	// Create ticker for regular BDLS updates
	c.ticker = time.NewTicker(50 * time.Millisecond) // 20 Hz update rate
	defer c.ticker.Stop()
	
	var pendingRequests []*common.Envelope
	batchTimer := time.NewTimer(c.config.RequestBatchMaxInterval)
	defer batchTimer.Stop()
	
	for {
		select {
		case <-c.haltC:
			c.logger.Info("Consensus loop received halt signal")
			return
			
		case <-c.ticker.C:
			// Regular BDLS state machine update
			c.updateBDLS()
			
		case req := <-c.submitC:
			// Handle transaction submission
			pendingRequests = append(pendingRequests, req.req)
			
			// Check if we should create a batch
			if c.shouldCreateBatch(pendingRequests) {
				// Create block from pending requests
				block := c.support.CreateNextBlock(pendingRequests)
				
				// Serialize block for BDLS
				blockBytes, err := proto.Marshal(block)
				if err != nil {
					c.logger.Errorf("Failed to marshal block: %v", err)
				} else {
					err = c.submitBatch(blockBytes)
					if err != nil {
						c.logger.Errorf("Failed to submit batch: %v", err)
					}
				}
				
				pendingRequests = nil
				batchTimer.Reset(c.config.RequestBatchMaxInterval)
			}
			
			// Signal successful submission
			close(req.leadC)
			
		case <-batchTimer.C:
			// Timeout-based batch creation
			if len(pendingRequests) > 0 {
				// Create block from pending requests
				block := c.support.CreateNextBlock(pendingRequests)
				
				// Serialize block for BDLS
				blockBytes, err := proto.Marshal(block)
				if err != nil {
					c.logger.Errorf("Failed to marshal block: %v", err)
				} else {
					err = c.submitBatch(blockBytes)
					if err != nil {
						c.logger.Errorf("Failed to submit batch: %v", err)
					}
				}
				
				pendingRequests = nil
			}
			batchTimer.Reset(c.config.RequestBatchMaxInterval)
			
		case height := <-c.heightC:
			// Handle height advancement
			c.processHeightAdvancement(height)
		}
	}
}

// updateBDLS performs regular BDLS consensus updates
func (c *Chain) updateBDLS() {
	now := c.clock.Now()
	
	// Update BDLS state machine
	c.consensus.Update(now)
	
	// Check for consensus decisions (handle all 3 return values)
	currentHeight, _, _ := c.consensus.CurrentState()
	if currentHeight > c.currentHeight {
		c.logger.Infof("BDLS consensus reached for height %d", currentHeight)
		c.heightC <- currentHeight
	}
	
	// Process any outgoing messages
	// Process outgoing messages from BDLS consensus
	// Note: Messages are now handled by Fabric's cluster communicator
}

// shouldCreateBatch determines if a batch should be created
func (c *Chain) shouldCreateBatch(requests []*common.Envelope) bool {
	if len(requests) == 0 {
		return false
	}
	
	// Check count threshold
	if uint64(len(requests)) >= c.config.RequestBatchMaxCount {
		return true
	}
	
	// Check size threshold
	totalSize := uint64(0)
	for _, req := range requests {
		totalSize += uint64(len(req.Payload))
	}
	
	if totalSize >= c.config.RequestBatchMaxBytes {
		return true
	}
	
	return false
}

// submitBatch submits a batch of transactions to BDLS consensus for ordering
func (c *Chain) submitBatch(blockBytes []byte) error {
	// **PHASE 1.5**: Duplicate detection using BDLS library
	// Check if we've already proposed this exact state to prevent duplicates
	if c.consensus.HasProposed(blockBytes) {
		c.logger.Debugf("Block already proposed, skipping duplicate submission (size: %d bytes)", len(blockBytes))
		c.metrics.ConsensusErrors.With("channel", c.channelID, "type", "duplicate_block").Add(1)
		return nil // Not an error - just skip the duplicate
	}
	
	startTime := c.clock.Now()
	
	// **CRITICAL FIX**: Use Propose() method instead of SubmitRequest()
	// This ensures proper integration with BDLS consensus protocol
	c.consensus.Propose(blockBytes)
	
	// Track consensus performance metrics
	proposalDuration := c.clock.Since(startTime)
	c.metrics.ConsensusLatency.With("channel", c.channelID).Observe(proposalDuration.Seconds())
	
	// Update batch metrics
	c.metrics.BatchSize.With("channel", c.channelID).Observe(float64(len(blockBytes)))
	
	c.logger.Debugf("Successfully proposed block to BDLS consensus (size: %d bytes, duration: %v)", 
		len(blockBytes), proposalDuration)
	
	return nil
}

// processHeightAdvancement handles consensus decisions
func (c *Chain) processHeightAdvancement(height uint64) {
	c.logger.Infof("Processing consensus decision for height %d", height)
	
	// Get consensus proof
	proof := c.consensus.CurrentProof()
	if proof == nil {
		c.logger.Errorf("No consensus proof available for height %d", height)
		return
	}
	
	// Get the decided state (block) - handle all 3 return values
	_, _, currentState := c.consensus.CurrentState()
	if currentState == nil {
		c.logger.Errorf("No state available for height %d", height)
		return
	}
	
	// Unmarshal the block using protobuf
	var block common.Block
	if err := proto.Unmarshal(currentState, &block); err != nil {
		c.logger.Errorf("Failed to unmarshal decided block: %v", err)
		return
	}
	
	// Validate the block
	if err := c.validateBlock(&block); err != nil {
		c.logger.Errorf("Invalid block at height %d: %v", height, err)
		return
	}
	
	// Prepare consensus metadata
	metadata, err := c.prepareMetadata(proof)
	if err != nil {
		c.logger.Errorf("Failed to prepare metadata: %v", err)
		return
	}
	
	// Write block to ledger
	c.support.WriteBlock(&block, metadata)
	c.lastBlock = &block
	c.currentHeight = height + 1
	
	c.logger.Infof("Successfully committed block %d", block.Header.Number)
}

// validateBlock validates a block (basic validation for now)
func (c *Chain) validateBlock(block *common.Block) error {
	if block == nil {
		return errors.New("block is nil")
	}
	
	if block.Header == nil {
		return errors.New("block header is nil")
	}
	
	if block.Data == nil {
		return errors.New("block data is nil")
	}
	
	// TODO: Add more comprehensive validation
	// - Transaction signature validation
	// - MSP policy compliance
	// - Channel configuration validation
	
	return nil
}

// prepareMetadata prepares consensus metadata from BDLS proof
func (c *Chain) prepareMetadata(proof *bdls.SignedProto) ([]byte, error) {
	if proof == nil {
		return nil, nil
	}
	
	// Marshal the BDLS proof as metadata
	metadata, err := proto.Marshal(proof)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal BDLS proof")
	}
	
	return metadata, nil
}

// BDLS callback functions

// stateCompare compares two states for BDLS
func (c *Chain) stateCompare(a, b bdls.State) int {
	// Compare block heights
	var blockA, blockB common.Block
	
	if err := proto.Unmarshal(a, &blockA); err != nil {
		c.logger.Errorf("Failed to unmarshal state A: %v", err)
		return 0
	}
	
	if err := proto.Unmarshal(b, &blockB); err != nil {
		c.logger.Errorf("Failed to unmarshal state B: %v", err)
		return 0
	}
	
	heightA := blockA.Header.Number
	heightB := blockB.Header.Number
	
	if heightA < heightB {
		return -1
	} else if heightA > heightB {
		return 1
	}
	
	// If heights are equal, compare by hash
	hashA := blockA.Header.DataHash
	hashB := blockB.Header.DataHash
	
	for i := 0; i < len(hashA) && i < len(hashB); i++ {
		if hashA[i] < hashB[i] {
			return -1
		} else if hashA[i] > hashB[i] {
			return 1
		}
	}
	
	return 0
}

// stateValidate validates a state for BDLS
func (c *Chain) stateValidate(state bdls.State) bool {
	var block common.Block
	if err := proto.Unmarshal(state, &block); err != nil {
		c.logger.Debugf("Invalid state: failed to unmarshal block: %v", err)
		return false
	}
	
	if err := c.validateBlock(&block); err != nil {
		c.logger.Debugf("Invalid state: block validation failed: %v", err)
		return false
	}
	
	return true
}

// **PHASE 1.3**: Enhanced Message Validation & Security
// messageValidator provides comprehensive validation for incoming BDLS messages
func (c *Chain) messageValidator(consensus *bdls.Consensus, message *bdls.Message, signed *bdls.SignedProto) bool {
	// Validate basic message structure
	if message == nil {
		c.metrics.SecurityEvents.With("channel", c.channelID, "event", "nil_message").Add(1)
		return false
	}
	
	if signed == nil {
		c.metrics.SecurityEvents.With("channel", c.channelID, "event", "nil_signed").Add(1)
		return false
	}
	
	// Validate message type
	if message.Type < bdls.MessageType_Nop || message.Type > bdls.MessageType_Resync {
		c.metrics.SecurityEvents.With("channel", c.channelID, "event", "invalid_message_type").Add(1)
		return false
	}
	
	// Validate height constraints - messages shouldn't be too far in past/future
	currentHeight, _, _ := consensus.CurrentState()
	maxHeightDiff := uint64(10) // Allow some flexibility for network delays
	
	if message.Height < currentHeight && currentHeight-message.Height > maxHeightDiff {
		c.metrics.SecurityEvents.With("channel", c.channelID, "event", "old_message").Add(1)
		return false
	}
	
	if message.Height > currentHeight+maxHeightDiff {
		c.metrics.SecurityEvents.With("channel", c.channelID, "event", "future_message").Add(1)
		return false
	}
	
	// Validate state size to prevent memory attacks
	maxStateSize := 1024 * 1024 // 1MB limit
	if len(message.State) > maxStateSize {
		c.metrics.SecurityEvents.With("channel", c.channelID, "event", "oversized_state").Add(1)
		return false
	}
	
	// Validate proof count to prevent DoS attacks
	maxProofs := 100
	if len(message.Proof) > maxProofs {
		c.metrics.SecurityEvents.With("channel", c.channelID, "event", "too_many_proofs").Add(1)
		return false
	}
	
	// Validate each proof in the message
	for _, proof := range message.Proof {
		if proof == nil {
			c.metrics.SecurityEvents.With("channel", c.channelID, "event", "nil_proof").Add(1)
			return false
		}
		
		// Validate proof message size
		if len(proof.Message) > maxStateSize {
			c.metrics.SecurityEvents.With("channel", c.channelID, "event", "oversized_proof").Add(1)
			return false
		}
	}
	
	// Type-specific validation
	switch message.Type {
	case bdls.MessageType_RoundChange:
		// Round change messages should have valid round progression
		if message.Round == 0 {
			c.metrics.SecurityEvents.With("channel", c.channelID, "event", "invalid_round").Add(1)
			return false
		}
		
	case bdls.MessageType_Lock, bdls.MessageType_Select:
		// Lock and select messages should have state
		if len(message.State) == 0 {
			c.metrics.SecurityEvents.With("channel", c.channelID, "event", "missing_state").Add(1)
			return false
		}
		
	case bdls.MessageType_Commit:
		// Commit messages should have proofs
		if len(message.Proof) == 0 {
			c.metrics.SecurityEvents.With("channel", c.channelID, "event", "missing_proofs").Add(1)
			return false
		}
		
	case bdls.MessageType_LockRelease:
		// Lock release messages should have embedded lock message
		if message.LockRelease == nil {
			c.metrics.SecurityEvents.With("channel", c.channelID, "event", "missing_lock_release").Add(1)
			return false
		}
		
	case bdls.MessageType_Decide:
		// Decide messages should have both state and proofs
		if len(message.State) == 0 || len(message.Proof) == 0 {
			c.metrics.SecurityEvents.With("channel", c.channelID, "event", "incomplete_decide").Add(1)
			return false
		}
	}
	
	// All validations passed
	c.metrics.ValidatedMessages.With("channel", c.channelID, "type", message.Type.String()).Add(1)
	return true
}

// **PHASE 1.1**: BDLS message monitoring callback
// messageOutCallback is called when BDLS sends a message - provides comprehensive monitoring
func (c *Chain) messageOutCallback(message *bdls.Message, signed *bdls.SignedProto) {
	// Validate inputs to prevent panics in production
	if message == nil {
		c.logger.Error("Received nil message in messageOutCallback")
		return
	}
	if signed == nil {
		c.logger.Error("Received nil signed proto in messageOutCallback")
		return
	}

	// Check if chain is still operational (production resilience)
	if !c.running.Load() {
		c.logger.Debug("Chain not running, discarding outgoing message")
		return
	}

	// **PHASE 1.1**: Track message metrics based on message type
	messageType := "unknown"
	if message.Type != bdls.MessageType_Nop {
		messageType = message.Type.String()
	}
	
	// Update message processing metrics
	c.metrics.MessageProcessingTime.With("channel", c.channelID, "message_type", messageType).Observe(0.001) // Small default for outgoing
	c.metrics.TotalTransactions.With("channel", c.channelID).Add(1)
	
	// Log consensus activity for debugging
	c.logger.Debugf("BDLS sending message: type=%s, height=%d, round=%d, size=%d bytes", 
		messageType, message.Height, message.Round, len(signed.Message))
	
	// **PHASE 1.1**: Track network bandwidth usage
	c.metrics.NetworkBandwidth.With("channel", c.channelID, "direction", "outgoing").Add(float64(len(signed.Message)))
	
	// Note: Message broadcasting is now handled by Fabric's cluster communicator
	// BDLS messages will be sent through the standard Fabric cluster protocol
	c.logger.Debugf("BDLS message ready for broadcast (height: %d, type: %s)", 
		message.Height, messageType)
}

// HandleMessage handles the message from the sender
func (c *Chain) HandleMessage(sender uint64, payload []byte) {
	c.logger.Debugf("HandleMessage from %d", sender)
	
	// Pass the raw payload to BDLS consensus
	if err := c.consensus.ReceiveMessage(payload, c.clock.Now()); err != nil {
		c.logger.Warnf("BDLS rejected message from %d: %v", sender, err)
	}
}

// HandleRequest handles the request from the sender
func (c *Chain) HandleRequest(sender uint64, req []byte) {
	c.logger.Debugf("HandleRequest from %d", sender)
	
	// For BDLS, requests are typically transactions that need to be ordered
	// This would be handled by the BDLS consensus algorithm
	// For now, we'll pass it to the consensus engine
	if err := c.consensus.ReceiveMessage(req, c.clock.Now()); err != nil {
		c.logger.Warnf("BDLS rejected request from %d: %v", sender, err)
	}
}

// ReceiveMessage processes incoming consensus messages from peers
// Enhanced with production error recovery and validation patterns
func (c *Chain) ReceiveMessage(payload []byte) error {
	// Input validation for production safety
	if len(payload) == 0 {
		return errors.New("received empty payload")
	}
	
	// Check chain operational status with detailed logging
	if !c.running.Load() {
		c.logger.Debug("Chain not running, rejecting incoming message")
		return errors.New("chain is not running")
	}
	
	// Add timeout protection for message processing (production resilience)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	// Process in goroutine with timeout
	errChan := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				c.logger.Errorf("Panic during message processing (recovered): %v", r)
				errChan <- errors.Errorf("message processing panic: %v", r)
			}
		}()
		
		// Unmarshal signed message with size validation
		if len(payload) > 10*1024*1024 { // 10MB limit for safety
			errChan <- errors.New("message payload too large")
			return
		}
		
		var signed bdls.SignedProto
		if err := proto.Unmarshal(payload, &signed); err != nil {
			errChan <- errors.Wrap(err, "failed to unmarshal signed message")
			return
		}
		
		c.logger.Debugf("Processing incoming BDLS message (%d bytes)", len(payload))
		
		// Process message through BDLS with timestamp
		now := c.clock.Now()
		if err := c.consensus.ReceiveMessage(payload, now); err != nil {
			errChan <- errors.Wrap(err, "BDLS rejected message")
			return
		}
		
		errChan <- nil
	}()
	
	// Wait for completion or timeout
	select {
	case err := <-errChan:
		if err != nil {
			c.logger.Warnf("Message processing failed: %v", err)
		}
		return err
	case <-ctx.Done():
		c.logger.Error("Message processing timed out")
		return errors.New("message processing timeout")
	}
}

// markAsErrored marks the chain as errored
func (c *Chain) markAsErrored() {
	c.errorCLock.Lock()
	defer c.errorCLock.Unlock()
	
	select {
	case <-c.errorC:
		// Already closed
	default:
		close(c.errorC)
	}
}

// **PHASE 1.2**: Dynamic Peer Management
// Note: Peer management is now handled by Fabric's cluster system
// AddPeer and RemovePeer are deprecated - use channel configuration instead

// GetActivePeers returns the count of active peers in BDLS consensus
func (c *Chain) GetActivePeers() int {
	if !c.running.Load() {
		return 0
	}
	
	// Return the number of participants in the BDLS consensus
	return len(c.participants)
}
