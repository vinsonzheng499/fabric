package bdls

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"time"

	"github.com/BDLS-bft/bdls"
	"github.com/hyperledger/fabric-lib-go/bccsp"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-lib-go/common/metrics"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	ab "github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/crypto"
	"github.com/hyperledger/fabric/common/policies"
	"github.com/hyperledger/fabric/internal/pkg/comm"
	"github.com/hyperledger/fabric/internal/pkg/identity"
	"github.com/hyperledger/fabric/orderer/common/cluster"
	"github.com/hyperledger/fabric/orderer/common/localconfig"
	"github.com/hyperledger/fabric/orderer/common/multichannel"
	"github.com/hyperledger/fabric/orderer/consensus"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

//go:generate mockery -dir . -name MessageReceiver -case underscore -output mocks

// MessageReceiver receives messages
type MessageReceiver interface {
	HandleMessage(sender uint64, payload []byte)
	HandleRequest(sender uint64, req []byte)
}

//go:generate mockery -dir . -name ReceiverGetter -case underscore -output mocks

// ReceiverGetter obtains instances of MessageReceiver given a channel ID
type ReceiverGetter interface {
	// ReceiverByChain returns the MessageReceiver if it exists, or nil if it doesn't
	ReceiverByChain(channelID string) MessageReceiver
}

// ChainGetter obtains instances of Chain given a channel ID
type ChainGetter interface {
	// GetChain obtains the Chain for the given channel.
	// Returns nil when the Chain for the given channel isn't found.
	GetChain(chainID string) *multichannel.ChainSupport
}

// Ingress dispatches Submit and Step requests to the designated per chain instances
type Ingress struct {
	Logger        *flogging.FabricLogger
	ChainSelector ReceiverGetter
}

// OnConsensus notifies the Ingress for a reception of a StepRequest from a given sender on a given channel
func (in *Ingress) OnConsensus(channel string, sender uint64, request *ab.ConsensusRequest) error {
	receiver := in.ChainSelector.ReceiverByChain(channel)
	if receiver == nil {
		in.Logger.Warningf("An attempt to send a consensus request to a non existing channel (%s) was made by %d", channel, sender)
		return errors.Errorf("channel %s doesn't exist", channel)
	}
	// Pass the raw payload to BDLS - let BDLS handle its own message parsing
	receiver.HandleMessage(sender, request.Payload)
	return nil
}

// OnSubmit notifies the Ingress for a reception of a SubmitRequest from a given sender on a given channel
func (in *Ingress) OnSubmit(channel string, sender uint64, request *ab.SubmitRequest) error {
	receiver := in.ChainSelector.ReceiverByChain(channel)
	if receiver == nil {
		in.Logger.Warningf("An attempt to submit a transaction to a non existing channel (%s) was made by %d", channel, sender)
		return errors.Errorf("channel %s doesn't exist", channel)
	}
	receiver.HandleRequest(sender, protoutil.MarshalOrPanic(request.Payload))
	return nil
}

// SignerSerializer provides the ability to get an identity and sign messages
type SignerSerializer interface {
	identity.SignerSerializer
}

// PolicyManagerRetriever is the policy manager retriever function
type PolicyManagerRetriever func(channel string) policies.Manager

// BDLSOptions is imported from the generated protobuf code (configuration.pb.go)

type Consenter struct {
	logger        *flogging.FabricLogger
	nodeIdentity  []byte
	cryptoProvider bccsp.BCCSP
	clusterDialer *cluster.PredicateDialer
	communicator  cluster.Communicator
	metricsProvider metrics.Provider
	chains        ChainGetter
	ingress       *Ingress
}

// New creates a new BDLS consenter
func New(
	signerSerializer SignerSerializer,
	clusterDialer *cluster.PredicateDialer,
	conf *localconfig.TopLevel,
	srvConf comm.ServerConfig,
	srv *comm.GRPCServer,
	registrar *multichannel.Registrar,
	metricsProvider metrics.Provider, // **PHASE 1.1**: Add metrics provider
	clusterMetrics *cluster.Metrics,
	cryptoProvider bccsp.BCCSP,
) *Consenter {
	identityBytes, err := signerSerializer.Serialize()
	if err != nil {
		logger := flogging.MustGetLogger("orderer.consensus.bdls.consenter")
		logger.Panicf("Failed to serialize signing identity: %v", err)
	}

	logger := flogging.MustGetLogger("orderer.consensus.bdls.consenter")

	consenter := &Consenter{
		logger:         logger,
		nodeIdentity:   identityBytes,
		cryptoProvider: cryptoProvider,
		clusterDialer:  clusterDialer,
		metricsProvider: metricsProvider, // **PHASE 1.1**: Store metrics provider
		chains:         registrar,
	}

	// Create cluster communicator following SmartBFT pattern
	consenter.communicator = &cluster.AuthCommMgr{
		Logger:         flogging.MustGetLogger("orderer.common.cluster"),
		Metrics:        clusterMetrics,
		SendBufferSize: conf.General.Cluster.SendBufferSize,
		Chan2Members:   make(cluster.MembersByChannel),
		Connections:    cluster.NewConnectionMgr(clusterDialer.Config),
		Signer:         signerSerializer,
		NodeIdentity:   identityBytes,
	}

	// Create ingress for cluster communication
	consenter.ingress = &Ingress{
		Logger:        logger,
		ChainSelector: consenter,
	}

	return consenter
}

// NewConsenter creates a new BDLS consenter (legacy method - deprecated)
// TODO: Remove this method and update callers to use New() with proper cluster.Communicator
func NewConsenter(nodeIdentity []byte, cryptoProvider bccsp.BCCSP) *Consenter {
	logger := flogging.MustGetLogger("orderer.consensus.bdls.consenter")
	logger.Warn("Using deprecated NewConsenter method - cluster communication will not work properly")
	return &Consenter{
		logger:         logger,
		nodeIdentity:   nodeIdentity,
		cryptoProvider: cryptoProvider,
		communicator:   nil, // This will cause issues in chain creation
	}
}

// HandleChain creates and returns a Chain for the given channel
func (c *Consenter) HandleChain(support consensus.ConsenterSupport, metadata *common.Metadata) (consensus.Chain, error) {
	c.logger.Infof("Creating BDLS chain for channel %s", support.ChannelID())
	
	// Get consenters from channel configuration
	consenters := support.SharedConfig().Consenters()
	
	// Extract participants from channel configuration
	participants, err := c.extractParticipants(consenters)
	if err != nil {
		c.logger.Errorf("Failed to extract participants for channel %s: %v", support.ChannelID(), err)
		return nil, errors.Wrap(err, "failed to extract participants")
	}
	
	if len(participants) < 4 {
		return nil, errors.Errorf("BDLS requires at least 4 participants, found %d", len(participants))
	}
	
	// Extract private key (simplified for now)
	privateKey, err := c.extractPrivateKey()
	if err != nil {
		c.logger.Errorf("Failed to extract private key for channel %s: %v", support.ChannelID(), err)
		return nil, errors.Wrap(err, "failed to extract private key")
	}
	
	// Create BDLS configuration with defaults
	config := &BDLSConfig{
		RequestBatchMaxCount:    100,
		RequestBatchMaxBytes:    1024 * 1024, // 1MB
		RequestBatchMaxInterval: 2 * time.Second,
		MessageTimeoutInterval:  30 * time.Second,
		HeartbeatInterval:      1 * time.Second,
		EnableCommitUnicast:    true,
		LatencyOptimization:    100 * time.Millisecond,
	}
	
	// Determine self ID (simplified - use index in participants)
	selfID := uint32(0) // TODO: Determine actual self ID
	
	// Create the full BDLS chain with proper cluster communicator  
	chain, err := NewChain(support, config, selfID, participants, privateKey, c.clusterDialer, c.communicator, c.metricsProvider)
	if err != nil {
		c.logger.Errorf("Failed to create BDLS chain for channel %s: %v", support.ChannelID(), err)
		return nil, errors.Wrap(err, "failed to create BDLS chain")
	}
	
	c.logger.Infof("Successfully created BDLS chain for channel %s with %d participants", support.ChannelID(), len(participants))
	return chain, nil
}

// ReceiverByChain returns the MessageReceiver for the given channelID or nil if not found.
func (c *Consenter) ReceiverByChain(channelID string) MessageReceiver {
	chainSupport := c.chains.GetChain(channelID)
	if chainSupport == nil {
		return nil
	}
	if bdlsChain, isBDLSChain := chainSupport.Chain.(*Chain); isBDLSChain {
		return bdlsChain
	}
	c.logger.Warningf("Chain %s is of type %v and not bdls.Chain", channelID, chainSupport.Chain)
	return nil
}

// ValidateConsensusMetadata implements consensus.MetadataValidator interface
// This validates BDLS configuration updates during channel config changes
func (c *Consenter) ValidateConsensusMetadata(oldOrdererConfig, newOrdererConfig channelconfig.Orderer, newChannel bool) error {
	if newOrdererConfig == nil {
		c.logger.Panic("Programming Error: ValidateConsensusMetadata called with nil new channel config")
		return nil
	}

	// If metadata was not updated, nothing to validate
	if newOrdererConfig.ConsensusMetadata() == nil {
		c.logger.Debug("No consensus metadata provided, using defaults")
		return nil
	}

	// Validate consensus type
	if newOrdererConfig.ConsensusType() != "BFT" {
		return errors.Errorf("invalid consensus type for BDLS: expected 'BFT', got '%s'", 
			newOrdererConfig.ConsensusType())
	}

	// Parse and validate new BDLS metadata
	newMetadata := &BDLSOptions{}
	if err := proto.Unmarshal(newOrdererConfig.ConsensusMetadata(), newMetadata); err != nil {
		return errors.Wrap(err, "failed to unmarshal new BDLS metadata configuration")
	}

	// Validate the new metadata
	if err := c.validateBDLSMetadata(newMetadata); err != nil {
		return errors.Wrap(err, "invalid new BDLS metadata")
	}

	// For new channels, validate against system channel constraints
	if newChannel {
		return c.validateNewChannelConfig(newMetadata, newOrdererConfig)
	}

	// For existing channels, validate the configuration update
	if oldOrdererConfig == nil {
		c.logger.Panic("Programming Error: ValidateConsensusMetadata called with nil old channel config")
		return nil
	}

	return c.validateConfigUpdate(oldOrdererConfig, newOrdererConfig, newMetadata)
}

// validateBDLSMetadata validates BDLS-specific configuration options
func (c *Consenter) validateBDLSMetadata(metadata *BDLSOptions) error {
	if metadata == nil {
		return errors.New("BDLS metadata cannot be nil")
	}

	// Validate timeouts
	if metadata.NetworkTimeout != "" {
		if _, err := time.ParseDuration(metadata.NetworkTimeout); err != nil {
			return errors.Wrapf(err, "invalid NetworkTimeout: %s", metadata.NetworkTimeout)
		}
	}

	if metadata.ConsensusTimeout != "" {
		if _, err := time.ParseDuration(metadata.ConsensusTimeout); err != nil {
			return errors.Wrapf(err, "invalid ConsensusTimeout: %s", metadata.ConsensusTimeout)
		}
	}

	if metadata.HeartbeatInterval != "" {
		if _, err := time.ParseDuration(metadata.HeartbeatInterval); err != nil {
			return errors.Wrapf(err, "invalid HeartbeatInterval: %s", metadata.HeartbeatInterval)
		}
	}

	if metadata.ViewChangeTimeout != "" {
		if _, err := time.ParseDuration(metadata.ViewChangeTimeout); err != nil {
			return errors.Wrapf(err, "invalid ViewChangeTimeout: %s", metadata.ViewChangeTimeout)
		}
	}

	// Validate numeric constraints
	if metadata.MaxMessageSize > 0 && metadata.MaxMessageSize < 1024 {
		return errors.Errorf("MaxMessageSize too small: %d (minimum: 1024)", metadata.MaxMessageSize)
	}

	if metadata.BatchSize > 0 && metadata.BatchSize < 1 {
		return errors.Errorf("BatchSize too small: %d (minimum: 1)", metadata.BatchSize)
	}

	if metadata.MaxRetries > 100 {
		return errors.Errorf("MaxRetries too large: %d (maximum: 100)", metadata.MaxRetries)
	}

	if metadata.BufferSize > 0 && metadata.BufferSize < 10 {
		return errors.Errorf("BufferSize too small: %d (minimum: 10)", metadata.BufferSize)
	}

	// Validate enum values
	if metadata.LogLevel != "" {
		validLogLevels := map[string]bool{
			"debug": true, "info": true, "warn": true, "error": true,
		}
		if !validLogLevels[metadata.LogLevel] {
			return errors.Errorf("invalid LogLevel: %s (valid: debug, info, warn, error)", metadata.LogLevel)
		}
	}

	if metadata.ResilienceMode != "" {
		validModes := map[string]bool{
			"normal": true, "aggressive": true, "conservative": true,
		}
		if !validModes[metadata.ResilienceMode] {
			return errors.Errorf("invalid ResilienceMode: %s (valid: normal, aggressive, conservative)", metadata.ResilienceMode)
		}
	}

	c.logger.Debugf("BDLS metadata validation successful: %+v", metadata)
	return nil
}

// validateNewChannelConfig validates BDLS config for new channels
func (c *Consenter) validateNewChannelConfig(metadata *BDLSOptions, ordererConfig channelconfig.Orderer) error {
	// Ensure sufficient consensus participants
	consenters := ordererConfig.Consenters()
	if len(consenters) < 4 {
		return errors.Errorf("BDLS requires at least 4 participants, found %d", len(consenters))
	}

	// Validate consenter identities and certificates
	for i, consenter := range consenters {
		if len(consenter.Identity) == 0 {
			return errors.Errorf("consenter %d has empty identity certificate", i)
		}

		if _, err := x509.ParseCertificate(consenter.Identity); err != nil {
			return errors.Wrapf(err, "consenter %d has invalid identity certificate", i)
		}

		if consenter.Id == 0 {
			return errors.Errorf("consenter %d has invalid ID (cannot be 0)", i)
		}
	}

	// Validate that endpoints per organization are configured
	for _, org := range ordererConfig.Organizations() {
		if len(org.Endpoints()) == 0 {
			return errors.Errorf("organization %s has no endpoints configured", org.Name())
		}
	}

	c.logger.Infof("New channel BDLS configuration validated successfully with %d consenters", len(consenters))
	return nil
}

// validateConfigUpdate validates BDLS configuration updates
func (c *Consenter) validateConfigUpdate(oldOrdererConfig, newOrdererConfig channelconfig.Orderer, newMetadata *BDLSOptions) error {
	// Parse old metadata if it exists
	var oldMetadata *BDLSOptions
	if oldOrdererConfig.ConsensusMetadata() != nil {
		oldMetadata = &BDLSOptions{}
		if err := proto.Unmarshal(oldOrdererConfig.ConsensusMetadata(), oldMetadata); err != nil {
			c.logger.Warnf("Failed to unmarshal old BDLS metadata, using defaults: %v", err)
			oldMetadata = nil
		}
	}

	// Check if critical parameters changed (these may require special handling)
	if oldMetadata != nil {
		if oldMetadata.MaxMessageSize != 0 && newMetadata.MaxMessageSize != 0 &&
			oldMetadata.MaxMessageSize != newMetadata.MaxMessageSize {
			c.logger.Warnf("MaxMessageSize changed from %d to %d - this may affect network compatibility",
				oldMetadata.MaxMessageSize, newMetadata.MaxMessageSize)
		}

		if oldMetadata.BatchSize != 0 && newMetadata.BatchSize != 0 &&
			oldMetadata.BatchSize != newMetadata.BatchSize {
			c.logger.Infof("BatchSize changed from %d to %d", oldMetadata.BatchSize, newMetadata.BatchSize)
		}
	}

	// Validate consenter set changes
	oldConsenters := oldOrdererConfig.Consenters()
	newConsenters := newOrdererConfig.Consenters()

	if len(newConsenters) < 4 {
		return errors.Errorf("BDLS requires at least 4 participants, new config has %d", len(newConsenters))
	}

	// Check for valid consenter changes (additions/removals should be gradual)
	consenterDiff := len(newConsenters) - len(oldConsenters)
	if consenterDiff > 1 || consenterDiff < -1 {
		return errors.Errorf("BDLS consenter changes must be gradual: cannot add/remove more than 1 consenter at a time (change: %d)", consenterDiff)
	}

	c.logger.Infof("BDLS configuration update validated successfully: %d -> %d consenters", 
		len(oldConsenters), len(newConsenters))
	return nil
}

// createBDLSConfig extracts BDLS-specific configuration from orderer config
func (c *Consenter) createBDLSConfig(ordererConfig channelconfig.Orderer) (*BDLSOptions, error) {
	// Start with default configuration
	config := &BDLSOptions{
		NetworkTimeout:    "5s",
		ConsensusTimeout:  "30s", 
		MaxMessageSize:    10485760, // 10MB
		BatchSize:         100,
		HeartbeatInterval: "3s",
		MaxRetries:        3,
		BufferSize:        1000,
		ViewChangeTimeout: "20s",
		EnableMetrics:     true,
		LogLevel:          "info",
		ResilienceMode:    "normal",
		SyncOnStart:       false,
	}

	// Override with consensus metadata if provided
	consensusMetadata := ordererConfig.ConsensusMetadata()
	if len(consensusMetadata) > 0 {
		metadataConfig := &BDLSOptions{}
		if err := proto.Unmarshal(consensusMetadata, metadataConfig); err != nil {
			c.logger.Warnf("Failed to unmarshal BDLS consensus metadata, using defaults: %v", err)
		} else {
			// Merge non-zero values from metadata
			if metadataConfig.NetworkTimeout != "" {
				config.NetworkTimeout = metadataConfig.NetworkTimeout
			}
			if metadataConfig.ConsensusTimeout != "" {
				config.ConsensusTimeout = metadataConfig.ConsensusTimeout
			}
			if metadataConfig.MaxMessageSize > 0 {
				config.MaxMessageSize = metadataConfig.MaxMessageSize
			}
			if metadataConfig.BatchSize > 0 {
				config.BatchSize = metadataConfig.BatchSize
			}
			if metadataConfig.HeartbeatInterval != "" {
				config.HeartbeatInterval = metadataConfig.HeartbeatInterval
			}
			if metadataConfig.MaxRetries > 0 {
				config.MaxRetries = metadataConfig.MaxRetries
			}
			if metadataConfig.BufferSize > 0 {
				config.BufferSize = metadataConfig.BufferSize
			}
			if metadataConfig.ViewChangeTimeout != "" {
				config.ViewChangeTimeout = metadataConfig.ViewChangeTimeout
			}
			config.EnableMetrics = metadataConfig.EnableMetrics
			if metadataConfig.LogLevel != "" {
				config.LogLevel = metadataConfig.LogLevel
			}
			if metadataConfig.ResilienceMode != "" {
				config.ResilienceMode = metadataConfig.ResilienceMode
			}
			config.SyncOnStart = metadataConfig.SyncOnStart

			c.logger.Infof("Applied BDLS consensus metadata configuration")
		}
	}

	// Override with orderer batch configuration
	batchSize := ordererConfig.BatchSize()
	if batchSize.MaxMessageCount > 0 {
		config.BatchSize = uint32(batchSize.MaxMessageCount)
	}

	// Validate the final configuration
	if err := c.validateBDLSMetadata(config); err != nil {
		return nil, errors.Wrap(err, "invalid BDLS configuration")
	}

	c.logger.Debugf("Final BDLS config: %+v", config)
	return config, nil
}

// detectSelfID finds this node's ID in the consenters list
func (c *Consenter) detectSelfID(consenters []*common.Consenter) (uint32, error) {
	for _, consenter := range consenters {
		sanitizedCert, err := crypto.SanitizeX509Cert(consenter.Identity)
		if err != nil {
			return 0, errors.Wrapf(err, "failed to sanitize consenter identity")
		}
		
		if bytes.Equal(c.nodeIdentity, sanitizedCert) {
			return consenter.Id, nil
		}
	}
	
	c.logger.Warning("Could not find this node in channel consenters set")
	return 0, cluster.ErrNotInChannel
}

// extractParticipants converts Fabric consenters to BDLS participants
func (c *Consenter) extractParticipants(consenters []*common.Consenter) ([]bdls.Identity, error) {
	participants := make([]bdls.Identity, 0, len(consenters))
	
	for _, consenter := range consenters {
		// Parse the identity certificate to get the public key
		cert, err := x509.ParseCertificate(consenter.Identity)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse consenter certificate")
		}
		
		// Extract ECDSA public key for BDLS
		ecdsaPubKey, ok := cert.PublicKey.(*ecdsa.PublicKey)
		if !ok {
			return nil, errors.Errorf("consenter %d does not have ECDSA public key", consenter.Id)
		}
		
		// Convert to BDLS identity
		identity := bdls.DefaultPubKeyToIdentity(ecdsaPubKey)
		participants = append(participants, identity)
	}
	
	return participants, nil
}

// extractPrivateKey extracts the private key for BDLS consensus
func (c *Consenter) extractPrivateKey() (*ecdsa.PrivateKey, error) {
	// For now, generate a temporary private key for testing
	// TODO: Extract actual private key from node identity/signer
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, errors.Wrap(err, "failed to generate private key")
	}
	
	c.logger.Debug("Generated temporary private key for BDLS consensus")
	return privateKey, nil
}

// IsChannelMember implements consensus.ClusterConsenter interface
// It inspects the join block and detects whether this orderer is a member of the channel
func (c *Consenter) IsChannelMember(joinBlock *common.Block) (bool, error) {
	if joinBlock == nil {
		return false, errors.New("nil block")
	}
	
	envelopeConfig, err := protoutil.ExtractEnvelope(joinBlock, 0)
	if err != nil {
		return false, err
	}
	
	bundle, err := channelconfig.NewBundleFromEnvelope(envelopeConfig, c.cryptoProvider)
	if err != nil {
		return false, err
	}
	
	oc, exists := bundle.OrdererConfig()
	if !exists {
		return false, errors.New("no orderer config in bundle")
	}
	
	// Check if this node's identity is in the consenters list
	for _, consenter := range oc.Consenters() {
		sanitizedCert, err := crypto.SanitizeX509Cert(consenter.Identity)
		if err != nil {
			return false, err
		}
		if bytes.Equal(c.nodeIdentity, sanitizedCert) {
			c.logger.Debugf("Found this node (ID: %d) in channel consenters", consenter.Id)
			return true, nil
		}
	}
	
	c.logger.Debug("This node is not a member of the channel")
	return false, nil
}
