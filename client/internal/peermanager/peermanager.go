package peermanager

import (
	"context"
	"github.com/ipfs/go-bitswap/client/replication/storage"
	"sync"

	logging "github.com/ipfs/go-log"
	"github.com/ipfs/go-metrics-interface"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

var log = logging.Logger("bs:peermgr")

// PeerQueue provides a queue of messages to be sent for a single peer.
type PeerQueue interface {
	AddBroadcastWantHaves([]cid.Cid)
	AddWants([]cid.Cid, []cid.Cid)
	AddCancels([]cid.Cid)
	ResponseReceived(ks []cid.Cid)
	Startup()
	Shutdown()
	AddInfo([]cid.Cid, []cid.Cid)
}

type Session interface {
	ID() uint64
	SignalAvailability(peer.ID, bool)
}

// PeerQueueFactory provides a function that will create a PeerQueue.
type PeerQueueFactory func(ctx context.Context, p peer.ID) PeerQueue

// PeerManager manages a pool of peers and sends messages to peers in the pool.
type PeerManager struct {
	// sync access to peerQueues and peerWantManager
	pqLk sync.RWMutex
	// peerQueues -- interact through internal utility functions get/set/remove/iterate
	peerQueues map[peer.ID]PeerQueue
	pwm        *peerWantManager

	createPeerQueue PeerQueueFactory
	ctx             context.Context

	psLk         sync.RWMutex
	sessions     map[uint64]Session
	peerSessions map[peer.ID]map[uint64]struct{}

	replicationIndex *storage.Index

	self peer.ID
}

// New creates a new PeerManager, given a context and a peerQueueFactory.
func New(ctx context.Context, createPeerQueue PeerQueueFactory, self peer.ID, replicationIndex *storage.Index) *PeerManager {
	wantGauge := metrics.NewCtx(ctx, "wantlist_total", "Number of items in wantlist.").Gauge()
	wantBlockGauge := metrics.NewCtx(ctx, "want_blocks_total", "Number of want-blocks in wantlist.").Gauge()
	return &PeerManager{
		peerQueues:      make(map[peer.ID]PeerQueue),
		pwm:             newPeerWantManager(wantGauge, wantBlockGauge),
		createPeerQueue: createPeerQueue,
		ctx:             ctx,
		self:            self,

		sessions:         make(map[uint64]Session),
		peerSessions:     make(map[peer.ID]map[uint64]struct{}),
		replicationIndex: replicationIndex,
	}
}

func (pm *PeerManager) AvailablePeers() []peer.ID {
	// TODO: Rate-limit peers
	return pm.ConnectedPeers()
}

// ConnectedPeers returns a list of peers this PeerManager is managing.
func (pm *PeerManager) ConnectedPeers() []peer.ID {
	pm.pqLk.RLock()
	defer pm.pqLk.RUnlock()

	peers := make([]peer.ID, 0, len(pm.peerQueues))
	for p := range pm.peerQueues {
		peers = append(peers, p)
	}
	return peers
}

func (pm *PeerManager) IsConnected(p peer.ID) bool {
	pm.pqLk.RLock()
	defer pm.pqLk.RUnlock()

	_, ok := pm.peerQueues[p]
	return ok
}

// Connected is called to add a new peer to the pool, and send it an initial set
// of wants.
func (pm *PeerManager) Connected(p peer.ID) {
	pm.pqLk.Lock()
	defer pm.pqLk.Unlock()

	pq := pm.getOrCreate(p)

	// Inform the peer want manager that there's a new peer
	pm.pwm.addPeer(pq, p)

	// Inform the sessions that the peer has connected
	pm.signalAvailability(p, true)
}

// Disconnected is called to remove a peer from the pool.
func (pm *PeerManager) Disconnected(p peer.ID) {
	pm.pqLk.Lock()
	defer pm.pqLk.Unlock()

	pq, ok := pm.peerQueues[p]

	if !ok {
		return
	}

	// Inform the sessions that the peer has disconnected
	pm.signalAvailability(p, false)

	// Clean up the peer
	delete(pm.peerQueues, p)
	pq.Shutdown()
	pm.pwm.removePeer(p)
}

// ResponseReceived is called when a message is received from the network.
// ks is the set of blocks, HAVEs and DONT_HAVEs in the message
// Note that this is just used to calculate latency.
func (pm *PeerManager) ResponseReceived(p peer.ID, ks []cid.Cid) {
	pm.pqLk.Lock()
	pq, ok := pm.peerQueues[p]
	pm.pqLk.Unlock()

	if ok {
		pq.ResponseReceived(ks)
	}
}

// BroadcastWantHaves broadcasts want-haves to all peers (used by the session
// to discover seeds).
// For each peer it filters out want-haves that have previously been sent to
// the peer.
func (pm *PeerManager) BroadcastWantHaves(ctx context.Context, wantHaves []cid.Cid) {
	pm.pqLk.Lock()
	defer pm.pqLk.Unlock()

	pm.pwm.broadcastWantHaves(wantHaves)
}

// FilteredBroadcast broadcasts want-haves to all peers if the block is not presented in the replication replicationIndex,
// sends wants to peers that may have it otherwise; A want-block is sent to one of these peers, the others receive want-haves.
func (pm *PeerManager) FilteredBroadcast(ctx context.Context, wants []cid.Cid) {
	// No replication
	if pm.replicationIndex == nil {
		pm.BroadcastWantHaves(ctx, wants)
		return
	}

	haves := make(map[peer.ID][]cid.Cid)
	blocks := make(map[peer.ID][]cid.Cid)
	leftovers := make([]cid.Cid, 0, len(wants))
	for _, c := range wants {

		// See if some connected peer has the block before broadcasting,
		// replicationIndex.Have() only returns information about connected peers
		_, peers := pm.replicationIndex.Have(c)

		// TODO A viable min should be defined here since if only peer has the block may not be
		// 		the best strategy only include him in a new session
		if len(peers) < 3 {
			leftovers = append(leftovers, c)
			continue
		}

		// The first peer will receive a want-block
		blocks[peers[0]] = append(blocks[peers[0]], c)
		for _, p := range peers[1:] {
			haves[p] = append(haves[p], c)
		}
	}

	for p, c := range haves {
		// If a peer has pending want-haves, want-blocks are also sent in this call
		pm.SendWants(ctx, p, blocks[p], c)
		delete(blocks, p)
	}

	// A peer may only have pending want-blocks so this has to be performed anyway
	for p, c := range blocks {
		pm.SendWants(ctx, p, c, []cid.Cid{})
	}

	log.Infow("FilteredBroadcast", "cids", wants, "leftovers", leftovers)
	pm.pqLk.Lock()
	defer pm.pqLk.Unlock()
	pm.pwm.broadcastWantHaves(leftovers)

}

// SendWants sends the given want-blocks and want-haves to the given peer.
// It filters out wants that have previously been sent to the peer.
func (pm *PeerManager) SendWants(ctx context.Context, p peer.ID, wantBlocks []cid.Cid, wantHaves []cid.Cid) {
	pm.pqLk.Lock()
	defer pm.pqLk.Unlock()

	if _, ok := pm.peerQueues[p]; ok {
		pm.pwm.sendWants(p, wantBlocks, wantHaves)
	}
}

// SendCancels sends cancels for the given keys to all peers who had previously
// received a want for those keys.
func (pm *PeerManager) SendCancels(ctx context.Context, cancelKs []cid.Cid) {
	pm.pqLk.Lock()
	defer pm.pqLk.Unlock()

	// Send a CANCEL to each peer that has been sent a want-block or want-have
	pm.pwm.sendCancels(cancelKs)
}

// CurrentWants returns the list of pending wants (both want-haves and want-blocks).
func (pm *PeerManager) CurrentWants() []cid.Cid {
	pm.pqLk.RLock()
	defer pm.pqLk.RUnlock()

	return pm.pwm.getWants()
}

// CurrentWantBlocks returns the list of pending want-blocks
func (pm *PeerManager) CurrentWantBlocks() []cid.Cid {
	pm.pqLk.RLock()
	defer pm.pqLk.RUnlock()

	return pm.pwm.getWantBlocks()
}

// CurrentWantHaves returns the list of pending want-haves
func (pm *PeerManager) CurrentWantHaves() []cid.Cid {
	pm.pqLk.RLock()
	defer pm.pqLk.RUnlock()

	return pm.pwm.getWantHaves()
}

func (pm *PeerManager) getOrCreate(p peer.ID) PeerQueue {
	pq, ok := pm.peerQueues[p]
	if !ok {
		pq = pm.createPeerQueue(pm.ctx, p)
		pq.Startup()
		pm.peerQueues[p] = pq
	}
	return pq
}

// RegisterSession tells the PeerManager that the given session is interested
// in events about the given peer.
func (pm *PeerManager) RegisterSession(p peer.ID, s Session) {
	pm.psLk.Lock()
	defer pm.psLk.Unlock()

	if _, ok := pm.sessions[s.ID()]; !ok {
		pm.sessions[s.ID()] = s
	}

	if _, ok := pm.peerSessions[p]; !ok {
		pm.peerSessions[p] = make(map[uint64]struct{})
	}
	pm.peerSessions[p][s.ID()] = struct{}{}
}

// UnregisterSession tells the PeerManager that the given session is no longer
// interested in PeerManager events.
func (pm *PeerManager) UnregisterSession(ses uint64) {
	pm.psLk.Lock()
	defer pm.psLk.Unlock()

	for p := range pm.peerSessions {
		delete(pm.peerSessions[p], ses)
		if len(pm.peerSessions[p]) == 0 {
			delete(pm.peerSessions, p)
		}
	}

	delete(pm.sessions, ses)
}

// signalAvailability is called when a peer's connectivity changes.
// It informs interested sessions.
func (pm *PeerManager) signalAvailability(p peer.ID, isConnected bool) {
	pm.psLk.Lock()
	defer pm.psLk.Unlock()

	sesIds, ok := pm.peerSessions[p]
	if !ok {
		return
	}
	for sesId := range sesIds {
		if s, ok := pm.sessions[sesId]; ok {
			s.SignalAvailability(p, isConnected)
		}
	}
}

func (pm *PeerManager) BroadcastInfo(haves, donts []cid.Cid) {
	pm.pqLk.Lock()
	defer pm.pqLk.Unlock()

	for _, pq := range pm.peerQueues {
		pq.AddInfo(haves, donts)
	}
}

func (pm *PeerManager) SendInfo(p peer.ID, haves, donts []cid.Cid) {
	pm.pqLk.Lock()
	defer pm.pqLk.Unlock()

	if pq, ok := pm.peerQueues[p]; ok {
		pq.AddInfo(haves, donts)
	}
}
