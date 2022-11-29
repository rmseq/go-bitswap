package replication

import (
	"context"
	"github.com/ipfs/go-bitswap/client/internal/peermanager"
	"github.com/ipfs/go-bitswap/client/replication/storage"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipfs/go-log"
	"github.com/libp2p/go-libp2p/core/peer"
	"sync"
	"time"
)

const (
	defaultPeriod = 10
)

var logger = log.Logger("replicator")

func NewReplicator(ctx context.Context, b blockstore.Blockstore, p *peermanager.PeerManager, sendPeriodically bool) *Replicator {
	r := &Replicator{
		ctx:          ctx,
		peers:        p,
		blockstore:   b,
		periodicMode: sendPeriodically,
	}

	logger.Debugw("Starting Replicator", "periodicMode", sendPeriodically)
	r.initHavelist()

	var onChange func([]cid.Cid, []cid.Cid)
	if r.periodicMode {
		// Basically a no op because, we only want to update the havelist which is sent periodically
		onChange = func([]cid.Cid, []cid.Cid) {}
		go r.broadcastPeriodically(defaultPeriod)
	} else {
		onChange = r.peers.BroadcastInfo
	}
	go r.watchChanges(onChange)
	return r
}

// Replicator broadcasts the havelist (blocks owned by the peer) periodically if in periodicMode mode OR broadcasts
// changes as soon as they happen otherwise. In the latter new peers receive a full havelist when a
// new connection is opened.
//
// Even through the changes are broadcast in a block basis, messages may have multiple changes because the PeerQueue
// combines entries before sending a message.
type Replicator struct {
	ctx        context.Context
	blockstore blockstore.Blockstore
	peers      *peermanager.PeerManager

	locker       sync.Mutex
	deletedHaves bool

	// Cids must be stored with raw codec because adds may be received using all sort of codecs but
	// removes usually have a raw codec; storage.Index also stores cids in raw so there's no real impact in sending
	// havelists with whatever codec.
	cachedHavelist []cid.Cid
	haves          map[cid.Cid]struct{}

	periodicMode bool
}

func (r *Replicator) PeerConnected(p peer.ID) {
	logger.Debugf("New  peer, peer:%s", p)
	// In periodicMode mode there's no need to send the havelist to new peers,
	// they will eventually receive it
	if r.periodicMode {
		return
	}

	r.peers.SendInfo(p, r.havelist(), nil)
}

func (r *Replicator) havelist() []cid.Cid {
	r.locker.Lock()
	defer r.locker.Unlock()

	if r.deletedHaves {
		logger.Debugf("Updating havelist, cached havelist:%s", r.cachedHavelist)
		r.cachedHavelist = make([]cid.Cid, 0, len(r.haves))
		for c := range r.haves {
			r.cachedHavelist = append(r.cachedHavelist, c)
		}

		r.deletedHaves = false
	}
	return r.cachedHavelist
}

func (r *Replicator) initHavelist() {
	ctx := context.Background()
	defer ctx.Done()
	ch, err := r.blockstore.AllKeysChan(ctx)
	if err != nil {
		logger.Fatalf("Error retrieving havelist from blockstore: %s", err)
	}

	r.cachedHavelist = []cid.Cid{}
	r.haves = make(map[cid.Cid]struct{})
	for c := range ch {
		k := storage.Raw(c)

		r.haves[k] = struct{}{}
		r.cachedHavelist = append(r.cachedHavelist, k)
	}
}

func (r *Replicator) addHave(c cid.Cid) {
	k := storage.Raw(c)

	r.locker.Lock()
	defer r.locker.Unlock()

	r.haves[storage.Raw(k)] = struct{}{}
	r.cachedHavelist = append(r.cachedHavelist, k)
}

func (r *Replicator) rmHave(c cid.Cid) {
	k := storage.Raw(c)

	r.locker.Lock()
	defer r.locker.Unlock()

	if _, ok := r.haves[k]; ok {
		delete(r.haves, k)
		// Mark cachedHavelist as dirty
		r.deletedHaves = true
	}
}

func (r *Replicator) watchChanges(onChange func([]cid.Cid, []cid.Cid)) {
	add, rm := make(chan cid.Cid), make(chan cid.Cid)
	r.blockstore.Subscribe(add, rm)
	defer r.blockstore.Unsubscribe(add, rm)

	for {
		select {
		case c := <-add:
			logger.Debugf("Block added, cid:%s", c)
			onChange([]cid.Cid{c}, nil)
			r.addHave(c)

		case c := <-rm:
			logger.Debugf("Block removed, cid:%s", c)
			onChange(nil, []cid.Cid{c})
			r.rmHave(c)

		case <-r.ctx.Done():
			return
		}
	}
}

func (r *Replicator) broadcastPeriodically(period time.Duration) {
	for {
		timer := time.NewTimer(period * time.Second)
		select {
		case <-timer.C:
			r.peers.BroadcastInfo(r.havelist(), nil)

		case <-r.ctx.Done():
			return
		}
	}
}
