package storage

import (
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"sync"
)

// Index keeps track of information that peers have shared, it only contains information about connected peers
// In a nutshell, is very similar to BlockPresenceManager but it is used by replication mechanisms
type Index struct {
	sync.RWMutex

	// Cids must be stored with raw codec
	haves map[cid.Cid]map[peer.ID]bool
	peers map[peer.ID]map[cid.Cid]bool // Reverse index, useful to remove peers
}

func NewBlockIndex() *Index {
	return &Index{
		haves: make(map[cid.Cid]map[peer.ID]bool),
		peers: make(map[peer.ID]map[cid.Cid]bool),
	}
}

func (i *Index) Update(p peer.ID, add, remove []cid.Cid) {
	i.Lock()
	defer i.Unlock()

	// New peer
	if _, ok := i.peers[p]; !ok {
		i.peers[p] = make(map[cid.Cid]bool)
	}

	// We remove first because adds must prevail over removes
	for _, c := range add {
		i.set(p, c, true)
	}
	for _, c := range remove {
		i.set(p, c, false)
	}

}

// Not thread safe
// Pre: peer exists
func (i *Index) set(p peer.ID, c cid.Cid, state bool) {
	k := Raw(c)

	// New cid
	if _, ok := i.haves[k]; !ok {
		i.haves[k] = make(map[peer.ID]bool)
	}

	// Skip if already set
	if has, ok := i.haves[k][p]; ok && has == state {
		return
	}

	i.haves[k][p] = state
	i.peers[p][k] = state

}

func (i *Index) Have(c cid.Cid) (bool, []peer.ID) {
	i.RLock()
	defer i.RUnlock()

	var peers []peer.ID
	for p, has := range i.haves[Raw(c)] {
		if has {
			peers = append(peers, p)
		}
	}

	return len(peers) > 0, peers
}

func (i *Index) PeerDisconnected(p peer.ID) {
	// Not really an error since the peer may have not shared replication info
	if _, ok := i.peers[p]; !ok {
		return
	}

	for c := range i.peers[p] {
		delete(i.haves[c], p)
	}
	delete(i.peers, p)
}

func (i *Index) PeerHas(p peer.ID, c cid.Cid) bool {
	i.RLock()
	defer i.RUnlock()

	return i.haves[Raw(c)][p]
}

// Raw changes the cid codec to raw, if the cid is using already the raw codec is a no op.
func Raw(c cid.Cid) cid.Cid {
	if c.Prefix().Codec == cid.Raw {
		return c
	}

	return cid.NewCidV1(cid.Raw, c.Hash())
}
