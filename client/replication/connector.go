package replication

import (
	"context"
	"github.com/ipfs/go-bitswap/client/internal/sessionmanager"
	"github.com/ipfs/go-bitswap/network"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"sync"
)

type empty struct{}

func NewConnector(manager *sessionmanager.SessionManager, net network.BitSwapNetwork) *Connector {
	return &Connector{
		connectingFor:  make(map[peer.ID]map[cid.Cid]empty),
		network:        net,
		sessionManager: manager,
	}
}

type Connector struct {
	connectingFor  map[peer.ID]map[cid.Cid]empty
	lock           sync.Mutex
	network        network.BitSwapNetwork
	sessionManager *sessionmanager.SessionManager
}

func (con *Connector) ConnectFor(ctx context.Context, p peer.ID, cids []cid.Cid) {
	go con.connectFor(ctx, p, cids)
}

func (con *Connector) connectFor(ctx context.Context, p peer.ID, cids []cid.Cid) {
	con.lock.Lock()
	// Already opening a connection, just add new haves if they exist
	if _, ok := con.connectingFor[p]; ok {
		defer con.lock.Unlock()
		for _, c := range cids {
			if _, has := con.connectingFor[p][c]; !has {
				con.connectingFor[p][c] = empty{}
			}
		}
		return
	}

	// New connection has to be open, the peers entry must be populated with the blocks that it (allegedly) has
	haves := make(map[cid.Cid]empty)
	for _, c := range cids {
		haves[c] = empty{}
	}
	con.connectingFor[p] = haves

	// Opening the connection is expected to take a long time,
	// the lock must be released now because other calls may be updating the haves
	con.lock.Unlock()

	err := con.network.ConnectTo(ctx, p)
	if err != nil {
		logger.Errorw("connectFor, failed to connect", "peer", p)
		delete(con.connectingFor, p)
		return
	}

	// After connecting we must delete the peer and haves must be collected to fake a message
	fakeHaves := con.deletePeer(p)
	con.sessionManager.ReceiveFrom(ctx, p, nil, fakeHaves, nil, nil)
	logger.Infow("ConnectFor, opened connection and sent fake haves", "peer", p, "cids", fakeHaves)

}

func (con *Connector) deletePeer(p peer.ID) []cid.Cid {
	con.lock.Lock()
	defer con.lock.Unlock()

	// Collect peer haves
	haves := make([]cid.Cid, 0, len(con.connectingFor[p]))
	for c := range con.connectingFor[p] {
		haves = append(haves, c)
	}

	delete(con.connectingFor, p)
	return haves
}
