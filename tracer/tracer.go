package tracer

import (
	bsmsg "github.com/ipfs/go-bitswap/message"
	pb "github.com/ipfs/go-bitswap/message/pb"
	"github.com/ipfs/go-log"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

// Tracer provides methods to access all messages sent and received by Bitswap.
// This interface can be used to implement various statistics (this is original intent).
type Tracer interface {
	MessageReceived(peer.ID, bsmsg.BitSwapMessage)
	MessageSent(peer.ID, bsmsg.BitSwapMessage)
}

var logger = log.Logger("simpletracer")

type SimpleTracer struct {
}

func (t *SimpleTracer) MessageReceived(p peer.ID, m bsmsg.BitSwapMessage) {
	LogMessage("Received", p, m)
}

func (t *SimpleTracer) MessageSent(p peer.ID, m bsmsg.BitSwapMessage) {
	LogMessage("Sent", p, m)

}

func LogMessage(tag string, p peer.ID, m bsmsg.BitSwapMessage) {
	blocks := make(map[cid.Cid]int)
	for _, b := range m.Blocks() {
		blocks[b.Cid()] = len(b.RawData())
	}

	wantlist := m.Wantlist()
	wants, cancels := make(map[cid.Cid]string, len(wantlist)), make(map[cid.Cid]string, len(wantlist))
	for _, w := range wantlist {
		// Splits want-blocks and want-haves
		var tp string
		if w.WantType == pb.Message_Wantlist_Have {
			tp = "have"
		} else {
			tp = "block"
		}
		// Splits wants and cancels
		if w.Cancel {
			cancels[w.Cid] = tp
		} else {
			wants[w.Cid] = tp
		}
	}

	haves, donts := m.Info()
	info := make(map[cid.Cid]string, len(haves)+len(donts))
	for _, c := range haves {
		info[c] = "have"
	}
	for _, c := range donts {
		info[c] = "dont-have"
	}

	logger.Infow(tag,
		"peer", p,
		"blocks", blocks,
		"haves", m.Haves(),
		"dont-haves", m.DontHaves(),
		"knows", m.Knows(),
		"wants", wants,
		"cancels", cancels,
		"info", info,
		"size", m.Size(),
	)
}
