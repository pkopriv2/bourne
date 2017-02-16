package elmer

import (
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
)

var (
	systemStore = []byte("Elmer")
	rosterStoreKey = []byte("Elmer.Roster.Key")
)

type roster struct {
	ctx     common.Context
	ctrl    common.Control
	logger  common.Logger
	indexer *indexer
}

func newRoster(i *indexer) *roster {
	ctx := i.ctx.Sub("Roster")
	return &roster{ctx, ctx.Control(), ctx.Logger(), i}
}

func (r *roster) Add(cancel <-chan struct{}, peer string) error {
	if err := r.indexer.StoreEnsure(cancel, systemStore); err != nil {
		return errors.Wrapf(err, "Unable to add peer [%v]", peer)
	}

	r.logger.Info("Adding peer [%v]", peer)
	_, _, err := r.indexer.StoreUpdateItem(cancel, systemStore, rosterStoreKey, func(cur []byte) []byte {
		curPeers, err := parsePeersBytes(cur)
		if err != nil {
			curPeers = []string{}
		}
		// if hasPeer(curPeers, peer) {
			// return nil
		// }

		return peers(addPeer(curPeers, peer)).Bytes()
	})
	return err
}

func (r *roster) Del(cancel <-chan struct{}, peer string) error {
	if err := r.indexer.StoreEnsure(cancel, systemStore); err != nil {
		return errors.Wrapf(err, "Unable to del peer [%v]", peer)
	}

	r.logger.Info("Removing peer [%v]", peer)
	_, _, err := r.indexer.StoreUpdateItem(cancel, systemStore, rosterStoreKey, func(cur []byte) []byte {
		curPeers, err := parsePeersBytes(cur)
		if err != nil {
			curPeers = []string{}
		}

		return peers(delPeer(curPeers, peer)).Bytes()
	})
	return err
}
