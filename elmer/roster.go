package elmer

import (
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
)

var (
	rosterStore = []byte("Elmer.Roster")
	rosterKey   = []byte("Elmer.Roster.Key")
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

func (r *roster) ensureStore(cancel <-chan struct{}) (path, error) {
	info, err := ensureStore(r.indexer, cancel, partialPath{emptyPath, rosterStore})
	return info.Path, err
}

func (r *roster) Add(cancel <-chan struct{}, peer string) error {
	roster, err := r.ensureStore(cancel)
	if err != nil {
		return errors.Wrapf(err, "Unable to add peer [%v]", peer)
	}

	r.logger.Info("Adding peer [%v]", peer)
	_, _, err = r.indexer.StoreUpdateItem(cancel, roster, rosterKey, func(cur []byte) ([]byte, error) {
		curPeers, err := parsePeersBytes(cur)
		if err != nil {
			return []byte{}, err
		}

		if hasPeer(curPeers, peer) {
			return peers(curPeers).Bytes(), nil
		}

		return peers(addPeer(curPeers, peer)).Bytes(), nil
	})
	return err
}

func (r *roster) Del(cancel <-chan struct{}, peer string) error {
	roster, err := r.ensureStore(cancel)
	if err != nil {
		return errors.Wrapf(err, "Unable to del peer [%v]", peer)
	}

	r.logger.Info("Removing peer [%v]", peer)
	_, _, err = r.indexer.StoreUpdateItem(cancel, roster, rosterKey, func(cur []byte) ([]byte, error) {
		curPeers, err := parsePeersBytes(cur)
		if err != nil {
			return []byte{}, err
		}

		if ! hasPeer(curPeers, peer) {
			return peers(curPeers).Bytes(), nil
		}

		return peers(delPeer(curPeers, peer)).Bytes(), nil
	})
	return err
}
