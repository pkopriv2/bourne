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

func (r *roster) Update(cancel <-chan struct{}, fn func([]string) ([]string, error)) ([]string, error) {
	roster, err := r.ensureStore(cancel)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to update roster")
	}

	var ret []string
	_, err = r.indexer.StoreUpdateItem(cancel, roster, rosterKey, func(cur []byte) ([]byte, error) {
		curPeers, err := parsePeersBytes(cur)
		if err != nil {
			return []byte{}, err
		}

		new, err := fn(curPeers)
		if err != nil {
			return []byte{}, err
		}

		ret = new
		return peers(new).Bytes(), nil
	})
	return ret, err
}

func (r *roster) Get(cancel <-chan struct{}) ([]string, error) {
	roster, err := r.ensureStore(cancel)
	if err != nil {
		return nil, errors.Wrap(err, "Error ensuring roster store")
	}

	item, ok, err := r.indexer.StoreItemRead(cancel, roster, rosterKey)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if ! ok || item.Del {
		return []string{}, nil
	}

	return parsePeersBytes(item.Val)
}

func (r *roster) Add(cancel <-chan struct{}, peer string) ([]string, error) {
	r.logger.Info("Adding peer [%v]", peer)
	return r.Update(cancel, func(peers []string) ([]string, error) {
		return addPeer(peers, peer), nil
	})
}

func (r *roster) Del(cancel <-chan struct{}, peer string) ([]string, error) {
	r.logger.Info("Removing peer [%v]", peer)
	return r.Update(cancel, func(peers []string) ([]string, error) {
		return delPeer(peers, peer), nil
	})
}
