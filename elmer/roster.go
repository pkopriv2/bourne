package elmer

import "github.com/pkopriv2/bourne/common"

var (
	rosterStore    = []byte("Elmer.Roster")
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
	return nil
	// if err := r.indexer.StoreEnsure(cancel, rosterStore); err != nil {
	// return errors.Wrapf(err, "Unable to add peer [%v]", peer)
	// }

	// r.logger.Info("Adding peer [%v]", peer)
	// _, _, err := r.indexer.StoreUpdateItem(cancel, rosterStore, rosterStoreKey, func(cur []byte) []byte {
	// curPeers, err := parsePeersBytes(cur)
	// if err != nil {
	// curPeers = []string{}
	// }
	//
	// if hasPeer(curPeers, peer) {
	// return nil
	// }
	//
	// return peers(addPeer(curPeers, peer)).Bytes()
	// })
	// return err
}

func (r *roster) Del(cancel <-chan struct{}, peer string) error {
	return nil
	// if err := r.indexer.StoreEnsure(cancel, rosterStore); err != nil {
	// return errors.Wrapf(err, "Unable to del peer [%v]", peer)
	// }
	//
	// r.logger.Info("Removing peer [%v]", peer)
	// _, _, err := r.indexer.StoreUpdateItem(cancel, rosterStore, rosterStoreKey, func(cur []byte) []byte {
	// curPeers, err := parsePeersBytes(cur)
	// if err != nil {
	// curPeers = []string{}
	// }
	//
	// if !hasPeer(curPeers, peer) {
	// return nil
	// }
	//
	// return peers(delPeer(curPeers, peer)).Bytes()
	// })
	// return err
}
