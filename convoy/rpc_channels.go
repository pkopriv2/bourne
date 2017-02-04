package convoy

import (
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
)

type rpcChannels struct {
	ctx             common.Context
	ctrl            common.Control
	healthStatus    chan *common.Request
	healthProxyPing chan *common.Request
	dir             chan *common.Request // good for read-only requests
	dirList         chan *common.Request
	dirApply        chan *common.Request
	dirPushPull     chan *common.Request
	leave           chan *common.Request
}

func newRpcChannels(ctx common.Context) *rpcChannels {
	return &rpcChannels{
		ctx,
		ctx.Control(),
		make(chan *common.Request),
		make(chan *common.Request),
		make(chan *common.Request),
		make(chan *common.Request),
		make(chan *common.Request),
		make(chan *common.Request),
		make(chan *common.Request),
	}
}

func (h *rpcChannels) sendRequest(ch chan<- *common.Request, cancel <-chan struct{}, val interface{}) (interface{}, error) {
	req := common.NewRequest(val)
	defer req.Cancel()

	select {
	case <-h.ctrl.Closed():
		return nil, errors.WithStack(common.ClosedError)
	case <-cancel:
		return nil, errors.WithStack(common.CanceledError)
	case ch <- req:
		select {
		case <-h.ctrl.Closed():
			return nil, errors.WithStack(common.ClosedError)
		case r := <-req.Acked():
			return r, nil
		case e := <-req.Failed():
			return nil, e
		case <-cancel:
			return nil, errors.WithStack(common.CanceledError)
		}
	}
}

func (r *rpcChannels) HealthProxyPing(cancel <-chan struct{}, req rpcPingProxyRequest) (rpcPingResponse, error) {
	raw, err := r.sendRequest(r.healthProxyPing, cancel, req)
	if err != nil {
		return false, errors.WithStack(err)
	}

	return raw.(rpcPingResponse), nil
}

func (r *rpcChannels) DirList(cancel <-chan struct{}) (rpcDirListResponse, error) {
	raw, err := r.sendRequest(r.dirList, cancel, nil)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return raw.(rpcDirListResponse), nil
}

func (r *rpcChannels) DirApply(cancel <-chan struct{}, rpc rpcDirApplyRequest) (rpcDirApplyResponse, error) {
	raw, err := r.sendRequest(r.dirApply, cancel, rpc)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return raw.(rpcDirApplyResponse), nil
}

func (r *rpcChannels) DirPushPull(cancel <-chan struct{}, rpc rpcPushPullRequest) (rpcPushPullResponse, error) {
	raw, err := r.sendRequest(r.dirPushPull, cancel, rpc)
	if err != nil {
		return rpcPushPullResponse{}, errors.WithStack(err)
	}

	return raw.(rpcPushPullResponse), nil
}

func (r *rpcChannels) Directory(cancel <-chan struct{}) (*directory, error) {
	raw, err := r.sendRequest(r.dir, cancel, nil)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return raw.(*directory), nil
}

func (r *rpcChannels) Leave(cancel <-chan struct{}) error {
	_, err := r.sendRequest(r.leave, cancel, nil)
	return err
}
