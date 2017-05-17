package warden

import (
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/micro"
	"github.com/pkopriv2/bourne/net"
	uuid "github.com/satori/go.uuid"
)

func dial(ctx common.Context, addr string, timeout time.Duration) (*rpcClient, error) {
	conn, err := net.NewTcpNetwork().Dial(timeout, addr)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	cl, err := micro.NewClient(ctx, conn, micro.Gob)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return newClient(cl), nil
}

type rpcClient struct {
	raw micro.Client
}

func connect(ctx common.Context, network net.Network, timeout time.Duration, addr string) (*rpcClient, error) {
	conn, err := network.Dial(timeout, addr)
	if conn == nil || err != nil {
		return nil, errors.Wrapf(err, "Unable to connect to [%v]", addr)
	}
	defer func() {
		if err != nil {
			conn.Close()
		}
	}()

	cl, err := micro.NewClient(ctx, conn, micro.Gob)
	if cl == nil || err != nil {
		return nil, errors.Wrapf(err, "Unable to connect to [%v]", addr)
	}

	return newClient(cl), nil
}

func newClient(raw micro.Client) *rpcClient {
	return &rpcClient{raw}
}

func (c *rpcClient) Close() error {
	return c.raw.Close()
}

func (r *rpcClient) Register(cancel <-chan struct{}, mem Membership, shard AccessShard, ttl time.Duration) (Member, AccessCode, Token, error) {
	raw, err := r.raw.Send(micro.NewRequest(rpcRegisterRequest{mem, shard, ttl}))
	if err != nil || ! raw.Ok {
		return Member{}, AccessCode{}, Token{}, errors.WithStack(common.Or(err, raw.Error()))
	}

	resp, ok := raw.Body.(rpcRegisterResponse)
	if !ok {
		return Member{}, AccessCode{}, Token{}, errors.Wrapf(RpcError, "Unexpected response type [%v]", raw)
	}

	return resp.Mem, resp.Access, resp.Token, nil
}

func (r *rpcClient) TokenBySignature(cancel <-chan struct{}, lookup []byte, challenge sigChallenge, sig Signature, ttl time.Duration) (Token, error) {
	raw, err := r.raw.Send(micro.NewRequest(rpcTokenBySignature{lookup, challenge, sig, ttl}))
	if err != nil || !raw.Ok {
		return Token{}, errors.WithStack(common.Or(err, raw.Error()))
	}

	resp, ok := raw.Body.(rpcTokenResponse)
	if !ok {
		return Token{}, errors.Wrapf(RpcError, "Unexpected response type [%v]", raw)
	}

	return resp.Token, nil
}

func (r *rpcClient) MemberByLookup(cancel <-chan struct{}, token Token, lookup []byte) (Member, AccessCode, bool, error) {
	raw, err := r.raw.Send(micro.NewRequest(rpcMemberByLookup{token, lookup}))
	if err != nil || !raw.Ok {
		return Member{}, AccessCode{}, false, errors.WithStack(common.Or(err, raw.Error()))
	}

	resp, ok := raw.Body.(rpcMemberResponse)
	if !ok {
		return Member{}, AccessCode{}, false, errors.Wrapf(RpcError, "Unexpected response type [%v]", raw)
	}

	return resp.Mem, resp.Access, resp.Found, nil
}

func (r *rpcClient) InvitationById(cancel <-chan struct{}, a Token, id uuid.UUID) (Invitation, bool, error) {
	panic("not implemented")
}

func (r *rpcClient) InvitationsBySubscriber(cancel <-chan struct{}, a Token, id uuid.UUID, opts PagingOptions) ([]Invitation, error) {
	panic("not implemented")
}

func (r *rpcClient) InvitationRegister(cancel <-chan struct{}, a Token, i Invitation) error {
	panic("not implemented")
}

func (r *rpcClient) InvitationRevoke(cancel <-chan struct{}, a Token, id uuid.UUID) error {
	panic("not implemented")
}

func (r *rpcClient) CertsBySubscriber(cancel <-chan struct{}, a Token, id uuid.UUID, opts PagingOptions) ([]Certificate, error) {
	panic("not implemented")
}

func (r *rpcClient) CertsByTrust(cancel <-chan struct{}, a Token, id uuid.UUID, opt PagingOptions) ([]Certificate, error) {
	panic("not implemented")
}

func (r *rpcClient) CertRegister(cancel <-chan struct{}, a Token, c SignedCertificate, k SignedEncryptedShard) error {
	panic("not implemented")
}

func (r *rpcClient) CertRevoke(cancel <-chan struct{}, a Token, id uuid.UUID) error {
	panic("not implemented")
}

func (r *rpcClient) TrustById(cancel <-chan struct{}, a Token, id uuid.UUID) (Trust, bool, error) {
	panic("not implemented")
}

func (r *rpcClient) TrustsByMember(cancel <-chan struct{}, a Token, id uuid.UUID, opts PagingOptions) ([]Trust, error) {
	panic("not implemented")
}

func (r *rpcClient) TrustRegister(cancel <-chan struct{}, a Token, dom Trust) error {
	panic("not implemented")
}

// type rpcClientPool struct {
// ctx common.Context
// raw common.ObjectPool
// }
//
// func newRpcClientPool(ctx common.Context, network net.Network, peer Peer, size int) *rpcClientPool {
// return &rpcClientPool{ctx, common.NewObjectPool(ctx.Control(), size, newRpcClientConstructor(ctx, network, peer))}
// }
//
// func (c *rpcClientPool) Close() error {
// return c.raw.Close()
// }
//
// func (c *rpcClientPool) Max() int {
// return c.raw.Max()
// }
//
// func (c *rpcClientPool) TakeTimeout(dur time.Duration) *rpcClient {
// raw := c.raw.TakeTimeout(dur)
// if raw == nil {
// return nil
// }
//
// return raw.(*rpcClient)
// }
//
// func (c *rpcClientPool) Return(cl *rpcClient) {
// c.raw.Return(cl)
// }
//
// func (c *rpcClientPool) Fail(cl *rpcClient) {
// c.raw.Fail(cl)
// }
//
// func newRpcClientConstructor(ctx common.Context, network net.Network, peer Peer) func() (io.Closer, error) {
// return func() (io.Closer, error) {
// if cl, err := peer.Client(ctx, network, 30*time.Second); cl != nil && err == nil {
// return cl, err
// }
//
// return nil, nil
// }
// }
