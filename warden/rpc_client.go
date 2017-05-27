package warden

import (
	"io"
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

func (r *rpcClient) Register(cancel <-chan struct{}, mem MemberCore, code MemberShard, ttl time.Duration) (SignedToken, error) {
	// raw, err := r.raw.Send(micro.NewRequest(rpcRegisterMemberReq{mem, code, ttl}))
	// if err != nil || !raw.Ok {
	// return Token{}, errors.WithStack(common.Or(err, raw.Error()))
	// }
	//
	// resp, ok := raw.Body.(rpcToken)
	// if !ok {
	// return Token{}, errors.Wrapf(RpcError, "Unexpected response type [%v]", raw)
	// }
	//
	// return resp.Token, nil
	return SignedToken{}, nil
}

func (r *rpcClient) Authenticate(cancel <-chan struct{}, rand io.Reader, creds credential, ttl time.Duration) (SignedToken, error) {
	args, err := creds.Auth(rand)
	if err != nil {
		return SignedToken{}, errors.WithStack(err)
	}

	raw, err := r.raw.Send(micro.NewRequest(rpcAuthReq{creds.Lookup(), args, ttl}))
	if err != nil || !raw.Ok {
		return SignedToken{}, errors.WithStack(common.Or(err, raw.Error()))
	}

	resp, ok := raw.Body.(rpcToken)
	if !ok {
		return SignedToken{}, errors.Wrapf(RpcError, "Unexpected response type [%v]", raw)
	}

	return resp.Token, nil
}

func (r *rpcClient) MemberByLookup(cancel <-chan struct{}, token SignedToken, lookup []byte) (MemberCore, MemberShard, bool, error) {
	raw, err := r.raw.Send(micro.NewRequest(rpcMemberByLookupReq{token, lookup}))
	if err != nil || !raw.Ok {
		return MemberCore{}, MemberShard{}, false, errors.WithStack(common.Or(err, raw.Error()))
	}

	resp, ok := raw.Body.(rpcMemberResponse)
	if !ok {
		return MemberCore{}, MemberShard{}, false, errors.Wrapf(RpcError, "Unexpected response type [%v]", raw)
	}

	return resp.Mem, resp.Access, resp.Found, nil
}

func (r *rpcClient) MemberSigningKeyById(cancel <-chan struct{}, token SignedToken, id uuid.UUID) (PublicKey, bool, error) {
	raw, err := r.raw.Send(micro.NewRequest(rpcMemberSigningKeyByIdReq{token, id}))
	if err != nil || !raw.Ok {
		return nil, false, errors.WithStack(common.Or(err, raw.Error()))
	}

	resp, ok := raw.Body.(rpcMemberKeyResponse)
	if !ok {
		return nil, false, errors.Wrapf(RpcError, "Unexpected response type [%v]", raw)
	}

	return resp.Key, resp.Found, nil
}

func (r *rpcClient) MemberInviteKeyById(cancel <-chan struct{}, token SignedToken, id uuid.UUID) (PublicKey, bool, error) {
	raw, err := r.raw.Send(micro.NewRequest(rpcMemberInviteKeyByIdReq{token, id}))
	if err != nil || !raw.Ok {
		return nil, false, errors.WithStack(common.Or(err, raw.Error()))
	}

	resp, ok := raw.Body.(rpcMemberKeyResponse)
	if !ok {
		return nil, false, errors.Wrapf(RpcError, "Unexpected response type [%v]", raw)
	}

	return resp.Key, resp.Found, nil
}

func (r *rpcClient) InvitationById(cancel <-chan struct{}, token SignedToken, id uuid.UUID) (Invitation, bool, error) {
	raw, err := r.raw.Send(micro.NewRequest(rpcInviteByIdReq{token, id}))
	if err != nil || !raw.Ok {
		return Invitation{}, false, errors.WithStack(common.Or(err, raw.Error()))
	}

	resp, ok := raw.Body.(rpcInviteResponse)
	if !ok {
		return Invitation{}, false, errors.Wrapf(RpcError, "Unexpected response type [%v]", raw)
	}

	return resp.Inv, resp.Found, nil
}

func (r *rpcClient) InvitationsByMember(cancel <-chan struct{}, token SignedToken, id uuid.UUID, opts PagingOptions) ([]Invitation, error) {
	raw, err := r.raw.Send(micro.NewRequest(rpcInvitesByMemberReq{token, id, opts}))
	if err != nil || !raw.Ok {
		return nil, errors.WithStack(common.Or(err, raw.Error()))
	}

	resp, ok := raw.Body.(rpcInvitesResponse)
	if !ok {
		return nil, errors.Wrapf(RpcError, "Unexpected response type [%v]", raw)
	}

	return resp.Invites, nil
}

func (r *rpcClient) InvitationRegister(cancel <-chan struct{}, a SignedToken, i Invitation) error {
	raw, err := r.raw.Send(micro.NewRequest(rpcInviteRegisterReq{a, i}))
	return errors.WithStack(common.Or(err, raw.Error()))
}

func (r *rpcClient) InvitationRevoke(cancel <-chan struct{}, a SignedToken, id uuid.UUID) error {
	panic("not implemented")
}

func (r *rpcClient) CertsByMember(cancel <-chan struct{}, token SignedToken, id uuid.UUID, opts PagingOptions) ([]SignedCertificate, error) {
	raw, err := r.raw.Send(micro.NewRequest(rpcCertsByMemberReq{token, id, opts}))
	if err != nil || !raw.Ok {
		return nil, errors.WithStack(common.Or(err, raw.Error()))
	}

	resp, ok := raw.Body.(rpcCertsResponse)
	if !ok {
		return nil, errors.Wrapf(RpcError, "Unexpected response type [%v]", raw)
	}

	return resp.Certs, nil
}

func (r *rpcClient) CertsByTrust(cancel <-chan struct{}, token SignedToken, id uuid.UUID, opts PagingOptions) ([]SignedCertificate, error) {
	raw, err := r.raw.Send(micro.NewRequest(rpcCertsByTrustReq{token, id, opts}))
	if err != nil || !raw.Ok {
		return nil, errors.WithStack(common.Or(err, raw.Error()))
	}

	resp, ok := raw.Body.(rpcCertsResponse)
	if !ok {
		return nil, errors.Wrapf(RpcError, "Unexpected response type [%v]", raw)
	}

	return resp.Certs, nil
}

func (r *rpcClient) CertRegister(cancel <-chan struct{}, a SignedToken, c SignedCertificate, k TrustCode) error {
	raw, err := r.raw.Send(micro.NewRequest(rpcCertRegisterReq{a, c, k}))
	return errors.WithStack(common.Or(err, raw.Error()))
}

func (r *rpcClient) CertRevoke(cancel <-chan struct{}, token SignedToken, trusteeId, trustId uuid.UUID) error {
	raw, err := r.raw.Send(micro.NewRequest(rpcCertRevokeReq{token, trusteeId, trustId}))
	return errors.WithStack(common.Or(err, raw.Error()))
}

func (r *rpcClient) TrustById(cancel <-chan struct{}, token SignedToken, id uuid.UUID) (Trust, bool, error) {
	raw, err := r.raw.Send(micro.NewRequest(rpcTrustByIdReq{token, id}))
	if err != nil || !raw.Ok {
		return Trust{}, false, errors.WithStack(common.Or(err, raw.Error()))
	}

	resp, ok := raw.Body.(rpcTrustResponse)
	if !ok {
		return Trust{}, false, errors.Wrapf(RpcError, "Unexpected response type [%v]", raw)
	}

	return resp.Core.privateTrust(resp.Code, resp.Cert), resp.Found, nil
}

func (r *rpcClient) TrustsByMember(cancel <-chan struct{}, token SignedToken, memberId uuid.UUID, opts PagingOptions) ([]Trust, error) {
	raw, err := r.raw.Send(micro.NewRequest(rpcTrustsByMemberReq{token, memberId, opts}))
	if err != nil || !raw.Ok {
		return nil, errors.WithStack(common.Or(err, raw.Error()))
	}

	resp, ok := raw.Body.(rpcTrustsResponse)
	if !ok {
		return nil, errors.Wrapf(RpcError, "Unexpected response type [%v]", raw)
	}

	trusts := make([]Trust, 0, len(resp.Trusts))
	for _, t := range resp.Trusts {
		trusts = append(trusts, t.Core.privateTrust(t.Code, t.Cert))
	}

	return trusts, nil
}

func (r *rpcClient) TrustRegister(cancel <-chan struct{}, a SignedToken, trust Trust) error {
	raw, err := r.raw.Send(micro.NewRequest(rpcTrustRegisterReq{a, trust.core(), trust.trusteeCode(), trust.trusteeCert}))
	return errors.WithStack(common.Or(err, raw.Error()))
}
