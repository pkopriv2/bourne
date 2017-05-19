package warden

import (
	"encoding/gob"
	"io"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/micro"
	"github.com/pkopriv2/bourne/net"
)

// Server endpoints
type rpcServer struct {
	ctx     common.Context
	logger  common.Logger
	rand    io.Reader
	sign    Signer
	storage storage
}

// Returns a new service handler for the ractlica
func newServer(ctx common.Context, storage storage, listener net.Listener, rand io.Reader, sign Signer, workers int) (micro.Server, error) {
	ctx = ctx.Sub("Server: %v", listener.Addr())
	server := &rpcServer{ctx: ctx, logger: ctx.Logger(), storage: storage, sign: sign, rand: rand}
	return micro.NewServer(ctx, listener, newServerHandler(server), workers)
}

func newServerHandler(s *rpcServer) func(micro.Request) micro.Response {
	return func(req micro.Request) micro.Response {
		if req.Body == nil {
			return micro.NewErrorResponse(errors.Errorf("Unknown request %v", req))
		}

		switch body := req.Body.(type) {
		default:
			panic(errors.New("Unreachable"))
		case rpcRegisterMemberReq:
			return s.RegisterMember(body)
		case rpcTokenBySignatureReq:
			return s.TokenBySignature(body)
		case rpcMemberByLookupReq:
			return s.MemberByLookup(body)
		case rpcRegisterTrustReq:
			return s.RegisterTrust(body)
		}
	}
}

func (s *rpcServer) RegisterMember(r rpcRegisterMemberReq) micro.Response {
	if e := s.storage.SaveMember(r.Mem, r.Access); e != nil {
		return micro.NewErrorResponse(errors.WithStack(e))
	}

	token, err := newAuth(r.Mem.Id, r.Expiration).Sign(s.rand, s.sign, SHA256)
	if err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	return micro.NewStandardResponse(rpcToken{token})
}

func (s *rpcServer) TokenBySignature(r rpcTokenBySignatureReq) micro.Response {
	mem, ac, o, e := s.storage.LoadMemberByLookup(r.Lookup)
	if e != nil {
		return micro.NewErrorResponse(errors.WithStack(e))
	}

	s.logger.Error("Generating a token by signature for member [%v]", mem.Id)
	if !o {
		s.logger.Error("No member found by signature [%v]", cryptoBytes(r.Lookup).Hex())
		return micro.NewErrorResponse(errors.Wrap(UnauthorizedError, "No such member"))
	}

	shard, ok := ac.MemberShard.(SignatureShard)
	if !ok {
		s.logger.Error("Unexpected request type", r.Lookup)
		return micro.NewErrorResponse(errors.Wrapf(RpcError, "Unexpected request [%v]", r))
	}
	if r.Sig.Key != shard.Pub.Id() {
		s.logger.Error("Unauthorized access attempt: %v", r.Lookup)
		return micro.NewErrorResponse(errors.Wrapf(UnauthorizedError, "Unauthorized"))
	}

	if time.Now().Sub(r.Challenge.Now) > 5*time.Minute {
		s.logger.Error("Unauthorized access.  Old token [%v] for member [%v]", r.Challenge.Now, mem.Id)
		return micro.NewErrorResponse(errors.Wrapf(UnauthorizedError, "Unauthorized"))
	}

	if err := verify(r.Challenge, shard.Pub, r.Sig); err != nil {
		s.logger.Error("Unauthorized access.")
		return micro.NewErrorResponse(errors.Wrapf(UnauthorizedError, "Unverified signature"))
	}

	token, err := newAuth(mem.Id, r.Exp).Sign(s.rand, s.sign, SHA256)
	if err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	return micro.NewStandardResponse(rpcToken{token})
}

func (s *rpcServer) authenticateToken(t Token, now time.Time) error {
	if t.Expired(now) {
		return errors.Wrapf(TokenExpiredError, "Expired token [created=%v,expired=%v,now=%v]", t.Created, t.Expires, now)
	}
	if err := verify(t.Auth, s.sign.Public(), t.Sig); err != nil {
		return errors.Wrapf(TokenInvalidError, "Invalid token: %v", t.Sig)
	}
	return nil
}

func (s *rpcServer) MemberByLookup(r rpcMemberByLookupReq) micro.Response {
	if err := s.authenticateToken(r.Token, time.Now()); err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	mem, ac, o, e := s.storage.LoadMemberByLookup(r.Lookup)
	if e != nil {
		return micro.NewErrorResponse(errors.WithStack(e))
	}

	if !o {
		return micro.NewStandardResponse(rpcMemberResponse{Found: false})
	}

	return micro.NewStandardResponse(rpcMemberResponse{true, mem, ac})
}

func (s *rpcServer) RegisterTrust(r rpcRegisterTrustReq) micro.Response {
	if err := s.authenticateToken(r.Token, time.Now()); err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	if e := s.storage.SaveTrust(r.Core, r.Code, r.Cert); e != nil {
		return micro.NewErrorResponse(errors.WithStack(e))
	}
	return micro.NewEmptyResponse()
}

type rpcToken struct {
	Token Token
}

type rpcMemberByLookupReq struct {
	Token  Token
	Lookup []byte
}

type rpcMemberResponse struct {
	Found  bool
	Mem    Member
	Access MemberCode
}

type rpcTokenBySignatureReq struct {
	Lookup    []byte
	Challenge sigChallenge
	Sig       Signature
	Exp       time.Duration
}

type rpcRegisterMemberReq struct {
	Mem        Member
	Access     MemberCode
	Expiration time.Duration
}

type rpcRegisterTrustReq struct {
	Token Token
	Core  TrustCore
	Code  TrustCode
	Cert  SignedCertificate
}

// Register all the gob types.
func init() {
	gob.Register(rpcRegisterMemberReq{})
	gob.Register(rpcRegisterTrustReq{})
	gob.Register(rpcTokenBySignatureReq{})
	gob.Register(rpcToken{})
	gob.Register(rpcMemberByLookupReq{})
	gob.Register(rpcMemberResponse{})
}
