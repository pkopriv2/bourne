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
		case rpcRegister:
			return s.Register(body)
		case rpcTokenBySignature:
			return s.TokenBySignature(body)
		case rpcMemberByLookup:
			return s.MemberByLookup(body)
		}
	}
}

func (s *rpcServer) Register(r rpcRegister) micro.Response {
	_, _, e := s.storage.SaveMember(r.Mem, r.Access)
	return micro.NewErrorResponse(errors.WithStack(e))
}

func (s *rpcServer) TokenBySignature(r rpcTokenBySignature) micro.Response {
	mem, ac, o, e := s.storage.LoadMemberByLookup(r.Lookup)
	if e != nil {
		return micro.NewErrorResponse(errors.WithStack(e))
	}

	if !o {
		return micro.NewErrorResponse(errors.Wrapf(RpcError, "No such member by lookup [%v]", r.Lookup))
	}

	shard, ok := ac.AccessShard.(SignatureShard)
	if !ok {
		return micro.NewErrorResponse(errors.Wrapf(RpcError, "Unexpected request [%v]", ac))
	}

	if r.Sig.Key != shard.Pub.Id() {
		return micro.NewErrorResponse(errors.Wrapf(RpcError, "Unauthorized"))
	}

	if time.Now().Sub(r.Challenge.Now) > 5 * time.Minute {
		return micro.NewErrorResponse(errors.Wrapf(RpcError, "Unauthorized"))
	}

	if err := verify(r.Challenge, shard.Pub, r.Sig); err != nil {
		return micro.NewErrorResponse(errors.Wrapf(RpcError, "Bad signature"))
	}

	token, err := newAuth(mem.Id, r.Exp).Sign(s.rand, s.sign, SHA256)
	if err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	return micro.NewStandardResponse(rpcTokenResponse{token})
}

func (s *rpcServer) Authenticate(t Token, now time.Time) error {
	if t.Expired(now) {
		return errors.Wrapf(TokenExpiredError, "Expired token [created=%v,expired=%v,now=%v]", t.Created, t.Expires, now)
	}
	if err := verify(t.Auth, s.sign.Public(), t.Sig); err != nil {
		return errors.Wrapf(TokenInvalidError, "Invalid token : %v", t.Sig)
	}
	return nil
}

func (s *rpcServer) MemberByLookup(r rpcMemberByLookup) micro.Response {
	if err := s.Authenticate(r.Token, time.Now()); err != nil {
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

type rpcTokenResponse struct {
	Token Token
}

type rpcMemberByLookup struct {
	Token  Token
	Lookup []byte
}

type rpcMemberResponse struct {
	Found  bool
	Mem    Member
	Access AccessCode
}

type rpcTokenBySignature struct {
	Lookup    []byte
	Challenge sigChallenge
	Sig       Signature
	Exp       time.Duration
}

type rpcRegister struct {
	Mem    Membership
	Access AccessShard
}

// Register all the gob types.
func init() {
	gob.Register(rpcRegister{})
	gob.Register(rpcTokenBySignature{})
	gob.Register(rpcTokenResponse{})
	gob.Register(rpcMemberByLookup{})
	gob.Register(rpcMemberResponse{})
}
