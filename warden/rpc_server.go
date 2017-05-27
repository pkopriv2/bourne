package warden

import (
	"encoding/gob"
	"io"
	"reflect"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/micro"
	"github.com/pkopriv2/bourne/net"
	uuid "github.com/satori/go.uuid"
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
			return micro.NewErrorResponse(errors.Wrapf(RpcError, "Unknown request type: %v", reflect.TypeOf(body)))
		case rpcRegisterMemberReq:
			return s.RegisterMember(body)
		case rpcMemberByLookupReq:
			return s.MemberByLookup(body)
		case rpcMemberSigningKeyByIdReq:
			return s.MemberSigningKeyById(body)
		case rpcMemberInviteKeyByIdReq:
			return s.MemberInviteKeyById(body)
		case rpcTrustRegisterReq:
			return s.RegisterTrust(body)
		case rpcTrustsByMemberReq:
			return s.TrustsByMember(body)
		case rpcTrustByIdReq:
			return s.LoadTrustById(body)
		case rpcInviteRegisterReq:
			return s.InvitationRegister(body)
		case rpcInviteByIdReq:
			return s.InvitationById(body)
		case rpcInvitesByMemberReq:
			return s.InvitationsByMember(body)
		case rpcCertRegisterReq:
			return s.CertRegister(body)
		case rpcCertRevokeReq:
			return s.CertRevoke(body)
		case rpcCertsByMemberReq:
			return s.CertificatesByMember(body)
		case rpcCertsByTrustReq:
			return s.CertificatesByTrust(body)
		}
	}
}

func (s *rpcServer) RegisterMember(r rpcRegisterMemberReq) micro.Response {
	s.logger.Debug("Registering new member: %v", r.Mem.Id)

	return micro.NewEmptyResponse()
	// if e := s.storage.SaveMember(r.Mem, r.Code); e != nil {
	// return micro.NewErrorResponse(errors.WithStack(e))
	// }

	// token, err := newAuth(r.Mem.Id, r.Expiration).Sign(s.rand, s.sign, SHA256)
	// if err != nil {
	// return micro.NewErrorResponse(errors.WithStack(err))
	// }

	// return micro.NewStandardResponse(rpcToken{token})
}

// func (s *rpcServer) TokenBySignature(r rpcTokenBySignatureReq) micro.Response {
	// return micro.NewEmptyResponse()
	// // member, code, o, e := s.storage.LoadMemberByLookup(r.Lookup)
	// // if e != nil {
	// // return micro.NewErrorResponse(errors.WithStack(e))
	// // }
	// //
	// // s.logger.Error("Generating a token by signature for member [%v]", member.Id)
	// // if !o {
	// // s.logger.Error("No member found by signature [%v]", cryptoBytes(r.Lookup).Hex())
	// // return micro.NewErrorResponse(errors.Wrap(UnauthorizedError, "No such member"))
	// // }
	// //
	// // shard, ok := code.MemberShard.(SignatureShard)
	// // if !ok {
	// // s.logger.Error("Unexpected request type", r.Lookup)
	// // return micro.NewErrorResponse(errors.Wrapf(RpcError, "Unexpected request [%v]", r))
	// // }
	// // if r.Sig.Key != shard.Pub.Id() {
	// // s.logger.Error("Unauthorized access attempt: %v", r.Lookup)
	// // return micro.NewErrorResponse(errors.Wrapf(UnauthorizedError, "Unauthorized"))
	// // }
	// //
	// // if time.Now().Sub(r.Challenge.Now) > 5*time.Minute {
	// // s.logger.Error("Unauthorized access.  Old token [%v] for member [%v]", r.Challenge.Now, member.Id)
	// // return micro.NewErrorResponse(errors.Wrapf(UnauthorizedError, "Unauthorized"))
	// // }
	// //
	// // if err := verify(r.Challenge, shard.Pub, r.Sig); err != nil {
	// // s.logger.Error("Unauthorized access.")
	// // return micro.NewErrorResponse(errors.Wrapf(UnauthorizedError, "Unverified signature"))
	// // }
	// //
	// // token, err := newAuth(member.Id, r.Exp).Sign(s.rand, s.sign, SHA256)
	// // if err != nil {
	// // return micro.NewErrorResponse(errors.WithStack(err))
	// // }
	// //
	// // return micro.NewStandardResponse(rpcToken{token})
// }

func (s *rpcServer) authenticate(t SignedToken, now time.Time) error {
	if t.Expired(now) {
		return errors.Wrapf(TokenExpiredError, "Expired token [created=%v,expired=%v,now=%v]", t.Created, t.Expires, now)
	}
	if err := verify(t.Token, s.sign.Public(), t.Sig); err != nil {
		return errors.Wrapf(TokenInvalidError, "Invalid token: %v", t.Sig)
	}
	return nil
}

func (s *rpcServer) authorizeTrustUse(memberId, trustId uuid.UUID, lvl LevelOfTrust, now time.Time) (SignedCertificate, error) {
	cert, ok, err := s.storage.LoadCertificateByMemberAndTrust(memberId, trustId)
	if err != nil {
		return SignedCertificate{}, errors.WithStack(err)
	}

	if !ok || !cert.Meets(lvl) || cert.Expired(now, 5*time.Minute) {
		return SignedCertificate{}, errors.WithStack(UnauthorizedError)
	}

	return cert, nil
}

func (s *rpcServer) authorizeMemberUse(memberId, by uuid.UUID) error {
	if memberId != by {
		return errors.WithStack(UnauthorizedError)
	}
	return nil
}

func (s *rpcServer) MemberByLookup(r rpcMemberByLookupReq) micro.Response {
	if err := s.authenticate(r.Token, time.Now()); err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	// authorize the owner of the token to view the member.
	mem, ac, o, e := s.storage.LoadMemberByLookup(r.Lookup)
	if e != nil {
		return micro.NewErrorResponse(errors.WithStack(e))
	}
	if !o {
		return micro.NewStandardResponse(rpcMemberResponse{Found: false})
	}
	if err := s.authorizeMemberUse(r.Token.MemberId, mem.Id); err != nil {
		return micro.NewErrorResponse(errors.WithStack(e))
	}

	return micro.NewStandardResponse(rpcMemberResponse{true, mem, ac.Shard})
}

func (s *rpcServer) MemberSigningKeyById(r rpcMemberSigningKeyByIdReq) micro.Response {
	if err := s.authenticate(r.Token, time.Now()); err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	mem, o, e := s.storage.LoadMemberById(r.Id)
	if e != nil {
		return micro.NewErrorResponse(errors.WithStack(e))
	}
	if !o {
		return micro.NewStandardResponse(rpcMemberResponse{Found: false})
	}

	return micro.NewStandardResponse(rpcMemberKeyResponse{true, mem.SigningKey.Pub})
}

func (s *rpcServer) MemberInviteKeyById(r rpcMemberInviteKeyByIdReq) micro.Response {
	if err := s.authenticate(r.Token, time.Now()); err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	mem, o, e := s.storage.LoadMemberById(r.Id)
	if e != nil {
		return micro.NewErrorResponse(errors.WithStack(e))
	}
	if !o {
		return micro.NewStandardResponse(rpcMemberResponse{Found: false})
	}

	return micro.NewStandardResponse(rpcMemberKeyResponse{true, mem.InviteKey.Pub})
}

func (s *rpcServer) RegisterTrust(r rpcTrustRegisterReq) micro.Response {
	if err := s.authenticate(r.Token, time.Now()); err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	if e := s.storage.SaveTrust(r.Core, r.Code, r.Cert); e != nil {
		return micro.NewErrorResponse(errors.WithStack(e))
	}

	return micro.NewEmptyResponse()
}

func (s *rpcServer) LoadTrustById(r rpcTrustByIdReq) micro.Response {
	now := time.Now()
	if err := s.authenticate(r.Token, now); err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	// validate that the trust exists
	core, ok, err := s.storage.LoadTrustCore(r.Id)
	if err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	if !ok {
		return micro.NewStandardResponse(rpcTrustResponse{Found: false})
	}

	// validate that the user has a valid trust code
	code, ok, err := s.storage.LoadTrustCode(core.Id, r.Token.MemberId)
	if err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	if !ok {
		return micro.NewStandardResponse(rpcTrustResponse{Found: true, Core: core.publicCore()})
	}

	// ensure the user of the token has been authorized to view the trust
	cert, err := s.authorizeTrustUse(r.Token.MemberId, r.Id, Beneficiary, now)
	if err != nil {
		s.logger.Error("Unauthorized attempt by member [%v] to load trust [%v]: %v", r.Token.MemberId, r.Id, cert)
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	return micro.NewStandardResponse(rpcTrustResponse{Found: true, Core: core, Code: code, Cert: cert})
}

func (s *rpcServer) InvitationRegister(r rpcInviteRegisterReq) micro.Response {
	now := time.Now()
	if err := s.authenticate(r.Token, now); err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	if cert, err := s.authorizeTrustUse(r.Invite.Cert.IssuerId, r.Invite.Cert.TrustId, Director, now); err != nil {
		s.logger.Error("Unauthorized attempt by member [%v] to register invitation [%v]: %v", r.Token.MemberId, r.Invite.Id, cert)
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	if err := s.storage.SaveInvitation(r.Invite); err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	return micro.NewEmptyResponse()
}

func (s *rpcServer) InvitationById(r rpcInviteByIdReq) micro.Response {
	now := time.Now()
	if err := s.authenticate(r.Token, now); err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	inv, ok, err := s.storage.LoadInvitationById(r.Id)
	if err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	if cert, err := s.authorizeTrustUse(r.Token.MemberId, inv.Cert.TrustId, Manager, now); err != nil {
		s.logger.Error("Unauthorized attempt by member [%v] to view invitation [%v]: %v", r.Token.MemberId, r.Id, cert)
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	return micro.NewStandardResponse(rpcInviteResponse{Found: ok, Inv: inv})
}

func (s *rpcServer) InvitationsByMember(r rpcInvitesByMemberReq) micro.Response {
	if err := s.authenticate(r.Token, time.Now()); err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	if err := s.authorizeMemberUse(r.Token.MemberId, r.MemberId); err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	s.logger.Debug("Loading member invitations: %v", r.MemberId)
	invs, err := s.storage.LoadInvitationsByMember(r.MemberId, r.Opts.Beg, r.Opts.End)
	if err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	return micro.NewStandardResponse(rpcInvitesResponse{invs})
}

func (s *rpcServer) CertRegister(r rpcCertRegisterReq) micro.Response {
	if err := s.authenticate(r.Token, time.Now()); err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	s.logger.Error("Register certificate: %v", r.Cert)
	if err := s.storage.SaveCertificate(r.Cert, r.Code); err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	return micro.NewEmptyResponse()
}

func (s *rpcServer) CertificatesByMember(r rpcCertsByMemberReq) micro.Response {
	if err := s.authenticate(r.Token, time.Now()); err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	if err := s.authorizeMemberUse(r.Token.MemberId, r.MemberId); err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	s.logger.Debug("Loading member certificates: %v", r.MemberId)
	certs, err := s.storage.LoadCertificatesByMember(r.MemberId, r.Opts)
	if err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}
	return micro.NewStandardResponse(rpcCertsResponse{certs})
}

func (s *rpcServer) CertificatesByTrust(r rpcCertsByTrustReq) micro.Response {
	now := time.Now()
	if err := s.authenticate(r.Token, now); err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	if _, err := s.authorizeTrustUse(r.Token.MemberId, r.TrustId, Manager, now); err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	s.logger.Debug("Loading trust certificates: %v", r.TrustId)
	certs, err := s.storage.LoadCertificatesByTrust(r.TrustId, r.Opts)
	if err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}
	return micro.NewStandardResponse(rpcCertsResponse{certs})
}

func (s *rpcServer) CertRevoke(r rpcCertRevokeReq) micro.Response {
	now := time.Now()
	if err := s.authenticate(r.Token, now); err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	if cert, err := s.authorizeTrustUse(r.Token.MemberId, r.TrustId, Director, now); err != nil {
		s.logger.Error("Unauthorized attempt by member [%v] to revoke certificate from member [%v]: %v", r.Token.MemberId, r.TrusteeId, cert)
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	return micro.NewErrorResponse(errors.WithStack(
		s.storage.RevokeCertificate(r.TrusteeId, r.TrustId)))
}

func (s *rpcServer) TrustsByMember(r rpcTrustsByMemberReq) micro.Response {
	if err := s.authenticate(r.Token, time.Now()); err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	if err := s.authorizeMemberUse(r.Token.MemberId, r.MemberId); err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	tmp, err := s.storage.LoadCertificatesByMember(r.MemberId, r.Opts)
	if err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	certs := make(map[uuid.UUID]SignedCertificate)
	for _, c := range tmp {
		certs[c.TrustId] = c
	}

	codes := make(map[uuid.UUID]TrustCode)
	for _, c := range tmp {
		code, o, err := s.storage.LoadTrustCode(c.TrustId, c.TrusteeId)
		if err != nil {
			return micro.NewErrorResponse(errors.WithStack(err))
		}

		if !o {
			return micro.NewErrorResponse(errors.Wrapf(StorageInvariantError, "Expected trust code [trust=%v,trustee=%v] to exist.", c.TrustId, c.TrusteeId))
		}

		codes[c.TrustId] = code
	}

	cores := make(map[uuid.UUID]TrustCore)
	for _, c := range tmp {
		core, o, err := s.storage.LoadTrustCore(c.TrustId)
		if err != nil {
			return micro.NewErrorResponse(errors.WithStack(err))
		}

		if !o {
			return micro.NewErrorResponse(errors.Wrapf(StorageInvariantError, "Expected trust core [trust=%v,trustee=%v] to exist.", c.TrustId, c.TrusteeId))
		}

		cores[c.TrustId] = core
	}

	trusts := make([]rpcTrustResponse, 0, len(certs))
	for _, c := range certs {
		trusts = append(trusts, rpcTrustResponse{
			Found: true,
			Core:  cores[c.TrustId],
			Code:  codes[c.TrustId],
			Cert:  c,
		})
	}

	return micro.NewStandardResponse(rpcTrustsResponse{trusts})
}

type rpcToken struct {
	Token SignedToken
}

type rpcMemberByLookupReq struct {
	Token  SignedToken
	Lookup []byte
}

type rpcMemberSigningKeyByIdReq struct {
	Token SignedToken
	Id    uuid.UUID
}

type rpcMemberInviteKeyByIdReq struct {
	Token SignedToken
	Id    uuid.UUID
}

type rpcMemberKeyResponse struct {
	Found bool
	Key   PublicKey
}

type rpcMemberResponse struct {
	Found  bool
	Mem    MemberCore
	Access MemberShard
}

type rpcAuthReq struct {
	AuthId    []byte
	AuthArgs  []byte
	Exp       time.Duration
}

type rpcRegisterMemberReq struct {
	Mem        MemberCore
	Code       MemberShard
	Expiration time.Duration
}

type rpcTrustRegisterReq struct {
	Token SignedToken
	Core  TrustCore
	Code  TrustCode
	Cert  SignedCertificate
}

type rpcTrustByIdReq struct {
	Token SignedToken
	Id    uuid.UUID
}

type rpcTrustResponse struct {
	Found bool
	Core  TrustCore
	Code  TrustCode
	Cert  SignedCertificate
}

type rpcInviteRegisterReq struct {
	Token  SignedToken
	Invite Invitation
}

type rpcInviteByIdReq struct {
	Token SignedToken
	Id    uuid.UUID
}

type rpcInvitesByMemberReq struct {
	Token    SignedToken
	MemberId uuid.UUID
	Opts     PagingOptions
}

type rpcInvitesByTrustReq struct {
	Token   SignedToken
	TrustId uuid.UUID
	Opts    PagingOptions
}

type rpcInviteResponse struct {
	Found bool
	Inv   Invitation
}

type rpcInvitesResponse struct {
	Invites []Invitation
}

type rpcCertRegisterReq struct {
	Token SignedToken
	Cert  SignedCertificate
	Code  TrustCode
}

type rpcCertsByMemberReq struct {
	Token    SignedToken
	MemberId uuid.UUID
	Opts     PagingOptions
}

type rpcCertsByTrustReq struct {
	Token   SignedToken
	TrustId uuid.UUID
	Opts    PagingOptions
}

type rpcCertRevokeReq struct {
	Token     SignedToken
	TrusteeId uuid.UUID
	TrustId   uuid.UUID
}

type rpcCertsResponse struct {
	Certs []SignedCertificate
}

type rpcTrustsByMemberReq struct {
	Token    SignedToken
	MemberId uuid.UUID
	Opts     PagingOptions
}

type rpcTrustsResponse struct {
	Trusts []rpcTrustResponse
}

// Register all the gob types.
func init() {
	gob.Register(rpcRegisterMemberReq{})
	gob.Register(rpcAuthReq{})
	gob.Register(rpcToken{})

	gob.Register(rpcMemberByLookupReq{})
	gob.Register(rpcMemberResponse{})
	gob.Register(rpcMemberSigningKeyByIdReq{})
	gob.Register(rpcMemberInviteKeyByIdReq{})
	gob.Register(rpcMemberKeyResponse{})

	gob.Register(rpcTrustRegisterReq{})
	gob.Register(rpcTrustByIdReq{})
	gob.Register(rpcTrustResponse{})
	gob.Register(rpcTrustsByMemberReq{})
	gob.Register(rpcTrustsResponse{})

	gob.Register(rpcInviteRegisterReq{})
	gob.Register(rpcInviteByIdReq{})
	gob.Register(rpcInviteResponse{})
	gob.Register(rpcInvitesByMemberReq{})
	gob.Register(rpcInvitesByTrustReq{})
	gob.Register(rpcInvitesResponse{})

	gob.Register(rpcCertRegisterReq{})
	gob.Register(rpcCertRevokeReq{})
	gob.Register(rpcCertsByTrustReq{})
	gob.Register(rpcCertsByMemberReq{})
	gob.Register(rpcCertsResponse{})
}
