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

// Security functions (these are extracted out for composability)

func tokenIsActive(now time.Time) func(SignedToken) error {
	return func(act SignedToken) error {
		if !act.Expired(now) {
			return nil
		}
		return errors.Wrapf(TokenExpiredError, "Token expired [%v]", act.Expires)
	}
}

func tokenHasRole(role Role) func(SignedToken) error {
	return func(act SignedToken) error {
		if act.Role >= role {
			return nil
		}
		return errors.Wrapf(UnauthorizedError, "Token has role [%v]. Needed [%v]", act.Role, role)
	}
}

func acctIs(acct MemberAgreement) func(MemberAgreement) error {
	return func(act MemberAgreement) error {
		if act == acct {
			return nil
		}
		return errors.Wrap(UnauthorizedError, "Unexpected agreement")
	}
}

func acctIsEnabled() func(MemberAgreement) error {
	return func(act MemberAgreement) error {
		if act.Enabled {
			return nil
		}
		return errors.Wrapf(UnauthorizedError, "Account disabled [%v]")
	}
}

func acctHasAll(all ...func(MemberAgreement) error) func(MemberAgreement) error {
	return func(act MemberAgreement) error {
		for _, cur := range all {
			if err := cur(act); err != nil {
				return err
			}
		}
		return nil
	}
}

func certHasAll(all ...func(SignedCertificate) bool) func(SignedCertificate) bool {
	return func(act SignedCertificate) bool {
		for _, cur := range all {
			if !cur(act) {
				return false
			}
		}
		return true
	}
}

func certHasOne(all ...func(SignedCertificate) bool) func(SignedCertificate) bool {
	return func(act SignedCertificate) bool {
		for _, cur := range all {
			if cur(act) {
				return true
			}
		}
		return false
	}
}

func certHasIssuer(id uuid.UUID) func(SignedCertificate) bool {
	return func(act SignedCertificate) bool {
		return act.IssuerId == id
	}
}

func certHasTrustee(id uuid.UUID) func(SignedCertificate) bool {
	return func(act SignedCertificate) bool {
		return act.TrusteeId == id
	}
}

func certHasTrust(id uuid.UUID) func(SignedCertificate) bool {
	return func(act SignedCertificate) bool {
		return act.TrustId == id
	}
}

func certIsActive(now time.Time, skew time.Duration) func(SignedCertificate) bool {
	return func(act SignedCertificate) bool {
		return !act.Expired(now, skew)
	}
}

func certHasLevel(lvl LevelOfTrust) func(SignedCertificate) bool {
	return func(act SignedCertificate) bool {
		return lvl.MetBy(act.Level)
	}
}

// Server endpoints
type rpcServer struct {
	ctx     common.Context
	logger  common.Logger
	rand    io.Reader
	sign    Signer
	storage storage
	skew    time.Duration
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
		case rpcMemberRegisterReq:
			return s.RegisterMember(body)
		case rpcMemberAuthRegisterReq:
			return s.RegisterMemberAuth(body)
		case rpcAuthReq:
			return s.Authenticate(body)
		case rpcMemberByIdAndAuthReq:
			return s.MemberByIdAndAuth(body)
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

func (s *rpcServer) verifyToken(token SignedToken, fns ...func(SignedToken) error) error {
	for _, fn := range fns {
		if err := fn(token); err != nil {
			return errors.WithStack(err)
		}
	}

	if err := verify(token.Token, s.sign.Public(), token.Sig); err != nil {
		return errors.Wrapf(UnauthorizedError, "Invalid token: %v", token.Sig)
	}
	return nil
}

func (s *rpcServer) verifyAgreement(acct MemberAgreement, fns ...func(MemberAgreement) error) error {
	err := acctHasAll(fns...)(acct)
	return errors.WithStack(err)
}

func (s *rpcServer) verifyCertificate(cert SignedCertificate, fns ...func(SignedCertificate) bool) error {
	if !certHasAll(fns...)(cert) {
		return errors.Wrapf(UnauthorizedError, "Unauthorized")
	}
	return nil
}

func (s *rpcServer) authorize(memberId uuid.UUID, fns ...func(MemberAgreement) error) (MemberAgreement, error) {
	acct, ok, err := s.storage.LoadMemberAgreement(memberId)
	if err != nil {
		return MemberAgreement{}, errors.WithStack(err)
	}
	if !ok {
		return MemberAgreement{}, errors.Wrapf(UnauthorizedError, "Unauthorized")
	}
	return acct, s.verifyAgreement(acct, fns...)
}

func (s *rpcServer) certify(trusteeId, trustId uuid.UUID, fns ...func(SignedCertificate) bool) (SignedCertificate, error) {
	cert, ok, err := s.storage.LoadCertificateByMemberAndTrust(trusteeId, trustId)
	if err != nil {
		return SignedCertificate{}, errors.WithStack(err)
	}
	if !ok {
		return SignedCertificate{}, errors.Wrapf(UnauthorizedError, "No cert [trustee=%v, trust=%v]", trusteeId, trustId)
	}
	return cert, s.verifyCertificate(cert, fns...)
}

func (s *rpcServer) RegisterMember(r rpcMemberRegisterReq) micro.Response {
	s.logger.Debug("Registering new member: %v", r.Core.Id)

	if r.Token.Agreement.MemberId != r.Core.Id {
		return micro.NewErrorResponse(RpcError)
	}

	now := time.Now()
	if err := s.verifyToken(r.Token, tokenIsActive(now), tokenHasRole(Basic)); err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	if err := s.verifyAgreement(r.Token.Agreement, acctIsEnabled()); err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	auth, err := newMemberAuth(s.rand, r.Core.Id, r.Shard, r.Auth)
	if err != nil {
		s.logger.Error("Error generating member auth: %+v", err)
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	if err := s.storage.SaveMember(r.Token.Agreement, r.Core, auth, r.Lookup); err != nil {
		s.logger.Error("Error generating member auth: %+v", err)
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	token, err := newToken(r.Token.Agreement, r.TokenTtl, r.Token.Role, nil).Sign(s.rand, s.sign, SHA256)
	if err != nil {
		s.logger.Error("Error generating member auth: %+v", err)
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	return micro.NewStandardResponse(rpcToken{token})
}

func (s *rpcServer) RegisterMemberAuth(r rpcMemberAuthRegisterReq) micro.Response {
	s.logger.Debug("Registering new member authenticator: %v", r.Id)

	if err := s.verifyToken(r.Token, tokenIsActive(time.Now()), tokenHasRole(Manager)); err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	if _, err := s.authorize(r.Token.Agreement.MemberId, acctIsEnabled(), acctIs(r.Token.Agreement)); err != nil {
		s.logger.Error("Unauthorized: %+v", err)
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	auth, err := newMemberAuth(s.rand, r.Id, r.Shard, r.Auth)
	if err != nil {
		s.logger.Error("Error generating member auth: %+v", err)
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	if err := s.storage.SaveMemberAuth(auth, r.Lookup); err != nil {
		s.logger.Error("Error storing member auth: %+v", err)
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	s.logger.Debug("Successfully registered new authenticator [%v] for member [%v]", string(auth.Shard.Id), r.Id)
	return micro.NewEmptyResponse()
}

func (s *rpcServer) Authenticate(r rpcAuthReq) micro.Response {
	s.logger.Info("Attempting to authenticate: %v", string(r.Lookup))

	core, o, e := s.storage.LoadMemberByLookup(r.Lookup)
	if e != nil {
		return micro.NewErrorResponse(errors.Wrap(e, "Error retrieving member"))
	}

	if !o {
		s.logger.Error("No such member [%v]", string(r.Lookup))
		return micro.NewErrorResponse(errors.Wrap(UnauthorizedError, "No such member"))
	}

	acct, o, e := s.storage.LoadMemberAgreement(core.Id)
	if e != nil {
		return micro.NewErrorResponse(errors.Wrap(e, "No member acct"))
	}

	if !o {
		s.logger.Error("No member agreement [%v]", string(r.Lookup))
		return micro.NewErrorResponse(errors.Wrap(UnauthorizedError, "No such member"))
	}

	auth, o, e := s.storage.LoadMemberAuth(core.Id, r.AuthId)
	if e != nil {
		s.logger.Error("Error retrieving memeber auth: %+v", e)
		return micro.NewErrorResponse(errors.WithStack(e))
	}

	if !o {
		s.logger.Error("No such member authenticator [%v] for member [%v]", string(r.AuthId), core.Id)
		return micro.NewErrorResponse(errors.Wrap(UnauthorizedError, "No such member"))
	}

	if err := auth.authenticate(r.Args); err != nil {
		s.logger.Error("Error authenticating [%v]", core.Id)
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	s.logger.Info("Generating a token for member [%v]", core.Id)

	token, err := newToken(acct, r.Exp, r.Role, nil).Sign(s.rand, s.sign, SHA256)
	if err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	return micro.NewStandardResponse(rpcToken{token})
}

func (s *rpcServer) MemberByIdAndAuth(r rpcMemberByIdAndAuthReq) micro.Response {
	if err := s.verifyToken(r.Token, tokenIsActive(time.Now()), tokenHasRole(Basic)); err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	if err := s.verifyAgreement(r.Token.Agreement, acctIsEnabled()); err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	// authorize the owner of the token to view the member.
	member, found, err := s.storage.LoadMemberById(r.Id)
	if err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}
	if !found {
		return micro.NewStandardResponse(rpcMemberResponse{Found: false})
	}

	auth, found, err := s.storage.LoadMemberAuth(r.Id, r.AuthId)
	if err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}
	if !found {
		return micro.NewStandardResponse(rpcMemberResponse{Found: false})
	}

	return micro.NewStandardResponse(rpcMemberResponse{true, member, auth.Shard})
}

func (s *rpcServer) MemberSigningKeyById(r rpcMemberSigningKeyByIdReq) micro.Response {
	if err := s.verifyToken(r.Token, tokenIsActive(time.Now()), tokenHasRole(Basic)); err != nil {
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
	if err := s.verifyToken(r.Token, tokenIsActive(time.Now()), tokenHasRole(Basic)); err != nil {
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
	if err := s.verifyToken(r.Token, tokenIsActive(time.Now()), tokenHasRole(Basic)); err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	if e := s.storage.SaveTrust(r.Core, r.Code, r.Cert); e != nil {
		return micro.NewErrorResponse(errors.WithStack(e))
	}

	return micro.NewEmptyResponse()
}

func (s *rpcServer) LoadTrustById(r rpcTrustByIdReq) micro.Response {
	now := time.Now()

	if err := s.verifyToken(r.Token, tokenIsActive(now), tokenHasRole(Basic)); err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	// ensure the user of the token has been authorized to view the trust
	cert, err := s.certify(r.Token.Agreement.MemberId, r.Id, certIsActive(now, 5*time.Minute))
	if err != nil {
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
	code, ok, err := s.storage.LoadTrustCode(core.Id, r.Token.Agreement.MemberId)
	if err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	if !ok {
		return micro.NewStandardResponse(rpcTrustResponse{Found: true, Core: core.publicCore()})
	}

	return micro.NewStandardResponse(rpcTrustResponse{Found: true, Core: core, Code: code, Cert: cert})
}

func (s *rpcServer) InvitationRegister(r rpcInviteRegisterReq) micro.Response {
	now := time.Now()

	if cert, err := s.certify(r.Invite.Cert.IssuerId, r.Invite.Cert.TrustId, certIsActive(now, s.skew), certHasLevel(Direct)); err != nil {
		s.logger.Error("Unauthorized attempt by member [%v] to register invitation [%v]: %v", r.Token.Agreement.MemberId, r.Invite.Id, cert)
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	if err := s.storage.SaveInvitation(r.Invite); err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	return micro.NewEmptyResponse()
}

func (s *rpcServer) InvitationById(r rpcInviteByIdReq) micro.Response {
	now := time.Now()

	if err := s.verifyToken(r.Token, tokenIsActive(now), tokenHasRole(Basic)); err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	inv, ok, err := s.storage.LoadInvitationById(r.Id)
	if err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}
	if !ok {
		return micro.NewStandardResponse(rpcInviteResponse{Found: false})
	}

	return micro.NewStandardResponse(rpcInviteResponse{Found: ok, Inv: inv})
}

func (s *rpcServer) InvitationsByMember(r rpcInvitesByMemberReq) micro.Response {
	s.logger.Debug("Loading member invitations: %v", r.MemberId)
	invs, err := s.storage.LoadInvitationsByMember(r.MemberId, r.Opts.Beg, r.Opts.End)
	if err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	return micro.NewStandardResponse(rpcInvitesResponse{invs})
}

func (s *rpcServer) CertRegister(r rpcCertRegisterReq) micro.Response {
	s.logger.Error("Register cert: %v", r.Cert)
	if err := s.storage.SaveCertificate(r.Cert, r.Code); err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	return micro.NewEmptyResponse()
}

func (s *rpcServer) CertificatesByMember(r rpcCertsByMemberReq) micro.Response {
	s.logger.Debug("Loading member certs: %v", r.MemberId)
	certs, err := s.storage.LoadCertificatesByMember(r.MemberId, r.Opts)
	if err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	return micro.NewStandardResponse(rpcCertsResponse{certs})
}

func (s *rpcServer) CertificatesByTrust(r rpcCertsByTrustReq) micro.Response {
	// now := time.Now()

	s.logger.Debug("Loading trust certs: %v", r.TrustId)
	certs, err := s.storage.LoadCertificatesByTrust(r.TrustId, r.Opts)
	if err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}
	return micro.NewStandardResponse(rpcCertsResponse{certs})
}

func (s *rpcServer) CertRevoke(r rpcCertRevokeReq) micro.Response {
	return micro.NewErrorResponse(errors.WithStack(
		s.storage.RevokeCertificate(r.TrusteeId, r.TrustId)))
}

func (s *rpcServer) TrustsByMember(r rpcTrustsByMemberReq) micro.Response {
	tmp, err := s.storage.LoadCertificatesByMember(r.MemberId, r.Opts)
	if err != nil {
		return micro.NewErrorResponse(errors.WithStack(err))
	}

	certs := make(map[uuid.UUID]SignedCertificate)
	for _, c := range tmp {
		certs[c.TrustId] = c
	}

	codes := make(map[uuid.UUID]trustCode)
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

	cores := make(map[uuid.UUID]trustCore)
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

type rpcMemberByIdAndAuthReq struct {
	Token  SignedToken
	Id     uuid.UUID
	AuthId []byte
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
	Mem    memberCore
	Access memberShard
}

// Returns a token response
type rpcAuthReq struct {
	Lookup []byte
	AuthId []byte
	Args   []byte
	Role   Role
	Exp    time.Duration
}

type rpcMemberRegisterReq struct {
	Token    SignedToken
	Core     memberCore
	Shard    memberShard
	Lookup   []byte
	Auth     []byte
	TokenTtl time.Duration
}

type rpcMemberAuthRegisterReq struct {
	Token  SignedToken
	Id     uuid.UUID
	Shard  memberShard
	Auth   []byte
	Lookup []byte
}

type rpcTrustRegisterReq struct {
	Token SignedToken
	Core  trustCore
	Code  trustCode
	Cert  SignedCertificate
}

type rpcTrustByIdReq struct {
	Token SignedToken
	Id    uuid.UUID
}

type rpcTrustResponse struct {
	Found bool
	Core  trustCore
	Code  trustCode
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
	Code  trustCode
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
	gob.Register(rpcAuthReq{})
	gob.Register(rpcToken{})

	gob.Register(rpcMemberRegisterReq{})
	gob.Register(rpcMemberByIdAndAuthReq{})
	gob.Register(rpcMemberResponse{})
	gob.Register(rpcMemberSigningKeyByIdReq{})
	gob.Register(rpcMemberInviteKeyByIdReq{})
	gob.Register(rpcMemberKeyResponse{})
	gob.Register(rpcMemberAuthRegisterReq{})

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
