package warden

import (
	"encoding/gob"
	"io"
	"math/rand"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

// the strength is an att
type Strength int

const (
	Minimal Strength = 1024 * iota // bits of entropy.
	Strong
	Stronger
	Strongest
)

type MemberOptions struct {
	// SecretStrength Strength
	// KeyStrength Strength
	Secret        SecretOptions
	InviteKey     KeyPairOptions
	SigningKey    KeyPairOptions
	SignatureHash Hash
	NonceSize     int
}

func (s *MemberOptions) SecretOptions(fn func(*SecretOptions)) {
	s.Secret = buildSecretOptions(fn)
}

func (s *MemberOptions) SigningOptions(fn func(*KeyPairOptions)) {
	s.SigningKey = buildKeyPairOptions(fn)
}

func (s *MemberOptions) InviteOptions(fn func(*KeyPairOptions)) {
	s.InviteKey = buildKeyPairOptions(fn)
}

func buildMemberOptions(fns ...func(*MemberOptions)) MemberOptions {
	ret := MemberOptions{buildSecretOptions(), buildKeyPairOptions(), buildKeyPairOptions(), SHA256, 16}
	for _, fn := range fns {
		fn(&ret)
	}
	return ret
}

// A MemberShard is an encrypted shard that may be unlocked with the
// use of a matching credential + some generic arguments
type MemberShard struct {
	Id   []byte
	Raw  SignedEncryptedShard
	Args []byte
	Type authProtocol
}

// Storage Only.
type MemberAuth struct {
	MemberId uuid.UUID
	Shard    MemberShard
	Args     []byte
}

// A MemberCore contains all the membership details of a particular user.
type MemberCore struct {
	Id         uuid.UUID
	Pub        SignedShard
	SigningKey SignedKeyPair
	InviteKey  SignedKeyPair
	Opts       MemberOptions
}

func newMember(rand io.Reader, creds credential, fns ...func(*MemberOptions)) (MemberCore, MemberShard, error) {
	opts := buildMemberOptions(fns...)

	// generate the user's secret.
	secret, err := genSecret(rand, opts.Secret)
	if err != nil {
		return MemberCore{}, MemberShard{}, errors.WithStack(err)
	}
	defer secret.Destroy()

	// generate the member's encryption seed
	encSeed, err := secret.Hash(opts.Secret.SecretHash)
	if err != nil {
		return MemberCore{}, MemberShard{}, errors.WithStack(err)
	}
	defer cryptoBytes(encSeed).Destroy()

	// generate the member's public shard (will be returned)
	pubShard, err := secret.Shard(rand)
	if err != nil {
		return MemberCore{}, MemberShard{}, errors.WithStack(err)
	}

	// generate the member's private shard (will return via MemberShard)
	privShard, err := secret.Shard(rand)
	if err != nil {
		return MemberCore{}, MemberShard{}, errors.WithStack(err)
	}

	rawSigningKey, err := opts.SigningKey.Algorithm.Gen(rand, opts.SigningKey.Strength)
	if err != nil {
		return MemberCore{}, MemberShard{}, errors.WithStack(err)
	}
	defer rawSigningKey.Destroy()

	rawInviteKey, err := opts.InviteKey.Algorithm.Gen(rand, opts.InviteKey.Strength)
	if err != nil {
		return MemberCore{}, MemberShard{}, errors.WithStack(err)
	}
	defer rawInviteKey.Destroy()

	// for now, just self sign.
	encSigningKey, err := encryptKey(rand, rawSigningKey, rawSigningKey, encSeed, opts.SigningKey)
	if err != nil {
		return MemberCore{}, MemberShard{}, errors.WithStack(err)
	}

	encInviteKey, err := encryptKey(rand, rawSigningKey, rawInviteKey, encSeed, opts.InviteKey)
	if err != nil {
		return MemberCore{}, MemberShard{}, errors.WithStack(err)
	}

	sigPubShard, err := signShard(rand, rawSigningKey, pubShard)
	if err != nil {
		return MemberCore{}, MemberShard{}, errors.WithStack(err)
	}

	encShard, err := creds.Encrypt(rand, rawSigningKey, privShard)
	if err != nil {
		return MemberCore{}, MemberShard{}, errors.WithStack(err)
	}

	id := uuid.NewV1()
	return MemberCore{
		Id:         id,
		Pub:        sigPubShard,
		SigningKey: encSigningKey,
		InviteKey:  encInviteKey,
		Opts:       opts,
	}, encShard, nil
}

func (s MemberCore) secret(rand io.Reader, shard MemberShard, login func(KeyPad) error) (Secret, error) {
	creds, err := enterCreds(login)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	priv, err := creds.Decrypt(shard)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	secret, err := s.Pub.Derive(priv)
	return secret, errors.WithStack(err)
}

func (s MemberCore) encryptionSeed(secret Secret) ([]byte, error) {
	key, err := secret.Hash(s.Opts.SignatureHash)
	return key, errors.WithStack(err)
}

func (s MemberCore) signingKey(secret Secret) (PrivateKey, error) {
	key, err := s.encryptionSeed(secret)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	signingKey, err := s.SigningKey.Decrypt(key)
	return signingKey, errors.WithStack(err)
}

func (s MemberCore) invitationKey(secret Secret) (PrivateKey, error) {
	key, err := s.encryptionSeed(secret)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	inviteKey, err := s.InviteKey.Decrypt(key)
	return inviteKey, errors.WithStack(err)
}

// func (m MemberCore) newCode(rand io.Reader, secret Secret, login func(KeyPad) error) (MemberCode, error) {
// pad, err := enterCreds(login)
// if err != nil {
// return MemberCode{}, errors.WithStack(err)
// }
//
// var auth MemberShard
// switch creds := pad.Creds.(type) {
// default:
// return MemberCode{}, errors.Wrap(UnsupportedLoginError, "Must provide at least one login method")
// case signCreds:
// auth, err = newSignatureShard(rand, secret, creds, m.Opts)
// if err != nil {
// return MemberCode{}, errors.WithStack(err)
// }
// case passCreds:
// auth, err = newPasswordShard(rand, secret, creds, m.Opts)
// if err != nil {
// return MemberCode{}, errors.WithStack(err)
// }
// }
//
// return MemberCode{auth, m.Id}, nil
// }

// Member shard implementations.

func init() {
	gob.Register(SignatureShard{})
	gob.Register(PasswordShard{})
}

type PasswordShard struct {
	MemberID uuid.UUID
	Priv     EncryptedShard // the hashed password is the encryption key
}

func (p *PasswordShard) MemberId() uuid.UUID {
	return p.MemberID
}

func (p *PasswordShard) Decrypt(key []byte) (Shard, error) {
	shard, err := p.Decrypt(key)
	return shard, errors.WithStack(err)
}

func newPasswordShard(random io.Reader, memberId uuid.UUID, secret Secret, key []byte, opts MemberOptions) (PasswordShard, error) {
	shard, err := secret.Shard(random)
	if err != nil {
		return PasswordShard{}, errors.WithStack(err)
	}
	defer shard.Destroy()

	shardEnc, err := encryptShard(random, shard, key)
	if err != nil {
		return PasswordShard{}, errors.WithStack(err)
	}

	return PasswordShard{MemberID: memberId, Priv: shardEnc}, nil
}

// Signature based authenticator.
type SignatureShard struct {
	Nonce []byte
	Hash  Hash
	Priv  SignedEncryptedShard
}

func newSignatureShard(random io.Reader, secret Secret, signer Signer, opts MemberOptions) (SignatureShard, error) {
	nonce, err := genRandomBytes(random, opts.NonceSize)
	if err != nil {
		return SignatureShard{}, errors.WithStack(err)
	}

	// We must use a stable random source for the signature based keys.
	sig, err := signer.Sign(rand.New(rand.NewSource(0)), opts.Secret.SecretHash, nonce)
	if err != nil {
		return SignatureShard{}, errors.WithStack(err)
	}
	defer destroyBytes(sig.Data)

	shard, err := secret.Shard(random)
	if err != nil {
		return SignatureShard{}, errors.WithStack(err)
	}
	defer shard.Destroy()

	shardEnc, err := encryptAndSignShard(random, signer, shard, sig.Data)
	if err != nil {
		return SignatureShard{}, errors.WithStack(err)
	}

	return SignatureShard{Nonce: nonce, Hash: opts.SignatureHash, Priv: shardEnc}, nil
}

func (s SignatureShard) Decrypt(key []byte) (Shard, error) {
	priv, err := s.Priv.Decrypt(key)
	return priv, errors.WithStack(err)
}

func (s SignatureShard) ExtractKey(signer Signer) ([]byte, error) {
	return nil, nil
}
