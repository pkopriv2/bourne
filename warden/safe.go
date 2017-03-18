package warden

import (
	"io"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/scribe"
	"github.com/pkopriv2/bourne/stash"
	uuid "github.com/satori/go.uuid"
)

var (
	CodeExpiredError      = errors.New("Warden:CodeExpired")
	CodeUnknownError      = errors.New("Warden:CodeUnknownError")
	StorageInvariateError = errors.New("Warden:StorageInvariateError")
)

// type SafeStore interface {
// GetSafe(id uuid.UUID) (StoredSafe, error)
// NewSafe(id uuid.UUID, secret []byte) (StoredSafe, error)
// }
//
// type StoredCodeInfo interface {
// Name() string
// Expire() int
// Used() (int, error)
// }
//
// type StoredSafe interface {
// Id() uuid.UUID
//
// AllCodes() ([]StoredCodeInfo, error)
//
// AddCode(secret []byte, name string, code []byte) error
// DelCode(secret []byte, name string, code []byte) error
//
// Access(codeName string, code []byte) (secret []byte, info StoredCodeInfo, err error)
// Rotate(codeName string, cur []byte, new []byte, ttl int) error
// }

// Bolt implementation of kayak log store.
var (
	codeBucket       = []byte("warden.access.code")
	codePointBucket  = []byte("warden.access.code.point")
	codeUsesBucket   = []byte("warden.access.code.usage")
	safeBucket       = []byte("warden.safe")
	safeCodeBucket   = []byte("warden.safe.codes")
	safeSecretBucket = []byte("warden.safe.secret")
)

func initBoltBuckets(db *bolt.DB) (err error) {
	return db.Update(func(tx *bolt.Tx) error {
		var e error
		_, e = tx.CreateBucketIfNotExists(codeBucket)
		err = common.Or(err, e)
		_, e = tx.CreateBucketIfNotExists(codePointBucket)
		return common.Or(err, e)
	})
}

var boltSafeVersion = 1

// An encrypted point (just wraps a simple cipher whose key is a PBKDF2 hash)
type pointDat symCipherText

func encryptPoint(rand io.Reader, alg SymCipher, salt []byte, iter int, point point, pass Bytes) (pointDat, error) {
	ct, err := symmetricEncrypt(rand, alg, pass.PBKDF2(salt, iter, alg.KeySize()), point.Bytes())
	if err != nil {
		return pointDat{}, errors.WithStack(err) // Dealing with secure data.  No additional context
	}

	return pointDat(ct), nil
}

func (e pointDat) Decrypt(salt []byte, iter int, code Bytes) (point, error) {
	raw, err := symCipherText(e).Decrypt(code.PBKDF2(salt, iter, e.Cipher.KeySize()))
	if err != nil {
		return point{}, errors.WithStack(err)
	}
	return parsePointBytes(raw)
}

func (e pointDat) Bytes() []byte {
	return symCipherText(e).Bytes()
}

// Access code options
type codeOptions struct {
	alg    SymCipher
	expire int
	salt   int
	point  int
	iter   int
}

func (o *codeOptions) WithCipher(alg SymCipher) {
	o.alg = alg
}

func (o *codeOptions) WithExpire(ttl int) {
	o.expire = ttl
}

func (o *codeOptions) WithSaltSize(size int) {
	o.salt = size
}

func (o *codeOptions) WithPointSize(size int) {
	o.point = size
}

func (o *codeOptions) WithKeyIter(iter int) {
	o.iter = iter
}

func defaultCodeOptions() *codeOptions {
	return &codeOptions{AES_128_GCM, 256, 8, 8, 4096}
}

func buildCodeOptions(fns ...func(*codeOptions)) *codeOptions {
	o := defaultCodeOptions()
	for _, fn := range fns {
		fn(o)
	}
	return o
}

type codeDat struct {
	id     uuid.UUID
	name   string
	salt   []byte
	iter   int
	expire int
}

func loadBoltCode(tx *bolt.Tx, id uuid.UUID) (codeDat, bool, error) {
	raw := tx.Bucket(codeBucket).Get(stash.UUID(id))
	if raw == nil {
		return codeDat{}, false, nil
	}

	dat, err := parseCodeBytes(raw)
	if err != nil {
		return codeDat{}, false, errors.Wrapf(err, "Unable to load code [%v]", id)
	}

	return dat, true, nil
}

func initBoltCode(tx *bolt.Tx, rand io.Reader, name string, line line, code []byte, fns ...func(*codeOptions)) (codeDat, error) {
	id := uuid.NewV1()

	opts := buildCodeOptions(fns...)

	salt, err := generateRandomBytes(rand, opts.salt)
	if err != nil {
		return codeDat{}, errors.Wrap(err, "Unable to initialize access code")
	}

	point, err := randomPoint(rand, line, opts.point)
	if err != nil {
		return codeDat{}, errors.Wrap(err, "Unable to initialize access code")
	}
	defer point.Destroy()

	enc, err := encryptPoint(rand, opts.alg, salt, opts.iter, point, code)
	if err != nil {
		return codeDat{}, errors.Wrap(err, "Unable to initialize access code")
	}

	if err := tx.Bucket(codeUsesBucket).Put(stash.UUID(id), stash.Int(0)); err != nil {
		return codeDat{}, errors.Wrap(err, "Unable to initialize access code")
	}

	if err := tx.Bucket(codePointBucket).Put(stash.UUID(id), enc.Bytes()); err != nil {
		return codeDat{}, errors.Wrap(err, "Unable to initialize access code")
	}

	if err := tx.Bucket(codeBucket).Put(stash.UUID(id), codeDat{id, name, salt, opts.iter, opts.expire}.Bytes()); err != nil {
		return codeDat{}, errors.Wrap(err, "Unable to initialize access code")
	}

	return codeDat{}, nil
}

func (b codeDat) Delete(tx *bolt.Tx) error {
	key := stash.UUID(b.id)

	var err error
	err = tx.Bucket(codeBucket).Delete(key)
	err = common.Or(err, tx.Bucket(codeUsesBucket).Delete(key))
	err = common.Or(err, tx.Bucket(codePointBucket).Delete(key))
	return err
}

func (b codeDat) Expired(tx *bolt.Tx) (bool, error) {
	num, err := b.GetUses(tx)
	if err != nil {
		return false, err
	}

	return num >= b.expire, nil
}

func (b codeDat) Access(tx *bolt.Tx, code []byte) (point, error) {
	exp, err := b.Expired(tx)
	if err != nil {
		return point{}, err
	}

	if exp {
		return point{}, errors.Wrapf(CodeExpiredError, "Code is expired [%v]", b.id)
	}

	enc, err := b.LoadPoint(tx)
	if err != nil {
		return point{}, err
	}

	defer b.IncUses(tx)
	return enc.Decrypt(b.salt, b.iter, code)
}

func (b codeDat) GetUses(tx *bolt.Tx) (int, error) {
	raw := tx.Bucket(codePointBucket).Get(stash.UUID(b.id))
	if raw == nil {
		return 0, errors.Wrapf(StorageInvariateError, "No usage data for code [%v]", b.id)
	}

	val, err := stash.ParseInt(raw)
	if err != nil {
		return 0, errors.Wrapf(err, "Error parsing access code usage data [%v]", b.id)
	}

	return val, nil
}

func (b codeDat) SetUses(tx *bolt.Tx, val int) error {
	err := tx.Bucket(codeUsesBucket).Put(stash.UUID(b.id), stash.Int(val))
	if err != nil {
		return errors.Wrapf(err, "Unable to set usage for code [%v]", b.id)
	}
	return nil
}

func (b codeDat) IncUses(tx *bolt.Tx) (int, error) {
	num, err := b.GetUses(tx)
	if err != nil {
		return 0, err
	}

	if err := b.SetUses(tx, num+1); err != nil {
		return 0, err
	}

	return num + 1, nil
}

func (b codeDat) LoadPoint(tx *bolt.Tx) (pointDat, error) {
	raw := tx.Bucket(codePointBucket).Get(stash.UUID(b.id))
	if raw == nil {
		return pointDat{}, errors.Wrapf(StorageInvariateError, "No curve data for code [%v]", b.id)
	}

	dat, err := parseSymCipherTextBytes(raw)
	if err != nil {
		return pointDat{}, errors.Wrapf(err, "Error parsing access code curve data [%v]", b.id)
	}

	return pointDat(dat), nil
}

func (c codeDat) Bytes() []byte {
	return scribe.Write(c).Bytes()
}

func (c codeDat) Write(w scribe.Writer) {
	w.WriteInt("version", boltSafeVersion)
	w.WriteUUID("id", c.id)
	w.WriteString("name", c.name)
	w.WriteBytes("salt", c.salt)
	w.WriteInt("expire", c.expire)
}

func readCode(r scribe.Reader) (c codeDat, err error) {
	err = common.Or(err, r.ReadUUID("id", &c.id))
	err = common.Or(err, r.ReadString("name", &c.name))
	err = common.Or(err, r.ReadBytes("salt", &c.salt))
	err = common.Or(err, r.ReadInt("expire", &c.expire))
	return
}

func parseCodeBytes(raw []byte) (codeDat, error) {
	msg, err := scribe.Parse(raw)
	if err != nil {
		return codeDat{}, err
	}
	return readCode(msg)
}

// the core safe data structure.
type safeDat struct {
	id      uuid.UUID
	cipher  SymCipher
	pub     point
	created int
	salt    []byte
	iter    int
}

func loadBoltSafe(tx *bolt.Tx, id uuid.UUID) (safeDat, bool, error) {
	raw := tx.Bucket(safeBucket).Get(stash.UUID(id))
	if raw == nil {
		return safeDat{}, false, nil
	}

	dat, err := parseSafeBytes(raw)
	if err != nil {
		return safeDat{}, false, errors.Wrapf(err, "Unable to parse safe data [%v]", id)
	}

	return dat, true, nil
}

func (s safeDat) SetCodeId(tx *bolt.Tx, name string, id uuid.UUID) error {
	return tx.Bucket(safeCodeBucket).Put(stash.UUID(s.id).ChildString(name), id.Bytes())
}

func (s safeDat) GetCodeId(tx *bolt.Tx, name string) (uuid.UUID, bool, error) {
	raw := tx.Bucket(safeCodeBucket).Get(stash.UUID(s.id).ChildString(name))
	if raw == nil {
		return uuid.UUID{}, false, nil
	}

	id, err := uuid.FromBytes(raw)
	if err != nil {
		return uuid.UUID{}, false, errors.Wrapf(err, "Unable to parse id for access code [%v@%v]", name, s.id)
	}

	return id, true, nil
}

func (s safeDat) GetCode(tx *bolt.Tx, name string) (codeDat, bool, error) {
	id, ok, err := s.GetCodeId(tx, name)
	if err != nil || !ok {
		return codeDat{}, false, err
	}

	return loadBoltCode(tx, id)
}

func (s safeDat) PutCode(tx *bolt.Tx, rand io.Reader, line line, name string, code []byte, opts ...func(*codeOptions)) (codeDat, error) {
	dat, err := initBoltCode(tx, rand, name, line, code, opts...)
	if err != nil {
		return codeDat{}, errors.Wrapf(err, "Error generating access code [%v@%v]", name, s.id)
	}

	cur, ok, err := s.GetCode(tx, name)
	if err != nil {
		return codeDat{}, errors.Wrapf(err, "Error retrieving access code [%v@%v]", name, s.id)
	}

	defer func() {
		if ok {
			cur.Delete(tx)
		}
	}()

	return dat, s.SetCodeId(tx, name, dat.id)
}

func (s safeDat) Open(tx *bolt.Tx, name string, code []byte) ([]byte, line, error) {
	ac, ok, err := s.GetCode(tx, name)
	if err != nil {
		return nil, line{}, errors.Wrapf(err, "Error retrieving access code [%v@%v]", name, s.id)
	}

	if !ok {
		return nil, line{}, errors.Wrapf(CodeUnknownError, "No such access code [%v@%v]", name, s.id)
	}

	priv, err := ac.Access(tx, code)
	if err != nil {
		return nil, line{}, errors.Wrapf(err, "Error using access code [%v@%v]", name, s.id)
	}
	defer priv.Destroy()

	curve, err := s.pub.Derive(priv)
	if err != nil {
		return nil, line{}, errors.Wrapf(err, "Error opening safe with access code [%v@%v]", name, s.id)
	}

	cipherText, err := s.LoadSecret(tx)
	if err != nil {
		curve.Destroy()
		return nil, line{}, errors.Wrapf(err, "Error opening safe with access code [%v@%v]", name, s.id)
	}

	plain, err := cipherText.Decrypt(Bytes(curve.Bytes()).PBKDF2(s.salt, s.iter, cipherText.Cipher.KeySize()))
	if err != nil {
		curve.Destroy()
		return nil, line{}, errors.Wrapf(err, "Error opening safe with access code [%v@%v]", name, s.id)
	}

	return plain, curve, nil
}

func (s safeDat) LoadSecret(tx *bolt.Tx) (symCipherText, error) {
	raw := tx.Bucket(safeSecretBucket).Get(stash.UUID(s.id))
	if raw == nil {
		return symCipherText{}, errors.Wrapf(StorageInvariateError, "No secret data for safe [%v]", s.id)
	}

	dat, err := parseSymCipherTextBytes(raw)
	if err != nil {
		return symCipherText{}, errors.Wrapf(err, "Unable to parse cipher text for safe [%v]", s.id)
	}

	return dat, nil
}

func (s safeDat) Bytes() []byte {
	return scribe.Write(s).Bytes()
}

func (s safeDat) Write(w scribe.Writer) {
	w.WriteInt("version", boltSafeVersion)
	w.WriteInt("cipher", int(s.cipher))
	w.WriteBytes("pub", s.pub.Bytes())
	w.WriteInt("created", s.created)
}

func parseSafeBytes(raw []byte) (safeDat, error) {
	msg, err := scribe.Parse(raw)
	if err != nil {
		return safeDat{}, err
	}
	return readSafe(msg)
}

func readSafe(r scribe.Reader) (c safeDat, err error) {
	var pubRaw []byte
	err = r.ReadInt("cipher", (*int)(&c.cipher))
	err = common.Or(err, r.ReadBytes("pub", &pubRaw))
	err = common.Or(err, r.ReadInt("created", &c.created))
	if err != nil {
		return
	}

	point, err := parsePointBytes(pubRaw)
	if err != nil {
		return
	}

	c.pub = point
	return
}
