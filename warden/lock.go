package warden

import (
	"crypto"
	"io"
	"time"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/scribe"
	"github.com/pkopriv2/bourne/stash"
	uuid "github.com/satori/go.uuid"
)

// Version of the encoding format. (only increased for non-compatible backwards changes)
var lockEncodingVersion = 1

// Basic errors
var (
	CodeExpiredError      = errors.New("Warden:CodeExpired")
	CodeUnknownError      = errors.New("Warden:CodeUnknownError")
	StorageInvariateError = errors.New("Warden:StorageInvariateError")
)

// Bolt buckets
var (
	codeBucket       = []byte("access.code")
	codePointBucket  = []byte("access.code.point")
	codeUsesBucket   = []byte("access.code.usage")
	lockBucket       = []byte("lock")
	lockCodeBucket   = []byte("lock.codes")
	lockSecretBucket = []byte("lock.secret")
)

func initBoltBuckets(db *bolt.DB) (err error) {
	return db.Update(func(tx *bolt.Tx) error {
		var e error
		_, e = tx.CreateBucketIfNotExists(codeBucket)
		err = common.Or(err, e)
		_, e = tx.CreateBucketIfNotExists(codePointBucket)
		err = common.Or(err, e)
		_, e = tx.CreateBucketIfNotExists(codeUsesBucket)
		err = common.Or(err, e)
		_, e = tx.CreateBucketIfNotExists(lockBucket)
		err = common.Or(err, e)
		_, e = tx.CreateBucketIfNotExists(lockCodeBucket)
		err = common.Or(err, e)
		_, e = tx.CreateBucketIfNotExists(lockSecretBucket)
		return common.Or(err, e)
	})
}

// Access code options.
type codeOptions struct {
	alg    SymCipher
	expire time.Duration
	salt   int
	point  int
	iter   int
}

// Sets the access code cipher
func (o *codeOptions) WithCipher(alg SymCipher) {
	o.alg = alg
}

// Sets the access code expiration (number of times to be used)
func (o *codeOptions) WithExpiration(ttl time.Duration) {
	o.expire = ttl
}

// Sets the salt size of the access code (typically > 8).  This is the server side equivalent of a password
func (o *codeOptions) WithSaltSize(size int) {
	o.salt = size
}

// Sets the curve size, which specifies the number of bytes to use when determining random points on the curve.
func (o *codeOptions) WithPointSize(size int) {
	o.point = size
}

// Sets the number of iterations when deriving PBKDF2 based keys.
// TODO: It may make more sense to remove this option altogether to further decouple the knowledge
// required to reverse a hash.
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

type boldCode struct {
	db  *bolt.DB
	dat codeDat
}

func (b *boldCode) Name() string {
	panic("not implemented")
}

func (b *boldCode) access(pub crypto.PublicKey, pass []byte) (Token, error) {
	panic("not implemented")
}

type boltLock struct {
}

func (b *boltLock) Codes() []Code {
	panic("not implemented")
}

func (b *boltLock) UseCode(code string, pass []byte) (Token, error) {
	panic("not implemented")
}

func (b *boltLock) ChgCode(code string, cur []byte, new []byte) (Token, error) {
	panic("not implemented")
}

func (b *boltLock) AddCode(token Token, code string, pass []byte, expire time.Duration) error {
	panic("not implemented")
}

func (b *boltLock) DelCode(token Token, code string, pass []byte, expire time.Duration) error {
	panic("not implemented")
}

func (b *boltLock) open(token Token) ([]byte, error) {
	panic("not implemented")
}

// An encrypted point (just wraps a simple cipher whose key is a PBKDF2 hash)
type securePointDat symCipherText

// Returns the point, encrypted by using the given pass as the key for the cipher.
func encryptPoint(rand io.Reader, alg SymCipher, salt []byte, iter int, point point, pass Bytes) (securePointDat, error) {
	ct, err := symmetricEncrypt(rand, alg, pass.PBKDF2(salt, iter, alg.KeySize()), point.Bytes())
	if err != nil {
		return securePointDat{}, errors.WithStack(err) // Dealing with secure data.  No additional context
	}

	return securePointDat(ct), nil
}

// Decryptes the point using the salt, iterations and raw bytes.
func (e securePointDat) Decrypt(salt []byte, iter int, code Bytes) (point, error) {
	raw, err := symCipherText(e).Decrypt(code.PBKDF2(salt, iter, e.Cipher.KeySize()))
	if err != nil {
		return point{}, errors.WithStack(err)
	}
	return parsePointBytes(raw)
}

// Returns the raw representation of the encrypted point.
func (e securePointDat) Bytes() []byte {
	return symCipherText(e).Bytes()
}

// The raw access code data as it is durably stored.  The fields of this object are immutable
// over the entire lifetime of the access code.
//
// An access code protects a point on an unknown curve.  The encryption key of the point is a pbkdf2
// derived key based off the accesscode. The point is then combined with other points to form
// a curve that may only be derived as a coordinated act.  This is basically shamir's distribution
// algorithm in its most basic form.
//
type codeDat struct {
	id   uuid.UUID
	name string
	salt []byte
	iter int
}

// Loads the access code from db.  Returns true if found.
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

// Creates and stores a new access code.
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

	// if err := tx.Bucket(codeBucket).Put(stash.UUID(id), codeDat{id, name, salt, opts.iter, int(opts.expire)}.Bytes()); err != nil {
	// return codeDat{}, errors.Wrap(err, "Unable to initialize access code")
	// }

	return codeDat{}, nil
}

// Permanently deletes the access code.  This cannot be undone.
func (b codeDat) Delete(tx *bolt.Tx) error {
	key := stash.UUID(b.id)

	var err error
	err = tx.Bucket(codeBucket).Delete(key)
	err = common.Or(err, tx.Bucket(codeUsesBucket).Delete(key))
	err = common.Or(err, tx.Bucket(codePointBucket).Delete(key))
	return err
}

// Determines whether the access code has expired.  Once expired, a code may only be rotated.
func (b codeDat) Expired(tx *bolt.Tx) (bool, error) {
	// num, err := b.Uses(tx)
	// if err != nil {
	// return false, err
	// }
	//
	// return num >= b.expire, nil
	return false, nil
}

// Decrypts and returns the point protected by the access code.  This method has the side-effect
// of incrementing the internal use counter.  Once the code has been used a specific number of
// times, it must be rotated.
func (b codeDat) Access(tx *bolt.Tx, code []byte) (point, error) {
	exp, err := b.Expired(tx)
	if err != nil {
		return point{}, err
	}

	if exp {
		return point{}, errors.Wrapf(CodeExpiredError, "Code is expired [%v]", b.id)
	}

	enc, err := b.SecurePoint(tx)
	if err != nil {
		return point{}, err
	}

	defer b.IncUses(tx)
	return enc.Decrypt(b.salt, b.iter, code)
}

// Returns the current number of uses for the access code.
func (b codeDat) Uses(tx *bolt.Tx) (int, error) {
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

// Increments the usage count.
func (b codeDat) IncUses(tx *bolt.Tx) (int, error) {
	num, err := b.Uses(tx)
	if err != nil {
		return 0, err
	}

	new := num + 1

	if err := tx.Bucket(codeUsesBucket).Put(stash.UUID(b.id), stash.Int(new)); err != nil {
		return 0, errors.Wrapf(err, "Unable to set usage for code [%v]", b.id)
	}

	return new, nil
}

// Loads the secure point.  This is encrypted and is safe to distribute.
func (b codeDat) SecurePoint(tx *bolt.Tx) (securePointDat, error) {
	raw := tx.Bucket(codePointBucket).Get(stash.UUID(b.id))
	if raw == nil {
		return securePointDat{}, errors.Wrapf(StorageInvariateError, "No curve data for code [%v]", b.id)
	}

	dat, err := parseSymCipherTextBytes(raw)
	if err != nil {
		return securePointDat{}, errors.Wrapf(err, "Error parsing access code curve data [%v]", b.id)
	}

	return securePointDat(dat), nil
}

// Returns a raw byte representation of the access code
func (c codeDat) Bytes() []byte {
	return scribe.Write(c).Bytes()
}

// Serializes the access code onto a writer.
func (c codeDat) Write(w scribe.Writer) {
	w.WriteInt("version", lockEncodingVersion)
	w.WriteUUID("id", c.id)
	w.WriteString("name", c.name)
	w.WriteBytes("salt", c.salt)
	// w.WriteInt("expire", c.expire)
}

// Parses an access code from a reader.
func readCode(r scribe.Reader) (c codeDat, err error) {
	err = common.Or(err, r.ReadUUID("id", &c.id))
	err = common.Or(err, r.ReadString("name", &c.name))
	err = common.Or(err, r.ReadBytes("salt", &c.salt))
	// err = common.Or(err, r.ReadInt("expire", &c.expire))
	return
}

// Parses an access code from a raw byte representation.
func parseCodeBytes(raw []byte) (codeDat, error) {
	msg, err := scribe.Parse(raw)
	if err != nil {
		return codeDat{}, err
	}
	return readCode(msg)
}

// The central lock data structure.  The lock
type lockDat struct {
	id      uuid.UUID
	cipher  SymCipher
	pub     point
	created int
	salt    []byte
	iter    int
}

// Loads a lock from the db, returning the data and true if found, false otherwise.
func loadBoltLock(tx *bolt.Tx, id uuid.UUID) (lockDat, bool, error) {
	raw := tx.Bucket(lockBucket).Get(stash.UUID(id))
	if raw == nil {
		return lockDat{}, false, nil
	}

	dat, err := parseLockBytes(raw)
	if err != nil {
		return lockDat{}, false, errors.Wrapf(err, "Unable to parse lock data [%v]", id)
	}

	return dat, true, nil
}

func (s lockDat) SetCodeId(tx *bolt.Tx, name string, id uuid.UUID) error {
	return tx.Bucket(lockCodeBucket).Put(stash.UUID(s.id).ChildString(name), id.Bytes())
}

func (s lockDat) CodeId(tx *bolt.Tx, name string) (uuid.UUID, bool, error) {
	raw := tx.Bucket(lockCodeBucket).Get(stash.UUID(s.id).ChildString(name))
	if raw == nil {
		return uuid.UUID{}, false, nil
	}

	id, err := uuid.FromBytes(raw)
	if err != nil {
		return uuid.UUID{}, false, errors.Wrapf(err, "Unable to parse id for access code [%v@%v]", name, s.id)
	}

	return id, true, nil
}

func (s lockDat) GetCode(tx *bolt.Tx, name string) (codeDat, bool, error) {
	id, ok, err := s.CodeId(tx, name)
	if err != nil || !ok {
		return codeDat{}, false, err
	}

	return loadBoltCode(tx, id)
}

func (s lockDat) AddCode(tx *bolt.Tx, rand io.Reader, line line, name string, code []byte, opts ...func(*codeOptions)) (codeDat, error) {
	dat, err := initBoltCode(tx, rand, name, line, code, opts...)
	if err != nil {
		return codeDat{}, errors.Wrapf(err, "Error generating access code [%v@%v]", name, s.id)
	}

	cur, exists, err := s.GetCode(tx, name)
	if err != nil {
		return codeDat{}, errors.Wrapf(err, "Error retrieving access code [%v@%v]", name, s.id)
	}

	if err := s.SetCodeId(tx, name, dat.id); err != nil {
		defer dat.Delete(tx)
		return codeDat{}, errors.Wrapf(err, "Error setting access code id [%v@%v]", name, s.id)
	}

	defer func() {
		if exists {
			cur.Delete(tx)
		}
	}()

	return dat, nil
}

func (s lockDat) DelCode(tx *bolt.Tx, name string) error {
	code, ok, err := s.GetCode(tx, name)
	if err != nil {
		return errors.Wrapf(err, "Error retrieving access code [%v@%v]", name, s.id)
	}

	if !ok {
		return nil
	}

	if err := code.Delete(tx); err != nil {
		return errors.Wrapf(err, "Error deleting access code [%v@%v]", name, s.id)
	}

	return nil
}

func (s lockDat) Open(tx *bolt.Tx, code string, pass []byte) ([]byte, line, error) {
	ac, ok, err := s.GetCode(tx, code)
	if err != nil {
		return nil, line{}, errors.Wrapf(err, "Error retrieving access code [%v@%v]", code, s.id)
	}

	if !ok {
		return nil, line{}, errors.Wrapf(CodeUnknownError, "No such access code [%v@%v]", code, s.id)
	}

	priv, err := ac.Access(tx, pass)
	if err != nil {
		return nil, line{}, errors.Wrapf(err, "Error using access code [%v@%v]", code, s.id)
	}
	defer priv.Destroy()

	curve, err := s.pub.Derive(priv)
	if err != nil {
		return nil, line{}, errors.Wrapf(err, "Error opening lock with access code [%v@%v]", code, s.id)
	}

	cipherText, err := s.LoadSecret(tx)
	if err != nil {
		curve.Destroy()
		return nil, line{}, errors.Wrapf(err, "Error opening lock with access code [%v@%v]", code, s.id)
	}

	plain, err := cipherText.Decrypt(Bytes(curve.Bytes()).PBKDF2(s.salt, s.iter, cipherText.Cipher.KeySize()))
	if err != nil {
		curve.Destroy()
		return nil, line{}, errors.Wrapf(err, "Error opening lock with access code [%v@%v]", code, s.id)
	}

	return plain, curve, nil
}

func (s lockDat) LoadSecret(tx *bolt.Tx) (symCipherText, error) {
	raw := tx.Bucket(lockSecretBucket).Get(stash.UUID(s.id))
	if raw == nil {
		return symCipherText{}, errors.Wrapf(StorageInvariateError, "No secret data for lock [%v]", s.id)
	}

	dat, err := parseSymCipherTextBytes(raw)
	if err != nil {
		return symCipherText{}, errors.Wrapf(err, "Unable to parse cipher text for lock [%v]", s.id)
	}

	return dat, nil
}

func (s lockDat) Bytes() []byte {
	return scribe.Write(s).Bytes()
}

func (s lockDat) Write(w scribe.Writer) {
	w.WriteInt("version", lockEncodingVersion)
	w.WriteInt("cipher", int(s.cipher))
	w.WriteBytes("pub", s.pub.Bytes())
	w.WriteInt("created", s.created)
}

func parseLockBytes(raw []byte) (lockDat, error) {
	msg, err := scribe.Parse(raw)
	if err != nil {
		return lockDat{}, err
	}
	return readLock(msg)
}

func readLock(r scribe.Reader) (c lockDat, err error) {
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
