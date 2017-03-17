package warden

// // A safe box is a local only storage
// type accessCode struct {
// point    symCipherText
// created  time.Time
// accessed time.Time
// }
//
// func genAccessCode(rand io.Reader, line line, size int) (accessCode, error) {
// panic("Not implemented")
// }
//
// func (a accessCode) decryptPoint(salt []byte, code []byte) (point, error) {
// secret := &secret{code}
// defer secret.Destroy()
//
// raw, err := a.point.Decrypt(secret.DeriveKey(salt, a.point.Cipher.KeySize()))
// if err != nil {
// return point{}, errors.WithStack(err)
// }
//
// return parsePointBytes(raw)
// }
//
// func (a accessCode) deriveLine(salt []byte, code []byte, pub point) (line, error) {
// priv, err := a.decryptPoint(salt, code)
// if err != nil {
// return line{}, errors.WithStack(err)
// }
//
// return priv.Derive(pub), nil
// }
//
// type safeContents struct {
// secret     secret
// secretLine line
// codeSize   int
// codes      map[string]accessCode
// salt       []byte
// }
//
// func (s *safeContents) Destroy() {
// s.secret.Destroy()
// s.secretLine.Destroy()
// }
//
// func (s *safeContents) Secret() []byte {
// return s.secret.raw
// }
//
// func (s *safeContents) AccessCodes() []AccessCodeInfo {
// panic("not implemented")
// }
//
// func (s *safeContents) AddAccessCode(name string, code []byte, ttl int) error {
// return nil
// // s.codes[name] = accessCode{s.line.Point()}
// }
//
// func (s *safeContents) DelAccessCode(name string) error {
// panic("not implemented")
// }
//
// type safe struct {
// id       uuid.UUID
// pubPoint point
// pubKey   crypto.PublicKey
// codes    map[string]accessCode
// secret   symCipherText
// }
//
// func (s *safe) Id() uuid.UUID {
// return s.id
// }
//
// func (s *safe) PublicKey() crypto.PublicKey {
// return s.pubKey
// }
//
// func (s *safe) AccessCodes() []AccessCodeInfo {
// panic("not implemented")
// }
//
// func (s *safe) Open(challengeName string, challengeSecret []byte, fn func(s SafeContents)) (err error) {
// panic("not implemented")
// }
