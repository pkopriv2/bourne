package warden

import "crypto"

// The decrypted token.  Internal only and only decrypted by the owner of the token key.
type session struct {
	token authToken
	hash1 []byte
}

func (s *session) PublicKey(cancel <-chan struct{}) crypto.PublicKey {
	panic("not implemented")
}

func (s *session) PrivateKey(cancel <-chan struct{}, fn func(crypto.PrivateKey)) error {
	panic("not implemented")
}

func (s *session) loadHash2Args(cancel <-chan struct{}) error {
	return nil
}

// // An encrypted point (just wraps a simple cipher whose key is a PBKDF2 hash)
// type securePrivateKey symCipherText
//
// // Returns the point, encrypted by using the given pass as the key for the cipher.
// func encryptPrivateKey(rand io.Reader, alg SymCipher, salt []byte, iter int, hash hash.Hash, pass Bytes) (securePoint, error) {
// ct, err := symmetricEncrypt(rand, alg, pass.PBKDF2(salt, iter, alg.KeySize(), hash), point.Bytes())
// if err != nil {
// return securePoint{}, errors.WithStack(err) // Dealing with secure data.  No additional context
// }
//
// return securePoint(ct), nil
// }
//
// // Decrypts the point using the salt, iterations and raw bytes.
// func (e securePrivateKey) Decrypt(salt []byte, iter int, hash hash.Hash, code Bytes) (point, error) {
// raw, err := symCipherText(e).Decrypt(code.PBKDF2(salt, iter, e.Cipher.KeySize(), hash))
// if err != nil {
// return point{}, errors.WithStack(err)
// }
// return parsePointBytes(raw)
// }
//
// // Returns the raw representation of the encrypted point.
// func (e securePoint) Bytes() []byte {
// return symCipherText(e).Bytes()
// }
//
// // An encrypted point (just wraps a simple cipher whose key is a PBKDF2 hash)
// type securePoint symCipherText
//
// // Returns the point, encrypted by using the given pass as the key for the cipher.
// func encryptPoint(rand io.Reader, alg SymCipher, salt []byte, iter int, hash hash.Hash, point point, pass Bytes) (securePoint, error) {
// ct, err := symmetricEncrypt(rand, alg, pass.PBKDF2(salt, iter, alg.KeySize(), hash), point.Bytes())
// if err != nil {
// return securePoint{}, errors.WithStack(err) // Dealing with secure data.  No additional context
// }
//
// return securePoint(ct), nil
// }
//
// // Decrypts the point using the salt, iterations and raw bytes.
// func (e securePoint) Decrypt(salt []byte, iter int, hash hash.Hash, code Bytes) (point, error) {
// raw, err := symCipherText(e).Decrypt(code.PBKDF2(salt, iter, e.Cipher.KeySize(), hash))
// if err != nil {
// return point{}, errors.WithStack(err)
// }
// return parsePointBytes(raw)
// }
//
// // Returns the raw representation of the encrypted point.
// func (e securePoint) Bytes() []byte {
// return symCipherText(e).Bytes()
// }
