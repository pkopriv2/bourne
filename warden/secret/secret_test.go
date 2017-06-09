package secret

// func TestOracle(t *testing.T) {
// priv, e := krypto.GenRsaKey(rand.Reader, 1024)
// assert.Nil(t, e)
//
// secret, e := genSecret(rand.Reader)
// assert.Nil(t, e)
//
// shard, e := secret.Shard(rand.Reader)
// assert.Nil(t, e)
//
// pub, e := secret.Shard(rand.Reader)
// assert.Nil(t, e)
//
// // t.Run("GenerateAndUnlock", func(t *testing.T) {
// // k, e := encryptAndSignShard(rand.Reader, priv, shard, []byte("pass"))
// // assert.Nil(t, e)
// //
// // sh, e := k.Decrypt([]byte("pass"))
// // assert.Nil(t, e)
// //
// // act, e := pub.Derive(sh)
// // assert.Nil(t, e)
// // assert.Equal(t, secret, act)
// // })
// }
