package warden

// func TestSubscriber(t *testing.T) {
// ctx := common.NewContext(common.NewEmptyConfig())
// log := ctx.Logger()
//
// sub, key, e := NewSubscriber(rand.Reader, []byte("pass"))
// assert.Nil(t, e)
//
// log.Info("Sub: %+v", sub)
// log.Info("Key: %+v", key)
//
// line, e := sub.Oracle.Unlock(key.oracleKey, []byte("pass"))
// assert.Nil(t, e)
//
// _, e = sub.Sign.Decrypt(line.Bytes())
// assert.Nil(t, e)
//
// _, e = sub.Invite.Decrypt(line.Bytes())
// assert.Nil(t, e)
// }
