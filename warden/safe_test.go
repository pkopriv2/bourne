package warden

import "testing"

func TestSafe_AccessCode_Gen(t *testing.T) {
	// source := rand.New(rand.NewSource(0))
	//
	// line, err := randomLine(source, 16)
	// assert.Nil(t, err)
	//
	// code, err := genAccessCode(AES_128_GCM, source, []byte{}, 4096, line, []byte("password"))
	// assert.Nil(t, err)
	//
	// derived, err := code.deriveLine([]byte{}, []byte("password"), line.Point(big.NewInt(10)))
	// assert.Nil(t, err)
	// assert.Equal(t, line, derived)
	//
	// fmt.Println("BEFORE: ", line)
	// fmt.Println("AFTER: ", derived)
	// fmt.Println("ACCESSCODE: ", code)
}
