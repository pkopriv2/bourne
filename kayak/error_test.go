package kayak

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestExtractError_NoCause(t *testing.T) {
	assert.Equal(t, ClosedError, extractError(ClosedError))
}

func TestExtractError_Cause(t *testing.T) {
	assert.Equal(t, ClosedError, extractError(errors.Wrapf(ClosedError, "Error")))
}

func TestExtractError_Cause_Deep(t *testing.T) {
	assert.Equal(t, ClosedError, extractError(errors.Wrap(errors.Wrap(ClosedError, "Error"), "")))
}

func TestExtractError_Cause_String(t *testing.T) {
	assert.Equal(t, ClosedError, extractError(errors.New(ClosedError.Error())))
}
