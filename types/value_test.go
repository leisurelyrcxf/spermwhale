package types

import (
	"testing"

	"github.com/leisurelyrcxf/spermwhale/consts"

	testifyassert "github.com/stretchr/testify/assert"
)

func TestValue(t *testing.T) {
	assert := testifyassert.New(t)

	v := NewValue([]byte("123"), 3)
	assert.True(v.IsDirty())

	{
		v := v.WithCommitted()
		assert.True(!v.IsDirty() && v.IsCommitted())
	}

	{
		v := v
		v.VFlag |= consts.ValueMetaBitMaskAborted
		assert.True(v.IsDirty() && v.IsAborted())
	}
}

func TestMeta_UpdateTxnState(t *testing.T) {
	assert := testifyassert.New(t)
	{
		m := Meta{InternalVersion: 1, Version: 111, VFlag: consts.ValueMetaBitMaskHasWriteIntent | consts.ValueMetaBitMaskPreventedFutureWrite}
		m.UpdateTxnState(TxnStateCommittedCleared)
		assert.True(m.IsCommitted())
		assert.True(m.IsTerminated())
		assert.True(m.IsFutureWritePrevented())
		assert.Equal(uint64(111), m.Version)
		assert.Equal(TxnInternalVersion(1), m.InternalVersion)
		assert.True(!m.IsDirty())
		assert.True(!m.IsAborted())
		assert.True(m.IsCleared())
		assert.True(m.IsCommittedCleared())
		assert.False(m.IsRollbackedCleared())
	}

	{
		m := Meta{InternalVersion: 1, Version: 111, VFlag: consts.ValueMetaBitMaskHasWriteIntent | consts.ValueMetaBitMaskPreventedFutureWrite}
		m.updateKeyState(KeyStateCommittedCleared)
		assert.True(m.IsCommitted())
		assert.True(m.IsTerminated())
		assert.True(m.IsFutureWritePrevented())
		assert.Equal(uint64(111), m.Version)
		assert.Equal(TxnInternalVersion(1), m.InternalVersion)
		assert.True(!m.IsDirty())
		assert.True(!m.IsAborted())
		assert.True(m.IsCleared())
		assert.True(m.IsCommittedCleared())
		assert.False(m.IsRollbackedCleared())
	}
	{
		m := Meta{InternalVersion: 1, Version: 111, VFlag: consts.ValueMetaBitMaskHasWriteIntent | consts.ValueMetaBitMaskPreventedFutureWrite}
		m.UpdateTxnState(TxnStateCommitted)
		assert.True(m.IsCommitted())
		assert.True(m.IsTerminated())
		assert.True(m.IsFutureWritePrevented())
		assert.Equal(uint64(111), m.Version)
		assert.Equal(TxnInternalVersion(1), m.InternalVersion)
		assert.True(!m.IsDirty())
		assert.True(!m.IsAborted())
		assert.True(!m.IsCleared())
		assert.False(m.IsCommittedCleared())
		assert.False(m.IsRollbackedCleared())
	}

	{
		m := Meta{InternalVersion: 1, Version: 111, VFlag: consts.ValueMetaBitMaskHasWriteIntent | consts.ValueMetaBitMaskPreventedFutureWrite}
		m.updateKeyState(KeyStateCommitted)
		assert.True(m.IsCommitted())
		assert.True(m.IsTerminated())
		assert.True(m.IsFutureWritePrevented())
		assert.Equal(uint64(111), m.Version)
		assert.Equal(TxnInternalVersion(1), m.InternalVersion)
		assert.True(!m.IsDirty())
		assert.True(!m.IsAborted())
		assert.True(!m.IsCleared())
		assert.False(m.IsCommittedCleared())
		assert.False(m.IsRollbackedCleared())
	}

	{
		m := Meta{InternalVersion: 1, Version: 111, VFlag: consts.ValueMetaBitMaskHasWriteIntent | consts.ValueMetaBitMaskPreventedFutureWrite}
		m.UpdateTxnState(TxnStateRollbackedCleared)
		assert.True(!m.IsCommitted())
		assert.True(m.IsTerminated())
		assert.True(m.IsFutureWritePrevented())
		assert.Equal(uint64(111), m.Version)
		assert.Equal(TxnInternalVersion(1), m.InternalVersion)
		assert.True(m.IsDirty())
		assert.True(m.IsAborted())
		assert.True(m.IsCleared())
		assert.False(m.IsCommittedCleared())
		assert.True(m.IsRollbackedCleared())
	}

	{
		m := Meta{InternalVersion: 1, Version: 111, VFlag: consts.ValueMetaBitMaskHasWriteIntent | consts.ValueMetaBitMaskPreventedFutureWrite}
		m.updateKeyState(KeyStateRollbackedCleared)
		assert.True(!m.IsCommitted())
		assert.True(m.IsTerminated())
		assert.True(m.IsFutureWritePrevented())
		assert.Equal(uint64(111), m.Version)
		assert.Equal(TxnInternalVersion(1), m.InternalVersion)
		assert.True(m.IsDirty())
		assert.True(m.IsAborted())
		assert.True(m.IsCleared())
		assert.False(m.IsCommittedCleared())
		assert.True(m.IsRollbackedCleared())
	}

	{
		m := Meta{InternalVersion: 1, Version: 111, VFlag: consts.ValueMetaBitMaskHasWriteIntent | consts.ValueMetaBitMaskPreventedFutureWrite}
		m.UpdateTxnState(TxnStateRollbacking)
		assert.True(!m.IsCommitted())
		assert.True(m.IsTerminated())
		assert.True(m.IsFutureWritePrevented())
		assert.Equal(uint64(111), m.Version)
		assert.Equal(TxnInternalVersion(1), m.InternalVersion)
		assert.True(m.IsDirty())
		assert.True(m.IsAborted())
		assert.True(!m.IsCleared())
		assert.False(m.IsCommittedCleared())
		assert.False(m.IsRollbackedCleared())
	}

	{
		m := Meta{InternalVersion: 1, Version: 111, VFlag: consts.ValueMetaBitMaskHasWriteIntent | consts.ValueMetaBitMaskPreventedFutureWrite}
		m.updateKeyState(KeyStateRollbacking)
		assert.True(!m.IsCommitted())
		assert.True(m.IsTerminated())
		assert.True(m.IsFutureWritePrevented())
		assert.Equal(uint64(111), m.Version)
		assert.Equal(TxnInternalVersion(1), m.InternalVersion)
		assert.True(m.IsDirty())
		assert.True(m.IsAborted())
		assert.True(!m.IsCleared())
		assert.False(m.IsCommittedCleared())
		assert.False(m.IsRollbackedCleared())
	}
}

func TestEmpty(t *testing.T) {
	assert := testifyassert.New(t)

	assert.True(EmptyValue.IsEmpty())
	assert.True(!EmptyValue.IsDirty())
	assert.True(!EmptyValue.IsCommitted())
	assert.True(!EmptyValue.IsAborted())
	assert.True(!EmptyValue.IsTerminated())

	assert.True(EmptyValueCC.IsEmpty())
	assert.True(!EmptyValueCC.IsDirty())
	assert.True(!EmptyValueCC.IsCommitted())
	assert.True(!EmptyValueCC.IsAborted())
	assert.True(!EmptyValueCC.IsTerminated())

	assert.True(EmptyTValue.IsEmpty())
	assert.True(!EmptyTValue.IsDirty())
	assert.True(!EmptyTValue.IsCommitted())
	assert.True(!EmptyTValue.IsAborted())
	assert.True(!EmptyTValue.IsTerminated())

	assert.True(!EmptyDBValue.IsDirty())
	assert.True(!EmptyDBValue.IsCommitted())
}
