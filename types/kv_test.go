package types

import (
	"testing"

	"github.com/leisurelyrcxf/spermwhale/consts"
	testifyassert "github.com/stretchr/testify/assert"
)

func TestDBMeta_UpdateTxnState(t *testing.T) {
	assert := testifyassert.New(t)
	//{
	//	m := DBMeta{InternalVersion: 1, VFlag: consts.ValueMetaBitMaskHasWriteIntent | consts.ValueMetaBitMaskAborted | consts.ValueMetaBitMaskCommitted}
	//	m.IsTerminated() // should panic
	//}

	{
		m := DBMeta{InternalVersion: 1, VFlag: consts.ValueMetaBitMaskHasWriteIntent | consts.ValueMetaBitMaskPreventedFutureWrite}
		m.UpdateTxnState(TxnStateCommittedCleared)
		assert.True(m.IsCommitted())
		assert.False(m.IsUncommitted())
		assert.True(m.IsTerminated())
		assert.True(m.IsFutureWritePrevented())
		assert.Equal(TxnInternalVersion(1), m.InternalVersion)
		assert.True(!m.IsDirty())
		assert.True(!m.IsAborted())
		assert.True(m.IsCleared())
		assert.False(m.IsKeyStateInvalid())
		assert.True(m.IsCommittedCleared())
		assert.False(m.IsRollbackedCleared())

		m.SetInvalidKeyState()
		assert.False(m.IsCommitted())
		assert.False(m.IsUncommitted())
		assert.False(m.IsTerminated())
		assert.True(m.IsFutureWritePrevented())
		assert.Equal(TxnInternalVersion(1), m.InternalVersion)
		assert.True(!m.IsDirty())
		assert.False(m.IsAborted())
		assert.False(m.IsCleared())
		assert.True(m.IsKeyStateInvalid())
		assert.False(m.IsCommittedCleared())
		assert.False(m.IsRollbackedCleared())
	}

	{
		m := DBMeta{InternalVersion: 1, VFlag: consts.ValueMetaBitMaskHasWriteIntent | consts.ValueMetaBitMaskPreventedFutureWrite}
		assert.True(m.IsUncommitted())
		m.SetInvalidKeyState()
		assert.False(m.IsUncommitted())
	}

	{
		m := DBMeta{InternalVersion: 1, VFlag: consts.ValueMetaBitMaskHasWriteIntent | consts.ValueMetaBitMaskPreventedFutureWrite}
		m.updateKeyState(KeyStateCommittedCleared)
		assert.True(m.IsCommitted())
		assert.True(m.IsTerminated())
		assert.False(m.IsTxnRecord())
		assert.True(m.IsFutureWritePrevented())
		assert.Equal(TxnInternalVersion(1), m.InternalVersion)
		assert.True(!m.IsDirty())
		assert.True(!m.IsAborted())
		assert.True(m.IsCleared())
	}
	{
		m := DBMeta{InternalVersion: 1, VFlag: consts.ValueMetaBitMaskHasWriteIntent | consts.ValueMetaBitMaskTxnRecord}
		m.UpdateTxnState(TxnStateCommitted)
		assert.True(m.IsCommitted())
		assert.True(m.IsTerminated())
		assert.False(m.IsFutureWritePrevented())
		assert.True(m.IsTxnRecord())
		assert.Equal(TxnInternalVersion(1), m.InternalVersion)
		assert.True(!m.IsDirty())
		assert.True(!m.IsAborted())
		assert.False(m.IsCleared())
	}

	{
		m := DBMeta{InternalVersion: 1, VFlag: consts.ValueMetaBitMaskHasWriteIntent | consts.ValueMetaBitMaskPreventedFutureWrite}
		m.updateKeyState(KeyStateCommitted)
		assert.True(m.IsCommitted())
		assert.True(m.IsTerminated())
		assert.True(m.IsFutureWritePrevented())
		assert.Equal(TxnInternalVersion(1), m.InternalVersion)
		assert.True(!m.IsDirty())
		assert.True(!m.IsAborted())
		assert.False(m.IsCleared())
	}

	{
		m := DBMeta{InternalVersion: 1, VFlag: consts.ValueMetaBitMaskHasWriteIntent | consts.ValueMetaBitMaskPreventedFutureWrite}
		m.UpdateTxnState(TxnStateRollbackedCleared)
		assert.True(!m.IsCommitted())
		assert.True(m.IsTerminated())
		assert.True(m.IsFutureWritePrevented())
		assert.Equal(TxnInternalVersion(1), m.InternalVersion)
		assert.True(m.IsDirty())
		assert.True(m.IsAborted())
		assert.True(m.IsCleared())
	}

	{
		m := DBMeta{InternalVersion: 1, VFlag: consts.ValueMetaBitMaskHasWriteIntent | consts.ValueMetaBitMaskPreventedFutureWrite}
		m.updateKeyState(KeyStateRollbackedCleared)
		assert.True(!m.IsCommitted())
		assert.True(m.IsTerminated())
		assert.True(m.IsFutureWritePrevented())
		assert.Equal(TxnInternalVersion(1), m.InternalVersion)
		assert.True(m.IsDirty())
		assert.True(m.IsAborted())
		assert.True(m.IsCleared())
	}

	{
		m := DBMeta{InternalVersion: 1, VFlag: consts.ValueMetaBitMaskHasWriteIntent | consts.ValueMetaBitMaskPreventedFutureWrite}
		m.UpdateTxnState(TxnStateRollbacking)
		assert.True(!m.IsCommitted())
		assert.True(m.IsTerminated())
		assert.True(m.IsFutureWritePrevented())
		assert.Equal(TxnInternalVersion(1), m.InternalVersion)
		assert.True(m.IsDirty())
		assert.True(m.IsAborted())
		assert.False(m.IsCleared())
	}

	{
		m := DBMeta{InternalVersion: 1, VFlag: consts.ValueMetaBitMaskHasWriteIntent | consts.ValueMetaBitMaskPreventedFutureWrite}
		m.updateKeyState(KeyStateRollbacking)
		assert.True(!m.IsCommitted())
		assert.True(m.IsTerminated())
		assert.True(m.IsFutureWritePrevented())
		assert.Equal(TxnInternalVersion(1), m.InternalVersion)
		assert.True(m.IsDirty())
		assert.True(m.IsAborted())
		assert.False(m.IsCleared())
	}
}

func TestDBValue_WithCommitted(t *testing.T) {
	assert := testifyassert.New(t)
	{
		v := DBValue{
			DBMeta: DBMeta{InternalVersion: 1, VFlag: consts.ValueMetaBitMaskHasWriteIntent | consts.ValueMetaBitMaskPreventedFutureWrite},
			V:      []byte{'1', '2', '3'},
		}
		v = v.WithCommitted()
		assert.Equal([]byte{'1', '2', '3'}, v.V)
		assert.Equal(TxnInternalVersion(1), v.InternalVersion)
		assert.True(!v.IsDirty())
		assert.True(v.IsCommitted())
		assert.True(v.IsTerminated())
		assert.False(v.IsCleared())
	}
}
