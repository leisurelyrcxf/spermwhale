package transaction

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/leisurelyrcxf/spermwhale/assert"

	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/types/basic"
)

type Writer struct {
	*Transaction

	types.DBMeta
	OnUnlocked func()

	writing   basic.AtomicBool
	succeeded basic.AtomicBool
	rw        sync.RWMutex
}

func NewWriter(dbMeta types.DBMeta, txn *Transaction) *Writer {
	return &Writer{DBMeta: dbMeta, Transaction: txn}
}

func (w *Writer) Lock() {
	w.rw.Lock()
	w.writing.Set(true)
}

func (w *Writer) Unlock() {
	w.writing.Set(false)
	w.rw.Unlock()

	w.OnUnlocked()
}

func (w *Writer) SetResult(err error) {
	w.succeeded.Set(err == nil)
}

func (w *Writer) IsWriting() bool {
	return w.writing.Get()
}

func (w *Writer) WaitWritten() {
	if w != nil && w.writing.Get() {
		w.rw.RLock()
		w.rw.RUnlock()
	}
}

// Deprecated
func (w *Writer) waitKeyRemoved(ctx context.Context, key string) error {
	waiter, keyEvent, err := w.registerKeyEventWaiter(key)
	if err != nil {
		return err
	}
	if waiter != nil {
		if keyEvent, err = waiter.Wait(ctx); err != nil {
			return err
		}
	}
	switch keyEvent.Type {
	case KeyEventTypeVersionRemoved:
		return nil
	case KeyEventTypeRemoveVersionFailed:
		return errors.ErrRemoveKeyFailed
	default:
		panic(fmt.Sprintf("impossible type: '%s'", keyEvent.Type))
	}
}

func (w *Writer) Succeeded() bool {
	return w.succeeded.Get()
}

// HasMoreWritingWriters is a dummy writer used to indicate that has
// more pending writers afterwards, the Next list is not complete
var HasMoreWritingWriters = NewWriter(types.DBMeta{}, newTransaction(types.MaxTxnId, nil, nil))

type WritingWriters []*Writer

func (writers WritingWriters) CheckRead(ctx context.Context, valVersion uint64, waitTimeout time.Duration) error {
	//return errors.ErrWriteReadConflictUnsafeRead
	ctx, cancel := context.WithTimeout(ctx, waitTimeout)
	defer cancel()

	valTxnId := types.TxnId(valVersion)

	for _, writer := range writers {
		if writer == HasMoreWritingWriters {
			return errors.ErrWriteReadConflictUnsafeRead
		}
		if valTxnId >= writer.ID {
			return nil
		}
		if err := writer.waitTerminate(ctx); err != nil {
			return errors.Annotatef(errors.ErrWriteReadConflictUnsafeReadWaitTxnTerminateFailed, err.Error())
		}
		state := writer.GetTxnState()
		switch {
		case state.IsCommitted():
			assert.Must(writer.Succeeded())
			return errors.ErrWriteReadConflictReaderSkippedCommittedData // Since we didn't wait the writer, this is possible
		case state.IsAborted(): // TODO check this
			break
		default:
			panic(fmt.Sprintf("impossible txn state: %s", state))
		}
	}
	return nil
}

func init() {
	HasMoreWritingWriters.SetTxnStateUnsafe(types.TxnStateCommitted)
}
