package transaction

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/types/basic"
)

type Writer struct {
	*Transaction

	OnUnlocked func()

	writing basic.AtomicBool
	rw      sync.RWMutex
}

func NewWriter(txn *Transaction) *Writer {
	return &Writer{Transaction: txn}
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

func (w *Writer) IsWriting() bool {
	return w.writing.Get()
}

func (w *Writer) WaitKeyDone() {
	if w != nil && w.writing.Get() {
		w.rw.RLock()
		w.rw.RUnlock()
	}
}

// HasMoreWritingWriters is a dummy writer used to indicate that has
// more pending writers afterwards, the Next list is not complete
var HasMoreWritingWriters = NewWriter(newTransaction(types.MaxTxnId, nil, nil))

type WritingWriters []*Writer

func (writers WritingWriters) CheckRead(ctx context.Context, valVersion uint64, waitTimeout time.Duration) error {
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
			return err
		}
		state := writer.GetTxnState()
		switch {
		case state.IsCommitted():
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
