package transaction

import (
	"context"
	"fmt"
	"time"

	"github.com/leisurelyrcxf/spermwhale/errors"

	"github.com/leisurelyrcxf/spermwhale/assert"
)

type KeyEventType int

const (
	KeyEventTypeInvalid          KeyEventType = iota
	KeyEventTypeClearWriteIntent              // cleared or clearing
	KeyEventTypeVersionRemoved
	KeyEventTypeRemoveVersionFailed
)

func (t KeyEventType) String() string {
	switch t {
	case KeyEventTypeInvalid:
		return "invalid"
	case KeyEventTypeClearWriteIntent:
		return "clear_write_intent"
	case KeyEventTypeVersionRemoved:
		return "version_removed"
	case KeyEventTypeRemoveVersionFailed:
		return "remove_version_failed"
	default:
		panic("unknown type")
	}
}

type KeyEvent struct {
	Key  string
	Type KeyEventType
}

var InvalidKeyEvent = KeyEvent{Type: KeyEventTypeInvalid}

func NewKeyEvent(key string, typ KeyEventType) KeyEvent {
	assert.Must(key != "")
	return KeyEvent{
		Key:  key,
		Type: typ,
	}
}

func (e KeyEvent) String() string {
	return fmt.Sprintf("key: %s, type: %s", e.Key, e.Type.String())
}

type KeyEventWaiter struct {
	key      string
	waitress chan KeyEvent
}

func newKeyEventWaiter(key string) *KeyEventWaiter {
	return &KeyEventWaiter{
		key:      key,
		waitress: make(chan KeyEvent, 1),
	}
}

func (w *KeyEventWaiter) WaitWithTimeout(ctx context.Context, timeout time.Duration) (KeyEvent, error) {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	return w.Wait(waitCtx)
}

func (w *KeyEventWaiter) Wait(ctx context.Context) (KeyEvent, error) {
	select {
	case <-ctx.Done():
		return InvalidKeyEvent, errors.Annotatef(errors.ErrWaitKeyEventFailed, ctx.Err().Error())
	case event := <-w.waitress:
		return event, nil
	}
}

func (w *KeyEventWaiter) signal(event KeyEvent) {
	w.waitress <- event
	close(w.waitress)
}

type ReadModifyWriteKeyEventType int

const (
	ReadModifyWriteKeyEventTypeInvalid ReadModifyWriteKeyEventType = iota
	ReadModifyWriteKeyEventTypeWriteIntentCleared
	ReadModifyWriteKeyEventTypeClearWriteIntentFailed
	ReadModifyWriteKeyEventTypeVersionRemoved
	ReadModifyWriteKeyEventTypeRemoveVersionFailed
	ReadModifyWriteKeyEventTypeKeyWritten
)

func GetReadModifyWriteKeyEventTypeClearWriteIntent(success bool) ReadModifyWriteKeyEventType {
	if success {
		return ReadModifyWriteKeyEventTypeWriteIntentCleared
	}
	return ReadModifyWriteKeyEventTypeClearWriteIntentFailed
}

func GetReadModifyWriteKeyEventTypeRemoveVersion(success bool) ReadModifyWriteKeyEventType {
	if success {
		return ReadModifyWriteKeyEventTypeVersionRemoved
	}
	return ReadModifyWriteKeyEventTypeRemoveVersionFailed
}

func (t ReadModifyWriteKeyEventType) String() string {
	switch t {
	case ReadModifyWriteKeyEventTypeInvalid:
		return "read_modify_write_key_event_type_invalid"
	case ReadModifyWriteKeyEventTypeWriteIntentCleared:
		return "read_modify_write_key_event_type_write_intent_cleared"
	case ReadModifyWriteKeyEventTypeClearWriteIntentFailed:
		return "read_modify_write_key_event_type_clear_write_intent_failed"
	case ReadModifyWriteKeyEventTypeVersionRemoved:
		return "read_modify_write_key_event_type_version_removed"
	case ReadModifyWriteKeyEventTypeRemoveVersionFailed:
		return "read_modify_write_key_event_type_remove_version_failed"
	case ReadModifyWriteKeyEventTypeKeyWritten:
		return "read_modify_write_key_event_type_key_written"
	default:
		panic("unknown type")
	}
}

type ReadModifyWriteKeyEvent struct {
	Key  string
	Type ReadModifyWriteKeyEventType
}

func NewReadModifyWriteKeyEvent(key string, typ ReadModifyWriteKeyEventType) ReadModifyWriteKeyEvent {
	assert.Must(key != "")
	return ReadModifyWriteKeyEvent{
		Key:  key,
		Type: typ,
	}
}

func (e ReadModifyWriteKeyEvent) String() string {
	return fmt.Sprintf("ReadModifyWriteKeyEvent{key: %s, type: %s}", e.Key, e.Type.String())
}
