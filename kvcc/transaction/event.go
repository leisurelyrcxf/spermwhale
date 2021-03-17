package transaction

import (
	"context"
	"fmt"
	"time"
)

type KeyEventType int

const (
	KeyEventTypeInvalid          KeyEventType = iota
	KeyEventTypeClearWriteIntent              // cleared or clearing
	KeyEventTypeVersionRemoved
	KeyEventTypeRemoveVersionFailed
)

func GetKeyEventTypeRemoveVersion(success bool) KeyEventType {
	if success {
		return KeyEventTypeVersionRemoved
	}
	return KeyEventTypeRemoveVersionFailed
}

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

func (w *KeyEventWaiter) Wait(ctx context.Context, timeout time.Duration) (KeyEvent, error) {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	select {
	case <-waitCtx.Done():
		return InvalidKeyEvent, waitCtx.Err()
	case event := <-w.waitress:
		return event, nil
	}
}

func (w *KeyEventWaiter) signal(event KeyEvent) {
	w.waitress <- event
	close(w.waitress)
}

type ReadForWriteKeyEventType int

const (
	ReadForWriteKeyEventTypeInvalid ReadForWriteKeyEventType = iota
	ReadForWriteKeyEventTypeWriteIntentCleared
	ReadForWriteKeyEventTypeClearWriteIntentFailed
	ReadForWriteKeyEventTypeVersionRemoved
	ReadForWriteKeyEventTypeRemoveVersionFailed
	ReadForWriteKeyEventTypeKeyWritten
)

func GetReadForWriteKeyEventTypeClearWriteIntent(success bool) ReadForWriteKeyEventType {
	if success {
		return ReadForWriteKeyEventTypeWriteIntentCleared
	}
	return ReadForWriteKeyEventTypeClearWriteIntentFailed
}

func GetReadForWriteKeyEventTypeRemoveVersion(success bool) ReadForWriteKeyEventType {
	if success {
		return ReadForWriteKeyEventTypeVersionRemoved
	}
	return ReadForWriteKeyEventTypeRemoveVersionFailed
}

func (t ReadForWriteKeyEventType) String() string {
	switch t {
	case ReadForWriteKeyEventTypeInvalid:
		return "read_for_write_key_event_type_invalid"
	case ReadForWriteKeyEventTypeWriteIntentCleared:
		return "read_for_write_key_event_type_write_intent_cleared"
	case ReadForWriteKeyEventTypeClearWriteIntentFailed:
		return "read_for_write_key_event_type_clear_write_intent_failed"
	case ReadForWriteKeyEventTypeVersionRemoved:
		return "read_for_write_key_event_type_version_removed"
	case ReadForWriteKeyEventTypeRemoveVersionFailed:
		return "read_for_write_key_event_type_remove_version_failed"
	case ReadForWriteKeyEventTypeKeyWritten:
		return "read_for_write_key_event_type_key_written"
	default:
		panic("unknown type")
	}
}

type ReadForWriteKeyEvent struct {
	Key  string
	Type ReadForWriteKeyEventType
}

func NewReadForWriteKeyEvent(key string, typ ReadForWriteKeyEventType) ReadForWriteKeyEvent {
	return ReadForWriteKeyEvent{
		Key:  key,
		Type: typ,
	}
}

func (e ReadForWriteKeyEvent) String() string {
	return fmt.Sprintf("ReadForWriteKeyEvent{key: %s, type: %s}", e.Key, e.Type.String())
}
