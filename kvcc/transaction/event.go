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
	KeyEventTypeRemoveVersionFailed
	KeyEventTypeVersionRemoved
)

func (t KeyEventType) String() string {
	switch t {
	case KeyEventTypeInvalid:
		return "invalid"
	case KeyEventTypeClearWriteIntent:
		return "clear_write_intent"
	case KeyEventTypeRemoveVersionFailed:
		return "removing_version"
	case KeyEventTypeVersionRemoved:
		return "version_removed"
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

func newkeyEventWaiter(key string) *KeyEventWaiter {
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
