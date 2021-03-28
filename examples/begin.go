package examples

import (
	"context"
	"fmt"

	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/txn"
	"github.com/leisurelyrcxf/spermwhale/txn/smart_txn_client"
	"github.com/leisurelyrcxf/spermwhale/types"
)

func UseTxn(ctx context.Context) error {
	const port = consts.DefaultTxnServerPort
	cli, err := txn.NewClient(fmt.Sprintf("localhost:%d", port))
	if err != nil {
		return err
	}
	cliTM := txn.NewClientTxnManager(cli)
	sm := smart_txn_client.NewSmartClient(cliTM, 100)

	const (
		key1         = "key1"
		initialValue = 101
		delta        = 5

		key2             = "key1"
		key2InitialValue = 101
		delta2           = 5
	)
	if err := sm.SetInt(ctx, key1, initialValue); err != nil {
		return err
	}
	if err := sm.SetInt(ctx, key2, key2InitialValue); err != nil {
		return err
	}
	return sm.DoTransactionOfType(ctx, types.TxnTypeReadModifyWrite|types.TxnTypeWaitWhenReadDirty, func(ctx context.Context, txn types.Txn) error {
		{
			key1Val, err := txn.Get(ctx, key1)
			if err != nil {
				return err
			}
			v1, err := key1Val.Int()
			if err != nil {
				return err
			}
			v1 += delta
			if err := txn.Set(ctx, key1, types.NewIntValue(v1).V); err != nil {
				return err
			}
		}

		{
			key2Val, err := txn.Get(ctx, key2)
			if err != nil {
				return err
			}
			v2, err := key2Val.Int()
			if err != nil {
				return err
			}
			v2 += delta2
			if err := txn.Set(ctx, key2, types.NewIntValue(v2).V); err != nil {
				return err
			}
		}
		return nil
	})
}
