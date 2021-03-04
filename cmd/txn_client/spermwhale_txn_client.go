package main

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/c-bata/go-prompt"
	"github.com/golang/glog"
	"github.com/leisurelyrcxf/spermwhale/cmd"
	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/txn"
	"github.com/leisurelyrcxf/spermwhale/types"
)

func completer(d prompt.Document) []prompt.Suggest {
	s := []prompt.Suggest{
		{Text: "begin", Description: "begin a transaction"},
		{Text: "get", Description: "get key"},
		{Text: "set", Description: "set key value"},
		{Text: "commit", Description: "commit the transaction"},
		{Text: "rollback", Description: "rollback the transaction"},
		{Text: "show", Description: "show current transaction"},
		{Text: "quit", Description: "quit terminal"},
		{Text: "exit", Description: "quit terminal"},
	}
	return prompt.FilterHasPrefix(s, d.GetWordBeforeCursor(), true)
}

func main() {
	flagHost := flag.String("host", "127.0.0.1", "host")
	flagNotAutoBegin := flag.Bool("not-auto-begin", false, "not auto begin")
	flagNotAutoClearTerminatedTxn := flag.Bool("not-auto-clear-txn", false, "not auto clear txn after transaction committed or terminated")
	flagShowTxnID := flag.Bool("show-txn-id", false, "show txn id instead of txn time")
	cmd.RegisterPortFlags(consts.DefaultTxnServerPort)
	flag.Parse()

	cli, err := txn.NewClient(fmt.Sprintf("%s:%d", *flagHost, *cmd.FlagPort))
	if err != nil {
		glog.Fatalf("can't create txn client")
	}
	var (
		autoBegin              = !*flagNotAutoBegin
		autoClearTerminatedTxn = !*flagNotAutoClearTerminatedTxn
		showTxnID              = *flagShowTxnID
	)
	tm := txn.NewClientTxnManager(cli)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var tx types.Txn
	showTxnInfo := func() {
		if tx == nil {
			fmt.Println("current transaction: nil")
		} else if showTxnID {
			fmt.Printf("Current transaction: {id: %d, state: %s}\n", tx.GetId(), tx.GetState())
		} else {
			fmt.Printf("Current transaction: {timestamp: %s, state: %s}\n", time.Unix(0, int64(tx.GetId())).Format("15:04:05.000000000"), tx.GetState())
		}
	}
	for {
		if clientTxn, ok := tx.(*txn.ClientTxn); ok {
			if clientTxn.State == types.TxnStateCommitted {
				fmt.Println("committed")
				if autoClearTerminatedTxn {
					tx = nil
				}
			} else if clientTxn.State.IsAborted() {
				fmt.Println("aborted")
				if autoClearTerminatedTxn {
					tx = nil
				}
			}
		}
		t := prompt.Input("> ", completer)
		if strings.HasPrefix(t, "begin") {
			if tx, err = tm.BeginTransaction(ctx); err != nil {
				fmt.Printf("begin failed: %v\n", err)
				continue
			}
			showTxnInfo()
		} else if strings.HasPrefix(t, "get") {
			remain := strings.TrimPrefix(t, "get")
			if remain == "" {
				fmt.Println("invalid get command, use 'get key'")
				continue
			}
			if !strings.HasPrefix(remain, " ") {
				fmt.Printf("unknown cmd: '%s'\n", strings.Split(t, " ")[0])
				continue
			}
			remain = strings.TrimPrefix(remain, " ")
			parts := strings.Split(remain, " ")
			if len(parts) != 1 {
				fmt.Println("invalid get command, use 'get key'")
				continue
			}
			if tx == nil {
				if !autoBegin {
					fmt.Println("transaction is nil, needs begin first")
					continue
				}
				if tx, err = tm.BeginTransaction(ctx); err != nil {
					fmt.Printf("begin failed: %v\n", err)
					continue
				}
				showTxnInfo()
			}
			key := parts[0]
			val, err := tx.Get(ctx, key)
			if err != nil {
				fmt.Printf("get failed: %v\n", err)
				continue
			}
			fmt.Println(string(val.V))
		} else if strings.HasPrefix(t, "set") {
			remain := strings.TrimPrefix(t, "set")
			if remain == "" {
				fmt.Println("invalid set command, use 'set key value'")
				continue
			}
			if !strings.HasPrefix(remain, " ") {
				fmt.Printf("unknown cmd: '%s'\n", strings.Split(t, " ")[0])
				continue
			}
			remain = strings.TrimPrefix(remain, " ")
			parts := strings.Split(remain, " ")
			if len(parts) != 2 {
				fmt.Println("invalid set command, use 'set key value'")
				continue
			}
			if tx == nil {
				if !autoBegin {
					fmt.Println("transaction is nil, needs begin first")
					continue
				}
				if tx, err = tm.BeginTransaction(ctx); err != nil {
					fmt.Printf("begin failed: %v\n", err)
					continue
				}
				showTxnInfo()
			}
			key, val := parts[0], parts[1]
			if err := tx.Set(ctx, key, []byte(val)); err != nil {
				fmt.Printf("set failed: %v\n", err)
				continue
			}
			fmt.Println("ok")
		} else if strings.HasPrefix(t, "commit") {
			if tx == nil {
				fmt.Println("transaction is nil, nothing to commit")
				continue
			}
			if err := tx.Commit(ctx); err != nil {
				fmt.Printf("commit failed: %v\n", err)
				continue
			}
		} else if strings.HasPrefix(t, "rollback") {
			if tx == nil {
				fmt.Println("transaction is nil, nothing to rollback")
				continue
			}
			if err := tx.Rollback(ctx); err != nil {
				fmt.Printf("rollback failed: %v\n", err)
				continue
			}
		} else if t == "show" {
			showTxnInfo()
		} else if t == "quit" || t == "exit" || t == "q" {
			break
		} else {
			fmt.Printf("cmd '%s' not supported\n", t)
			continue
		}
	}
}
