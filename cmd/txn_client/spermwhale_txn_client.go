package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/leisurelyrcxf/spermwhale/utils"

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
	cmd.ParseFlags()

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

	var (
		tx types.Txn

		showTxnInfo = func(began bool) {
			desc := "Current"
			if began {
				desc = "Began"
			}
			if tx == nil {
				fmt.Printf("%s transaction: nil\n", desc)
			} else if showTxnID {
				fmt.Printf("%s transaction: {id: %d, state: %s}\n", desc, tx.GetId(), tx.GetState())
			} else {
				fmt.Printf("%s transaction: {timestamp: %s, state: %s}\n", desc, time.Unix(0, int64(tx.GetId())).Format("15:04:05.000000000"), tx.GetState())
			}
		}

		prevTxnState types.TxnState
		quit         = false

		executor = func(promptText string) {
			cmds := utils.TrimmedSplit(promptText, ";")
			for i := 0; ; i++ {
				if clientTxn, ok := tx.(*txn.ClientTxn); ok && clientTxn != nil {
					if prevTxnState != types.TxnStateCommitted && clientTxn.State == types.TxnStateCommitted {
						fmt.Println("COMMITTED")
					} else if !prevTxnState.IsAborted() && clientTxn.State.IsAborted() {
						fmt.Println("ABORTED")
					}
					if autoClearTerminatedTxn && clientTxn.State.IsTerminated() {
						tx = nil
					}
					prevTxnState = clientTxn.State
				}

				if quit {
					os.Exit(1)
				}
				if i >= len(cmds) {
					break
				}

				var t = cmds[i]
				if strings.HasPrefix(t, "begin") {
					if tx, err = tm.BeginTransaction(ctx); err != nil {
						fmt.Printf("BEGIN failed: %v\n", err)
						continue
					}
					showTxnInfo(true)
				} else if strings.HasPrefix(t, "get") {
					remain := strings.TrimPrefix(t, "get")
					if remain == "" {
						fmt.Println("Invalid get command, use 'get key'")
						continue
					}
					if !strings.HasPrefix(remain, " ") {
						fmt.Printf("unknown cmd: '%s'\n", strings.Split(t, " ")[0])
						continue
					}
					remain = strings.TrimPrefix(remain, " ")
					parts := strings.Split(remain, " ")
					if len(parts) != 1 {
						fmt.Println("Invalid get command, use 'get key'")
						continue
					}
					if tx == nil {
						if !autoBegin {
							fmt.Println("Transaction is nil, needs begin first")
							continue
						}
						if tx, err = tm.BeginTransaction(ctx); err != nil {
							fmt.Printf("BEGIN failed: %v\n", err)
							continue
						}
						showTxnInfo(true)
					}
					key := parts[0]
					val, err := tx.Get(ctx, key)
					if err != nil {
						fmt.Printf("GET failed: %v\n", err)
						continue
					}
					fmt.Printf("\"%s\": %s\n", key, string(val.V))
				} else if strings.HasPrefix(t, "set") {
					remain := strings.TrimPrefix(t, "set")
					if remain == "" {
						fmt.Println("Invalid set command, use 'set key value'")
						continue
					}
					if !strings.HasPrefix(remain, " ") {
						fmt.Printf("Unknown command: '%s'\n", strings.Split(t, " ")[0])
						continue
					}
					remain = strings.TrimPrefix(remain, " ")
					parts := strings.Split(remain, " ")
					if len(parts) != 2 {
						fmt.Println("Invalid set command, use 'set key value'")
						continue
					}
					if tx == nil {
						if !autoBegin {
							fmt.Println("Transaction is nil, needs begin first")
							continue
						}
						if tx, err = tm.BeginTransaction(ctx); err != nil {
							fmt.Printf("BEGIN failed: %v\n", err)
							continue
						}
						showTxnInfo(true)
					}
					key, val := parts[0], parts[1]
					if err := tx.Set(ctx, key, []byte(val)); err != nil {
						fmt.Printf("set failed: %v\n", err)
						continue
					}
					fmt.Println("ok")
				} else if strings.HasPrefix(t, "commit") {
					if tx == nil {
						fmt.Println("Transaction is nil, nothing to commit")
						continue
					}
					if err := tx.Commit(ctx); err != nil {
						fmt.Printf("commit failed: %v\n", err)
						continue
					}
				} else if strings.HasPrefix(t, "rollback") {
					if tx == nil {
						fmt.Println("Transaction is nil, nothing to rollback")
						continue
					}
					if err := tx.Rollback(ctx); err != nil {
						fmt.Printf("rollback failed: %v\n", err)
						continue
					}
				} else if t == "show" {
					showTxnInfo(false)
				} else if t == "quit" || t == "exit" || t == "q" {
					quit = true
					if tx != nil {
						if err := tx.Rollback(ctx); err != nil {
							fmt.Printf("rollback failed: %v\n", err)
							continue
						}
					}
				} else {
					fmt.Printf("Command '%s' not supported\n", t)
					continue
				}
			}
		}
	)

	p := prompt.New(
		executor,
		completer,
		prompt.OptionPrefix("> "),
		prompt.OptionTitle("spermwhale txn client"),
	)
	p.Run()
}
