package main

import (
    "context"
    "flag"
    "fmt"
    "github.com/c-bata/go-prompt"
    "github.com/golang/glog"
    "github.com/leisurelyrcxf/spermwhale/cmd"
    "github.com/leisurelyrcxf/spermwhale/consts"
    "github.com/leisurelyrcxf/spermwhale/txn"
    "github.com/leisurelyrcxf/spermwhale/types"
    "strings"
)

func completer(d prompt.Document) []prompt.Suggest {
    s := []prompt.Suggest{
        {Text: "begin", Description: "begin a transaction"},
        {Text: "get", Description: "get key"},
        {Text: "set", Description: "set key value"},
        {Text: "commit", Description: "commit the transaction"},
        {Text: "rollback", Description: "rollback the transaction"},
        {Text: "quit", Description: "quit terminal"},
        {Text: "exit", Description: "quit terminal"},
    }
    return prompt.FilterHasPrefix(s, d.GetWordBeforeCursor(), true)
}

func main() {
    flagHost := flag.String("host", "127.0.0.1", "host")
    cmd.RegisterPortFlags(consts.DefaultTxnServerPort)

    cli, err := txn.NewClient(fmt.Sprintf("%s:%d", *flagHost, *cmd.FlagPort))
    if err != nil {
        glog.Fatalf("can't create txn client")
    }
    tm := txn.NewClientTxnManager(cli)
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    var tx types.Txn
    for {
        t := prompt.Input("> ", completer)
        if strings.HasPrefix(t, "begin") {
            if tx, err = tm.BeginTransaction(ctx); err != nil {
                fmt.Printf("begin failed: %v\n", err)
                continue
            }
            fmt.Printf("Started transaction: {id: %d, state: %s}\n", tx.GetId(), tx.GetState())
        } else if strings.HasPrefix(t, "get") {
            remain := strings.TrimPrefix(t, "get")
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
                fmt.Println("transaction is nil, needs begin first")
                continue
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
                fmt.Println("transaction is nil, needs begin first")
                continue
            }
            key, val := parts[0], parts[1]
            if err := tx.Set(ctx, key, []byte(val)); err != nil {
                fmt.Printf("set failed: %v\n", err)
                continue
            }
            fmt.Println("ok")
        } else if strings.HasPrefix(t, "commit") {
            if tx == nil {
                fmt.Println("transaction is nil, needs begin first")
                continue
            }
            if err := tx.Commit(ctx); err != nil {
                fmt.Printf("commit failed: %v\n", err)
                continue
            }
            fmt.Println("ok")
        } else if strings.HasPrefix(t, "rollback") {
            if tx == nil {
                fmt.Println("transaction is nil, needs begin first")
                continue
            }
            if err := tx.Rollback(ctx); err != nil {
                fmt.Printf("rollback failed: %v\n", err)
                continue
            }
            fmt.Println("ok")
        } else if t == "quit" || t == "exit" || t == "q" {
            break
        }
    }


    //exitCh := make(chan os.Signal, 1)
    //signal.Notify(exitCh, syscall.SIGTERM)
    //var currentJob atomic.Value
    //go func() {
    //    sig := <-exitCh
    //    glog.Warningf("Received signal %d", sig)
    //    cancel()
    //
    //    obj := currentJob.Load()
    //    if obj != nil {
    //        <- obj.(chan struct{})
    //    }
    //    os.Exit(1)
    //}()
}


