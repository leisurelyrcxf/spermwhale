package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/c-bata/go-prompt"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/cmd"
	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/kv"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/utils"
)

func completer(d prompt.Document) []prompt.Suggest {
	s := []prompt.Suggest{
		{Text: "get", Description: "get key"},
		{Text: "quit", Description: "quit terminal"},
		{Text: "exit", Description: "quit terminal"},
	}
	return prompt.FilterHasPrefix(s, d.GetWordBeforeCursor(), true)
}

func main() {
	flagHost := flag.String("host", "127.0.0.1", "host")
	cmd.RegisterPortFlags(consts.DefaultKVServerPort)
	cmd.ParseFlags()

	kvClient, err := kv.NewClient(fmt.Sprintf("%s:%d", *flagHost, *cmd.FlagPort))
	if err != nil {
		glog.Fatalf("can't create kv client")
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		quit = false

		executor = func(promptText string) {
			cmds := utils.TrimmedSplit(promptText, ";")
			for i := 0; ; i++ {
				if quit {
					os.Exit(1)
				}

				if i >= len(cmds) {
					break
				}

				var t = cmds[i]
				if strings.HasPrefix(t, "get") {
					remain := strings.TrimPrefix(t, "get")
					if remain == "" {
						fmt.Println("invalid get command, use 'get key [version] [exact_version]")
						continue
					}
					if !strings.HasPrefix(remain, " ") {
						fmt.Printf("unknown cmd: '%s'\n", strings.Split(t, " ")[0])
						continue
					}
					remain = strings.TrimPrefix(remain, " ")
					parts := strings.Split(remain, " ")
					key := parts[0]
					version := types.MaxTxnVersion
					if len(parts) >= 2 {
						if version, err = strconv.ParseUint(parts[1], 10, 64); err != nil {
							fmt.Printf("invalid version '%s'\n", parts[1])
							continue
						}
					}
					readOpt := types.NewKVReadOption(version)
					if len(parts) >= 3 {
						if parts[2] != "exact-version" {
							fmt.Println("invalid command, use 'get key [version] [exact_version]'")
						}
						readOpt = types.NewKVReadOptionWithExactVersion(version)
					}
					val, err := kvClient.Get(ctx, key, readOpt)
					if err != nil {
						fmt.Printf("get failed: %v\n", err)
						continue
					}
					fmt.Println(string(val.V))
				} else if strings.HasPrefix(t, "set") {
					fmt.Println("set is not supported")
					continue
				} else if t == "quit" || t == "exit" || t == "q" {
					break
				} else {
					fmt.Printf("cmd '%s' not supported\n", t)
					continue
				}
			}
		}
	)

	p := prompt.New(
		executor,
		completer,
		prompt.OptionPrefix("> "),
		prompt.OptionTitle("spermwhale kv client"),
	)
	p.Run()
}
