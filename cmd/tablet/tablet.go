package main

import (
	"flag"
	"fmt"

	"github.com/leisurelyrcxf/spermwhale/kv/impl/memory"
	"github.com/leisurelyrcxf/spermwhale/kv/impl/redis"
	"github.com/leisurelyrcxf/spermwhale/types"

	"github.com/leisurelyrcxf/spermwhale/consts"

	"github.com/leisurelyrcxf/spermwhale/cmd"

	"github.com/leisurelyrcxf/spermwhale/kvcc"

	"github.com/golang/glog"
)

func main() {
	cmd.RegisterPortFlags(consts.DefaultTabletServerPort)
	flagGid := flag.Int("gid", -1, "gid, range [0, groupNum)")
	flagDBType := flag.String("db", "memory", fmt.Sprintf("db types,  could be one of %v", types.AllDBTypes))
	flagRedisHost := flag.String("redis-host", "127.0.0.1", "redis host")
	flagRedisPort := flag.Int("redis-port", 6379, "redis port")
	flagRedisAuth := flag.String("redis-auth", "", "redis auth")
	flagTestMode := flag.Bool("test", false, "test mode, won't sleep at start")
	cmd.RegisterStoreFlags()
	cmd.RegisterTxnConfigFlags()
	flag.Parse()

	if *flagGid == -1 {
		glog.Fatalf("must provide gid")
	}

	var db types.KV
	switch types.DBType(*flagDBType) {
	case types.DBTypeMemory:
		db = memory.NewMemoryDB()
	case types.DBTypeRedis:
		var err error
		if db, err = redis.NewClient(fmt.Sprintf("%s:%d", *flagRedisHost, *flagRedisPort), *flagRedisAuth); err != nil {
			glog.Fatalf("cant create redis client: '%v'", err)
		}
	default:
		glog.Fatalf("unknown db type '%s'", *flagDBType)
	}

	store := cmd.NewStore()
	cfg := cmd.NewTxnConfig()
	var server *kvcc.Server
	if *flagTestMode {
		server = kvcc.NewServerForTesting(*cmd.FlagPort, db, cfg, *flagGid, store)
	} else {
		server = kvcc.NewServer(*cmd.FlagPort, db, cfg, *flagGid, store)
	}
	if err := server.Start(); err != nil {
		glog.Fatalf("failed to start kvcc server: %v", err)
	}
	<-server.Done
}
