package redis

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/leisurelyrcxf/spermwhale/types"

	"github.com/golang/glog"

	"github.com/go-redis/redis"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/kv"
	"github.com/leisurelyrcxf/spermwhale/utils"
)

const DebugLevel = 101

type RVVS struct {
	cli                    *redis.Client
	discardedTimestampBits int

	once sync.Once
}

func NewVersionedValues(cli *redis.Client, discardedTimestampBits int) *RVVS {
	return &RVVS{cli: cli, discardedTimestampBits: discardedTimestampBits}
}

func (vvs RVVS) encodeRedis(version uint64, val kv.Value) redis.Z {
	return redis.Z{
		Score:  float64(version >> vvs.discardedTimestampBits),
		Member: val.Encode(),
	}
}

func (vvs RVVS) decodeRedis(z redis.Z) (_ kv.Value, version uint64, err error) {
	var v kv.Value
	if err = v.Decode([]byte(z.Member.(string))); err != nil {
		return kv.EmptyValue, 0, err
	}
	return v, uint64(math.Round(z.Score)) << vvs.discardedTimestampBits, nil
}

func (vvs RVVS) versionToScore(version uint64) string {
	assert.Must(version&((1<<vvs.discardedTimestampBits)-1) == 0)
	return strconv.FormatUint(version>>vvs.discardedTimestampBits, 10)
}

func (vvs RVVS) GetTxnRecord(ctx context.Context, version uint64) (kv.Value, error) {
	return vvs.GetKey(ctx, types.TxnId(version).String(), version)
}

func (vvs RVVS) UpsertTxnRecord(ctx context.Context, version uint64, val kv.Value) error {
	return vvs.UpsertKey(ctx, types.TxnId(version).String(), version, val)
}

func (vvs RVVS) RemoveTxnRecord(ctx context.Context, version uint64) error {
	return vvs.RemoveKey(ctx, types.TxnId(version).String(), version)
}

func (vvs RVVS) GetKey(_ context.Context, key string, version uint64) (kv.Value, error) {
	versionDesc := vvs.versionToScore(version)
	cmd := vvs.cli.ZRangeByScoreWithScores(key, redis.ZRangeBy{
		Min:   versionDesc,
		Max:   versionDesc,
		Count: 1,
	})
	ret, err := cmd.Result()
	val, prevVersion, err := vvs.getOne(ret, err)
	if err != nil && glog.V(DebugLevel) {
		glog.Infof("[Get] get version %d of key '%s' failed: '%v'", version, key, err)
	}
	assert.Must(err != nil || prevVersion == version)
	return val, err
}

func (vvs RVVS) UpsertKey(_ context.Context, key string, version uint64, val kv.Value) error {
	versionDesc := vvs.versionToScore(version)
	_, err := vvs.cli.Pipelined(func(pipe redis.Pipeliner) error {
		pipe.ZRemRangeByScore(key, versionDesc, versionDesc)
		glog.V(DebugLevel).Infof("[UpsertKey] removed version %d of key '%s'", version, key)
		pipe.ZAdd(key, vvs.encodeRedis(version, val))
		return nil
	})
	return err
}

func (vvs RVVS) UpdateFlagOfKey(_ context.Context, key string, version uint64, newFlag uint8) error {
	return errors.ErrNotSupported
}

func (vvs RVVS) ReadModifyWriteKey(_ context.Context, key string, version uint64, modifyFlag func(val kv.Value) kv.Value, onNotExists func(err error) error) error {
	versionDesc := vvs.versionToScore(version)
	return utils.WithContextRetryEx(context.Background(), time.Millisecond*100, time.Second, func(_ context.Context) error {
		return vvs.cli.Watch(func(tx *redis.Tx) error {
			cmd := tx.ZRevRangeByScoreWithScores(key, redis.ZRangeBy{
				Min:   versionDesc,
				Max:   versionDesc,
				Count: 1,
			})
			prev, prevVersion, err := vvs.getOne(cmd.Result())
			if err != nil {
				if glog.V(1) {
					glog.Errorf("want to clear write intent for version %d of key '%s', but got err: '%v'", version, key, err)
				}
				if errors.IsNotExistsErr(err) {
					return onNotExists(err)
				}
				return err
			}
			assert.Must(prevVersion == version)
			cur := modifyFlag(prev)
			if cur.Flag == prev.Flag {
				// already cleared
				return nil
			}

			_, err = tx.Pipelined(func(pipe redis.Pipeliner) error {
				pipe.ZRemRangeByScore(key, versionDesc, versionDesc)
				glog.V(DebugLevel).Infof("[ReadModifyWriteKey] removed version %d of key '%s'", version, key)
				pipe.ZAdd(key, vvs.encodeRedis(version, cur))
				glog.V(DebugLevel).Infof("[ReadModifyWriteKey] modified version %d of key '%s', new value: '%s'(flag: %d)", version, key, string(cur.V), cur.Meta.Flag)
				return nil
			})
			return err
		}, key)
	}, func(err error) bool {
		return err == redis.TxFailedErr
	})
}

func (vvs RVVS) Max(_ context.Context, key string) (kv.Value, uint64, error) {
	cmd := vvs.cli.ZRevRangeWithScores(key, 0, 0)
	ret, err := cmd.Result()
	return vvs.getOne(ret, err)
}

func (vvs RVVS) Min(_ context.Context, key string) (kv.Value, uint64, error) {
	cmd := vvs.cli.ZRangeWithScores(key, 0, 0)
	ret, err := cmd.Result()
	return vvs.getOne(ret, err)
}

func (vvs RVVS) FindMaxBelowOfKey(_ context.Context, key string, upperVersion uint64) (kv.Value, uint64, error) {
	cmd := vvs.cli.ZRevRangeByScoreWithScores(key, redis.ZRangeBy{
		Min:   "-inf",
		Max:   vvs.versionToScore(upperVersion),
		Count: 1,
	})
	ret, err := cmd.Result()
	return vvs.getOne(ret, err)
}

func (vvs RVVS) RemoveKey(_ context.Context, key string, version uint64) error {
	versionDesc := vvs.versionToScore(version)
	if err := vvs.cli.ZRemRangeByScore(key, versionDesc, versionDesc).Err(); err != nil {
		glog.V(DebugLevel).Infof("[RemoveKey] remove version %d of key '%s' failed: '%s'", version, key, err)
		return err
	}
	glog.V(DebugLevel).Infof("[RemoveKey] removed version %d of key '%s'", version, key)
	return nil
}

func (vvs RVVS) RemoveKeyIf(_ context.Context, key string, version uint64, pred func(prev kv.Value) error) error {
	versionDesc := vvs.versionToScore(version)
	return utils.WithContextRetryEx(context.Background(), time.Millisecond*100, time.Second, func(_ context.Context) error {
		return vvs.cli.Watch(func(tx *redis.Tx) error {
			cmd := tx.ZRevRangeByScoreWithScores(key, redis.ZRangeBy{
				Min:   versionDesc,
				Max:   versionDesc,
				Count: 1,
			})
			prev, gotVersion, err := vvs.getOne(cmd.Result())
			if err != nil {
				if err == errors.ErrKeyOrVersionNotExist {
					return nil
				}
				return err
			}
			assert.Must(version == gotVersion)

			if err := pred(prev); err != nil {
				return err
			}

			_, err = tx.Pipelined(func(pipe redis.Pipeliner) error {
				pipe.ZRemRangeByScore(key, versionDesc, versionDesc)
				glog.V(DebugLevel).Infof("[RemoveKeyIf] removed version %d of key '%s'", version, key)
				return nil
			})
			return err
		}, key)
	}, func(err error) bool {
		return err == redis.TxFailedErr
	})
}

func (vvs RVVS) getOne(zs []redis.Z, err error) (kv.Value, uint64, error) {
	if err != nil {
		return kv.EmptyValue, 0, err
	}
	if len(zs) == 0 {
		return kv.EmptyValue, 0, errors.ErrKeyOrVersionNotExist
	}
	if len(zs) > 1 {
		panic(fmt.Sprintf("RVVS::getOne() len(ret%v) > 1", zs))
	}
	return vvs.decodeRedis(zs[0])
}

func (vvs *RVVS) Close() error {
	var err error
	vvs.once.Do(func() {
		err = vvs.cli.Close()
	})
	return err
}

func mustNewDB(sourceAddr string, auth string) *kv.DB {
	cli, err := newDB(sourceAddr, auth, 0)
	assert.MustNoError(err)
	return cli
}

func NewDB(sourceAddr string, auth string) (*kv.DB, error) {
	return newDB(sourceAddr, auth, consts.LoosedOracleDiscardedBits)
}

func newDB(sourceAddr string, auth string, discardedTimestampBits int) (*kv.DB, error) {
	cli := redis.NewClient(&redis.Options{
		Network:      "tcp",
		Addr:         sourceAddr,
		Password:     auth,
		DialTimeout:  25 * time.Second,
		ReadTimeout:  25 * time.Second,
		WriteTimeout: 25 * time.Second,
	})
	if err := cli.Ping().Err(); err != nil {
		return nil, errors.Annotatef(err, "can't connect to %s", sourceAddr)
	}
	s := NewVersionedValues(cli, discardedTimestampBits)
	return kv.NewDB(s, s), nil
}

func (vvs RVVS) Insert(key string, version uint64, val kv.Value) error {
	versionDesc := vvs.versionToScore(version)
	return utils.WithContextRetryEx(context.Background(), time.Millisecond*100, time.Second, func(_ context.Context) error {
		return vvs.cli.Watch(func(tx *redis.Tx) error {
			cmd := tx.ZRevRangeByScoreWithScores(key, redis.ZRangeBy{
				Min:   versionDesc,
				Max:   versionDesc,
				Count: 1,
			})
			if _, _, err := vvs.getOne(cmd.Result()); err != errors.ErrKeyOrVersionNotExist {
				return errors.ErrVersionAlreadyExists
			}

			_, err := tx.Pipelined(func(pipe redis.Pipeliner) error {
				pipe.ZAdd(key, vvs.encodeRedis(version, val))
				return nil
			})
			return err
		}, key)
	}, func(err error) bool {
		return err == redis.TxFailedErr
	})
}
