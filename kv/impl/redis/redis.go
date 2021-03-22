package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/golang/glog"

	"github.com/go-redis/redis"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/kv"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/utils"
)

const DebugLevel = 101

type Value struct {
	Meta            uint8  `json:"M"`
	InternalVersion uint8  `json:"I"`
	V               []byte `json:"V"`
}

func NewValue(v types.Value) Value {
	return Value{
		Meta:            v.Flag,
		InternalVersion: uint8(v.InternalVersion),
		V:               v.V,
	}
}

func (v Value) WithVersion(version uint64) types.Value {
	return types.Value{
		Meta: types.Meta{
			Version:         version,
			InternalVersion: types.TxnInternalVersion(v.InternalVersion),
			Flag:            v.Meta,
		},
		V: v.V,
	}
}

func (v Value) Encode() []byte {
	b, err := json.Marshal(v)
	if err != nil {
		glog.Fatalf("encode to json failed: '%v'", err)
	}
	return b
}

func (v *Value) Decode(data []byte) error {
	return json.Unmarshal(data, v)
}

type VersionedValues struct {
	cli                    *redis.Client
	discardedTimestampBits int
}

func NewVersionedValues(cli *redis.Client, discardedTimestampBits int) VersionedValues {
	return VersionedValues{cli: cli, discardedTimestampBits: discardedTimestampBits}
}

func (vvs VersionedValues) encodeRedis(val types.Value) redis.Z {
	return redis.Z{
		Score:  float64(val.Version >> vvs.discardedTimestampBits),
		Member: NewValue(val).Encode(),
	}
}

func (vvs VersionedValues) decodeRedis(z redis.Z) (_ types.Value, err error) {
	var v Value
	if err = v.Decode([]byte(z.Member.(string))); err != nil {
		return types.EmptyValue, err
	}
	return v.WithVersion(uint64(math.Round(z.Score)) << vvs.discardedTimestampBits), nil
}

func (vvs VersionedValues) versionToScore(version uint64) string {
	assert.Must(version&((1<<vvs.discardedTimestampBits)-1) == 0)
	return strconv.FormatUint(version>>vvs.discardedTimestampBits, 10)
}

func (vvs VersionedValues) Get(key string, version uint64) (types.Value, error) {
	versionDesc := vvs.versionToScore(version)
	cmd := vvs.cli.ZRangeByScoreWithScores(key, redis.ZRangeBy{
		Min:   versionDesc,
		Max:   versionDesc,
		Count: 1,
	})
	ret, err := cmd.Result()
	val, err := vvs.getOne(ret, err)
	assert.Must(err != nil || val.Version == version)
	if err != nil && glog.V(DebugLevel) {
		glog.Infof("[Get] get version %d of key '%s' failed: '%v'", version, key, err)
	}
	return val, err
}

func (vvs VersionedValues) Upsert(key string, val types.Value) error {
	versionDesc := vvs.versionToScore(val.Version)
	_, err := vvs.cli.Pipelined(func(pipe redis.Pipeliner) error {
		pipe.ZRemRangeByScore(key, versionDesc, versionDesc)
		glog.V(DebugLevel).Infof("[Upsert] removed version %d of key '%s'", val.Version, key)
		pipe.ZAdd(key, vvs.encodeRedis(val))
		return nil
	})
	return err
}

func (vvs VersionedValues) UpdateFlag(key string, version uint64, modifyFlag func(val types.Value) types.Value, onNotExists func(err error) error) error {
	versionDesc := vvs.versionToScore(version)
	return utils.WithContextRetryEx(context.Background(), time.Millisecond*100, time.Second, func(ctx context.Context) error {
		return vvs.cli.Watch(func(tx *redis.Tx) error {
			cmd := tx.ZRevRangeByScoreWithScores(key, redis.ZRangeBy{
				Min:   versionDesc,
				Max:   versionDesc,
				Count: 1,
			})
			prev, err := vvs.getOne(cmd.Result())
			if err != nil {
				glog.Errorf("want to clear write intent for version %d of key '%s', but got err: '%v'", version, key, err)
				if errors.IsNotExistsErr(err) {
					return onNotExists(err)
				}
				return err
			}
			assert.Must(prev.Version == version)
			cur := modifyFlag(prev)
			if cur.Flag == prev.Flag {
				// already cleared
				return nil
			}

			_, err = tx.Pipelined(func(pipe redis.Pipeliner) error {
				pipe.ZRemRangeByScore(key, versionDesc, versionDesc)
				glog.V(DebugLevel).Infof("[UpdateFlag] removed version %d of key '%s'", version, key)
				pipe.ZAdd(key, vvs.encodeRedis(cur))
				glog.V(DebugLevel).Infof("[UpdateFlag] modified version %d of key '%s', new value: '%s'(flag: %d)", version, key, string(cur.V), cur.Meta.Flag)
				return nil
			})
			return err
		}, key)
	}, func(err error) bool {
		return err == redis.TxFailedErr
	})
}

func (vvs VersionedValues) Max(key string) (types.Value, error) {
	cmd := vvs.cli.ZRevRangeWithScores(key, 0, 0)
	ret, err := cmd.Result()
	return vvs.getOne(ret, err)
}

func (vvs VersionedValues) Min(key string) (types.Value, error) {
	cmd := vvs.cli.ZRangeWithScores(key, 0, 0)
	ret, err := cmd.Result()
	return vvs.getOne(ret, err)
}

func (vvs VersionedValues) FindMaxBelow(key string, upperVersion uint64) (types.Value, error) {
	cmd := vvs.cli.ZRevRangeByScoreWithScores(key, redis.ZRangeBy{
		Min:   "-inf",
		Max:   vvs.versionToScore(upperVersion),
		Count: 1,
	})
	ret, err := cmd.Result()
	return vvs.getOne(ret, err)
}

func (vvs VersionedValues) Remove(key string, version uint64) error {
	versionDesc := vvs.versionToScore(version)
	if err := vvs.cli.ZRemRangeByScore(key, versionDesc, versionDesc).Err(); err != nil {
		glog.V(DebugLevel).Infof("[Remove] remove version %d of key '%s' failed: '%s'", version, key, err)
		return err
	}
	glog.V(DebugLevel).Infof("[Remove] removed version %d of key '%s'", version, key)
	return nil
}

func (vvs VersionedValues) RemoveIf(key string, version uint64, pred func(prev types.Value) error) error {
	versionDesc := vvs.versionToScore(version)
	return utils.WithContextRetryEx(context.Background(), time.Millisecond*100, time.Second, func(ctx context.Context) error {
		return vvs.cli.Watch(func(tx *redis.Tx) error {
			cmd := tx.ZRevRangeByScoreWithScores(key, redis.ZRangeBy{
				Min:   versionDesc,
				Max:   versionDesc,
				Count: 1,
			})
			prev, err := vvs.getOne(cmd.Result())
			if err != nil {
				if err == errors.ErrVersionNotExists {
					return nil
				}
				return err
			}

			if err := pred(prev); err != nil {
				return err
			}

			_, err = tx.Pipelined(func(pipe redis.Pipeliner) error {
				pipe.ZRemRangeByScore(key, versionDesc, versionDesc)
				glog.V(DebugLevel).Infof("[RemoveIf] removed version %d of key '%s'", version, key)
				return nil
			})
			return err
		}, key)
	}, func(err error) bool {
		return err == redis.TxFailedErr
	})
}

func (vvs VersionedValues) getOne(zs []redis.Z, err error) (types.Value, error) {
	if err != nil {
		return types.EmptyValue, err
	}
	if len(zs) == 0 {
		return types.EmptyValue, errors.ErrVersionNotExists
	}
	if len(zs) > 1 {
		panic(fmt.Sprintf("VersionedValues::getOne() len(ret%v) > 1", zs))
	}
	return vvs.decodeRedis(zs[0])
}

func (vvs VersionedValues) Close() error {
	return vvs.cli.Close()
}

func mustNewClient(sourceAddr string, auth string) *kv.DB {
	cli, err := newClient(sourceAddr, auth, 0)
	assert.MustNoError(err)
	return cli
}

func NewClient(sourceAddr string, auth string) (*kv.DB, error) {
	return newClient(sourceAddr, auth, consts.LoosedOracleDiscardedBits)
}

func newClient(sourceAddr string, auth string, discardedTimestampBits int) (*kv.DB, error) {
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
	return kv.NewDB(NewVersionedValues(cli, discardedTimestampBits)), nil
}

func (vvs VersionedValues) Insert(key string, val types.Value) error {
	versionDesc := vvs.versionToScore(val.Version)
	return utils.WithContextRetryEx(context.Background(), time.Millisecond*100, time.Second, func(ctx context.Context) error {
		return vvs.cli.Watch(func(tx *redis.Tx) error {
			cmd := tx.ZRevRangeByScoreWithScores(key, redis.ZRangeBy{
				Min:   versionDesc,
				Max:   versionDesc,
				Count: 1,
			})
			if _, err := vvs.getOne(cmd.Result()); err != errors.ErrVersionNotExists {
				return errors.ErrVersionAlreadyExists
			}

			_, err := tx.Pipelined(func(pipe redis.Pipeliner) error {
				pipe.ZAdd(key, vvs.encodeRedis(val))
				return nil
			})
			return err
		}, key)
	}, func(err error) bool {
		return err == redis.TxFailedErr
	})
}
