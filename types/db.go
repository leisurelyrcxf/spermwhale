package types

type DBType string

var AllDBTypes []DBType

var (
	addDBType = func(s string) DBType {
		t := DBType(s)
		AllDBTypes = append(AllDBTypes, t)
		return t
	}
	DBTypeMemory = addDBType("memory")
	DBTypeRedis  = addDBType("redis")
	DBTypeMongo  = addDBType("mongo")
)
