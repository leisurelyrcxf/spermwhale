package consts

type buildOpt bool

func (opt buildOpt) IsDebug() bool {
	return bool(opt)
}

const (
	buildOptDebug   buildOpt = true
	buildOptRelease buildOpt = false
)
