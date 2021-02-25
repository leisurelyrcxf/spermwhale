package tabletpb

import (
	"github.com/leisurelyrcxf/spermwhale/errors"
)

func (x *SetRequest) Validate() error {
	if err := x.Opt.Validate(); err != nil {
		return err
	}
	if x.Opt.ClearWriteIntent && x.Value.Meta.WriteIntent {
		return errors.Annotatef(errors.ErrInvalidRequest, "x.Opt.ClearWriteIntent && x.Value.Meta.WriteIntent")
	}
	return nil
}
