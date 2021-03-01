package tabletpb

import (
	"github.com/leisurelyrcxf/spermwhale/errors"
)

func (x *SetRequest) Validate() error {
	if err := x.Opt.Validate(); err != nil {
		return err
	}
	if x.Opt.IsClearWriteIntent() && x.Value.Meta.HasWriteIntent() {
		return errors.Annotatef(errors.ErrInvalidRequest, "x.Opt.IsClearWriteIntent() && x.Value.Meta.HasWriteIntent()")
	}
	return nil
}
