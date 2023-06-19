package projections

import (
	"github.com/dop251/goja"
)

type gojaFunc func(goja.FunctionCall) goja.Value

func (f gojaFunc) Call(vm *goja.Runtime, values ...any) any {
	params := make([]goja.Value, 0, len(values))
	for _, v := range values {
		params = append(params, vm.ToValue(v))
	}

	out := f(goja.FunctionCall{
		Arguments: params,
	})

	var x any
	vm.ExportTo(out, &x)

	return x
}
