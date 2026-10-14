package work

import "context"

func (h *harness) sweep() { h.s.Sweep(context.Background()) }
