package v1

import (
	"errors"
	"fmt"
	"sync"
)

// Reloader re-reads one configuration file. When the file is invalid it
// returns the error and keeps its previous values.
type Reloader func() error

// Reloaders is the set a SIGHUP runs. The zero value is ready to use.
type Reloaders struct {
	mu   sync.Mutex
	list []namedReloader
}

type namedReloader struct {
	name string
	fn   Reloader
}

func (rs *Reloaders) Add(name string, fn Reloader) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.list = append(rs.list, namedReloader{name, fn})
}

// Reload runs every reloader in registration order, one Reload at a time, and
// returns their errors joined, each prefixed with its name. A failing or
// panicking reloader does not stop the others.
func (rs *Reloaders) Reload() error {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	var errs []error
	for _, r := range rs.list {
		if err := r.call(); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", r.name, err))
		}
	}
	return errors.Join(errs...)
}

func (r namedReloader) call() (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("panic: %v", p)
		}
	}()
	return r.fn()
}
