package dsc

import (
	"github.com/viant/toolbox"
	"sync"
)

type commonManagerRegistry struct {
	mux      *sync.RWMutex
	registry map[string](Manager)
}

func (r commonManagerRegistry) Register(name string, manager Manager) {
	r.mux.Lock()
	defer r.mux.Unlock()
	if previousManager, found := r.registry[name]; found {
		previousManager.ConnectionProvider().Close()
	}
	r.registry[name] = manager
}



func (r commonManagerRegistry) Get(name string) Manager {
	r.mux.RLock()
	result, ok := r.registry[name]
	r.mux.RUnlock()
	if ok {
		return result
	}
	return nil
}

func (r commonManagerRegistry) Names() []string {
	r.mux.RLock()
	defer r.mux.RUnlock()
	return toolbox.MapKeysToStringSlice(r.registry)
}

//NewManagerRegistry create a new ManagerRegistry
func NewManagerRegistry() ManagerRegistry {
	var result = &commonManagerRegistry{
		registry: make(map[string](Manager)),
		mux:      &sync.RWMutex{},
	}
	return result
}
