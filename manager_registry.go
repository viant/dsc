package dsc

import (
	"github.com/viant/toolbox"
)

type commonManagerRegistry struct {
	registry map[string](Manager)
}

func (r commonManagerRegistry) Register(name string, manager Manager) {
	if previousManager, found := r.registry[name]; found {
		previousManager.ConnectionProvider().Close()
	}
	r.registry[name] = manager
}

func (r commonManagerRegistry) Get(name string) Manager {
	if result, ok := r.registry[name]; ok {
		return result
	}
	return nil
}

func (r commonManagerRegistry) Names() []string {
	return toolbox.MapKeysToStringSlice(r.registry)
}

//NewManagerRegistry create a new ManagerRegistry
func NewManagerRegistry() ManagerRegistry {
	var result = &commonManagerRegistry{registry: make(map[string](Manager))}
	return result
}
