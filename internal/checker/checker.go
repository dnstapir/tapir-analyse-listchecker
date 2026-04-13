package checker

import (
	"sync"
)

type Conf struct {
	UseBloom bool `toml:"use_bloom"`
}

type Checker struct {
	sync.RWMutex
	m        map[string]struct{}
	useBloom bool
}

func Create(conf Conf) (*Checker, error) {
	c := new(Checker)

	if conf.UseBloom {
		panic("not impl")
	}
	c.useBloom = conf.UseBloom

	c.m = make(map[string]struct{})

	return c, nil
}

func (c *Checker) Add(domain string) {
	if c.useBloom {
		panic("not impl")
		return
	} else {
		c.Lock()
		defer c.Unlock()
		c.m[domain] = struct{}{}
	}
}

func (c *Checker) Check(domain string) bool {
	if c.useBloom {
		panic("not impl")
		return false
	} else {
		c.RLock()
		defer c.RUnlock()
		_, ok := c.m[domain]

		return ok
	}
}

func (c *Checker) Empty() {
	if c.useBloom {
		panic("not impl")
		return
	} else {
		c.Lock()
		defer c.Unlock()
		c.m = make(map[string]struct{})
		return
	}
}
