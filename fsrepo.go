package pubsub

import (
	"github.com/dgraph-io/badger/v2"
	"os"
	"path/filepath"
)

func Create(path string) (*Datastore, error) {
	p := "badgers"
	if !filepath.IsAbs(p) {
		p = filepath.Join(path, p)
	}

	err := os.MkdirAll(p, 0755)
	if err != nil {
		return nil, err
	}

	defaultOptions := badger.DefaultOptions("")

	defaultOptions = defaultOptions.WithInMemory(true)
	defaultOptions = defaultOptions.WithValueThreshold(1024)
	defaultOptions = defaultOptions.WithMaxTableSize(8 << 20)

	return NewDatastore(p, defaultOptions)
}
