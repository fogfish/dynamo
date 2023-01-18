//
// Copyright (C) 2022 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

//
// The file declares config options
//

package dynamo

import "github.com/fogfish/curie"

// Config options for the client
type Config struct {
	Service        any
	Prefixes       curie.Prefixes
	WithStrictType bool
}

func (*Config) Config() {}

func NewConfig() Config {
	return Config{
		Prefixes:       curie.Namespaces{},
		WithStrictType: true,
	}
}

// Option type to configure the connection
type Option func(cfg interface{ Config() })

// Configure AWS Service for the client
func WithService(service any) Option {
	return func(conf interface{ Config() }) {
		switch c := conf.(type) {
		case *Config:
			c.Service = service
		}
	}
}

// WithPrefixes defines prefixes for CURIEs
func WithPrefixes(prefixes curie.Prefixes) Option {
	return func(conf interface{ Config() }) {
		switch c := conf.(type) {
		case *Config:
			c.Prefixes = prefixes
		}
	}
}

// WithTypeSchema demand that storage schema "knows" all type attributes
func WithStrictType(use bool) Option {
	return func(conf interface{ Config() }) {
		switch c := conf.(type) {
		case *Config:
			c.WithStrictType = use
		}
	}
}
