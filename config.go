//
// Copyright (C) 2019 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

//
// The file declares configuration options for KeyVal storages
//

package dynamo

import (
	"context"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/fogfish/curie"
)

/*

Config options for the connection
*/
type Config struct {
	URI      *URL
	Prefixes curie.Prefixes
	AWS      aws.Config
}

// NewConfig creates Config with default options
func NewConfig(opts ...Option) (*Config, error) {
	aws, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, err
	}

	cfg := &Config{
		Prefixes: curie.Namespaces{},
		AWS:      aws,
	}

	for _, opt := range opts {
		if err := opt(cfg); err != nil {
			return nil, err
		}
	}

	return cfg, nil
}

// Option type to configure the connection
type Option func(cfg *Config) error

// WithURI defines destination URI
func WithURI(uri string) Option {
	return func(cfg *Config) (err error) {
		uri, err := url.Parse(uri)
		if err != nil {
			return
		}

		cfg.URI = (*URL)(uri)
		return
	}
}

// WithPrefixes defines prefixes for CURIEs
func WithPrefixes(prefixes curie.Prefixes) Option {
	return func(cfg *Config) (err error) {
		cfg.Prefixes = prefixes
		return
	}
}

// WithSession defines AWS I/O Session to be used in the context
func WithAwsConfig(aws aws.Config) Option {
	return func(cfg *Config) (err error) {
		cfg.AWS = aws
		return
	}
}

/*

URL custom type with helper functions
*/
type URL url.URL

func (uri *URL) String() string {
	return (*url.URL)(uri).String()
}

// query parameters
func (uri *URL) Query(key, def string) string {
	val := (*url.URL)(uri).Query().Get(key)

	if val == "" {
		return def
	}

	return val
}

// path segments of length
func (uri *URL) Segments() []string {
	return strings.Split((*url.URL)(uri).Path, "/")[1:]
}
