package ddb

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/fogfish/curie"
)

// DynamoDB declares interface of original AWS DynamoDB API used by the library
type DynamoDB interface {
	GetItem(context.Context, *dynamodb.GetItemInput, ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	PutItem(context.Context, *dynamodb.PutItemInput, ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	DeleteItem(context.Context, *dynamodb.DeleteItemInput, ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
	UpdateItem(context.Context, *dynamodb.UpdateItemInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	Query(context.Context, *dynamodb.QueryInput, ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
	BatchGetItem(context.Context, *dynamodb.BatchGetItemInput, ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error)
}

// Option type to configure the S3
type Option func(*Config)

// Config Options
type Config struct {
	prefixes      curie.Prefixes
	table         string
	index         string
	hashKey       string
	sortKey       string
	useStrictType bool
	service       DynamoDB
}

// NewConfig creates Config with default options
func defaultConfig() *Config {
	return &Config{
		prefixes: curie.Namespaces{},
		hashKey:  "prefix",
		sortKey:  "suffix",
	}
}

// WithPrefixes defines prefixes for CURIEs
func WithPrefixes(prefixes curie.Prefixes) Option {
	return func(c *Config) {
		c.prefixes = prefixes
	}
}

// WithTable defines dynamodb table
func WithTable(table string) Option {
	return func(c *Config) {
		c.table = table
	}
}

// WithTable defines dynamodb table
func WithGlobalSecondaryIndex(index string) Option {
	return func(c *Config) {
		c.index = index
	}
}

// WithHashKey defines custom name of HashKey, default one is "prefix"
func WithHashKey(hashKey string) Option {
	return func(c *Config) {
		c.hashKey = hashKey
	}
}

// WithHashKey defines custom name of SortKey, default one is "suffix"
func WithSortKey(sortKey string) Option {
	return func(c *Config) {
		c.sortKey = sortKey
	}
}

// WithTypeSchema demand that storage schema "knows" all type attributes
func WithStrictType(strict bool) Option {
	return func(c *Config) {
		c.useStrictType = strict
	}
}

// Configure AWS Service for broker instance
func WithService(service DynamoDB) Option {
	return func(c *Config) {
		c.service = service
	}
}
