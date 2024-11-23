//
// Copyright (C) 2022 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

package ddb

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/fogfish/curie"
	"github.com/fogfish/opts"
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
type Option = opts.Option[Options]

// Config Options
type Options struct {
	table         string
	index         string
	hashKey       string
	sortKey       string
	useStrictType bool
	service       DynamoDB
}

func (c *Options) checkRequired() error {
	return opts.Required(c,
		WithTable(""),
	)
}

var (
	// Set DynamoDB table for session, the option is required
	WithTable = opts.ForName[Options, string]("table")

	// Set Global Secondary Index for the session
	WithGlobalSecondaryIndex = opts.ForName[Options, string]("index")

	// Configure the custom name for HashKey, default one is "prefix"
	WithHashKey = opts.ForName[Options, string]("hashKey")

	// Configure the custom name for SortKey, default one is "suffix"
	WithSortKey = opts.ForName[Options, string]("sortKey")

	// Configure CURIE prefixes, does nothing for DynamoDB
	WithPrefixes = opts.FMap(func(*Options, curie.Prefixes) error { return nil })

	// Enables strict serialization of the type, the I/O would fails
	// if type attributes does not match the storage schema.
	// It demand that storage schema "knows" all type attributes.
	WithStrictType = opts.ForName[Options, bool]("useStrictType")

	// Set DynamoDB client for the client
	WithService = opts.ForType[Options, DynamoDB]()

	// Set DynamoDB client for the client
	WithDynamoDB = opts.ForType[Options, DynamoDB]()

	// Configure client's DynamoDB to use provided the aws.Config
	WithConfig = opts.FMap(optsFromConfig)

	// Use default aws.Config for all DynamoDB clients
	WithDefaultDDB = opts.From(optsDefaultDDB)
)

// creates default config options
func optsDefault() Options {
	return Options{
		hashKey: "prefix",
		sortKey: "suffix",
	}
}

func optsDefaultDDB(c *Options) error {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return err
	}
	return optsFromConfig(c, cfg)
}

func optsFromConfig(c *Options, cfg aws.Config) error {
	if c.service == nil {
		c.service = dynamodb.NewFromConfig(cfg)
	}
	return nil
}
