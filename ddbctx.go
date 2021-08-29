package dynamo

import (
	"context"

	"github.com/aws/aws-sdk-go/aws/session"
)

type DBNoContext struct{ KeyValContextual }

func newNoContextDB(io *session.Session, spec *dbURL) KeyVal {
	return &DBNoContext{newDB(io, spec)}
}

func (db *DBNoContext) Get(entity Thing) (err error) {
	return db.KeyValContextual.Get(context.Background(), entity)
}

func (db *DBNoContext) Put(entity Thing, config ...Constrain) (err error) {
	return db.KeyValContextual.Put(context.Background(), entity, config...)
}

func (db *DBNoContext) Remove(entity Thing, config ...Constrain) (err error) {
	return db.KeyValContextual.Remove(context.Background(), entity, config...)
}

func (db *DBNoContext) Update(entity Thing, config ...Constrain) (err error) {
	return db.KeyValContextual.Update(context.Background(), entity, config...)
}

func (db *DBNoContext) Match(key Thing) Seq {
	return db.KeyValContextual.Match(context.Background(), key)
}
