package main

import (
	"context"
	esl "github.com/yeqown/enchanted-sleeve"
)

type eslDBType uint

var eslDbContextKey eslDBType = 0

func contextWithDB(ctx context.Context, db *esl.DB) context.Context {
	return context.WithValue(ctx, eslDbContextKey, db)
}

func dbFromContext(ctx context.Context) *esl.DB {
	v := ctx.Value(eslDbContextKey)
	if v == nil {
		panic("no db in context")
	}

	return v.(*esl.DB)
}
