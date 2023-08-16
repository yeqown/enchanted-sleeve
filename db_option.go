package esl

type options struct{}

type Option interface {
	apply(*options)
}
