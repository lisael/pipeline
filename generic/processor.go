package generic

type PipeProcessor func(input interface{}) (output interface{}, err error)
