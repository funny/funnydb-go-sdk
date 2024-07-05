package utils

import jsoniter "github.com/json-iterator/go"

var NumberEncoding = jsoniter.Config{
	EscapeHTML:             true,
	SortMapKeys:            true,
	ValidateJsonRawMessage: true,
	UseNumber:              true,
}.Froze()

func MarshalToString(data interface{}) (string, error) {
	return NumberEncoding.MarshalToString(data)
}
