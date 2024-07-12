package internal

import (
	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
)

var numberEncoding = jsoniter.Config{
	EscapeHTML:             true,
	SortMapKeys:            true,
	ValidateJsonRawMessage: true,
	UseNumber:              true,
}.Froze()

func marshalToString(data interface{}) (string, error) {
	return numberEncoding.MarshalToString(data)
}

func GenerateLogId() (string, error) {
	uuid, err := uuid.NewV7()
	if err == nil {
		return uuid.String(), nil
	}
	return "", err
}
