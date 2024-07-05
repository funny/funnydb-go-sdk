package utils

import "github.com/google/uuid"

func GenerateLogId() (string, error) {
	uuid, err := uuid.NewV7()
	if err == nil {
		return uuid.String(), nil
	}
	return "", err
}
