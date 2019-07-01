package work

import (
	"encoding/json"
	"github.com/google/uuid"
)


func JsonEncode(v interface{}) (string, error) {
	bytes, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

func GenUUID() string {
	u, _ := uuid.NewRandom()
	return u.String()
}
