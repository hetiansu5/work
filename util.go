package work

import "encoding/json"

func arrayToMap(arr []string) map[string]bool {
	m := make(map[string]bool)
	for _, v := range arr {
		m[v] = true
	}
	return m
}

func getFreeTopics(topics map[string]bool) []string {
	arr := make([]string, 0)
	for k, v := range m {
		if v == true {
			arr = append(arr, k)
		}
	}
	return arr
}

func JsonEncode(v interface{}) (string, error) {
	bytes, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}