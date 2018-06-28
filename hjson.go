package schd_job

import (
	"bytes"
	"encoding/json"

	hjson "github.com/hjson/hjson-go"
)

func fixJSON(data []byte) []byte {
	data = bytes.Replace(data, []byte("\\u003c"), []byte("<"), -1)
	data = bytes.Replace(data, []byte("\\u003e"), []byte(">"), -1)
	data = bytes.Replace(data, []byte("\\u0026"), []byte("&"), -1)
	data = bytes.Replace(data, []byte("\\u0008"), []byte("\\b"), -1)
	data = bytes.Replace(data, []byte("\\u000c"), []byte("\\f"), -1)
	return data
}

func HjsonToJSON(bs []byte) ([]byte, error) {
	var value interface{}
	if err := hjson.Unmarshal(bs, &value); err != nil {
		return nil, err
	}

	out, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return fixJSON(out), nil
}

func Unmarshal(bs []byte, v interface{}) error {
	data, err := HjsonToJSON(bs)
	if err != nil {
		return err
	}

	return json.Unmarshal(data, v)
}
