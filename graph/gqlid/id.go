package gqlid

import "encoding/base64"

// Encode an input string to a GraphQL ID.
func Encode(s string) string {
	return base64.StdEncoding.EncodeToString([]byte(s))
}

// Decode an input GraphQL ID to the underlying string.
func Decode(s string) (id string, err error) {
	var b []byte
	b, err = base64.StdEncoding.DecodeString(s)
	id = string(b)
	return
}
