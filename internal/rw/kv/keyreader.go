package kv

type valueReader interface {
	readValue(expectedKeyVersion int64, p []byte) (int, error)
}

type KeyReader struct {
	keyVersion int64
	reader     valueReader
}

func (r *KeyReader) ReadValue(p []byte) (int, error) {
	return r.reader.readValue(r.keyVersion, p)
}
