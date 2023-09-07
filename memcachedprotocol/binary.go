package memcachedprotocol

import (
	"bufio"
	"fmt"
	"nefelim4ag/go-memcached-server/memstore"
)

func CommandBinary(magic byte, client *bufio.ReadWriter, store *memstore.SharedStore) error {
	return fmt.Errorf("Not implemented")
}
