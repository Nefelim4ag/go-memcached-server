package memcachedprotocol

type MemcachedEntry struct {
	key string
	flags uint32
	exptime uint32
	len uint32
	cas uint64
	value []byte
}
