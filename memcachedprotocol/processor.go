package memcachedprotocol

import (
	"bufio"
	"nefelim4ag/go-memcached-server/memstore"
	"net"
)

type Processor struct {
    store *memstore.SharedStore
    rb    *bufio.Reader
    conn  *net.TCPConn

    raw_request  [24]byte
    flags        [4]byte
    exptime      [4]byte
    request      RequestHeader
    response     ResponseHeader
    response_raw []byte
    key          []byte
}

func CreateProcessor(rb *bufio.Reader, conn *net.TCPConn, store *memstore.SharedStore) *Processor {
    b := Processor{
        store:        store,
        rb:           rb,
        conn:         conn,
        response_raw: make([]byte, 128),
    }

    return &b
}

func (ctx *Processor) CloseProcessor() {

}
