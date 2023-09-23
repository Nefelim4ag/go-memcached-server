package memcachedprotocol

import (
	"bufio"
	"fmt"
	"io"
	"nefelim4ag/go-memcached-server/memstore"
	"net"

	"log/slog"
)

type Processor struct {
	store *memstore.SharedStore
	rb    *bufio.Reader
	wb    *bufio.Writer
	conn  *net.TCPConn

	raw_request  [24]byte
	flags        [4]byte
	exptime      [4]byte
	request      RequestHeader
	response     ResponseHeader
	raw_response [24]byte
	key          []byte
	debug        bool
}

func CreateProcessor(conn *net.TCPConn, store *memstore.SharedStore) *Processor {
	rb := bufio.NewReaderSize(conn, 64*1024)
	wb := bufio.NewWriterSize(conn, 4*1024)
	b := Processor{
		store: store,
		rb:    rb,
		wb:    wb,
		conn:  conn,
		debug: slog.Default().Handler().Enabled(nil, slog.LevelDebug),
	}

	return &b
}

func (ctx *Processor) Handle() {
	// Waiting for the client request
	for {
		magic, err := ctx.rb.ReadByte()
		ctx.rb.UnreadByte()
		switch err {
		case nil:
		case io.EOF:
			slog.Debug("Closed", "connection", ctx.conn.RemoteAddr())
			return
		default:
			slog.Error(err.Error())
			return
		}

		if magic < 0x80 {
			err = ctx.CommandAscii()
			if err != nil {
				return
			}
		} else if magic == 0x80 {
			err = ctx.CommandBinary()
			if err != nil {
				slog.Error(err.Error())
				return
			}
		} else {
			slog.Error("Unsupported protocol", "magic", fmt.Sprintf("%02x", magic), "client", ctx.conn.RemoteAddr())
		}

		err = ctx.wb.Flush()
		if err != nil {
			slog.Error(err.Error())
			return
		}
	}
}

func (ctx *Processor) CloseProcessor() {

}
