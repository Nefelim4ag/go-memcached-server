package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"nefelim4ag/go-memcached-server/memcachedprotocol"
	"nefelim4ag/go-memcached-server/memstore"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"github.com/pawelgaczynski/gain"
	"github.com/pawelgaczynski/gain/logger"
	"github.com/rs/zerolog"
	log "github.com/sirupsen/logrus"
)

var (
	store *memstore.SharedStore
)

type EventHandler struct {
	server gain.Server

	logger zerolog.Logger
}

func (e *EventHandler) OnStart(server gain.Server) {
	e.server = server
	e.logger = zerolog.New(os.Stdout).With().Logger().Level(zerolog.InfoLevel)
}

type protocolContext struct {
  r *bufio.Reader
  binary *memcachedprotocol.BinaryProcessor
  ascii *memcachedprotocol.ASCIIProcessor
}

func (e *EventHandler) OnAccept(conn gain.Conn) {
	_r := bufio.NewReaderSize(conn, 64 * 1024)
	_w := bufio.NewWriterSize(conn, 64 * 1024)

	binaryProcessor := memcachedprotocol.CreateBinaryProcessor(_r, _w, store)
	asciiProcessor := memcachedprotocol.CreateASCIIProcessor(_r, _w, store)

	c := protocolContext{
		r: _r,
		binary: binaryProcessor,
        ascii: asciiProcessor,
	}

	conn.SetContext(c)
}

func (e *EventHandler) OnRead(conn gain.Conn, n int) {
	c := conn.Context()
	_r := c.(protocolContext).r
	binaryProcessor := c.(protocolContext).binary
	asciiProcessor := c.(protocolContext).ascii

	magic, err := _r.ReadByte()
	_r.UnreadByte()
	switch err {
	case nil:
	case io.EOF:
		log.Infof("client %s closed connection", conn.RemoteAddr())
		return
	default:
		log.Errorf("error: %v\n", err)
		return
	}

	if magic < 0x80 {
		err = asciiProcessor.CommandAscii()
		if err != nil {
			conn.Close()
			return
		}
	} else if magic == 0x80 {
		err = binaryProcessor.CommandBinary()
		if err != nil {
			log.Println(err)
			conn.Close()
			return
		}
	} else {
		log.Errorf("client %s aborted - unsupported protocol 0x%02x ~ %s", conn.RemoteAddr(), magic, string(magic))
	}
}

func (e *EventHandler) OnWrite(conn gain.Conn, n int) {
}

func (e *EventHandler) OnClose(conn gain.Conn, err error) {
}

func main() {
	log.SetOutput(os.Stdout)
	log.SetLevel(log.DebugLevel)

	_memstore_size := flag.Uint64("m", 512, "items memory in megabytes, default is 512")
	_memstore_item_size := flag.Uint("I", 1024*1024, "max item sizem, default is 1m")
	logLevel := flag.Uint("loglevel", 3, "log level, 5=debug, 4=info, 3=warning, 2=error, 1=fatal, 0=panic")
	pprof := flag.Bool("pprof", false, "enable pprof server")
	flag.Parse()

	log.SetLevel(log.Level(*logLevel))

	memstore_size := uint64(*_memstore_size) * 1024 * 1024
	memstore_item_size := uint32(*_memstore_item_size)

	// Wait for a SIGINT or SIGTERM signal to gracefully shut down the server
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	if *pprof {
		go func() {
			log.Error(http.ListenAndServe("127.0.0.1:6060", nil))
		}()
	}
	store = memstore.NewSharedStore()
	store.SetMemoryLimit(memstore_size)
	store.SetItemSizeLimit(memstore_item_size)

	srvInstance := EventHandler{}

	go func() {
		err := gain.ListenAndServe(
			fmt.Sprintf("tcp://127.0.0.1:%d", 11211), &srvInstance, gain.WithLoggerLevel(logger.WarnLevel))
		if err != nil {
			log.Panic(err)
		}
	}()

    <-sigChan
	fmt.Println("Shutting down server...")
	srvInstance.server.Shutdown()
}
