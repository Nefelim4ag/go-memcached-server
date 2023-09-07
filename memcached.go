package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"nefelim4ag/go-memcached-server/memcachedprotocol"
	"nefelim4ag/go-memcached-server/memstore"
	"nefelim4ag/go-memcached-server/tcpserver"
	"net"
	"os"
	"os/signal"
	"syscall"

	"net/http"
	_ "net/http/pprof"
)

var (
	store *memstore.SharedStore
)

func main() {
	_memstore_size := flag.Uint64("m", 512, "items memory in megabytes, default is 512")
	_memstore_item_size := flag.Uint64("I", 1024*1024, "max item sizem, default is 1m")
	flag.Parse()

	memstore_size := uint64(*_memstore_size) * 1024 * 1024
	memstore_item_size := uint64(*_memstore_item_size)

	// Wait for a SIGINT or SIGTERM signal to gracefully shut down the server
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.Println(http.ListenAndServe("127.0.0.1:6060", nil))
	}()

	store = memstore.NewSharedStore()
	store.SetMemoryLimit(memstore_size)
	store.SetItemSizeLimit(memstore_item_size)

	srv, err := tcpserver.ListenAndServe(":11211", 32)
	if err!= nil {
        log.Fatal(err)
    }

	acceptThreads := 4
	for acceptThreads > 0 {
		acceptThreads -= 1
		go srv.AcceptConnections(HandleConnection)
	}

	<-sigChan
	fmt.Println("Shutting down server...")
	srv.Stop()
	fmt.Println("Server stopped.")
}

func HandleConnection(conn net.Conn, err error) {
	if err!= nil {
        log.Println(err)
        return
    }

	defer conn.Close()

	_r := bufio.NewReader(conn)
	_w := bufio.NewWriter(conn)
	client := bufio.NewReadWriter(_r, _w)
	for {
		// Waiting for the client request
		magic, err := client.Reader.ReadByte()

		switch err {
		case nil:
		case io.EOF:
			log.Printf("client %s closed connection", conn.RemoteAddr())
			return
		default:
			log.Printf("error: %v\n", err)
			return
		}
		if magic < 0x80 {
			err = memcachedprotocol.CommandAscii(magic, client, store)
			if err != nil {
				return
			}
		} else if magic == 0x80 {
			err = memcachedprotocol.CommandBinary(magic, client, store)
			if err != nil {
				log.Println(err)
				return
			}
		} else {
			log.Printf("client %s aborted - unsupported protocol", conn.RemoteAddr())
		}
	}
}
