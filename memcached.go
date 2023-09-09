package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"nefelim4ag/go-memcached-server/memcachedprotocol"
	"nefelim4ag/go-memcached-server/memstore"
	"nefelim4ag/go-memcached-server/tcpserver"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sync"
	"syscall"

	log "github.com/sirupsen/logrus"
)

var (
	store *memstore.SharedStore[memcachedprotocol.MemcachedEntry]
)

func ConnectionHandler(conn *net.TCPConn, wg *sync.WaitGroup, err error) {
	if err != nil {
		log.Errorf(err.Error())
		return
	}

	wg.Add(1)
	defer wg.Done()
	defer conn.Close()

	_r := bufio.NewReader(conn)
	_w := bufio.NewWriter(conn)

	// Reuse context between binary commands
	binaryProcessor := memcachedprotocol.CreateBinaryProcessor(_r, _w, store)
	// go binaryProcessor.ASyncWriter()
	// defer binaryProcessor.Close()
	asciiProcessor := memcachedprotocol.CreateASCIIProcessor(_r, _w, store)

	// Waiting for the client request
	for {
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
				return
			}
		} else if magic == 0x80 {
			err = binaryProcessor.CommandBinary()
			if err != nil {
				log.Println(err)
				return
			}
		} else {
			log.Errorf("client %s aborted - unsupported protocol 0x%02x ~ %s", conn.RemoteAddr(), magic, string(magic))
		}
	}
}

func main() {
    log.SetOutput(os.Stdout)
    log.SetLevel(log.DebugLevel)

	_memstore_size := flag.Uint64("m", 512, "items memory in megabytes, default is 512")
	_memstore_item_size := flag.Uint64("I", 1024*1024, "max item sizem, default is 1m")
	logLevel := flag.Uint("loglevel", 3, "log level, 5=debug, 4=info, 3=warning, 2=error, 1=fatal, 0=panic")
	pprof := flag.Bool("pprof", false, "enable pprof server")
	flag.Parse()

	log.SetLevel(log.Level(*logLevel))

	memstore_size := uint64(*_memstore_size) * 1024 * 1024
	memstore_item_size := uint64(*_memstore_item_size)

	// Wait for a SIGINT or SIGTERM signal to gracefully shut down the server
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	if *pprof {
		go func() {
			log.Error(http.ListenAndServe("127.0.0.1:6060", nil))
		}()
	}
	store = memstore.NewSharedStore[memcachedprotocol.MemcachedEntry]()
	store.SetMemoryLimit(memstore_size)
	store.SetItemSizeLimit(memstore_item_size)

	srvInstance := tcpserver.Server{}
	err := srvInstance.ListenAndServe(":11211", ConnectionHandler)
	if err != nil {
		log.Fatal(err)
	}

	acceptThreads := 2
	for acceptThreads > 0 {
		acceptThreads -= 1
		go srvInstance.AcceptConnections()
	}

	<-sigChan
	fmt.Println("Shutting down server...")
	srvInstance.Stop()
	fmt.Println("Server stopped.")
}
