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
	"syscall"

	log "github.com/sirupsen/logrus"
)

type memcachedServer struct {
	store *memstore.SharedStore
}

func (mS *memcachedServer) ConnectionHandler(conn *net.TCPConn, err error) {
	if err != nil {
		log.Errorf(err.Error())
		return
	}

	defer conn.Close()

	_r := bufio.NewReaderSize(conn, 64 * 1024)

	// Reuse context between binary commands
	Processor := memcachedprotocol.CreateProcessor(_r, conn, mS.store)
	defer Processor.CloseProcessor()

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
			err = Processor.CommandAscii()
			if err != nil {
				return
			}
		} else if magic == 0x80 {
			err = Processor.CommandBinary()
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

	_memstoreSize := flag.Uint64("m", 512, "items memory in megabytes, default is 512")
	_memstoreItemSize := flag.Uint("I", 1024*1024, "max item sizem, default is 1m")
	logLevel := flag.Uint("loglevel", 3, "log level, 5=debug, 4=info, 3=warning, 2=error, 1=fatal, 0=panic")
	pprof := flag.Bool("pprof", false, "enable pprof server")
	flag.Parse()

	log.SetLevel(log.Level(*logLevel))

	memstoreSize := uint64(*_memstoreSize) * 1024 * 1024
	memstoreItemSize := uint32(*_memstoreItemSize)

	// Wait for a SIGINT or SIGTERM signal to gracefully shut down the server
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	if *pprof {
		go func() {
			log.Error(http.ListenAndServe("127.0.0.1:6060", nil))
		}()
	}

	memcachedSrv := &memcachedServer{
		store: memstore.NewSharedStore(),
	}
	memcachedSrv.store.SetMemoryLimit(memstoreSize)
	memcachedSrv.store.SetItemSizeLimit(memstoreItemSize)

	srvInstance := tcpserver.Server{}
	err := srvInstance.ListenAndServe(":11211", memcachedSrv.ConnectionHandler)
	if err != nil {
		log.Fatal(err)
	}

	<-sigChan
	fmt.Println("Shutting down server...")
	srvInstance.Stop()
	fmt.Println("Server stopped.")
}
