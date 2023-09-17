package main

import (
	"flag"
	"fmt"
	"nefelim4ag/go-memcached-server/memcachedprotocol"
	"nefelim4ag/go-memcached-server/memstore"
	"nefelim4ag/go-memcached-server/tcpserver"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"log/slog"
)

type memcachedServer struct {
	store *memstore.SharedStore
}

func (mS *memcachedServer) ConnectionHandler(conn *net.TCPConn, err error) {
	if err != nil {
		slog.Error(err.Error())
		return
	}

	defer conn.Close()

	// Reuse context between binary commands
	Processor := memcachedprotocol.CreateProcessor(conn, mS.store)
	defer Processor.CloseProcessor()
	Processor.Handle()
}

func main() {
	rawMemstoreSize := flag.Uint64("m", 512, "items memory in megabytes, default is 512")
	rawMemstoreItemSize := flag.Uint("I", 1024*1024, "max item sizem, default is 1m")
	logLevel := flag.Int("loglevel", 3, "log level, 4=debug, 3=info, 2=warning, 1=error")
	pprof := flag.Bool("pprof", false, "enable pprof server")
	flag.Parse()

	programLevel := new(slog.LevelVar)

	switch *logLevel {
	case 4:
		programLevel.Set(slog.LevelDebug)
		fmt.Println("Set debug")
	case 3:
		programLevel.Set(slog.LevelInfo)
		fmt.Println("Set info")
	case 2:
		programLevel.Set(slog.LevelWarn)
		fmt.Println("Set warn")
	case 1:
		programLevel.Set(slog.LevelError)
		fmt.Println("Set error")
	default:
		panic("Unsupported log level")
	}

	logger := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: programLevel})
	slog.SetDefault(slog.New(logger))

	memstoreSize := int64(*rawMemstoreSize) * 1024 * 1024
	memstoreItemSize := int32(*rawMemstoreItemSize)

	// Wait for a SIGINT or SIGTERM signal to gracefully shut down the server
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	if *pprof {
		go func() {
			slog.Error(http.ListenAndServe("127.0.0.1:6060", nil).Error())
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
		slog.Error(err.Error())
	}

	<-sigChan
	slog.Info("Shutting down server...")
	srvInstance.Stop()
	slog.Info("Server stopped.")
}
