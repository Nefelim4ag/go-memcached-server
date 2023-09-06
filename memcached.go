package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"nefelim4ag/go-memcached-server/memstore"
	"nefelim4ag/go-memcached-server/tcpserver"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"net/http"
	_ "net/http/pprof"
)

var (
	store *memstore.SharedStore
)

func main() {
	// Wait for a SIGINT or SIGTERM signal to gracefully shut down the server
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.Println(http.ListenAndServe("127.0.0.1:6060", nil))
	}()

	store = memstore.NewSharedStore()

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
		clientRequest, err := client.Reader.ReadString('\n')

		switch err {
		case nil:
			clientRequest := strings.TrimSpace(clientRequest)
			err := HandleCommand(clientRequest, client)

			if err!= nil {
                log.Println(clientRequest, err)
				client.Writer.Write([]byte("ERROR\r\n"))
				return
            }
		case io.EOF:
			log.Println("client closed connection")
			return
		default:
			log.Printf("error: %v\n", err)
			return
		}
	}


}

type memcachedEntry struct {
	key string
	flags uint32
	exptime uint32
	len uint64
	cas uint64
	value []byte
}

func HandleCommand(request string, client *bufio.ReadWriter) error {
	request_parsed := strings.Split(request, " ")
	command := request_parsed[0]
	args := request_parsed[1:]
	store := store

	switch command {
	case "set", "add", "replace":
		key := args[0]
		if command == "add" {
			_, exist := store.Get(key)
			if exist{
				client.Writer.Write([]byte("NOT_STORED\r\n"))
			}
		}
		flags, err := strconv.ParseUint(args[1], 10, 32)
		if err != nil {
			return err
		}
		exptime, err := strconv.ParseUint(args[2], 10, 32)
		if err != nil {
			return err
		}
		bytes, err := strconv.ParseUint(args[3], 10, 64)
		if err!= nil {
			return err
        }

		entry := memcachedEntry{
			key: key,
            flags: uint32(flags),
            exptime: uint32(exptime),
            len: bytes,
            cas: 0,
            value: make([]byte, bytes),
        }

		if bytes > 0 {
			_, err := io.ReadFull(client.Reader, entry.value)
			if err != nil {
				return err
			}
		}
		// Read message last \r\n possibly
		client.Reader.ReadString('\n')

		store.Set(entry.key, entry)

		client.Writer.Write([]byte("STORED\r\n"))

		// - "NOT_STORED\r\n" to indicate the data was not stored, but not
		// because of an error. This normally means that the
		// condition for an "add" or a "replace" command wasn't met.

		// - "EXISTS\r\n" to indicate that the item you are trying to store with
		// a "cas" command has been modified since you last fetched it.

		// - "NOT_FOUND\r\n" to indicate that the item you are trying to store
		// with a "cas" command did not exist.
	case "get", "gets":
		for _, v := range args {
			value, exist := store.Get(v)
			if !exist{
				continue
			}
			var entry memcachedEntry = value.(memcachedEntry)
			// VALUE <key> <flags> <bytes> [<cas unique>]\r\n
			// <data block>\r\n
			if command == "get" {
				resp := fmt.Sprintf("VALUE %s %d %d\r\n", entry.key, entry.flags, entry.len)
				client.Writer.Write([]byte(resp))
			} else {
				resp := fmt.Sprintf("VALUE %s %d %d %d\r\n", entry.key, entry.flags, entry.len, entry.cas)
				client.Writer.Write([]byte(resp))
			}
			client.Writer.Write(entry.value)
			client.Writer.Write([]byte("\r\n"))
		}

		client.Writer.Write([]byte("END\r\n"))

	case "stats":
		switch args[0] {
			case "items":
                client.Writer.Write([]byte("END\r\n"))
			case "slabs":
				client.Writer.Write([]byte("END\r\n"))
			case "sizes":
				client.Writer.Write([]byte("END\r\n"))
            default:
                return fmt.Errorf("not supported")
		}
	case "lru_crawler":
		switch args[0] {
		case "metadump":
			switch args[1] {
			case "all":
				// key=fake%2Fee49a9a0d462d1fa%2F18a6af34196%3A18a6af34253%3Afa5766e2 exp=1694013261 la=1694012361 cas=12434 fetch=no cls=12 size=1139
				// key=fake%2F886f3db85b3da0c2%2F18a6af60139%3A18a6af60c05%3A97e2dba9 exp=1694013435 la=1694012535 cas=12440 fetch=no cls=13 size=1420
				// key=fake%2Fc437f5f7aa7cb20b%2F18a6b03682a%3A18a6b03be70%3A123ad4e4 exp=1694013435 la=1694012535 cas=12439 fetch=no cls=39 size=1918339
				client.Writer.Write([]byte("END\r\n"))
			default:
                return fmt.Errorf("not supported")
			}
		}
	case "flush_all":
		client.Writer.Write([]byte("END\r\n"))
	default:
		return fmt.Errorf("not supported")
	}

	client.Writer.Flush()
	return nil
}
