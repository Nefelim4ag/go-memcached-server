package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"nefelim4ag/go-memcached-server/tcpserver"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

func main() {
	// Wait for a SIGINT or SIGTERM signal to gracefully shut down the server
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

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

	clientReader := bufio.NewReader(conn)
	for {
		// Waiting for the client request
		clientRequest, err := clientReader.ReadString('\n')
		log.Println(clientRequest)

		switch err {
		case nil:
			clientRequest := strings.TrimSpace(clientRequest)
			if clientRequest == "quit" {
				log.Println("client requested server to close the connection so closing")
				return
			} else {
				log.Println(clientRequest)
			}
		case io.EOF:
			log.Println("client closed connection")
			return
		default:
			log.Printf("error: %v\n", err)
			return
		}

		// Responding to the client request
		if _, err = conn.Write([]byte("recieved\n")); err != nil {
			log.Printf("failed to respond to client: %v\n", err)
		}
	}
}
