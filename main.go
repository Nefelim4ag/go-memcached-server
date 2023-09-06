package main

import (
	"bufio"
	"io"
	"log"
	"net"
	"strings"
	"sync"
)

type client struct {
	wg *sync.WaitGroup
	connection net.Conn
}

func main() {
	wg := &sync.WaitGroup{}
	listener, err := net.Listen("tcp", "0.0.0.0:11211")
	if err != nil {
		log.Fatalln(err)
	}
	defer listener.Close()

	for {
		con, err := listener.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		obj := client{
			wg: wg,
            connection: con,
		}

		// If you want, you can increment a counter here and inject to handleClientRequest below as client identifier
		wg.Add(1)
		go handleClientRequest(&obj)
	}

	wg.Wait()
}

func handleClientRequest(obj *client) {
	defer obj.wg.Done()
	defer obj.connection.Close()

	clientReader := bufio.NewReader(obj.connection)

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
		if _, err = obj.connection.Write([]byte("GOT IT!\n")); err != nil {
			log.Printf("failed to respond to client: %v\n", err)
		}
	}
}
