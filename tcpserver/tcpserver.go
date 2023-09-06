package tcpserver

import (
	"fmt"
	"net"
	"sync"
	"time"
)

type tcpserver interface {
	AcceptConnections()
	Stop() error
}

type Server struct {
 wg         sync.WaitGroup
 listener   net.Listener
 shutdown   chan struct{}
}

func ListenAndServe(address string, connectionQueue uint) (*Server, error) {
 listener, err := net.Listen("tcp", address)
 if err != nil {
  return nil, fmt.Errorf("failed to listen on address %s: %w", address, err)
 }

 if connectionQueue == 0 {
	connectionQueue = 32
 }

 srv := &Server{
	wg:         sync.WaitGroup{},
    listener:   listener,
    shutdown:   make(chan struct{}),
 }

 return srv, nil
}


type ConnectionHandler func(connection net.Conn, err error)

func (s *Server) AcceptConnections(handleConnection ConnectionHandler) {
	s.wg.Add(1)
	defer s.wg.Done()

	for {
		select {
			case <-s.shutdown:
			return

		default:
			connection, err := s.listener.Accept()
			go handleConnection(connection, err)
		}
	}
}

func (s *Server) Stop() error {
	close(s.shutdown)
	s.listener.Close()

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
		case <-done:
			return nil
		case <-time.After(2 * time.Second):
			fmt.Println("Timed out waiting for connections to finish.")
			return nil
	}
}
