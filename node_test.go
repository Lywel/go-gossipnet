package gossipnet_test

import (
	"bitbucket.org/ventureslash/go-gossipnet"
	"bytes"
	"net"
	"sync"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	var local, remote *gossipnet.Node
	local = gossipnet.New("localhost:3000", []string{})
	remote = gossipnet.New("localhost:3001", []string{":3000"})

	var wg sync.WaitGroup
	wg.Add(1)

	if err := local.Start(); err != nil {
		t.Fatal(err)
	}
	defer local.Stop()

	go func() {
		defer wg.Done()
		if err := remote.Start(); err != nil {
			t.Fatal(err)
		}
		defer remote.Stop()
	}()

	wg.Wait()
}

type netMan struct {
	list map[net.Addr]bool
}

func (netMan) IsInteressted(net.Addr) bool {
	return true
}

func TestBroadcast(t *testing.T) {
	var local, remote *gossipnet.Node
	localAddr := ":3000"
	remoteAddr := ":3001"
	ref := []byte("This is a test")

	// two nodes
	local = gossipnet.New(localAddr, []string{})
	remote = gossipnet.New(remoteAddr, []string{localAddr})

	// Start the first one
	if err := local.Start(); err != nil {
		t.Fatal(err)
	}
	defer local.Stop()

	// wait for it to be ready
	listenCh := make(chan struct{})
	// Read local events
	go func() {
		defer close(listenCh)
		for {
			e := <-local.EventChan()
			switch e.(type) {
			case gossipnet.ListenEvent:
				return
			default:
			}
		}
	}()
	// wait for listenEvent or timeout
	select {
	case <-listenCh:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("local node start() timed out")
	}

	// Local node is istening
	// Start the second one
	if err := remote.Start(); err != nil {
		t.Fatal(err)
	}
	defer remote.Stop()

	// wait for the 2 connections notifications
	wg := sync.WaitGroup{}
	wg.Add(2)
	connectedCh := make(chan struct{})
	// Read local events
	go func() {
		defer wg.Done()
		for {
			e := <-local.EventChan()
			switch e.(type) {
			case gossipnet.ConnOpenEvent:
				return
			default:
			}
		}
	}()
	// Read remote events
	go func() {
		defer wg.Done()
		for {
			e := <-local.EventChan()
			switch e.(type) {
			case gossipnet.ConnOpenEvent:
				return
			default:
			}
		}
	}()
	// Wait for both
	go func() {
		defer close(connectedCh)
		wg.Wait()
	}()

	// wait for listenEvent or timeout
	select {
	case <-connectedCh:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("connection timed out")
	}

	// Nodes are connected
	// Let's start listening for messges
	receivedCh := make(chan struct{})
	// Read local events
	go func() {
		defer close(receivedCh)
		for {
			e := <-local.EventChan()
			switch ev := e.(type) {
			case gossipnet.DataEvent:
				if bytes.Compare(ev.Data, ref) != 0 {
					t.Fatal("received msg expected to be '" + string(ref) + "' but got '" + string(ev.Data) + "' instead.")
				}
				return
			default:
			}
		}
	}()

	// Send data to remote->local
	remote.Broadcast(ref)

	// wait for data to be received or timeout
	select {
	case <-receivedCh:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("local receive timed out")
	}
}
