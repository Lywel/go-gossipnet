package gossipnet

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"flag"
	"github.com/google/logger"
	"github.com/hashicorp/golang-lru"
	"io"
	"io/ioutil"
	"net"
)

const (
	inmemoryPeers          = 40
	inmemoryMessages       = 1024
	eventChannelBufferSize = 256
)

var verbose = flag.Bool("verbose-network", false, "print gossipnet info level logs")

// Node is the local Node
type Node struct {
	localAddr      string
	remoteAddrs    []string
	ln             net.Listener
	running        bool
	remoteNodes    map[net.Addr]net.Conn
	eventChan      chan Event
	recentMessages *lru.ARCCache // the cache of peer's messages
	knownMessages  *lru.ARCCache // the cache of self messages
	debug          *logger.Logger
}

// New Creates a Network Gossiping Node
func New(localAddr string, remoteAddrs []string) *Node {
	recentMessages, _ := lru.NewARC(inmemoryPeers)
	knownMessages, _ := lru.NewARC(inmemoryMessages)

	return &Node{
		localAddr:      localAddr,
		remoteAddrs:    remoteAddrs,
		running:        false,
		remoteNodes:    make(map[net.Addr]net.Conn),
		eventChan:      make(chan Event, eventChannelBufferSize),
		recentMessages: recentMessages,
		knownMessages:  knownMessages,
	}
}

func (n *Node) emit(event Event) {
	// protection against blocking channel
	select {
	case n.eventChan <- event:
	default:
	}
}

func (n *Node) readNextMessage(conn net.Conn, rest []byte) ([]byte, []byte, error) {
	buf := bytes.Buffer{}
	tmp := make([]byte, 256)

	// Reinsert rest from previous read
	buf.Write(rest)

	// Read message len
	for buf.Len() < 4 {
		n, err := conn.Read(tmp)
		if err != nil {
			return nil, nil, err
		}
		buf.Write(tmp[:n])
	}

	// Parse 4 first byte to get the message length
	messageLength := binary.LittleEndian.Uint32(buf.Bytes()[:4])
	n.debug.Infof("receiving message of len %d", messageLength)

	for buf.Len() < int(messageLength+4) {
		n, err := conn.Read(tmp)
		if err != nil {
			return nil, nil, err
		}
		buf.Write(tmp[:n])
	}

	return buf.Bytes()[4 : messageLength+4], buf.Bytes()[messageLength+4:], nil
}

// Save the new remote node
func (n *Node) registerRemote(conn net.Conn) {
	n.debug.Infof("Connection opened with %s", conn.RemoteAddr())
	n.remoteNodes[conn.RemoteAddr()] = conn
	n.emit(ConnOpenEvent{conn.RemoteAddr().String()})
	defer conn.Close()
	defer delete(n.remoteNodes, conn.RemoteAddr())

	// Start reading
	var rest []byte
	var payload []byte
	var err error

	for {
		payload, rest, err = n.readNextMessage(conn, rest)
		switch err {
		case nil:
			n.handleData(conn.RemoteAddr().String(), payload)
			continue
		case io.EOF:
		default:
			n.debug.Warningf("read error on %s: %v", conn.RemoteAddr(), err)
			n.emit(ErrorEvent{err})
		}
		break
	}
	n.debug.Infof("Connection closed with %s", conn.RemoteAddr())
	n.emit(ConnCloseEvent{conn.RemoteAddr().String()})
}

func (n *Node) handleData(addr string, payload []byte) {
	n.debug.Infof("Receiving data from %s", addr)
	hash := sha256.Sum256(payload)
	n.cacheEventFor(addr, hash)

	if _, alreadyKnew := n.knownMessages.Get(hash); alreadyKnew {
		return
	}
	n.knownMessages.Add(hash, true)

	n.Gossip(payload)
	n.emit(DataEvent{
		Data: payload,
		Addr: addr,
	})
}

func (n *Node) cacheEventFor(addr string, hash [32]byte) (alreadyKnew bool) {
	cached, hasCache := n.recentMessages.Get(addr)
	var recentMsgs *lru.ARCCache
	if hasCache {
		recentMsgs, _ = cached.(*lru.ARCCache)
		_, alreadyKnew = recentMsgs.Get(hash)
	} else {
		recentMsgs, _ = lru.NewARC(inmemoryMessages)
	}
	recentMsgs.Add(hash, true)
	n.recentMessages.Add(addr, recentMsgs)
	return
}

// EventChan returns a readonly chanel for data events
func (n *Node) EventChan() <-chan Event {
	return n.eventChan
}

// Gossip sends a Message to all peers passing selection (except self)
func (n *Node) Gossip(payload []byte) {
	n.debug.Infof("Gossip")
	hash := sha256.Sum256(payload)

	payloadLen := make([]byte, 4)
	binary.LittleEndian.PutUint32(payloadLen, uint32(len(payload)))

	for addr, conn := range n.remoteNodes {
		alreadyKnew := n.cacheEventFor(addr.String(), hash)
		if !alreadyKnew {
			n.debug.Infof("Gossiping to %s", addr)
			conn.Write(payloadLen)
			conn.Write(payload)
		} else {
			n.debug.Infof("%s already knew :o", addr)
		}
	}
}

// Broadcast sends a Message to all peers passing selection (including self)
func (n *Node) Broadcast(payload []byte) {
	n.debug.Infof("Broadcast")
	n.Gossip(payload)
	n.handleData(n.localAddr, payload)
}

// Start starts the node (client / server)
func (n *Node) Start() error {
	n.running = true
	n.debug = logger.Init("Gossipnet", *verbose, false, ioutil.Discard)
	n.debug.Infof("Start")

	n.debug.Infof("Dialing peers")
	for _, addr := range n.remoteAddrs {
		n.debug.Infof("Dialing %s...", addr)
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			n.emit(ErrorEvent{err})
			n.debug.Warningf("Dialing %s failed: %v", addr, err)
			continue
		}
		go n.registerRemote(conn)
	}

	var err error
	if n.ln, err = net.Listen("tcp", n.localAddr); err != nil {
		return err
	}
	n.debug.Infof("Starting to listen on %s", n.localAddr)
	n.emit(ListenEvent{n.ln.Addr().String()})

	go func() {
		defer n.ln.Close()
		for n.running {
			conn, err := n.ln.Accept()
			if err != nil {
				n.debug.Infof("Accept error: %v", err)
				n.emit(ErrorEvent{err})
				continue
			}
			n.debug.Infof("Accepting connection from %s", conn.RemoteAddr())
			go n.registerRemote(conn)
		}
	}()

	return nil
}

// Stop closes all connection and stops listening
func (n *Node) Stop() {
	if !n.running {
		return
	}
	n.debug.Infof("Stop")
	n.running = false
	n.debug.Close()
	n.ln.Close()
	for _, conn := range n.remoteNodes {
		conn.Close()
		n.emit(ConnCloseEvent{conn.RemoteAddr().String()})
	}
	n.emit(CloseEvent{})
	close(n.eventChan)
}
