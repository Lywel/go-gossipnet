package gossipnet

// Event is a data wrapper for network events
type Event interface{}

// ConnOpenEvent is emitted when a connection is established
type ConnOpenEvent struct {
	Addr string
}

// ConnCloseEvent is emitted when a connection is lost
type ConnCloseEvent struct {
	Addr string
}

// DataEvent is emitted when data is received from the network
type DataEvent struct {
	Data []byte
	Addr string
}

// ListenEvent is emitted when the node starts to listen
type ListenEvent struct {
	Addr string
}

// CloseEvent is emitted when the node closes
type CloseEvent struct{}

// ErrorEvent is triggered when a non fatal error occurs
type ErrorEvent struct {
	Err error
}
