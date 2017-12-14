package connpool

import (
	"errors"
	"net"
	"sync/atomic"
	"time"
)

const (
	// MaxFreeConns is the maximum number of connections that can ever be created by pool.
	MaxFreeConns = 500
	// MinFreeConns is the mimimum number of connections that will be maintained in the pool.
	MinFreeConns = 10
	// ConnTimeout is the connection timeout.
	ConnTimeout = time.Millisecond * 1000
	// IdleTimeout controls inactive connections. If a connection is not used longer than
	// IdleTimeout, it will be closed.
	IdleTimeout = time.Minute * 60
	// BatchConns is the number of connections that will be created in one batch.
	BatchConns = 50
	// CreateConnRetries is the number of retries when creating a connection.
	CreateConnRetries = 1
	// ConnReqSize is the channel size for pending connection creation requests.
	ConnReqSize = 10

	// PoolIdle means pool is not creating any connection.
	PoolIdle = int32(0)
	// PoolCreating means pool is currently creating connections.
	PoolCreating = int32(1)
	// PoolClosed means pool is closed.
	PoolClosed = int32(2)
)

// ConnPool is the connection pool interface.
type ConnPool interface {
	GetConn(timeout int) (c net.Conn, err error)
	PutConn(c net.Conn, good bool) (err error)
	TotalConns() int32
	Close()
}

// Opt is the connection pool option.
type Opt struct {
	MaxConns    int           // Maximum conns that can be created
	MinConns    int           // Minimum conns to be maintained
	ConnTimeout time.Duration // Connection timeout
	IdleTimeout time.Duration // Idle time beyond which a connection will be discarded
}

var (
	// DftOpt is the default configuration.
	DftOpt = Opt{
		MaxConns:    MaxFreeConns,
		MinConns:    MinFreeConns,
		ConnTimeout: ConnTimeout,
		IdleTimeout: IdleTimeout,
	}

	// ErrNilConn means a connection is nil.
	ErrNilConn = errors.New("nil connection")
	// ErrPoolEmpty means no free connection.
	ErrPoolEmpty = errors.New("pool has no free connection")
	// ErrPoolNoSpace means pool is full.
	ErrPoolNoSpace = errors.New("pool has no space")
	// ErrPoolClosed means pool is closed and should no longer be used.
	ErrPoolClosed = errors.New("pool is closed")
	// ErrInvalidType means the resource is not a connection.
	ErrInvalidType = errors.New("type is not *Conn")
)

// Conn is a wrapper of net.Conn with last used time.
type Conn struct {
	net.Conn
	lastUsedAt time.Time
}

// Dialer is an interface for dialing a connection.
type Dialer interface {
	Dial(addr string, timeout time.Duration) (net.Conn, error)
}

// TCPDialer dials TCP connections.
type TCPDialer struct{}

// Dial creates a TCP connection.
func (f *TCPDialer) Dial(addr string, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", addr, timeout)
}

// TimedPool manages a connection pool.
type TimedPool struct {
	addr       string
	dialer     Dialer
	basePool   chan *Conn
	expandPool chan *Conn
	numOfConn  int32
	isCreating int32
	isClosed   int32
	opt        *Opt
	connReq    chan int
}

// NewConnPool creates a connection pool.
func NewConnPool(addr string, dialer Dialer, c *Opt) *TimedPool {
	maxConns := c.MaxConns
	if c.MaxConns <= c.MinConns {
		maxConns = c.MinConns + 1 // make sure the expandPool is not nil
	}
	opt := *c
	opt.MaxConns = maxConns
	pool := &TimedPool{
		addr:       addr,
		dialer:     dialer,
		opt:        &opt,
		isCreating: 0,
		isClosed:   0,
	}

	pool.basePool = make(chan *Conn, pool.opt.MinConns)
	pool.expandPool = make(chan *Conn, pool.opt.MaxConns-pool.opt.MinConns)
	pool.connReq = make(chan int, ConnReqSize)
	go pool.createConnection()
	return pool
}

// TotalConns returns the total number of active connections.
func (pool *TimedPool) TotalConns() int32 {
	return atomic.LoadInt32(&pool.numOfConn)
}

// Close closes all connections in the pool.
func (pool *TimedPool) Close() {
	canClose := atomic.CompareAndSwapInt32(&pool.isClosed, PoolIdle, PoolClosed)
	if !canClose {
		return
	}
	close(pool.expandPool)
	for c := range pool.expandPool {
		pool.destroyConn(c)
	}
	close(pool.basePool)
	for c := range pool.basePool {
		pool.destroyConn(c)
	}
	close(pool.connReq)
}

// GetConn gets a connection from the pool. It tries to create a new connection
// if the pool is empty. When the total connections reach the limit (MaxConns),
// i.e. no more connections can be created, it will wait.
// If timeout > 0, it waits for timeout milliseconds before returning an error.
// If timeout == 0, it returns immediately.
// If timeout < 0, it waits indefinitely until a connection can be obtained.
// from the pool.
// Note that There is a rare situation where it can block indefinitely
// when two conditions are true:
//   (1)  All connections returned to the pool (by calling PutConn) are bad,
//        i.e. they are destroyed instead of joining the pool;
//   (2) No other call of GetConn after the current call, i.e. no attempt to
//       create a new connection.
// This should be an extremely rare situation.
func (pool *TimedPool) GetConn(timeout int) (c net.Conn, err error) {
	var conn *Conn
	var ok bool
	defer func() {
		if conn != nil {
			conn.lastUsedAt = time.Now()
		}
	}()

	// Try to get from the expandPool, discard idle connections.
	// It may discard all connections in the expandPool.
	for {
		conn = nil
		select {
		case conn = <-pool.expandPool:
		default:
			break
		}
		if conn == nil {
			// expandPool empty, need to get from the basePool
			break
		}

		if pool.opt.IdleTimeout > 0 && time.Since(conn.lastUsedAt) > pool.opt.IdleTimeout {
			pool.destroyConn(conn)
			continue
		}
		if conn == nil {
			return nil, ErrPoolClosed
		}
		return conn, nil
	}

	// Try to get from the basePool
	select {
	case conn, ok := <-pool.basePool:
		if !ok {
			return nil, ErrPoolClosed
		}
		return conn, nil
	default:
		break
	}
	// Both expandPool and basePool are empty
	pool.createMoreConns()
	// Try again
	switch {
	case timeout == 0:
		select {
		case conn, ok = <-pool.basePool:
			if !ok {
				return nil, ErrPoolClosed
			}
			return conn, nil
		default:
			break
		}
	case timeout > 0:
		t := time.NewTimer(time.Millisecond * time.Duration(timeout))
		select {
		case conn, ok = <-pool.basePool:
			t.Stop()
			if !ok {
				return nil, ErrPoolClosed
			}
			return conn, nil
		case <-t.C:
			break
		}
	case timeout < 0:
		conn, ok = <-pool.basePool
		if !ok {
			return nil, ErrPoolClosed
		}
		return conn, nil
	}
	return nil, ErrPoolEmpty
}

// PutConn puts the connection back to the pool. If 'good' is true,
// the connection will be put in the pool. If 'good' is false,
// the connection will be closed and will not be put in the pool.
func (pool *TimedPool) PutConn(c net.Conn, good bool) (err error) {
	if c == nil {
		return ErrNilConn
	}
	conn, ok := c.(*Conn)
	if !ok {
		return ErrInvalidType
	}
	if !good {
		pool.destroyConn(conn)
		return nil
	}

	// We should use a lock to protect this critical section,
	// but that would mean a performance hit as this is a frequently.
	// Instead, we avoid the locking by allowing it to panic.
	defer func() {
		e := recover()
		if e != nil {
			// If not nil, that means the pool is closed.
			pool.destroyConn(conn)
			err = ErrPoolClosed
		}
	}()
	select {
	case pool.basePool <- conn:
	default:
		select {
		case pool.expandPool <- conn:
		default:
			// Both basePool and expandPool are full
			pool.destroyConn(conn)
			return ErrPoolNoSpace
		}
	}
	return nil
}

// createConn creates a connection and increment the internal counter accordingly.
func (pool *TimedPool) createConn() (*Conn, error) {
	conn := &Conn{}
	var c net.Conn
	var err error
	c, err = pool.dialer.Dial(pool.addr, pool.opt.ConnTimeout)
	if err == nil {
		conn.Conn = c
		conn.lastUsedAt = time.Now()
		pool.incrementConn(1)
		return conn, nil
	}
	for i := 1; i < CreateConnRetries; i++ {
		time.Sleep(time.Second)
		c, err = pool.dialer.Dial(pool.addr, pool.opt.ConnTimeout)
		if err != nil {
			continue
		}
		conn.Conn = c
		conn.lastUsedAt = time.Now()
		pool.incrementConn(1)
		return conn, nil
	}
	return nil, err
}

// destroyConn destroys a connection and decrement the internal counter accordingly.
func (pool *TimedPool) destroyConn(conn *Conn) {
	if conn != nil {
		conn.Close()
		pool.decrementConn(1)
	}
}

func (pool *TimedPool) createMoreConns() {
	defer func() {
		// connReq might be closed, so recover to avoid panic.
		recover()
	}()
	select {
	case pool.connReq <- BatchConns:
	default:
	}
}

func (pool *TimedPool) createConnection() {
	for r := range pool.connReq {
		pool.createConns(r)
		time.Sleep(time.Millisecond * 100)
	}
}

func (pool *TimedPool) createConns(n int) {
	for i := 0; i < n && pool.hasSpace(); i++ {
		conn, err := pool.createConn()
		if err != nil {
			continue
		}
		err = pool.PutConn(conn, true)
		if err != nil {
			break
		}
	}
}

func (pool *TimedPool) hasSpace() bool {
	return pool.numOfConn < int32(pool.opt.MaxConns)
}

func (pool *TimedPool) incrementConn(i int32) {
	atomic.AddInt32(&pool.numOfConn, i)
}

func (pool *TimedPool) decrementConn(d int32) {
	atomic.AddInt32(&pool.numOfConn, (-d))
}
