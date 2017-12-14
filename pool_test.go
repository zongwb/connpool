package connpool

import (
	"fmt"
	"net"
	"testing"
	"time"
)

type FakeConn struct {
	net.Conn
}

func (f *FakeConn) Close() error {
	return nil
}

func (f *FakeConn) Dial(addr string, timeout time.Duration) (net.Conn, error) {
	return &FakeConn{}, nil
}

func TestConnPool(t *testing.T) {
	opt := Opt{}
	opt.MaxConns = 6
	opt.MinConns = 2
	opt.IdleTimeout = time.Second
	timeout := 1000 // 1000 ms
	f := &FakeConn{}
	pool := NewConnPool("localhost:12345", f, &opt)
	fmt.Printf("conns = %d\n", pool.TotalConns())
	var conn []net.Conn
	// this loop should all succeed
	for i := 0; i < 6; i++ {
		c, err := pool.GetConn(0)
		if err == nil {
			conn = append(conn, c)
		}
		fmt.Printf("ite %d, conns = %d, err = %v\n", i, pool.TotalConns(), err)
	}
	// this loop should produce errors
	for i := 0; i < 3; i++ {
		c, err := pool.GetConn(timeout)
		if err == nil {
			conn = append(conn, c)
		}
		fmt.Printf("ite %d, conns = %d, err = %v\n", i, pool.TotalConns(), err)
	}

	for i := 0; i < 3; i++ {
		c := conn[0]
		conn = conn[1:]
		err := pool.PutConn(c, true)
		fmt.Printf("put ite %d, conns = %d, err = %v\n", i, pool.TotalConns(), err)
	}
	for _, c := range conn {
		err := pool.PutConn(c, false)
		fmt.Printf("put bad conns = %d, err = %v\n", pool.TotalConns(), err)
	}
	// all conns should be stale
	time.Sleep(time.Second * 2)
	conns := make(chan net.Conn, 6)
	go func() {
		time.Sleep(time.Second)
		i := 0
		for c := range conns {
			// this will unblock the next for loop
			//pool.PutConn(c, true)
			// this will not unblock the next for loop, but the following Close will.
			pool.PutConn(c, false)
			if i == 5 {
				pool.Close()
			}
			i++
		}
	}()

	for i := 0; i < 9; i++ {
		c, err := pool.GetConn(-1)
		conns <- c
		fmt.Printf("ite %d, conns = %d, c is nil = %v, err = %v\n", i, pool.TotalConns(), c == nil, err)
		time.Sleep(time.Millisecond)
	}
	close(conns)
}
