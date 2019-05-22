package redcon

import (
	"net"
	"reflect"
	"syscall"
)

type nonBlockingReader struct {
	sysfd int
	net.Conn
}

// NewNonBlockingReader returns a non-blocking reader for net.Conn.
func NewNonBlockingReader(conn net.Conn) net.Conn {
	sysfd := socketFD(conn)

	return &nonBlockingReader{sysfd: sysfd, Conn: conn}
}

func (r *nonBlockingReader) Read(p []byte) (n int, err error) {
	n, err = syscall.Read(r.sysfd, p)
	if err != nil {
		if err == syscall.EAGAIN {
			err = nil
		}
		if n < 0 {
			n = 0
		}
	}
	return n, err
}

func (r *nonBlockingReader) BlockingRead(p []byte) (n int, err error) {
	return r.Conn.Read(p)
}

func socketFD(conn net.Conn) int {
	tcpConn := reflect.Indirect(reflect.ValueOf(conn)).FieldByName("conn")
	fdVal := tcpConn.FieldByName("fd")
	pfdVal := reflect.Indirect(fdVal).FieldByName("pfd")

	return int(pfdVal.FieldByName("Sysfd").Int())
}
