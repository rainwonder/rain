package main

import (
	"bufio"
	"crypto/rand"
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"net"
	"sync"
)

type Conn struct {
	conn   net.Conn
	mu     sync.Mutex
	reader *bufio.Reader
	writer *bufio.Writer
}

type message struct {
	Key  string
	Data []byte
}

func NewConn(conn net.Conn) *Conn {
	return &Conn{
		conn:   conn,
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
	}
}

func (c *Conn) Send(key string) (io.WriteCloser, error) {
	pr, pw := io.Pipe()

	go func() {
		defer pw.Close()

		enc := gob.NewEncoder(c.writer)
		for {
			buf := make([]byte, 1024)
			n, err := pr.Read(buf)
			if err == io.EOF {
				break
			}
			if err != nil {
				panic(err)
			}
			err = enc.Encode(message{Key: key, Data: buf[:n]})
			if err != nil {
				panic(err)
			}
			c.writer.Flush()
		}
	}()

	return pw, nil
}

func (c *Conn) Receive() (string, io.Reader, error) {
	pr, pw := io.Pipe()

	go func() {
		defer pw.Close()

		dec := gob.NewDecoder(c.reader)
		var msg message
		err := dec.Decode(&msg)
		if err != nil {
			pw.CloseWithError(err)
			return
		}
		_, err = pw.Write(msg.Data)
		if err != nil {
			pw.CloseWithError(err)
			return
		}
	}()

	return "", pr, nil
}

func (c *Conn) Close() {
	c.conn.Close()
}

func dial(serverAddr string) *Conn {
	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		panic(err)
	}
	return NewConn(conn)
}

func startServer(handle func(*Conn)) net.Listener {
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				fmt.Println("[WARNING] ln.Accept", err)
				return
			}
			go handle(NewConn(conn))
		}
	}()
	return ln
}

func assertEqual[T comparable](actual T, expected T) {
	if actual != expected {
		panic(fmt.Sprintf("actual:%v expected:%v\n", actual, expected))
	}
}

func testCase0() {
	const (
		key  = "Bible"
		data = `Then I heard the voice of the Lord saying, “Whom shall I send? And who will go for us?”
And I said, “Here am I. Send me!”
Isaiah 6:8`
	)
	ln := startServer(func(conn *Conn) {
		_key, reader, err := conn.Receive()
		if err != nil {
			panic(err)
		}
		assertEqual(_key, key)
		dataB, err := io.ReadAll(reader)
		if err != nil {
			panic(err)
		}
		assertEqual(string(dataB), data)

		writer, err := conn.Send(key)
		if err != nil {
			panic(err)
		}
		n, err := writer.Write([]byte(data))
		if err != nil {
			panic(err)
		}
		if n != len(data) {
			panic(n)
		}
		conn.Close()
	})

	defer ln.Close()

	conn := dial(ln.Addr().String())
	writer, err := conn.Send(key)
	if err != nil {
		panic(err)
	}
	n, err := writer.Write([]byte(data))
	if n != len(data) {
		panic(n)
	}
	err = writer.Close()
	if err != nil {
		panic(err)
	}
	_key, reader, err := conn.Receive()
	if err != nil {
		panic(err)
	}
	assertEqual(_key, key)
	dataB, err := io.ReadAll(reader)
	if err != nil {
		panic(err)
	}
	assertEqual(string(dataB), data)
	conn.Close()
}

func newRandomKey() string {
	buf := make([]byte, 8)
	_, err := rand.Read(buf)
	if err != nil {
		panic(err)
	}
	return hex.EncodeToString(buf)
}

func readRandomData(reader io.Reader, hash hash.Hash) (checksum string) {
	hash.Reset()
	var buf = make([]byte, 23<<20)
	for {
		n, err := reader.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		_, err = hash.Write(buf[:n])
		if err != nil {
			panic(err)
		}
	}
	checksum = hex.EncodeToString(hash.Sum(nil))
	return checksum
}

func writeRandomData(writer io.Writer, hash hash.Hash) (checksum string) {
	hash.Reset()
	const (
		dataSize = 500 << 20
		bufSize  = 1 << 20
	)
	var (
		buf  = make([]byte, bufSize)
		size = 0
	)
	for i := 0; i < dataSize/bufSize; i++ {
		_, err := rand.Read(buf)
		if err != nil {
			panic(err)
		}
		_, err = hash.Write(buf)
		if err != nil {
			panic(err)
		}
		n, err := writer.Write(buf)
		if err != nil {
			panic(err)
		}
		size += n
	}
	if size != dataSize {
		panic(size)
	}
	checksum = hex.EncodeToString(hash.Sum(nil))
	return checksum
}

func testCase1() {
	var (
		mapKeyToChecksum = map[string]string{}
		lock             sync.Mutex
	)
	ln := startServer(func(conn *Conn) {
		key, reader, err := conn.Receive()
		if err != nil {
			panic(err)
		}
		var (
			h         = sha256.New()
			_checksum = readRandomData(reader, h)
		)
		lock.Lock()
		checksum, keyExist := mapKeyToChecksum[key]
		lock.Unlock()
		if !keyExist {
			panic(fmt.Sprintln(key, "not exist"))
		}
		assertEqual(_checksum, checksum)

		for _, key := range []string{newRandomKey(), newRandomKey()} {
			writer, err := conn.Send(key)
			if err != nil {
				panic(err)
			}
			checksum := writeRandomData(writer, h)
			lock.Lock()
			mapKeyToChecksum[key] = checksum
			lock.Unlock()
			err = writer.Close()
			if err != nil {
				panic(err)
			}
		}
		conn.Close()
	})

	defer ln.Close()

	conn := dial(ln.Addr().String())
	var (
		key = newRandomKey()
		h   = sha256.New()
	)
	writer, err := conn.Send(key)
	if err != nil {
		panic(err)
	}
	checksum := writeRandomData(writer, h)
	lock.Lock()
	mapKeyToChecksum[key] = checksum
	lock.Unlock()
	err = writer.Close()
	if err != nil {
		panic(err)
	}

	keyCount := 0
	for {
		key, reader, err := conn.Receive()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		_checksum := readRandomData(reader, h)
		lock.Lock()
		checksum, keyExist := mapKeyToChecksum[key]
		lock.Unlock()
		if !keyExist {
			panic(fmt.Sprintln(key, "not exist"))
		}
		assertEqual(_checksum, checksum)
		keyCount++
	}
	assertEqual(keyCount, 2)
	conn.Close()
}

func main() {
	testCase0()
	testCase1()
}
