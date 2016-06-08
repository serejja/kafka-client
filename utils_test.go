package client

import (
	crand "crypto/rand"
	"github.com/yanzay/log"
	"math/rand"
	"net"
	"reflect"
	"runtime"
	"testing"
	"time"
)

const tcpListenerAddress = "localhost:0"

func assert(t *testing.T, actual interface{}, expected interface{}) {
	if !reflect.DeepEqual(actual, expected) {
		_, fn, line, _ := runtime.Caller(1)
		t.Errorf("Expected %v, actual %v\n@%s:%d", expected, actual, fn, line)
	}
}

func assertFatal(t *testing.T, actual interface{}, expected interface{}) {
	if !reflect.DeepEqual(actual, expected) {
		_, fn, line, _ := runtime.Caller(1)
		t.Fatalf("Expected %v, actual %v\n@%s:%d", expected, actual, fn, line)
	}
}

func assertNot(t *testing.T, actual interface{}, expected interface{}) {
	if reflect.DeepEqual(actual, expected) {
		_, fn, line, _ := runtime.Caller(1)
		t.Fatalf("Expected anything but %v, actual %v\n@%s:%d", expected, actual, fn, line)
	}
}

func checkErr(t *testing.T, err error) {
	if err != nil {
		_, fn, line, _ := runtime.Caller(1)
		t.Errorf("%s\n @%s:%d", err, fn, line)
	}
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	_, err := crand.Read(b)
	if err != nil {
		panic(err)
	}
	return b
}

func randomString(n int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZйцукенгшщзхъфывапролджэжячсмитьбюЙЦУКЕНГШЩЗХЪФЫВАПРОЛДЖЭЯЧСМИТЬБЮ0123456789!@#$%^&*()")

	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)[:n]
}

func testRequest(t *testing.T, request Request, expected []byte) {
	sizing := NewSizingEncoder()
	request.Write(sizing)
	bytes := make([]byte, sizing.Size())
	encoder := NewBinaryEncoder(bytes)
	request.Write(encoder)

	assert(t, bytes, expected)
}

func decode(t *testing.T, response Response, bytes []byte) {
	decoder := NewBinaryDecoder(bytes)
	err := response.Read(decoder)
	assert(t, err, (*DecodingError)(nil))
}

func decodeErr(t *testing.T, response Response, bytes []byte, expected *DecodingError) {
	decoder := NewBinaryDecoder(bytes)
	err := response.Read(decoder)
	assert(t, err.Error(), expected.Error())
	assert(t, err.Reason(), expected.Reason())
}

func awaitForTCPRequestAndReturn(t *testing.T, bufferSize int, resultChannel chan []byte) net.Listener {
	netName := "tcp"
	addr, _ := net.ResolveTCPAddr(netName, tcpListenerAddress)
	listener, err := net.ListenTCP(netName, addr)
	if err != nil {
		t.Errorf("Unable to start tcp request listener: %s", err)
	}
	go func() {
		buffer := make([]byte, bufferSize)
		conn, err := listener.AcceptTCP()
		if err != nil {
			t.Error(err)
		}
		_, err = conn.Read(buffer)
		if err != nil {
			t.Error(err)
		}

		resultChannel <- buffer
	}()

	return listener
}

func startTCPListener(t *testing.T) net.Listener {
	netName := "tcp"
	addr, _ := net.ResolveTCPAddr(netName, tcpListenerAddress)
	listener, err := net.ListenTCP(netName, addr)
	if err != nil {
		t.Errorf("Unable to start tcp request listener: %s", err)
	}

	return listener
}

func testClient(t *testing.T) *KafkaClient {
	config := NewConfig()
	config.BrokerList = []string{"localhost:9092"}

	kafkaClient, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	return kafkaClient
}

func closeWithin(t *testing.T, timeout time.Duration, kafkaClient Client) {
	select {
	case <-kafkaClient.Close():
		{
			log.Info("Successfully closed client")
		}
	case <-time.After(timeout):
		{
			t.Errorf("Failed to close client within %s seconds", timeout)
		}
	}
}
