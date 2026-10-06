package listeners

import (
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wind-c/comqtt/v2/mqtt/system"
)

func TestHTTPGuardClassify(t *testing.T) {
	g := newHTTPGuard("t1", time.Minute, false, logger)
	require.Empty(t, g.classify([]byte("GET / HTTP/1.1\r\n")))
	require.Empty(t, g.classify(nil))
	require.Equal(t, "mqtt connect packet", g.classify([]byte{0x10, 0x10, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04}))
	require.Equal(t, "probable mqtt packet (type 3)", g.classify([]byte{0x30, 0x05, 0x00, 0x01, 'a', 'b'}))
	require.Equal(t, "probable mqtt packet (type 1)", g.classify([]byte{0x16, 0x03, 0x01}))
	require.Equal(t, "non-http data (first byte 0xff)", g.classify([]byte{0xff}))
	require.Equal(t, "non-http data (first byte 0x00)", g.classify([]byte{0x00}))

	tg := newHTTPGuard("t1", time.Minute, true, logger)
	require.Empty(t, tg.classify([]byte{0x16, 0x03, 0x01}))
}

func TestHTTPGuardBanExpiry(t *testing.T) {
	g := newHTTPGuard("t1", 20*time.Millisecond, false, logger)
	require.False(t, g.isBanned("1.2.3.4"))
	g.ban("1.2.3.4", "test")
	require.True(t, g.isBanned("1.2.3.4"))
	require.False(t, g.isBanned("5.6.7.8"))
	time.Sleep(30 * time.Millisecond)
	require.False(t, g.isBanned("1.2.3.4"))
}

func TestHTTPStatsDropsMQTTClient(t *testing.T) {
	l := NewHTTPStats("t1", testAddr, nil, &system.Info{Version: "test"})
	require.NoError(t, l.Init(logger))

	o := make(chan bool)
	go func() {
		l.Serve(MockEstablisher)
		o <- true
	}()
	defer func() {
		l.Close(MockCloser)
		<-o
	}()
	time.Sleep(10 * time.Millisecond)

	// http works before the ban
	resp, err := http.Get("http://localhost" + testAddr)
	require.NoError(t, err)
	_ = resp.Body.Close()

	// an mqtt client sending a CONNECT packet is dropped
	c, err := net.Dial("tcp", "127.0.0.1"+testAddr)
	require.NoError(t, err)
	_, err = c.Write([]byte{0x10, 0x10, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x02, 0x00, 0x3c, 0x00, 0x04, 'z', 'e', 'n', '3'})
	require.NoError(t, err)
	_ = c.SetReadDeadline(time.Now().Add(time.Second))
	_, err = io.ReadAll(c)
	require.NoError(t, err) // connection closed by server
	_ = c.Close()
	require.True(t, l.guard.isBanned("127.0.0.1"))

	// subsequent connections from the same ip, even valid http, are refused
	_, err = http.Get("http://127.0.0.1" + testAddr)
	require.Error(t, err)
}
