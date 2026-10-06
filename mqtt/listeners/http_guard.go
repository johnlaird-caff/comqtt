package listeners

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"
)

// DefaultHTTPBanDuration is how long a client which sends non-HTTP data (such as
// MQTT packets) to an HTTP listener is refused for.
const DefaultHTTPBanDuration = 5 * time.Minute

// errNotHTTP is returned from a guarded connection's first read when the client is
// not speaking HTTP, causing the http server to drop the connection.
var errNotHTTP = errors.New("non-http data received")

// httpGuard tracks clients which have been banned from an HTTP listener.
type httpGuard struct {
	mu       sync.Mutex
	banned   map[string]time.Time // ip -> ban expiry
	duration time.Duration
	tls      bool
	log      *slog.Logger
	id       string
}

func newHTTPGuard(id string, duration time.Duration, tls bool, log *slog.Logger) *httpGuard {
	if log == nil {
		log = slog.Default()
	}
	return &httpGuard{
		banned:   map[string]time.Time{},
		duration: duration,
		tls:      tls,
		log:      log,
		id:       id,
	}
}

// isBanned reports whether ip is currently banned, clearing expired bans.
func (g *httpGuard) isBanned(ip string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	until, ok := g.banned[ip]
	if !ok {
		return false
	}
	if time.Now().After(until) {
		delete(g.banned, ip)
		return false
	}
	return true
}

// ban bans ip for the guard's duration and logs the reason.
func (g *httpGuard) ban(ip, reason string) {
	now := time.Now()
	g.mu.Lock()
	for k, until := range g.banned {
		if now.After(until) {
			delete(g.banned, k)
		}
	}
	g.banned[ip] = now.Add(g.duration)
	g.mu.Unlock()

	g.log.Warn("dropped non-http client on http listener",
		"listener", g.id, "ip", ip, "reason", reason, "ban", g.duration)
}

// classify inspects the first bytes sent by a client and returns a non-empty
// reason if they are not the start of an HTTP request (or TLS handshake, when
// the listener uses TLS).
func (g *httpGuard) classify(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	first := b[0]
	if first >= 'A' && first <= 'Z' { // http method token
		return ""
	}
	if g.tls && first == 0x16 { // tls handshake record
		return ""
	}
	if first == 0x10 && (bytes.Contains(b, []byte("MQTT")) || bytes.Contains(b, []byte("MQIsdp"))) {
		return "mqtt connect packet"
	}
	if first>>4 >= 1 && first>>4 <= 14 { // mqtt control packet types 1-14
		return fmt.Sprintf("probable mqtt packet (type %d)", first>>4)
	}
	return fmt.Sprintf("non-http data (first byte 0x%02x)", first)
}

// guardedListener wraps a net.Listener, refusing banned clients and inspecting
// the first bytes of each new connection for non-HTTP traffic.
type guardedListener struct {
	net.Listener
	guard *httpGuard
}

func (l *guardedListener) Accept() (net.Conn, error) {
	for {
		c, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}

		ip := remoteIP(c)
		if l.guard.isBanned(ip) {
			_ = c.Close()
			continue
		}

		return &guardedConn{Conn: c, guard: l.guard, ip: ip}, nil
	}
}

// guardedConn checks the first data read from a client and fails the read if
// the client is not speaking HTTP.
type guardedConn struct {
	net.Conn
	guard   *httpGuard
	ip      string
	checked bool
}

func (c *guardedConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if !c.checked && n > 0 {
		c.checked = true
		if reason := c.guard.classify(p[:n]); reason != "" {
			c.guard.ban(c.ip, reason)
			_ = c.Conn.Close()
			return 0, errNotHTTP
		}
	}
	return n, err
}

func remoteIP(c net.Conn) string {
	addr := c.RemoteAddr().String()
	if host, _, err := net.SplitHostPort(addr); err == nil {
		return host
	}
	return addr
}
