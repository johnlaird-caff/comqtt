package postgresql

import (
	"io"
	"log/slog"
	"net"
	"testing"

	"github.com/johnlaird-caff/comqtt/mqtt"
	"github.com/johnlaird-caff/comqtt/mqtt/hooks/auth"
	"github.com/johnlaird-caff/comqtt/mqtt/packets"
	"github.com/johnlaird-caff/comqtt/plugin"
	"github.com/stretchr/testify/require"
)

const path = "./testdata/conf.yml"

var (
	// Currently, the input is directed to /dev/null. If you need to
	// output to stdout, just modify 'io.Discard' here to 'os.Stdout'.
	logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	client = &mqtt.Client{
		ID: "test",
		Net: mqtt.ClientConnection{
			Remote:   "test.addr",
			Listener: "listener",
		},
		Properties: mqtt.ClientProperties{
			Username: []byte("zhangsan"),
			Clean:    false,
		},
	}

	//pkf = packets.Packet{Filters: packets.Subscriptions{{Filter: "a/b/c"}}}

	pkc = packets.Packet{Connect: packets.ConnectParams{Password: []byte("321654")}}
)

func teardown(a *Auth, t *testing.T) {
	if a.db != nil {
		a.Stop()
	}
}

func newAuth(t *testing.T) *Auth {
	a := new(Auth)
	a.SetOpts(logger, nil)

	err := a.Init(&Options{
		AuthMode: byte(auth.AuthUsername),
		AclMode:  byte(auth.AuthUsername),
		Dsn: DsnInfo{
			Host:          "localhost",
			Port:          5432,
			Schema:        "comqtt",
			SslMode:       "disable",
			LoginName:     "postgres",
			LoginPassword: "12345678",
		},
		Auth: AuthTable{
			Table:          "auth",
			UserColumn:     "username",
			PasswordColumn: "password",
			AllowColumn:    "allow",
		},
		Acl: AclTable{
			Table:        "acl",
			UserColumn:   "username",
			TopicColumn:  "topic",
			AccessColumn: "access",
		},
	})
	require.NoError(t, err)

	return a
}

func TestInitFromConfFile(t *testing.T) {
	if !hasPostgresql() {
		t.Skip("no postgresql server running")
	}
	a := new(Auth)
	a.SetOpts(logger, nil)
	opts := Options{}
	err := plugin.LoadYaml(path, &opts)
	require.NoError(t, err)

	err = a.Init(&opts)
	require.NoError(t, err)
}

func TestInitBadConfig(t *testing.T) {
	a := new(Auth)
	a.SetOpts(logger, nil)

	err := a.Init(map[string]any{})
	require.Error(t, err)
}

func TestOnConnectAuthenticate(t *testing.T) {
	if !hasPostgresql() {
		t.Skip("no postgresql server running")
	}
	a := newAuth(t)
	defer teardown(a, t)
	result := a.OnConnectAuthenticate(client, pkc)
	require.Equal(t, true, result)
}

func TestOnACLCheck(t *testing.T) {
	if !hasPostgresql() {
		t.Skip("no postgresql server running")
	}
	a := newAuth(t)
	defer teardown(a, t)
	topic := "topictest/1"
	topic2 := "topictest/2"
	result := a.OnACLCheck(client, topic, true) //publish
	require.Equal(t, true, result)
	result = a.OnACLCheck(client, topic, false) //subscribe
	require.Equal(t, false, result)
	result = a.OnACLCheck(client, topic2, true)
	require.Equal(t, false, result)
	result = a.OnACLCheck(client, topic2, false)
	require.Equal(t, false, result)
}

// hasPostgresql does a TCP connect to port 3306 to see if there is a MySQL server running on localhost.
func hasPostgresql() bool {
	c, err := net.Dial("tcp", "localhost:5432")
	if err != nil {
		return false
	}
	_ = c.Close()
	return true
}
