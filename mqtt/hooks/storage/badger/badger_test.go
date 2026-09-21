// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2022 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

package badger

import (
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	badgerhold "github.com/timshannon/badgerhold/v4"
	"github.com/wind-c/comqtt/v2/mqtt"
	"github.com/wind-c/comqtt/v2/mqtt/hooks/storage"
	"github.com/wind-c/comqtt/v2/mqtt/packets"
	"github.com/wind-c/comqtt/v2/mqtt/system"
)

var (
	logger = slog.New(slog.NewTextHandler(os.Stdout, nil))

	client = &mqtt.Client{
		ID: "test",
		Net: mqtt.ClientConnection{
			Remote:   "test.addr",
			Listener: "listener",
		},
		Properties: mqtt.ClientProperties{
			Username: []byte("username"),
			Clean:    false,
		},
	}

	pkf = packets.Packet{Filters: packets.Subscriptions{{Filter: "a/b/c"}}}
)

func teardown(t *testing.T, path string, h *Hook) {
	_ = h.Stop()
	_ = h.db.Badger().Close()
	err := os.RemoveAll("./" + strings.Replace(path, "..", "", -1))
	require.NoError(t, err)
}

func TestClientKey(t *testing.T) {
	k := clientKey(&mqtt.Client{ID: "cl1"})
	require.Equal(t, "cl1", k)
}

func TestSubscriptionKey(t *testing.T) {
	k := subscriptionKey(&mqtt.Client{ID: "cl1"}, "a/b/c")
	require.Equal(t, storage.SubscriptionKey+"_cl1:a/b/c", k)
}

func TestRetainedKey(t *testing.T) {
	k := retainedKey("a/b/c")
	require.Equal(t, storage.RetainedKey+"_a/b/c", k)
}

func TestInflightKey(t *testing.T) {
	k := inflightKey(&mqtt.Client{ID: "cl1"}, packets.Packet{PacketID: 1})
	require.Equal(t, storage.InflightKey+"_cl1:1", k)
}

func TestSysInfoKey(t *testing.T) {
	require.Equal(t, storage.SysInfoKey, sysInfoKey())
}

func TestID(t *testing.T) {
	h := new(Hook)
	require.Equal(t, "badger-db", h.ID())
}

func TestProvides(t *testing.T) {
	h := new(Hook)
	require.True(t, h.Provides(mqtt.OnSessionEstablished))
	require.True(t, h.Provides(mqtt.OnDisconnect))
	require.True(t, h.Provides(mqtt.OnSubscribed))
	require.True(t, h.Provides(mqtt.OnUnsubscribed))
	require.True(t, h.Provides(mqtt.OnRetainMessage))
	require.True(t, h.Provides(mqtt.OnQosPublish))
	require.True(t, h.Provides(mqtt.OnQosComplete))
	require.True(t, h.Provides(mqtt.OnQosDropped))
	require.True(t, h.Provides(mqtt.OnSysInfoTick))
	require.True(t, h.Provides(mqtt.StoredClients))
	require.True(t, h.Provides(mqtt.StoredInflightMessages))
	require.True(t, h.Provides(mqtt.StoredRetainedMessages))
	require.True(t, h.Provides(mqtt.StoredSubscriptions))
	require.True(t, h.Provides(mqtt.StoredSysInfo))
	require.True(t, h.Provides(mqtt.StoredClientByCid))
	require.True(t, h.Provides(mqtt.StoredSubscriptionsByCid))
	require.True(t, h.Provides(mqtt.StoredInflightMessagesByCid))
	require.False(t, h.Provides(mqtt.OnACLCheck))
	require.False(t, h.Provides(mqtt.OnConnectAuthenticate))
}

func TestInitBadConfig(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)

	err := h.Init(map[string]any{})
	require.Error(t, err)
}

func TestInitUseDefaults(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	defer teardown(t, h.config.Path, h)

	require.Equal(t, defaultDbFile, h.config.Path)
}

func TestOnSessionEstablishedThenOnDisconnect(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	defer teardown(t, h.config.Path, h)

	h.OnSessionEstablished(client, packets.Packet{})

	r := new(storage.Client)
	err = h.db.Get(clientKey(client), r)
	require.NoError(t, err)
	require.Equal(t, client.ID, r.ID)
	require.Equal(t, client.Properties.Username, r.Username)
	require.Equal(t, client.Properties.Clean, r.Clean)
	require.Equal(t, client.Net.Remote, r.Remote)
	require.Equal(t, client.Net.Listener, r.Listener)
	require.NotSame(t, client, r)

	h.OnDisconnect(client, nil, false)
	r2 := new(storage.Client)
	err = h.db.Get(clientKey(client), r2)
	require.NoError(t, err)
	require.Equal(t, client.ID, r.ID)

	h.OnDisconnect(client, nil, true)
	r3 := new(storage.Client)
	err = h.db.Get(clientKey(client), r3)
	require.Error(t, err)
	require.ErrorIs(t, badgerhold.ErrNotFound, err)
	require.Empty(t, r3.ID)
}

func TestOnClientExpired(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	defer teardown(t, h.config.Path, h)

	cl := &mqtt.Client{ID: "cl1"}
	clientKey := clientKey(cl)

	err = h.db.Upsert(clientKey, &storage.Client{ID: cl.ID})
	require.NoError(t, err)

	r := new(storage.Client)
	err = h.db.Get(clientKey, r)
	require.NoError(t, err)
	require.Equal(t, cl.ID, r.ID)

	h.OnClientExpired(cl)
	err = h.db.Get(clientKey, r)
	require.Error(t, err)
	require.ErrorIs(t, badgerhold.ErrNotFound, err)
}

func TestOnClientExpiredNoDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	h.OnClientExpired(client)
}

func TestOnClientExpiredClosedDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	teardown(t, h.config.Path, h)
	h.OnClientExpired(client)
}

func TestOnSessionEstablishedNoDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	h.OnSessionEstablished(client, packets.Packet{})
}

func TestOnSessionEstablishedClosedDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	teardown(t, h.config.Path, h)
	h.OnSessionEstablished(client, packets.Packet{})
}

func TestOnWillSent(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	defer teardown(t, h.config.Path, h)

	c1 := client
	c1.Properties.Will.Flag = 1
	h.OnWillSent(c1, packets.Packet{})

	r := new(storage.Client)
	err = h.db.Get(clientKey(client), r)
	require.NoError(t, err)

	require.Equal(t, uint32(1), r.Will.Flag)
	require.NotSame(t, client, r)
}

func TestOnDisconnectNoDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	h.OnDisconnect(client, nil, false)
}

func TestOnDisconnectClosedDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	teardown(t, h.config.Path, h)
	h.OnDisconnect(client, nil, false)
}

func TestOnDisconnectSessionTakenOver(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)

	testClient := &mqtt.Client{
		ID: "test",
		Net: mqtt.ClientConnection{
			Remote:   "test.addr",
			Listener: "listener",
		},
		Properties: mqtt.ClientProperties{
			Username: []byte("username"),
			Clean:    false,
		},
	}

	testClient.Stop(packets.ErrSessionTakenOver)
	teardown(t, h.config.Path, h)
	h.OnDisconnect(testClient, nil, true)
}

func TestOnSubscribedThenOnUnsubscribed(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	defer teardown(t, h.config.Path, h)

	h.OnSubscribed(client, pkf, []byte{0}, []int{0})
	r := new(storage.Subscription)

	err = h.db.Get(subscriptionKey(client, pkf.Filters[0].Filter), r)
	require.NoError(t, err)
	require.Equal(t, client.ID, r.Client)
	require.Equal(t, pkf.Filters[0].Filter, r.Filter)
	require.Equal(t, byte(0), r.Qos)

	h.OnUnsubscribed(client, pkf, []byte{0}, []int{0})
	err = h.db.Get(subscriptionKey(client, pkf.Filters[0].Filter), r)
	require.Error(t, err)
	require.Equal(t, badgerhold.ErrNotFound, err)
}

func TestOnSubscribedNoDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	h.OnSubscribed(client, pkf, []byte{0}, []int{0})
}

func TestOnSubscribedClosedDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	teardown(t, h.config.Path, h)
	h.OnSubscribed(client, pkf, []byte{0}, []int{0})
}

func TestOnUnsubscribedNoDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	h.OnUnsubscribed(client, pkf, []byte{0}, []int{0})
}

func TestOnUnsubscribedClosedDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	teardown(t, h.config.Path, h)
	h.OnUnsubscribed(client, pkf, []byte{0}, []int{0})
}

func TestOnRetainMessageThenUnset(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	defer teardown(t, h.config.Path, h)

	pk := packets.Packet{
		FixedHeader: packets.FixedHeader{
			Retain: true,
		},
		Payload:   []byte("hello"),
		TopicName: "a/b/c",
	}

	h.OnRetainMessage(client, pk, 1)

	r := new(storage.Message)
	err = h.db.Get(retainedKey(pk.TopicName), r)
	require.NoError(t, err)
	require.Equal(t, pk.TopicName, r.TopicName)
	require.Equal(t, pk.Payload, r.Payload)

	h.OnRetainMessage(client, pk, -1)
	err = h.db.Get(retainedKey(pk.TopicName), r)
	require.Error(t, err)
	require.ErrorIs(t, err, badgerhold.ErrNotFound)

	// coverage: delete deleted
	h.OnRetainMessage(client, pk, -1)
	err = h.db.Get(retainedKey(pk.TopicName), r)
	require.Error(t, err)
	require.ErrorIs(t, err, badgerhold.ErrNotFound)
}

func TestOnRetainedExpired(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	defer teardown(t, h.config.Path, h)

	m := &storage.Message{
		ID:        retainedKey("a/b/c"),
		T:         storage.RetainedKey,
		TopicName: "a/b/c",
	}

	err = h.db.Upsert(m.ID, m)
	require.NoError(t, err)

	r := new(storage.Message)
	err = h.db.Get(m.ID, r)
	require.NoError(t, err)
	require.Equal(t, m.TopicName, r.TopicName)

	h.OnRetainedExpired(m.TopicName)
	err = h.db.Get(m.ID, r)
	require.Error(t, err)
	require.ErrorIs(t, err, badgerhold.ErrNotFound)
}

func TestOnRetainExpiredNoDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	h.OnRetainedExpired("a/b/c")
}

func TestOnRetainExpiredClosedDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	teardown(t, h.config.Path, h)
	h.OnRetainedExpired("a/b/c")
}

func TestOnRetainMessageNoDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	h.OnRetainMessage(client, packets.Packet{}, 0)
}

func TestOnRetainMessageClosedDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	teardown(t, h.config.Path, h)
	h.OnRetainMessage(client, packets.Packet{}, 0)
}

func TestOnQosPublishThenQOSComplete(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	defer teardown(t, h.config.Path, h)

	pk := packets.Packet{
		FixedHeader: packets.FixedHeader{
			Retain: true,
			Qos:    2,
		},
		Payload:   []byte("hello"),
		TopicName: "a/b/c",
	}

	h.OnQosPublish(client, pk, time.Now().Unix(), 0)

	r := new(storage.Message)
	err = h.db.Get(inflightKey(client, pk), r)
	require.NoError(t, err)
	require.Equal(t, pk.TopicName, r.TopicName)
	require.Equal(t, pk.Payload, r.Payload)

	// ensure dates are properly saved
	require.True(t, r.Sent > 0)
	require.True(t, time.Now().Unix()-1 < r.Sent)

	// OnQosDropped is a passthrough to OnQosComplete here
	h.OnQosDropped(client, pk)
	err = h.db.Get(inflightKey(client, pk), r)
	require.Error(t, err)
	require.ErrorIs(t, err, badgerhold.ErrNotFound)
}

func TestOnQosPublishNoDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	h.OnQosPublish(client, packets.Packet{}, time.Now().Unix(), 0)
}

func TestOnQosPublishClosedDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	teardown(t, h.config.Path, h)
	h.OnQosPublish(client, packets.Packet{}, time.Now().Unix(), 0)
}

func TestOnQosCompleteNoDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	h.OnQosComplete(client, packets.Packet{})
}

func TestOnQosCompleteClosedDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	teardown(t, h.config.Path, h)
	h.OnQosComplete(client, packets.Packet{})
}

func TestOnQosDroppedNoDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	h.OnQosDropped(client, packets.Packet{})
}

func TestOnSysInfoTick(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	defer teardown(t, h.config.Path, h)

	info := &system.Info{
		Version:       "2.0.0",
		BytesReceived: 100,
	}

	h.OnSysInfoTick(info)

	r := new(storage.SystemInfo)
	err = h.db.Get(storage.SysInfoKey, r)
	require.NoError(t, err)
	require.Equal(t, info.Version, r.Version)
	require.Equal(t, info.BytesReceived, r.BytesReceived)
	require.NotSame(t, info, r)
}

func TestOnSysInfoTickNoDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	h.OnSysInfoTick(new(system.Info))
}

func TestOnSysInfoTickClosedDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	teardown(t, h.config.Path, h)
	h.OnSysInfoTick(new(system.Info))
}

func TestStoredClients(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	defer teardown(t, h.config.Path, h)

	// populate with clients
	err = h.db.Upsert("cl1", &storage.Client{ID: "cl1", T: storage.ClientKey})
	require.NoError(t, err)

	err = h.db.Upsert("cl2", &storage.Client{ID: "cl2", T: storage.ClientKey})
	require.NoError(t, err)

	err = h.db.Upsert("cl3", &storage.Client{ID: "cl3", T: storage.ClientKey})
	require.NoError(t, err)

	r, err := h.StoredClients()
	require.NoError(t, err)
	require.Len(t, r, 3)
	require.Equal(t, "cl1", r[0].ID)
	require.Equal(t, "cl2", r[1].ID)
	require.Equal(t, "cl3", r[2].ID)
}

func TestStoredClientsNoDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	v, err := h.StoredClients()
	require.Empty(t, v)
	require.NoError(t, err)
}

func TestStoredSubscriptions(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	defer teardown(t, h.config.Path, h)

	// populate with subscriptions
	err = h.db.Upsert("sub1", &storage.Subscription{ID: "sub1", T: storage.SubscriptionKey})
	require.NoError(t, err)

	err = h.db.Upsert("sub2", &storage.Subscription{ID: "sub2", T: storage.SubscriptionKey})
	require.NoError(t, err)

	err = h.db.Upsert("sub3", &storage.Subscription{ID: "sub3", T: storage.SubscriptionKey})
	require.NoError(t, err)

	r, err := h.StoredSubscriptions()
	require.NoError(t, err)
	require.Len(t, r, 3)
	require.Equal(t, "sub1", r[0].ID)
	require.Equal(t, "sub2", r[1].ID)
	require.Equal(t, "sub3", r[2].ID)
}

func TestStoredSubscriptionsNoDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	v, err := h.StoredSubscriptions()
	require.Empty(t, v)
	require.NoError(t, err)
}

func TestStoredRetainedMessages(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	defer teardown(t, h.config.Path, h)

	// populate with messages
	err = h.db.Upsert("m1", &storage.Message{ID: "m1", T: storage.RetainedKey})
	require.NoError(t, err)

	err = h.db.Upsert("m2", &storage.Message{ID: "m2", T: storage.RetainedKey})
	require.NoError(t, err)

	err = h.db.Upsert("m3", &storage.Message{ID: "m3", T: storage.RetainedKey})
	require.NoError(t, err)

	err = h.db.Upsert("i3", &storage.Message{ID: "i3", T: storage.InflightKey})
	require.NoError(t, err)

	r, err := h.StoredRetainedMessages()
	require.NoError(t, err)
	require.Len(t, r, 3)
	require.Equal(t, "m1", r[0].ID)
	require.Equal(t, "m2", r[1].ID)
	require.Equal(t, "m3", r[2].ID)
}

func TestStoredRetainedMessagesNoDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	v, err := h.StoredRetainedMessages()
	require.Empty(t, v)
	require.NoError(t, err)
}

func TestStoredInflightMessages(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	defer teardown(t, h.config.Path, h)

	// populate with messages
	err = h.db.Upsert("i1", &storage.Message{ID: "i1", T: storage.InflightKey})
	require.NoError(t, err)

	err = h.db.Upsert("i2", &storage.Message{ID: "i2", T: storage.InflightKey})
	require.NoError(t, err)

	err = h.db.Upsert("i3", &storage.Message{ID: "i3", T: storage.InflightKey})
	require.NoError(t, err)

	err = h.db.Upsert("m1", &storage.Message{ID: "m1", T: storage.RetainedKey})
	require.NoError(t, err)

	r, err := h.StoredInflightMessages()
	require.NoError(t, err)
	require.Len(t, r, 3)
	require.Equal(t, "i1", r[0].ID)
	require.Equal(t, "i2", r[1].ID)
	require.Equal(t, "i3", r[2].ID)
}

func TestStoredInflightMessagesNoDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	v, err := h.StoredInflightMessages()
	require.Empty(t, v)
	require.NoError(t, err)
}

func TestStoredClientByCid(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	defer teardown(t, h.config.Path, h)

	h.OnSessionEstablished(client, packets.Packet{})

	other := &mqtt.Client{ID: "other-client"}
	h.OnSessionEstablished(other, packets.Packet{})

	r, err := h.StoredClientByCid(client.ID)
	require.NoError(t, err)
	require.Equal(t, client.ID, r.ID)
	require.Equal(t, client.Net.Remote, r.Remote)
}

func TestStoredClientByCidNotFound(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	defer teardown(t, h.config.Path, h)

	r, err := h.StoredClientByCid("does-not-exist")
	require.NoError(t, err)
	require.Empty(t, r.ID)
}

func TestStoredClientByCidNoDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	v, err := h.StoredClientByCid(client.ID)
	require.Empty(t, v)
	require.NoError(t, err)
}

func TestStoredSubscriptionsByCid(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	defer teardown(t, h.config.Path, h)

	other := &mqtt.Client{ID: "other-client"}
	otherFilter := packets.Packet{Filters: packets.Subscriptions{{Filter: "x/y/z"}}}

	// two filters for the client under test, one for a different client, to
	// confirm the by-cid query only returns the requested client's own rows
	h.OnSubscribed(client, pkf, []byte{0}, []int{0})
	h.OnSubscribed(client, otherFilter, []byte{1}, []int{0})
	h.OnSubscribed(other, pkf, []byte{0}, []int{0})

	r, err := h.StoredSubscriptionsByCid(client.ID)
	require.NoError(t, err)
	require.Len(t, r, 2)
	for _, sub := range r {
		require.Equal(t, client.ID, sub.Client)
	}
}

func TestStoredSubscriptionsByCidNoMatch(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	defer teardown(t, h.config.Path, h)

	h.OnSubscribed(client, pkf, []byte{0}, []int{0})

	r, err := h.StoredSubscriptionsByCid("does-not-exist")
	require.NoError(t, err)
	require.Empty(t, r)
}

func TestStoredSubscriptionsByCidNoDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	v, err := h.StoredSubscriptionsByCid(client.ID)
	require.Empty(t, v)
	require.NoError(t, err)
}

func TestStoredInflightMessagesByCid(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	defer teardown(t, h.config.Path, h)

	other := &mqtt.Client{ID: "other-client"}

	pk1 := packets.Packet{PacketID: 1, TopicName: "a/b/c", Payload: []byte("one")}
	pk2 := packets.Packet{PacketID: 2, TopicName: "a/b/c", Payload: []byte("two")}
	otherPk := packets.Packet{PacketID: 1, TopicName: "a/b/c", Payload: []byte("other")}

	// two messages queued for the client under test, one for a different
	// client with a colliding packet id, to confirm the by-cid query is
	// scoped correctly rather than matching on packet id alone
	h.OnQosPublish(client, pk1, time.Now().Unix(), 0)
	h.OnQosPublish(client, pk2, time.Now().Unix(), 0)
	h.OnQosPublish(other, otherPk, time.Now().Unix(), 0)

	r, err := h.StoredInflightMessagesByCid(client.ID)
	require.NoError(t, err)
	require.Len(t, r, 2)
	payloads := []string{string(r[0].Payload), string(r[1].Payload)}
	require.ElementsMatch(t, []string{"one", "two"}, payloads)
}

func TestStoredInflightMessagesByCidNoMatch(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	defer teardown(t, h.config.Path, h)

	h.OnQosPublish(client, packets.Packet{PacketID: 1}, time.Now().Unix(), 0)

	r, err := h.StoredInflightMessagesByCid("does-not-exist")
	require.NoError(t, err)
	require.Empty(t, r)
}

func TestStoredInflightMessagesByCidNoDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	v, err := h.StoredInflightMessagesByCid(client.ID)
	require.Empty(t, v)
	require.NoError(t, err)
}

// Regression test: by-cid history must survive an actual process restart, not
// just a query against a still-open store — i.e. this is the storage layer's
// half of the standalone (same-node) reconnect-after-restart scenario the
// broker's inflight/reconnect QA plan (P0a) targets. Simulates a restart by
// closing the store and opening a brand new Hook against the same on-disk
// path, rather than reusing the same *Hook/*badgerhold.Store handle.
func TestStoredByCidSurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(&Options{Path: dir})
	require.NoError(t, err)

	h.OnSessionEstablished(client, packets.Packet{})
	h.OnSubscribed(client, pkf, []byte{0}, []int{0})
	h.OnQosPublish(client, packets.Packet{PacketID: 1, TopicName: "a/b/c", Payload: []byte("queued")}, time.Now().Unix(), 0)

	require.NoError(t, h.Stop())

	restarted := new(Hook)
	restarted.SetOpts(logger, nil)
	err = restarted.Init(&Options{Path: dir})
	require.NoError(t, err)
	defer teardown(t, dir, restarted)

	cl, err := restarted.StoredClientByCid(client.ID)
	require.NoError(t, err)
	require.Equal(t, client.ID, cl.ID)

	subs, err := restarted.StoredSubscriptionsByCid(client.ID)
	require.NoError(t, err)
	require.Len(t, subs, 1)
	require.Equal(t, pkf.Filters[0].Filter, subs[0].Filter)

	msgs, err := restarted.StoredInflightMessagesByCid(client.ID)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, []byte("queued"), msgs[0].Payload)
}

func TestStoredSysInfo(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	defer teardown(t, h.config.Path, h)

	// populate with messages
	err = h.db.Upsert(storage.SysInfoKey, &storage.SystemInfo{
		ID: storage.SysInfoKey,
		Info: system.Info{
			Version: "2.0.0",
		},
		T: storage.SysInfoKey,
	})
	require.NoError(t, err)

	r, err := h.StoredSysInfo()
	require.NoError(t, err)
	require.Equal(t, "2.0.0", r.Info.Version)
}

func TestStoredSysInfoNoDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	v, err := h.StoredSysInfo()
	require.Empty(t, v)
	require.NoError(t, err)
}

func TestErrorf(t *testing.T) {
	// coverage: one day check log hook
	h := new(Hook)
	h.SetOpts(logger, nil)
	h.Errorf("test", 1, 2, 3)
}

func TestWarningf(t *testing.T) {
	// coverage: one day check log hook
	h := new(Hook)
	h.SetOpts(logger, nil)
	h.Warningf("test", 1, 2, 3)
}

func TestInfof(t *testing.T) {
	// coverage: one day check log hook
	h := new(Hook)
	h.SetOpts(logger, nil)
	h.Infof("test", 1, 2, 3)
}

func TestDebugf(t *testing.T) {
	// coverage: one day check log hook
	h := new(Hook)
	h.SetOpts(logger, nil)
	h.Debugf("test", 1, 2, 3)
}
