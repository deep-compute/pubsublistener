// pubsublistener calls handlers when a message is received
// on a subscribed channel
package pubsublistener

import (
	"github.com/deep-compute/log"
	"github.com/garyburd/redigo/redis"
	"sync"
)

// All handlers must be of this form taking channel and data
type ListenHandler func(string, []byte)

// RedisPubSub represents a pubsub connection
type RedisPubSub struct {
	PubConn   redis.PubSubConn
	Listeners map[string]ListenHandler
	WaitGroup sync.WaitGroup
	sync.RWMutex
}

// Init takes the network and address of the redis server
func (r *RedisPubSub) Init(network, addr string) error {
	log.Info("connecting to redis", "network", network, "addr", addr)
	conn, e := redis.Dial(network, addr)
	if e != nil {
		return e
	}
	r.PubConn = redis.PubSubConn{conn}
	r.Listeners = make(map[string]ListenHandler)
	return nil
}

// Listen starts listening for subscribed channel broadcasts
func (r *RedisPubSub) Listen() {
	r.WaitGroup.Add(1)
	go r.eavesDrop()
}

// eavesDrop is an internal function to call the respective handler
func (r *RedisPubSub) eavesDrop() {
	defer r.WaitGroup.Done()
	for {
		switch n := r.PubConn.Receive().(type) {
		case redis.Message:
			r.RLock()
			handler := r.Listeners[n.Channel]
			r.RUnlock()
			handler(n.Channel, n.Data)

		// TODO : should I return if 0 subscribers ? or let it stay
		case redis.Subscription:
			if n.Count == 0 {
				return
			}
		case error:
			log.Error("failed to eavesdrop", "error", n)
			return
		}
	}
}

// Subscribe registers/updates a handler with a channel
func (r *RedisPubSub) Subscribe(station string, handler ListenHandler) {
	log.Info("subscribing", "station", station)
	r.Lock()
	defer r.Unlock()
	_, ok := r.Listeners[station]
	if ok {
		// already present for that station, changing handler
		r.Listeners[station] = handler
		return
	}

	r.PubConn.Subscribe(station)
	r.Listeners[station] = handler
}

// Unsubscribe removes the handler and the subscription to the channel
func (r *RedisPubSub) Unsubscribe(station string) {
	r.Lock()
	defer r.Unlock()
	log.Info("unsubscribing", "station", station)
	_, ok := r.Listeners[station]
	if !ok {
		// TODO : error you werent subscribed ?
		return
	}
	r.PubConn.Unsubscribe(station)
	delete(r.Listeners, station)
}

// UnsubscribeAll unsubscribes from all channels
func (r *RedisPubSub) UnsubscribeAll() {
	for listener, _ := range r.Listeners {
		r.Unsubscribe(listener)
	}
}

// Quit closes the redis connection
func (r *RedisPubSub) Quit() {
	log.Info("quitting redis pubsub")
	r.UnsubscribeAll()
	r.PubConn.Close()
}
