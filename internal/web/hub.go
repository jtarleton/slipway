package web

import "sync"

// event is one Server-Sent Event: a name the browser listens for and a JSON
// payload.
type event struct {
	name string
	data string
}

// hub fans one stream of events out to every connected browser.
//
// Slipway is single-tenant and the event volume is a handful per second, so
// this is deliberately the naive design: a map of channels under a mutex, a
// slow consumer drops events rather than stalling the others.
type hub struct {
	mu   sync.Mutex
	subs map[chan event]struct{}
}

func newHub() *hub {
	return &hub{subs: make(map[chan event]struct{})}
}

// subscribe returns a channel of events and a function that unsubscribes it.
func (h *hub) subscribe() (<-chan event, func()) {
	ch := make(chan event, 32)
	h.mu.Lock()
	h.subs[ch] = struct{}{}
	h.mu.Unlock()

	return ch, func() {
		h.mu.Lock()
		if _, ok := h.subs[ch]; ok {
			delete(h.subs, ch)
			close(ch)
		}
		h.mu.Unlock()
	}
}

// broadcast delivers e to every subscriber, skipping any whose buffer is full.
// A browser that cannot keep up misses intermediate events and catches up on
// the next full snapshot.
func (h *hub) broadcast(name, data string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.subs {
		select {
		case ch <- event{name: name, data: data}:
		default:
		}
	}
}
