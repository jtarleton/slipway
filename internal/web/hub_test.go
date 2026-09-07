package web

import "testing"

func TestHubDeliversToEverySubscriber(t *testing.T) {
	h := newHub()
	a, cancelA := h.subscribe()
	b, cancelB := h.subscribe()
	defer cancelA()
	defer cancelB()

	h.broadcast("grid", "[]")

	for _, ch := range []<-chan event{a, b} {
		select {
		case e := <-ch:
			if e.name != "grid" || e.data != "[]" {
				t.Fatalf("got %+v", e)
			}
		default:
			t.Fatal("subscriber received nothing")
		}
	}
}

func TestHubUnsubscribeStopsDelivery(t *testing.T) {
	h := newHub()
	ch, cancel := h.subscribe()
	cancel()

	h.broadcast("jobs", "[]")

	if _, ok := <-ch; ok {
		t.Fatal("channel still open and delivering after unsubscribe")
	}
	cancel() // must be safe to call twice
}

func TestHubDropsForASlowConsumer(t *testing.T) {
	h := newHub()
	_, cancel := h.subscribe()
	defer cancel()

	// Far more than the channel buffer; broadcast must not block.
	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			h.broadcast("log", "x")
		}
		close(done)
	}()

	<-done
}
