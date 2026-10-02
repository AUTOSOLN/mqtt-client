package mqttclient

import "sync"

// dispatcher runs callbacks one at a time in the order they were posted. A
// goroutine runs only while callbacks are queued, so an idle client holds no
// goroutine.
type dispatcher struct {
	mu      sync.Mutex
	queue   []func()
	running bool
}

func (d *dispatcher) post(f func()) {
	d.mu.Lock()
	d.queue = append(d.queue, f)
	if !d.running {
		d.running = true
		go d.run()
	}
	d.mu.Unlock()
}

func (d *dispatcher) run() {
	for {
		d.mu.Lock()
		if len(d.queue) == 0 {
			d.running = false
			d.mu.Unlock()
			return
		}
		f := d.queue[0]
		d.queue[0] = nil
		d.queue = d.queue[1:]
		d.mu.Unlock()
		f()
	}
}
