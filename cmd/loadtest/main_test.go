package main

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// memNode is a fake single kv node. If dropEvery > 0 it silently "loses"
// every Nth acknowledged write, so we can prove the verifier catches loss.
type memNode struct {
	mu        sync.Mutex
	data      map[string]string
	dropEvery int
	writes    int
}

func newMemNode(t *testing.T, dropEvery int) string {
	t.Helper()
	n := &memNode{data: map[string]string{}, dropEvery: dropEvery}
	h := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.mu.Lock()
		defer n.mu.Unlock()
		if r.URL.Path == "/status" {
			w.Write([]byte(`{"is_leader":true}`))
			return
		}
		key := strings.TrimPrefix(r.URL.Path, "/kv/")
		switch r.Method {
		case http.MethodPut:
			b, _ := io.ReadAll(r.Body)
			n.writes++
			if n.dropEvery == 0 || n.writes%n.dropEvery != 0 {
				n.data[key] = string(b)
			}
		case http.MethodGet:
			v, ok := n.data[key]
			if !ok {
				http.NotFound(w, r)
				return
			}
			w.Write([]byte(`{"key":"` + key + `","value":"` + v + `"}`))
		}
	}))
	t.Cleanup(h.Close)
	return strings.TrimPrefix(h.URL, "http://")
}

func TestParseFlagsDefaults(t *testing.T) {
	cfg, err := parseFlags(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.clients != 50 || cfg.mode != "write" || cfg.target != "any" || len(cfg.addrs) != 3 || !cfg.verify {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}

func TestParseFlagsRejectsInvalidValues(t *testing.T) {
	for _, args := range [][]string{
		{"-mode", "delete"},
		{"-target", "random"},
		{"-clients", "0"},
		{"-value-size", "0"},
		{"-unknown"},
	} {
		if _, err := parseFlags(args); err == nil {
			t.Errorf("expected an error for %v", args)
		}
	}
}

func TestPercentile(t *testing.T) {
	var lat []time.Duration
	for i := 1; i <= 100; i++ {
		lat = append(lat, time.Duration(i)*time.Millisecond)
	}
	cases := map[float64]time.Duration{50: 50 * time.Millisecond, 95: 95 * time.Millisecond, 99: 99 * time.Millisecond, 100: 100 * time.Millisecond, 0: time.Millisecond}
	for p, want := range cases {
		if got := percentile(lat, p); got != want {
			t.Errorf("p%v: got %v, want %v", p, got, want)
		}
	}
	if percentile(nil, 50) != 0 {
		t.Error("percentile of empty slice should be 0")
	}
}

func TestWriteRunVerifiesCleanly(t *testing.T) {
	addr := newMemNode(t, 0)
	cfg := config{addrs: []string{addr}, clients: 4, duration: 200 * time.Millisecond,
		mode: "write", target: "any", valueSize: 8, verify: true}

	r, err := runLoad(cfg)
	if err != nil {
		t.Fatalf("runLoad: %v", err)
	}
	if len(r.writeLat) == 0 || r.writeErrs != 0 {
		t.Fatalf("expected successful writes, got ok=%d errs=%d", len(r.writeLat), r.writeErrs)
	}
	if r.verifyFailed() {
		t.Fatalf("verification should pass on a node that loses nothing: %+v", r.verify)
	}

	var out bytes.Buffer
	r.print(&out, cfg)
	if !strings.Contains(out.String(), "verify: PASS") {
		t.Fatalf("expected PASS in report, got:\n%s", out.String())
	}
}

// The verifier is only useful if it actually catches lost data.
func TestVerifyDetectsLostWrites(t *testing.T) {
	addr := newMemNode(t, 5) // silently drops every 5th write
	acked := map[string]string{}
	client := &http.Client{Timeout: time.Second}
	for i := 0; i < 20; i++ {
		key := "k" + string(rune('a'+i))
		if err := put(client, addr, key, "v"); err != nil {
			t.Fatal(err)
		}
		acked[key] = "v"
	}

	v := verifyAcked(client, []string{addr}, acked, 300*time.Millisecond)
	if v.missing[addr] != 4 {
		t.Fatalf("expected 4 lost writes to be detected, got %v", v.missing)
	}
	r := &report{verify: v}
	if !r.verifyFailed() {
		t.Fatal("report should be marked failed")
	}
	var out bytes.Buffer
	r.print(&out, config{addrs: []string{addr}})
	if !strings.Contains(out.String(), "FAIL") {
		t.Fatalf("expected FAIL in report, got:\n%s", out.String())
	}
}

func TestVerifySkipsUnreachableNodes(t *testing.T) {
	addr := newMemNode(t, 0)
	client := &http.Client{Timeout: time.Second}
	put(client, addr, "k", "v")

	v := verifyAcked(client, []string{addr, "127.0.0.1:1"}, map[string]string{"k": "v"}, 300*time.Millisecond)
	if len(v.unreachable) != 1 || len(v.missing) != 0 {
		t.Fatalf("expected one unreachable node and no missing writes, got %+v", v)
	}
}

func TestReadAndMixedModes(t *testing.T) {
	addr := newMemNode(t, 0)
	for _, mode := range []string{"read", "mixed"} {
		cfg := config{addrs: []string{addr}, clients: 4, duration: 200 * time.Millisecond,
			mode: mode, target: "any", valueSize: 8, readKeys: 20, verify: true}
		r, err := runLoad(cfg)
		if err != nil {
			t.Fatalf("%s: %v", mode, err)
		}
		if len(r.readLat) == 0 || r.readErrs != 0 {
			t.Fatalf("%s: expected successful reads, got ok=%d errs=%d", mode, len(r.readLat), r.readErrs)
		}
		if mode == "mixed" && len(r.writeLat) == 0 {
			t.Fatal("mixed mode should also write")
		}
	}
}

func TestLeaderTargetFailsWithoutLeader(t *testing.T) {
	cfg := config{addrs: []string{"127.0.0.1:1"}, clients: 1, duration: time.Millisecond,
		mode: "write", target: "leader", valueSize: 1}
	if _, err := runLoad(cfg); err == nil {
		t.Fatal("expected an error when no leader can be found")
	}
}
