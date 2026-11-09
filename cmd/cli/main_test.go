package main

import (
	"bytes"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// fakeServer is an in-memory stand-in for a kv node, so CLI tests check
// the CLI's own behaviour (requests sent, output printed, exit codes)
// without starting Raft.
type fakeServer struct {
	mu   sync.Mutex
	data map[string]string
	// lastMethod/lastPath record the most recent request for assertions.
	lastMethod, lastPath string
}

func newFakeServer(t *testing.T) (*fakeServer, string) {
	t.Helper()
	f := &fakeServer{data: map[string]string{}}
	h := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.lastMethod, f.lastPath = r.Method, r.URL.Path

		if r.URL.Path == "/status" {
			w.Write([]byte(`{"status":"ok"}`))
			return
		}
		key := strings.TrimPrefix(r.URL.Path, "/kv/")
		switch r.Method {
		case http.MethodGet:
			v, ok := f.data[key]
			if !ok {
				http.Error(w, "key not found", http.StatusNotFound)
				return
			}
			w.Write([]byte(`{"key":"` + key + `","value":"` + v + `"}` + "\n"))
		case http.MethodPut:
			b, _ := io.ReadAll(r.Body)
			if key == "reject" {
				http.Error(w, "boom", http.StatusInternalServerError)
				return
			}
			f.data[key] = string(b)
		case http.MethodDelete:
			if key == "reject" {
				http.Error(w, "unavailable", http.StatusServiceUnavailable)
				return
			}
			delete(f.data, key)
		}
	}))
	t.Cleanup(h.Close)
	return f, strings.TrimPrefix(h.URL, "http://")
}

func runCLI(args ...string) (code int, stdout, stderr string) {
	var out, errOut bytes.Buffer
	code = run(args, &out, &errOut)
	return code, out.String(), errOut.String()
}

func TestSetSendsPutAndPrintsOK(t *testing.T) {
	f, addr := newFakeServer(t)
	code, out, _ := runCLI("-addr", addr, "set", "username", "John")

	if code != 0 || strings.TrimSpace(out) != "OK" {
		t.Fatalf("expected exit 0 and OK, got %d %q", code, out)
	}
	if f.lastMethod != http.MethodPut || f.lastPath != "/kv/username" || f.data["username"] != "John" {
		t.Fatalf("unexpected request: %s %s, data %v", f.lastMethod, f.lastPath, f.data)
	}
}

func TestGetPrintsServerResponse(t *testing.T) {
	f, addr := newFakeServer(t)
	f.data["username"] = "John"

	code, out, _ := runCLI("-addr", addr, "get", "username")
	if code != 0 || strings.TrimSpace(out) != `{"key":"username","value":"John"}` {
		t.Fatalf("unexpected output: %d %q", code, out)
	}
}

func TestGetMissingKeyPrintsNotFound(t *testing.T) {
	_, addr := newFakeServer(t)
	code, out, _ := runCLI("-addr", addr, "get", "ghost")

	if code != 0 || !strings.Contains(out, `key "ghost" not found`) {
		t.Fatalf("unexpected output: %d %q", code, out)
	}
}

func TestDeleteSendsDelete(t *testing.T) {
	f, addr := newFakeServer(t)
	f.data["k"] = "v"

	code, out, _ := runCLI("-addr", addr, "delete", "k")
	if code != 0 || strings.TrimSpace(out) != "OK" {
		t.Fatalf("unexpected output: %d %q", code, out)
	}
	if f.lastMethod != http.MethodDelete {
		t.Fatalf("expected DELETE, got %s", f.lastMethod)
	}
	if _, ok := f.data["k"]; ok {
		t.Fatal("key should have been deleted")
	}
}

func TestStatusPrintsBody(t *testing.T) {
	_, addr := newFakeServer(t)
	code, out, _ := runCLI("-addr", addr, "status")
	if code != 0 || strings.TrimSpace(out) != `{"status":"ok"}` {
		t.Fatalf("unexpected output: %d %q", code, out)
	}
}

func TestSetReportsServerFailureStatus(t *testing.T) {
	_, addr := newFakeServer(t)
	_, out, _ := runCLI("-addr", addr, "set", "reject", "v")
	if !strings.Contains(out, "failed: status 500") {
		t.Fatalf("expected failure status to be printed, got %q", out)
	}
}

func TestDeleteReportsServerFailureStatus(t *testing.T) {
	_, addr := newFakeServer(t)
	_, out, _ := runCLI("-addr", addr, "delete", "reject")
	if !strings.Contains(out, "failed: status 503") {
		t.Fatalf("expected failure status to be printed, got %q", out)
	}
}

func TestValueWithSpacesIsSentIntact(t *testing.T) {
	f, addr := newFakeServer(t)
	runCLI("-addr", addr, "set", "msg", "hello world")
	if f.data["msg"] != "hello world" {
		t.Fatalf("value changed in transit: %q", f.data["msg"])
	}
}

func TestWrongArgCountsExitNonZero(t *testing.T) {
	cases := [][]string{
		{"get"},
		{"get", "a", "b"},
		{"set", "only-key"},
		{"set", "k", "v", "extra"},
		{"delete"},
	}
	for _, args := range cases {
		code, out, _ := runCLI(args...)
		if code != 1 || !strings.Contains(out, "usage:") {
			t.Errorf("args %v: expected exit 1 with usage, got %d %q", args, code, out)
		}
	}
}

func TestNoCommandPrintsUsage(t *testing.T) {
	code, out, _ := runCLI()
	if code != 1 || !strings.Contains(out, "usage: cli") {
		t.Fatalf("expected usage and exit 1, got %d %q", code, out)
	}
}

func TestUnknownCommandPrintsUsage(t *testing.T) {
	code, out, _ := runCLI("frobnicate")
	if code != 1 || !strings.Contains(out, "usage: cli") {
		t.Fatalf("expected usage and exit 1, got %d %q", code, out)
	}
}

func TestBadFlagExitsNonZero(t *testing.T) {
	if code, _, _ := runCLI("-nope"); code != 1 {
		t.Fatalf("expected exit 1 for an unknown flag, got %d", code)
	}
}

// With no server listening, every command must fail cleanly on stderr
// with exit 1 instead of panicking.
func TestUnreachableServerReportsError(t *testing.T) {
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := l.Addr().String()
	l.Close() // now nothing is listening on addr

	for _, args := range [][]string{
		{"get", "k"}, {"set", "k", "v"}, {"delete", "k"}, {"status"},
	} {
		code, _, errOut := runCLI(append([]string{"-addr", addr}, args...)...)
		if code != 1 || !strings.Contains(errOut, "error:") {
			t.Errorf("%v: expected exit 1 with an error, got %d %q", args, code, errOut)
		}
	}
}

// A key containing a space produces an invalid URL; that must surface as an
// error rather than a crash.
func TestInvalidURLReportsError(t *testing.T) {
	for _, args := range [][]string{{"set", "k", "v"}, {"delete", "k"}} {
		code, _, errOut := runCLI(append([]string{"-addr", "bad host"}, args...)...)
		if code != 1 || !strings.Contains(errOut, "error:") {
			t.Errorf("%v: expected exit 1 with an error, got %d %q", args, code, errOut)
		}
	}
}
