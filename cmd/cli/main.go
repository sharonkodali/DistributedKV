// Command cli is a small client for talking to a running kv server.
//
// Usage:
//
//	go run ./cmd/cli -addr localhost:8080 get username
//	go run ./cmd/cli -addr localhost:8080 set username John
//	go run ./cmd/cli -addr localhost:8080 delete username
//	go run ./cmd/cli -addr localhost:8080 status
package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run is the whole CLI minus the process exit, so tests can call it with
// any arguments and inspect what it printed and its exit code.
func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("cli", flag.ContinueOnError)
	fs.SetOutput(stderr)
	addr := fs.String("addr", "localhost:8080", "server address (host:port)")
	if err := fs.Parse(args); err != nil {
		return 1
	}

	rest := fs.Args()
	if len(rest) < 1 {
		usage(stdout)
		return 1
	}

	cmd := rest[0]
	base := "http://" + *addr

	var err error
	switch cmd {
	case "get":
		if len(rest) != 2 {
			fmt.Fprintln(stdout, "usage: cli get <key>")
			return 1
		}
		err = get(stdout, base, rest[1])

	case "set":
		if len(rest) != 3 {
			fmt.Fprintln(stdout, "usage: cli set <key> <value>")
			return 1
		}
		err = set(stdout, base, rest[1], rest[2])

	case "delete":
		if len(rest) != 2 {
			fmt.Fprintln(stdout, "usage: cli delete <key>")
			return 1
		}
		err = del(stdout, base, rest[1])

	case "status":
		err = status(stdout, base)

	default:
		usage(stdout)
		return 1
	}

	if err != nil {
		fmt.Fprintln(stderr, "error:", err)
		return 1
	}
	return 0
}

func usage(w io.Writer) {
	fmt.Fprintln(w, "usage: cli [-addr host:port] <get|set|delete|status> [args]")
}

func get(w io.Writer, base, key string) error {
	resp, err := http.Get(base + "/kv/" + key)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusNotFound {
		fmt.Fprintf(w, "key %q not found\n", key)
		return nil
	}
	fmt.Fprintln(w, strings.TrimSpace(string(body)))
	return nil
}

func set(w io.Writer, base, key, value string) error {
	req, err := http.NewRequest(http.MethodPut, base+"/kv/"+key, strings.NewReader(value))
	if err != nil {
		return err
	}
	return sendWrite(w, req)
}

func del(w io.Writer, base, key string) error {
	req, err := http.NewRequest(http.MethodDelete, base+"/kv/"+key, nil)
	if err != nil {
		return err
	}
	return sendWrite(w, req)
}

// sendWrite runs a PUT/DELETE and prints OK or the failing status code.
func sendWrite(w io.Writer, req *http.Request) error {
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		fmt.Fprintln(w, "OK")
	} else {
		fmt.Fprintf(w, "failed: status %d\n", resp.StatusCode)
	}
	return nil
}

func status(w io.Writer, base string) error {
	resp, err := http.Get(base + "/status")
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	fmt.Fprintln(w, string(body))
	return nil
}
