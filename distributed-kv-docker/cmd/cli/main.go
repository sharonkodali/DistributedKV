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
	addr := flag.String("addr", "localhost:8080", "server address (host:port)")
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		usage()
		os.Exit(1)
	}

	cmd := args[0]
	base := "http://" + *addr

	switch cmd {
	case "get":
		if len(args) != 2 {
			fmt.Println("usage: cli get <key>")
			os.Exit(1)
		}
		get(base, args[1])

	case "set":
		if len(args) != 3 {
			fmt.Println("usage: cli set <key> <value>")
			os.Exit(1)
		}
		set(base, args[1], args[2])

	case "delete":
		if len(args) != 2 {
			fmt.Println("usage: cli delete <key>")
			os.Exit(1)
		}
		del(base, args[1])

	case "status":
		status(base)

	default:
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Println("usage: cli [-addr host:port] <get|set|delete|status> [args]")
}

func get(base, key string) {
	resp, err := http.Get(base + "/kv/" + key)
	check(err)
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusNotFound {
		fmt.Printf("key %q not found\n", key)
		return
	}
	fmt.Println(strings.TrimSpace(string(body)))
}

func set(base, key, value string) {
	req, err := http.NewRequest(http.MethodPut, base+"/kv/"+key, strings.NewReader(value))
	check(err)

	resp, err := http.DefaultClient.Do(req)
	check(err)
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		fmt.Println("OK")
	} else {
		fmt.Printf("failed: status %d\n", resp.StatusCode)
	}
}

func del(base, key string) {
	req, err := http.NewRequest(http.MethodDelete, base+"/kv/"+key, nil)
	check(err)

	resp, err := http.DefaultClient.Do(req)
	check(err)
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		fmt.Println("OK")
	} else {
		fmt.Printf("failed: status %d\n", resp.StatusCode)
	}
}

func status(base string) {
	resp, err := http.Get(base + "/status")
	check(err)
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	fmt.Println(string(body))
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
