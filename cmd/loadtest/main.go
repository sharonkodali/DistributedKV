// Command loadtest drives concurrent clients against a running cluster and
// reports throughput and latency, then (optionally) checks that every
// write the cluster acknowledged is actually readable on every live node.
//
// Examples:
//
//	# 50 clients writing for 10s, spread across all nodes (followers forward to the leader)
//	go run ./cmd/loadtest -addrs localhost:8081,localhost:8082,localhost:8083 -mode write
//
//	# 50 clients reading for 10s from all nodes
//	go run ./cmd/loadtest -addrs localhost:8081,localhost:8082,localhost:8083 -mode read
//
//	# send writes straight to the leader (no forwarding hop)
//	go run ./cmd/loadtest -addrs ... -mode write -target leader
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type config struct {
	addrs     []string
	clients   int
	duration  time.Duration
	mode      string // write | read | mixed
	target    string // any | leader
	valueSize int
	readKeys  int
	verify    bool
}

func main() {
	cfg, err := parseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	report, err := runLoad(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	report.print(os.Stdout, cfg)
	if report.verifyFailed() {
		os.Exit(1)
	}
}

func parseFlags(args []string) (config, error) {
	fs := flag.NewFlagSet("loadtest", flag.ContinueOnError)
	addrs := fs.String("addrs", "localhost:8081,localhost:8082,localhost:8083", "comma-separated HTTP addresses of cluster nodes")
	clients := fs.Int("clients", 50, "number of concurrent clients")
	duration := fs.Duration("duration", 10*time.Second, "how long to run")
	mode := fs.String("mode", "write", "write | read | mixed (90% reads, 10% writes)")
	target := fs.String("target", "any", "where writes go: any (round-robin, followers forward) | leader")
	valueSize := fs.Int("value-size", 64, "bytes per written value")
	readKeys := fs.Int("read-keys", 1000, "keys to pre-load for read/mixed mode")
	verify := fs.Bool("verify", true, "after the run, check every acknowledged write on every live node")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}

	cfg := config{
		addrs: strings.Split(*addrs, ","), clients: *clients, duration: *duration,
		mode: *mode, target: *target, valueSize: *valueSize, readKeys: *readKeys, verify: *verify,
	}
	switch {
	case cfg.mode != "write" && cfg.mode != "read" && cfg.mode != "mixed":
		return cfg, fmt.Errorf("-mode must be write, read or mixed, got %q", cfg.mode)
	case cfg.target != "any" && cfg.target != "leader":
		return cfg, fmt.Errorf("-target must be any or leader, got %q", cfg.target)
	case cfg.clients < 1:
		return cfg, errors.New("-clients must be at least 1")
	case cfg.valueSize < 1:
		return cfg, errors.New("-value-size must be at least 1")
	}
	return cfg, nil
}

// ---------- the load run ----------

type clientResult struct {
	writeLat, readLat []time.Duration
	writeErrs         int
	readErrs          int
	acked             map[string]string // key -> value, only for successful writes
}

type report struct {
	elapsed           time.Duration
	writeLat, readLat []time.Duration
	writeErrs         int
	readErrs          int
	acked             map[string]string
	verify            *verifyResult
}

func runLoad(cfg config) (*report, error) {
	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        cfg.clients * len(cfg.addrs) * 2,
			MaxIdleConnsPerHost: cfg.clients * 2,
		},
	}
	value := strings.Repeat("x", cfg.valueSize)

	var leader atomic.Value // string: current leader HTTP address
	if cfg.target == "leader" || cfg.mode != "write" {
		addr, err := findLeader(client, cfg.addrs)
		if err != nil {
			return nil, err
		}
		leader.Store(addr)
	}

	// Read/mixed modes need data to read: pre-load keys through the leader.
	if cfg.mode != "write" {
		for i := 0; i < cfg.readKeys; i++ {
			if err := put(client, leader.Load().(string), fmt.Sprintf("preload-%d", i), value); err != nil {
				return nil, fmt.Errorf("preloading read keys: %w", err)
			}
		}
		// Give followers a moment to apply the preload before reading from them.
		time.Sleep(500 * time.Millisecond)
	}

	results := make([]*clientResult, cfg.clients)
	var wg sync.WaitGroup
	start := time.Now()
	deadline := start.Add(cfg.duration)

	for c := 0; c < cfg.clients; c++ {
		res := &clientResult{acked: map[string]string{}}
		results[c] = res
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			for i := 0; time.Now().Before(deadline); i++ {
				// Rotate through nodes; offset by client so load is spread evenly.
				node := cfg.addrs[(c+i)%len(cfg.addrs)]

				doWrite := cfg.mode == "write" || (cfg.mode == "mixed" && i%10 == 0)
				if doWrite {
					if cfg.target == "leader" {
						node = leader.Load().(string)
					}
					key := fmt.Sprintf("lt-%d-%d", c, i)
					t0 := time.Now()
					err := put(client, node, key, value)
					if err != nil {
						res.writeErrs++
						// Back off briefly: a dead node refuses connections
						// instantly, and without a pause one client would spin
						// out thousands of failures per second against it.
						time.Sleep(10 * time.Millisecond)
						if cfg.target == "leader" {
							// The leader may have died: look for the new one.
							if addr, ferr := findLeader(client, cfg.addrs); ferr == nil {
								leader.Store(addr)
							}
						}
						continue
					}
					res.writeLat = append(res.writeLat, time.Since(t0))
					res.acked[key] = value
				} else {
					key := fmt.Sprintf("preload-%d", (c*7919+i)%cfg.readKeys)
					t0 := time.Now()
					if _, ok, err := get(client, node, key); err != nil || !ok {
						res.readErrs++
						time.Sleep(10 * time.Millisecond)
						continue
					}
					res.readLat = append(res.readLat, time.Since(t0))
				}
			}
		}(c)
	}
	wg.Wait()

	r := &report{elapsed: time.Since(start), acked: map[string]string{}}
	for _, res := range results {
		r.writeLat = append(r.writeLat, res.writeLat...)
		r.readLat = append(r.readLat, res.readLat...)
		r.writeErrs += res.writeErrs
		r.readErrs += res.readErrs
		for k, v := range res.acked {
			r.acked[k] = v
		}
	}

	if cfg.verify && len(r.acked) > 0 {
		r.verify = verifyAcked(client, cfg.addrs, r.acked, 15*time.Second)
	}
	return r, nil
}

// ---------- verification ----------

type verifyResult struct {
	checked     int
	missing     map[string]int // node -> acknowledged writes it doesn't have
	unreachable []string
}

// verifyAcked reads every acknowledged key back from every reachable node.
// Followers can lag slightly behind the leader, so a node is re-checked
// until it has everything or the timeout passes.
func verifyAcked(client *http.Client, addrs []string, acked map[string]string, timeout time.Duration) *verifyResult {
	res := &verifyResult{checked: len(acked), missing: map[string]int{}}

	for _, addr := range addrs {
		if _, err := client.Get("http://" + addr + "/status"); err != nil {
			res.unreachable = append(res.unreachable, addr)
			continue
		}
		pending := make(map[string]string, len(acked))
		for k, v := range acked {
			pending[k] = v
		}
		deadline := time.Now().Add(timeout)
		for len(pending) > 0 && time.Now().Before(deadline) {
			for k, want := range pending {
				if got, ok, err := get(client, addr, k); err == nil && ok && got == want {
					delete(pending, k)
				}
			}
			if len(pending) > 0 {
				time.Sleep(200 * time.Millisecond)
			}
		}
		if len(pending) > 0 {
			res.missing[addr] = len(pending)
		}
	}
	return res
}

func (r *report) verifyFailed() bool {
	return r.verify != nil && len(r.verify.missing) > 0
}

// ---------- HTTP helpers ----------

func put(client *http.Client, addr, key, value string) error {
	req, err := http.NewRequest(http.MethodPut, "http://"+addr+"/kv/"+key, strings.NewReader(value))
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body) // drain so the connection is reused
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("PUT %s: status %d", key, resp.StatusCode)
	}
	return nil
}

// get returns (value, found, error).
func get(client *http.Client, addr, key string) (string, bool, error) {
	resp, err := client.Get("http://" + addr + "/kv/" + key)
	if err != nil {
		return "", false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		io.Copy(io.Discard, resp.Body)
		return "", false, nil
	}
	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body)
		return "", false, fmt.Errorf("GET %s: status %d", key, resp.StatusCode)
	}
	var body struct {
		Value string `json:"value"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "", false, err
	}
	return body.Value, true, nil
}

// findLeader asks each node's /status and returns the HTTP address of the
// one that reports is_leader=true.
func findLeader(client *http.Client, addrs []string) (string, error) {
	for _, addr := range addrs {
		resp, err := client.Get("http://" + addr + "/status")
		if err != nil {
			continue
		}
		var s struct {
			IsLeader bool `json:"is_leader"`
		}
		err = json.NewDecoder(resp.Body).Decode(&s)
		resp.Body.Close()
		if err == nil && s.IsLeader {
			return addr, nil
		}
	}
	return "", errors.New("no node reports itself as leader")
}

// ---------- reporting ----------

// percentile returns the p-th percentile (0-100) of an ascending-sorted slice
// using the nearest-rank method.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(p/100*float64(len(sorted))+0.5) - 1
	if rank < 0 {
		rank = 0
	}
	if rank >= len(sorted) {
		rank = len(sorted) - 1
	}
	return sorted[rank]
}

func summarize(w io.Writer, name string, lat []time.Duration, errs int, elapsed time.Duration) {
	if len(lat) == 0 && errs == 0 {
		return
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	var total time.Duration
	for _, l := range lat {
		total += l
	}
	mean := time.Duration(0)
	if len(lat) > 0 {
		mean = total / time.Duration(len(lat))
	}
	ms := func(d time.Duration) string { return fmt.Sprintf("%.2fms", float64(d.Microseconds())/1000) }
	fmt.Fprintf(w, "%-6s ok=%-8d errors=%-6d throughput=%.0f/s\n", name, len(lat), errs, float64(len(lat))/elapsed.Seconds())
	fmt.Fprintf(w, "       latency mean=%s p50=%s p95=%s p99=%s max=%s\n",
		ms(mean), ms(percentile(lat, 50)), ms(percentile(lat, 95)), ms(percentile(lat, 99)), ms(percentile(lat, 100)))
}

func (r *report) print(w io.Writer, cfg config) {
	fmt.Fprintf(w, "mode=%s target=%s clients=%d nodes=%d duration=%s value=%dB\n",
		cfg.mode, cfg.target, cfg.clients, len(cfg.addrs), r.elapsed.Round(time.Millisecond), cfg.valueSize)
	summarize(w, "writes", r.writeLat, r.writeErrs, r.elapsed)
	summarize(w, "reads", r.readLat, r.readErrs, r.elapsed)

	if r.verify == nil {
		return
	}
	v := r.verify
	fmt.Fprintf(w, "verify: %d acknowledged writes checked on %d live node(s)",
		v.checked, len(cfg.addrs)-len(v.unreachable))
	if len(v.unreachable) > 0 {
		fmt.Fprintf(w, " (skipped unreachable: %s)", strings.Join(v.unreachable, ", "))
	}
	fmt.Fprintln(w)
	if len(v.missing) == 0 {
		fmt.Fprintln(w, "verify: PASS - zero acknowledged writes lost")
		return
	}
	for addr, n := range v.missing {
		fmt.Fprintf(w, "verify: FAIL - %s is missing %d acknowledged writes\n", addr, n)
	}
}
