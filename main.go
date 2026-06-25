package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type Config struct {
	ServiceDNS          string
	GarageBin           string
	RPCPort             string
	AdminPort           string
	RPCSecret           string
	AdminToken          string
	ExpectedNodes       int
	LayoutCapacity      string
	ZonePrefix          string
	BucketName          string
	AccessKeyID         string
	SecretKey           string
	Interval            time.Duration
	RequestTimeout      time.Duration
	CreateBucket        bool
	ImportKey           bool
	ReplaceOfflineNodes bool
	DryRun              bool
}

type Node struct {
	FullID string
	Short  string
	IP     string
	Peer   string
}

type APIClient struct {
	base  string
	token string
	hc    *http.Client
}

var (
	fullIDRe  = regexp.MustCompile(`(?i)\b[0-9a-f]{64}\b`)
	shortIDRe = regexp.MustCompile(`(?i)\b[0-9a-f]{16}\b`)
	versionRe = regexp.MustCompile(`(?m)Current cluster layout version:\s*(\d+)`)

	clusterBootstrapped atomic.Bool
	keyBucketEnsured    atomic.Bool
	lastNodeSet         atomic.Value
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	log.Printf("garage-reconciler starting: dns=%s expected_nodes=%d interval=%s dry_run=%v", cfg.ServiceDNS, cfg.ExpectedNodes, cfg.Interval, cfg.DryRun)

	for {
		ctx, cancel := context.WithTimeout(context.Background(), maxDuration(6*cfg.RequestTimeout, 60*time.Second))
		err := reconcile(ctx, cfg)
		cancel()

		if err != nil {
			log.Printf("reconcile failed: %v", err)
		}

		time.Sleep(cfg.Interval)
	}
}

func reconcile(ctx context.Context, cfg Config) error {
	ips, err := resolveIPs(ctx, cfg.ServiceDNS)
	if err != nil {
		return err
	}
	if len(ips) == 0 {
		return fmt.Errorf("no A/AAAA records for %q", cfg.ServiceDNS)
	}

	nodes := make([]Node, 0, len(ips))
	for _, ip := range ips {
		fullID, err := garageNodeIDV23(ctx, cfg, ip)
		if err != nil {
			log.Printf("node id not ready at %s:%s: %v", ip, cfg.AdminPort, err)
			continue
		}

		n := Node{
			FullID: strings.ToLower(fullID),
			Short:  shortID(fullID),
			IP:     ip,
			Peer:   fmt.Sprintf("%s@%s:%s", strings.ToLower(fullID), ip, cfg.RPCPort),
		}
		nodes = append(nodes, n)
	}

	nodes = uniqueNodes(nodes)
	if len(nodes) == 0 {
		return errors.New("no live garage nodes reachable through Admin API")
	}

	nodeSet := nodeSetKey(nodes)
	if prev, ok := lastNodeSet.Load().(string); !ok || prev != nodeSet {
		clusterBootstrapped.Store(false)
		keyBucketEnsured.Store(false)
		lastNodeSet.Store(nodeSet)
	}

	if len(nodes) < cfg.ExpectedNodes {
		log.Printf("only %d/%d garage nodes visible; layout apply skipped", len(nodes), cfg.ExpectedNodes)
		if !keyBucketEnsured.Load() && cfg.CreateBucket {
			client := newClient(endpointForIP(nodes[0].IP, cfg.AdminPort), cfg)
			if err := ensureBucketAndKey(ctx, cfg, client); err != nil {
				log.Printf("bucket/key reconcile warning: %v", err)
			} else {
				keyBucketEnsured.Store(true)
			}
		}
		return nil
	}

	if !clusterBootstrapped.Load() {
		for _, n := range nodes {
			for _, peer := range nodes {
				if peer.FullID == n.FullID {
					continue
				}

				if cfg.DryRun {
					log.Printf("dry-run node connect on %s -> %s", n.Short, peer.Short)
					continue
				}

				out, err := garageCLI(ctx, cfg, n.Peer, "node", "connect", peer.Peer)
				if err != nil {
					log.Printf("node connect on %s -> %s warning: %v output=%s", n.Short, peer.Short, err, trimOutput(out))
				} else {
					log.Printf("node connect on %s -> %s OK", n.Short, peer.Short)
				}
			}
		}

		changed, err := reconcileLayout(ctx, cfg, nodes[0].Peer, nodes)
		if err != nil {
			return err
		}

		if changed {
			log.Printf("layout changed/applied; data repair will be handled by Garage background workers")
		}

		clusterBootstrapped.Store(true)
	} else {
		log.Printf("cluster layout already bootstrapped for %d nodes; no RPC action needed", len(nodes))
	}

	if cfg.CreateBucket && !keyBucketEnsured.Load() {
		client := newClient(endpointForIP(nodes[0].IP, cfg.AdminPort), cfg)
		if err := ensureBucketAndKey(ctx, cfg, client); err != nil {
			log.Printf("bucket/key reconcile warning: %v", err)
		} else {
			keyBucketEnsured.Store(true)
		}
	}

	return nil
}

func reconcileLayout(ctx context.Context, cfg Config, primaryPeer string, nodes []Node) (bool, error) {
	layoutBefore, err := garageCLI(ctx, cfg, primaryPeer, "layout", "show")
	if err != nil {
		return false, fmt.Errorf("layout show before: %w output=%s", err, trimOutput(layoutBefore))
	}

	roleIDs := parseLayoutRoleIDs(layoutBefore)
	currentVersion := parseLayoutVersion(layoutBefore)

	activeShorts := map[string]bool{}
	for _, n := range nodes {
		activeShorts[n.Short] = true
	}

	changed := false

	for i, n := range nodes {
		if roleIDs[n.Short] {
			continue
		}

		changed = true
		zone := fmt.Sprintf("%s%d", cfg.ZonePrefix, i+1)
		log.Printf("layout assign %s zone=%s capacity=%s", n.Short, zone, cfg.LayoutCapacity)

		if !cfg.DryRun {
			out, err := garageCLI(ctx, cfg, primaryPeer, "layout", "assign", n.Short, "-z", zone, "-c", cfg.LayoutCapacity)
			if err != nil {
				return false, fmt.Errorf("layout assign %s: %w output=%s", n.Short, err, trimOutput(out))
			}
		}
	}

	if cfg.ReplaceOfflineNodes {
		for id := range roleIDs {
			if activeShorts[id] {
				continue
			}

			changed = true
			log.Printf("layout remove offline node %s", id)

			if !cfg.DryRun {
				out, err := garageCLI(ctx, cfg, primaryPeer, "layout", "remove", id)
				if err != nil {
					log.Printf("layout remove %s warning: %v output=%s", id, err, trimOutput(out))
				}
			}
		}
	}

	if !changed {
		log.Printf("layout already contains all visible nodes; no layout update needed")
		return false, nil
	}

	if cfg.DryRun {
		return true, nil
	}

	applyVersion := currentVersion + 1
	if applyVersion <= 0 {
		applyVersion = 1
	}

	log.Printf("layout apply --version %d", applyVersion)
	out, err := garageCLI(ctx, cfg, primaryPeer, "layout", "apply", "--version", strconv.FormatInt(applyVersion, 10))
	if err != nil {
		return false, fmt.Errorf("layout apply: %w output=%s", err, trimOutput(out))
	}

	return true, nil
}

func garageNodeIDV23(ctx context.Context, cfg Config, ip string) (string, error) {
	client := newClient(endpointForIP(ip, cfg.AdminPort), cfg)

	body, err := client.getRaw(ctx, "/v2/GetClusterStatus", nil)
	if err != nil {
		return "", err
	}

	var decoded any
	if err := json.Unmarshal(body, &decoded); err == nil {
		if id := findNodeIDForAddr(decoded, ip, cfg.RPCPort); id != "" {
			return id, nil
		}
	}

	ids := uniqueStrings(fullIDRe.FindAllString(strings.ToLower(string(body)), -1))
	if len(ids) == 1 {
		return ids[0], nil
	}

	if len(ids) == 0 {
		return "", fmt.Errorf("GetClusterStatus returned no 64-hex node id")
	}

	return "", fmt.Errorf("GetClusterStatus returned multiple node ids and none matched %s:%s", ip, cfg.RPCPort)
}

func findNodeIDForAddr(v any, ip string, port string) string {
	want1 := ip + ":" + port
	want2 := "[" + ip + "]:" + port

	var walk func(any) string

	walk = func(x any) string {
		switch t := x.(type) {
		case map[string]any:
			hasAddr := false
			var ids []string

			for _, raw := range t {
				if s, ok := raw.(string); ok {
					ls := strings.ToLower(s)
					if strings.Contains(ls, strings.ToLower(want1)) || strings.Contains(ls, strings.ToLower(want2)) {
						hasAddr = true
					}
					ids = append(ids, fullIDRe.FindAllString(ls, -1)...)
				}
			}

			if hasAddr && len(ids) > 0 {
				return strings.ToLower(ids[0])
			}

			for _, raw := range t {
				if out := walk(raw); out != "" {
					return out
				}
			}

		case []any:
			for _, raw := range t {
				if out := walk(raw); out != "" {
					return out
				}
			}
		}

		return ""
	}

	return walk(v)
}

func garageCLI(ctx context.Context, cfg Config, remote string, args ...string) (string, error) {
	cliArgs := []string{"-h", remote, "-s", cfg.RPCSecret}
	cliArgs = append(cliArgs, args...)

	cmd := exec.CommandContext(ctx, cfg.GarageBin, cliArgs...)
	cmd.Env = cleanChildEnv(os.Environ(),
		"GARAGE_RPC_SECRET",
		"GARAGE_RPC_SECRET_FILE",
		"GARAGE_CONFIG_FILE",
	)

	var b bytes.Buffer
	cmd.Stdout = &b
	cmd.Stderr = &b

	err := cmd.Run()
	return b.String(), err
}

func cleanChildEnv(env []string, dropKeys ...string) []string {
	drop := make(map[string]bool, len(dropKeys))
	for _, k := range dropKeys {
		drop[k] = true
	}

	out := make([]string, 0, len(env))
	for _, kv := range env {
		key := kv
		if i := strings.IndexByte(kv, '='); i >= 0 {
			key = kv[:i]
		}
		if drop[key] {
			continue
		}
		out = append(out, kv)
	}

	return out
}

func ensureBucketAndKey(ctx context.Context, cfg Config, c APIClient) error {
	if cfg.BucketName == "" {
		return nil
	}

	if cfg.ImportKey && cfg.AccessKeyID != "" && cfg.SecretKey != "" {
		if _, err := c.getQuery(ctx, "/v2/GetKeyInfo", map[string]string{"id": cfg.AccessKeyID}); err == nil {
			log.Printf("key already exists: %s", cfg.AccessKeyID)
		} else {
			keyBody := map[string]any{
				"accessKeyId":     cfg.AccessKeyID,
				"secretAccessKey": cfg.SecretKey,
				"name":            cfg.BucketName,
			}

			if cfg.DryRun {
				log.Printf("ImportKey dry-run: %s", cfg.AccessKeyID)
			} else if err := c.post(ctx, "/v2/ImportKey", keyBody, nil); err != nil {
				return fmt.Errorf("ImportKey %s: %w", cfg.AccessKeyID, err)
			} else {
				log.Printf("key ensured: %s", cfg.AccessKeyID)
			}
		}
	}

	if cfg.DryRun {
		log.Printf("bucket/key dry-run: bucket=%s key=%s", cfg.BucketName, cfg.AccessKeyID)
		return nil
	}

	bucketID, err := ensureBucketID(ctx, cfg, c)
	if err != nil {
		return err
	}

	log.Printf("bucket ensured: %s id=%s", cfg.BucketName, bucketID)

	if cfg.AccessKeyID != "" {
		allowBody := map[string]any{
			"bucketId":    bucketID,
			"accessKeyId": cfg.AccessKeyID,
			"permissions": map[string]any{
				"read":  true,
				"write": true,
				"owner": true,
			},
		}

		if err := c.post(ctx, "/v2/AllowBucketKey", allowBody, nil); err != nil {
			return fmt.Errorf("AllowBucketKey bucket=%s id=%s key=%s: %w", cfg.BucketName, bucketID, cfg.AccessKeyID, err)
		}

		log.Printf("bucket permissions ensured: bucket=%s id=%s key=%s", cfg.BucketName, bucketID, cfg.AccessKeyID)
	}

	return nil
}

func ensureBucketID(ctx context.Context, cfg Config, c APIClient) (string, error) {
	id, err := getBucketIDByAlias(ctx, c, cfg.BucketName)
	if err == nil && id != "" {
		return id, nil
	}

	bucketBody := map[string]any{
		"globalAlias": cfg.BucketName,
	}

	var created any
	if err := c.post(ctx, "/v2/CreateBucket", bucketBody, &created); err != nil {
		return "", fmt.Errorf("CreateBucket %s: %w", cfg.BucketName, err)
	}

	if id := extractStringField(created, "id"); id != "" {
		return id, nil
	}

	id, err = getBucketIDByAlias(ctx, c, cfg.BucketName)
	if err != nil {
		return "", fmt.Errorf("CreateBucket %s succeeded but GetBucketInfo failed: %w", cfg.BucketName, err)
	}

	if id == "" {
		return "", fmt.Errorf("CreateBucket %s succeeded but no bucket id was returned", cfg.BucketName)
	}

	return id, nil
}

func getBucketIDByAlias(ctx context.Context, c APIClient, alias string) (string, error) {
	v, err := c.getQuery(ctx, "/v2/GetBucketInfo", map[string]string{
		"globalAlias": alias,
	})
	if err != nil {
		return "", err
	}

	id := extractStringField(v, "id")
	if id == "" {
		return "", fmt.Errorf("GetBucketInfo %s returned no id", alias)
	}

	return id, nil
}

func parseLayoutRoleIDs(s string) map[string]bool {
	ids := map[string]bool{}
	for _, m := range shortIDRe.FindAllString(s) {
		ids[strings.ToLower(m)] = true
	}
	return ids
}

func parseLayoutVersion(s string) int64 {
	m := versionRe.FindStringSubmatch(s)
	if len(m) != 2 {
		return 0
	}

	n, _ := strconv.ParseInt(m[1], 10, 64)
	return n
}

func shortID(full string) string {
	full = strings.ToLower(full)
	if len(full) >= 16 {
		return full[:16]
	}
	return full
}

func newClient(base string, cfg Config) APIClient {
	return APIClient{
		base:  strings.TrimRight(base, "/"),
		token: cfg.AdminToken,
		hc: &http.Client{
			Timeout: cfg.RequestTimeout,
		},
	}
}

func (c APIClient) getRaw(ctx context.Context, path string, q map[string]string) ([]byte, error) {
	u, err := url.Parse(c.base + path)
	if err != nil {
		return nil, err
	}

	if q != nil {
		qq := u.Query()
		for k, v := range q {
			qq.Set(k, v)
		}
		u.RawQuery = qq.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}

	c.auth(req)

	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	b, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("HTTP %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}

	return b, nil
}

func (c APIClient) getQuery(ctx context.Context, path string, q map[string]string) (any, error) {
	b, err := c.getRaw(ctx, path, q)
	if err != nil {
		return nil, err
	}

	if len(bytes.TrimSpace(b)) == 0 {
		return map[string]any{}, nil
	}

	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return nil, fmt.Errorf("decode json: %w: %s", err, strings.TrimSpace(string(b)))
	}

	return v, nil
}

func (c APIClient) post(ctx context.Context, path string, body any, out *any) error {
	var r io.Reader

	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		r = bytes.NewReader(b)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+path, r)
	if err != nil {
		return err
	}

	c.auth(req)

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	decoded, err := decodeResponse(resp)
	if err != nil {
		return err
	}

	if out != nil {
		*out = decoded
	}

	return nil
}

func (c APIClient) auth(req *http.Request) {
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
}

func decodeResponse(resp *http.Response) (any, error) {
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("HTTP %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}

	if len(bytes.TrimSpace(b)) == 0 {
		return map[string]any{}, nil
	}

	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return nil, fmt.Errorf("decode json: %w: %s", err, strings.TrimSpace(string(b)))
	}

	return v, nil
}

func resolveIPs(ctx context.Context, names string) ([]string, error) {
	parts := strings.Split(names, ",")

	seen := map[string]bool{}
	out := []string{}

	for _, raw := range parts {
		name := strings.TrimSpace(raw)
		if name == "" {
			continue
		}

		if ip := net.ParseIP(name); ip != nil {
			s := ip.String()
			if !seen[s] {
				seen[s] = true
				out = append(out, s)
			}
			continue
		}

		addrs, err := net.DefaultResolver.LookupIPAddr(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("resolve %s: %w", name, err)
		}

		for _, a := range addrs {
			ip := a.IP.String()
			if ip == "" || seen[ip] {
				continue
			}
			seen[ip] = true
			out = append(out, ip)
		}
	}

	sort.Strings(out)
	return out, nil
}

func endpointForIP(ip, port string) string {
	if strings.Contains(ip, ":") {
		return "http://[" + ip + "]:" + port
	}
	return "http://" + ip + ":" + port
}

func uniqueNodes(nodes []Node) []Node {
	seen := map[string]bool{}
	out := []Node{}

	for _, n := range nodes {
		if seen[n.FullID] {
			continue
		}

		seen[n.FullID] = true
		out = append(out, n)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].Short < out[j].Short
	})

	return out
}

func uniqueStrings(in []string) []string {
	seen := map[string]bool{}
	out := []string{}

	for _, s := range in {
		s = strings.ToLower(s)
		if s == "" || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}

	sort.Strings(out)
	return out
}

func nodeSetKey(nodes []Node) string {
	ids := make([]string, 0, len(nodes))
	for _, n := range nodes {
		ids = append(ids, n.FullID+"@"+n.IP)
	}
	sort.Strings(ids)
	return strings.Join(ids, ",")
}

func extractStringField(v any, field string) string {
	m, ok := v.(map[string]any)
	if !ok {
		return ""
	}

	s, _ := m[field].(string)
	return s
}

func trimOutput(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > 800 {
		return s[:800] + "..."
	}
	return s
}

func loadConfig() (Config, error) {
	rpcSecret := env("GARAGE_RPC_SECRET", "")
	if rpcSecret == "" {
		p := env("GARAGE_RPC_SECRET_FILE", "/run/secrets/garage_rpc_secret")
		if p != "" {
			b, err := os.ReadFile(p)
			if err == nil {
				rpcSecret = strings.TrimSpace(string(b))
			}
		}
	}

	serviceDNS := env("GARAGE_TASKS_DNS", "")
	if serviceDNS == "" {
		serviceDNS = env("GARAGE_SERVICE_DNS", "tasks.garage")
	}

	cfg := Config{
		ServiceDNS:          serviceDNS,
		GarageBin:           env("GARAGE_BIN", "/garage"),
		RPCPort:             env("GARAGE_RPC_PORT", "3901"),
		AdminPort:           env("GARAGE_ADMIN_PORT", "3903"),
		RPCSecret:           rpcSecret,
		AdminToken:          env("GARAGE_ADMIN_TOKEN", ""),
		ExpectedNodes:       envInt("GARAGE_EXPECTED_NODES", 2),
		LayoutCapacity:      env("GARAGE_LAYOUT_CAPACITY", "10G"),
		ZonePrefix:          env("GARAGE_ZONE_PREFIX", "dc"),
		BucketName:          env("GARAGE_BUCKET", "docker-registry"),
		AccessKeyID:         env("GARAGE_S3_ACCESS_KEY_ID", ""),
		SecretKey:           env("GARAGE_S3_SECRET_KEY", ""),
		Interval:            envDuration("GARAGE_RECONCILE_INTERVAL", 30*time.Second),
		RequestTimeout:      envDuration("GARAGE_REQUEST_TIMEOUT", 10*time.Second),
		CreateBucket:        envBool("GARAGE_CREATE_BUCKET", true),
		ImportKey:           envBool("GARAGE_IMPORT_KEY", true),
		ReplaceOfflineNodes: envBool("GARAGE_REPLACE_OFFLINE_NODES", true),
		DryRun:              envBool("GARAGE_DRY_RUN", false),
	}

	if cfg.RPCSecret == "" {
		return cfg, errors.New("GARAGE_RPC_SECRET or GARAGE_RPC_SECRET_FILE is required")
	}

	if cfg.AdminToken == "" {
		return cfg, errors.New("GARAGE_ADMIN_TOKEN is required")
	}

	if cfg.ExpectedNodes < 1 {
		return cfg, errors.New("GARAGE_EXPECTED_NODES must be >= 1")
	}

	return cfg, nil
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	if v == "" {
		return fallback
	}

	switch v {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
}

func envInt(key string, fallback int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}

	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}

	return n
}

func envDuration(key string, fallback time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}

	d, err := time.ParseDuration(v)
	if err == nil {
		return d
	}

	if n, err := strconv.Atoi(v); err == nil {
		return time.Duration(n) * time.Second
	}

	return fallback
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}
