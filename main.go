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
	"sort"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	ServiceDNS          string
	AdminPort           string
	RPCPort             string
	AdminToken          string
	ExpectedNodes       int
	CapacityBytes       int64
	ZonePrefix          string
	TagPrefix           string
	BucketName          string
	AccessKeyID         string
	SecretKey           string
	Interval            time.Duration
	RequestTimeout      time.Duration
	OfflineGrace        time.Duration
	CreateBucket        bool
	ImportKey           bool
	ReplaceOfflineNodes bool
	RepairOnChange      bool
	DryRun              bool
}

type Node struct {
	ID       string
	IP       string
	Endpoint string
	Peer     string
}

type APIClient struct {
	base  string
	token string
	hc    *http.Client
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	log.Printf("garage-reconciler starting: dns=%s expected_nodes=%d interval=%s dry_run=%v", cfg.ServiceDNS, cfg.ExpectedNodes, cfg.Interval, cfg.DryRun)

	for {
		ctx, cancel := context.WithTimeout(context.Background(), maxDuration(2*cfg.RequestTimeout, 30*time.Second))
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
		endpoint := endpointForIP(ip, cfg.AdminPort)
		client := newClient(endpoint, cfg)
		if err := client.health(ctx); err != nil {
			log.Printf("garage admin not ready at %s: %v", endpoint, err)
			continue
		}
		info, err := client.get(ctx, "/v2/GetNodeInfo/self")
		if err != nil {
			log.Printf("cannot get self node info from %s: %v", endpoint, err)
			continue
		}
		id := firstNodeID(info)
		if id == "" {
			log.Printf("cannot extract node id from %s response", endpoint)
			continue
		}
		n := Node{ID: id, IP: ip, Endpoint: endpoint, Peer: fmt.Sprintf("%s@%s:%s", id, ip, cfg.RPCPort)}
		nodes = append(nodes, n)
	}

	nodes = uniqueNodes(nodes)
	if len(nodes) == 0 {
		return errors.New("no live garage nodes with readable admin API")
	}
	if len(nodes) < cfg.ExpectedNodes {
		log.Printf("only %d/%d garage nodes visible; connect will run, layout apply will be skipped", len(nodes), cfg.ExpectedNodes)
	}

	// Make all visible nodes connect to all other visible nodes. Garage nodes do not discover Swarm peers by themselves.
	peers := make([]string, 0, len(nodes))
	for _, n := range nodes {
		peers = append(peers, n.Peer)
	}
	for _, n := range nodes {
		others := excludePeer(peers, n.Peer)
		if len(others) == 0 {
			continue
		}
		log.Printf("connect peers on %s: %s", n.Endpoint, strings.Join(others, ","))
		if !cfg.DryRun {
			if err := newClient(n.Endpoint, cfg).post(ctx, "/v2/ConnectClusterNodes", others, nil); err != nil {
				log.Printf("ConnectClusterNodes on %s failed: %v", n.Endpoint, err)
			}
		}
	}

	primary := newClient(nodes[0].Endpoint, cfg)
	status, err := primary.get(ctx, "/v2/GetClusterStatus")
	if err != nil {
		return fmt.Errorf("GetClusterStatus: %w", err)
	}
	layout, err := primary.get(ctx, "/v2/GetClusterLayout")
	if err != nil {
		return fmt.Errorf("GetClusterLayout: %w", err)
	}

	currentRoleIDs := extractRoleIDs(layout)
	knownIDs := extractStatusNodeIDs(status)
	if len(knownIDs) == 0 {
		knownIDs = map[string]bool{}
		for _, id := range idsFromNodes(nodes) {
			knownIDs[id] = true
		}
	}
	log.Printf("status: visible=%d known=%d role_ids=%d", len(nodes), len(knownIDs), len(currentRoleIDs))

	if len(nodes) >= cfg.ExpectedNodes {
		changed, err := reconcileLayout(ctx, cfg, primary, nodes, currentRoleIDs, layout)
		if err != nil {
			return err
		}
		if changed && cfg.RepairOnChange && !cfg.DryRun {
			launchRepairs(ctx, cfg, primary, nodes)
		}
	}

	if cfg.CreateBucket {
		if err := ensureBucketAndKey(ctx, cfg, primary); err != nil {
			log.Printf("bucket/key reconcile warning: %v", err)
		}
	}

	health, err := primary.get(ctx, "/v2/GetClusterHealth")
	if err == nil {
		log.Printf("health: %s", compactJSON(health))
	}

	return nil
}

func reconcileLayout(ctx context.Context, cfg Config, c APIClient, nodes []Node, currentRoleIDs map[string]bool, layout any) (bool, error) {
	active := idsFromNodes(nodes)
	activeSet := map[string]bool{}
	for _, id := range active {
		activeSet[id] = true
	}

	roles := make([]map[string]any, 0)
	changed := false
	for i, n := range nodes {
		if currentRoleIDs[n.ID] {
			continue
		}
		changed = true
		roles = append(roles, map[string]any{
			"id":       n.ID,
			"zone":     fmt.Sprintf("%s%d", cfg.ZonePrefix, i+1),
			"capacity": cfg.CapacityBytes,
			"tags":     []string{fmt.Sprintf("%s%d", cfg.TagPrefix, i+1)},
		})
	}

	// Optional unsafe removal. This is disabled by default. Enable only if Garage service replicas are stable and the admin token is trusted.
	if cfg.ReplaceOfflineNodes {
		for id := range currentRoleIDs {
			if !activeSet[id] {
				changed = true
				// The Admin API v2 update schema accepts role changes. Official docs show assign examples;
				// removal shape is not always documented in summaries, so we send the common explicit marker.
				roles = append(roles, map[string]any{"id": id, "remove": true})
			}
		}
	}

	if !changed {
		log.Printf("layout already contains all visible nodes; no layout update needed")
		return false, nil
	}
	if len(roles) == 0 {
		return false, nil
	}

	body := map[string]any{"roles": roles}
	log.Printf("UpdateClusterLayout: %s", compactJSON(body))
	if cfg.DryRun {
		return true, nil
	}
	if err := c.post(ctx, "/v2/UpdateClusterLayout", body, nil); err != nil {
		return false, fmt.Errorf("UpdateClusterLayout: %w", err)
	}

	newLayout, err := c.get(ctx, "/v2/GetClusterLayout")
	if err != nil {
		return false, fmt.Errorf("GetClusterLayout after update: %w", err)
	}
	version := nextLayoutVersion(layout, newLayout)
	if version <= 0 {
		return false, fmt.Errorf("could not infer next layout version from GetClusterLayout; refusing to apply")
	}
	applyBody := map[string]any{"version": version}
	log.Printf("ApplyClusterLayout: %s", compactJSON(applyBody))
	if err := c.post(ctx, "/v2/ApplyClusterLayout", applyBody, nil); err != nil {
		return false, fmt.Errorf("ApplyClusterLayout: %w", err)
	}
	return true, nil
}

func launchRepairs(ctx context.Context, cfg Config, c APIClient, nodes []Node) {
	for _, n := range nodes {
		for _, repairType := range []string{"tables", "blocks"} {
			body := map[string]any{"repairType": repairType}
			path := "/v2/LaunchRepairOperation/" + url.PathEscape(n.ID)
			if err := c.post(ctx, path, body, nil); err != nil {
				log.Printf("repair %s on %s warning: %v", repairType, n.ID, err)
			} else {
				log.Printf("repair %s launched on %s", repairType, n.ID)
			}
		}
	}
}

func ensureBucketAndKey(ctx context.Context, cfg Config, c APIClient) error {
	if cfg.BucketName == "" {
		return nil
	}
	if cfg.ImportKey && cfg.AccessKeyID != "" && cfg.SecretKey != "" {
		keyBody := map[string]any{
			"accessKeyId": cfg.AccessKeyID,
			"secretKey":   cfg.SecretKey,
			"name":        "docker-registry",
		}
		if cfg.DryRun {
			log.Printf("ImportKey dry-run: %s", cfg.AccessKeyID)
		} else if err := c.post(ctx, "/v2/ImportKey", keyBody, nil); err != nil {
			// Existing keys may return a conflict. This is intentionally non-fatal.
			log.Printf("ImportKey warning for %s: %v", cfg.AccessKeyID, err)
		}
	}

	bucketBody := map[string]any{"globalAlias": cfg.BucketName}
	if cfg.AccessKeyID != "" {
		bucketBody["localAlias"] = map[string]any{
			"accessKeyId": cfg.AccessKeyID,
			"alias":       cfg.BucketName,
			"allow": map[string]any{
				"read": true, "write": true, "owner": true,
			},
		}
	}
	if cfg.DryRun {
		log.Printf("CreateBucket dry-run: %s", cfg.BucketName)
		return nil
	}
	if err := c.post(ctx, "/v2/CreateBucket", bucketBody, nil); err != nil {
		log.Printf("CreateBucket warning for %s: %v", cfg.BucketName, err)
	}

	if cfg.AccessKeyID != "" {
		info, err := c.getQuery(ctx, "/v2/GetBucketInfo", map[string]string{"globalAlias": cfg.BucketName})
		if err == nil {
			bucketID := firstStringByKeys(info, "id", "bucketId")
			if bucketID != "" {
				allowBody := map[string]any{
					"bucketId":    bucketID,
					"accessKeyId": cfg.AccessKeyID,
					"permissions": map[string]any{"read": true, "write": true, "owner": true},
				}
				if err := c.post(ctx, "/v2/AllowBucketKey", allowBody, nil); err != nil {
					log.Printf("AllowBucketKey warning: %v", err)
				}
			}
		}
	}
	return nil
}

func newClient(base string, cfg Config) APIClient {
	return APIClient{base: strings.TrimRight(base, "/"), token: cfg.AdminToken, hc: &http.Client{Timeout: cfg.RequestTimeout}}
}

func (c APIClient) health(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/health", nil)
	if err != nil {
		return err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP %s", resp.Status)
	}
	return nil
}

func (c APIClient) get(ctx context.Context, path string) (any, error) {
	return c.getQuery(ctx, path, nil)
}

func (c APIClient) getQuery(ctx context.Context, path string, q map[string]string) (any, error) {
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
	return decodeResponse(resp)
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

func resolveIPs(ctx context.Context, name string) ([]string, error) {
	addrs, err := net.DefaultResolver.LookupIPAddr(ctx, name)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	out := make([]string, 0, len(addrs))
	for _, a := range addrs {
		ip := a.IP.String()
		if ip == "" || seen[ip] {
			continue
		}
		seen[ip] = true
		out = append(out, ip)
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

func firstNodeID(v any) string {
	return firstStringByKeys(v, "nodeId", "nodeID", "id")
}

func firstStringByKeys(v any, keys ...string) string {
	wanted := map[string]bool{}
	for _, k := range keys {
		wanted[strings.ToLower(k)] = true
	}
	var walk func(any) string
	walk = func(x any) string {
		switch t := x.(type) {
		case map[string]any:
			for k, val := range t {
				if wanted[strings.ToLower(k)] {
					if s, ok := val.(string); ok && looksLikeIDOrValue(s) {
						return s
					}
				}
			}
			for _, val := range t {
				if s := walk(val); s != "" {
					return s
				}
			}
		case []any:
			for _, val := range t {
				if s := walk(val); s != "" {
					return s
				}
			}
		}
		return ""
	}
	return walk(v)
}

func looksLikeIDOrValue(s string) bool {
	s = strings.TrimSpace(s)
	return len(s) >= 4 && !strings.ContainsAny(s, " \n\t")
}

func extractStatusNodeIDs(v any) map[string]bool {
	ids := map[string]bool{}
	var walk func(any)
	walk = func(x any) {
		switch t := x.(type) {
		case map[string]any:
			id := ""
			for _, key := range []string{"id", "nodeId", "nodeID"} {
				if s, ok := t[key].(string); ok && len(s) >= 8 {
					id = s
					break
				}
			}
			if id != "" {
				ids[id] = true
			}
			for _, val := range t {
				walk(val)
			}
		case []any:
			for _, val := range t {
				walk(val)
			}
		}
	}
	walk(v)
	return ids
}

func extractRoleIDs(v any) map[string]bool {
	ids := map[string]bool{}
	var walk func(any, string)
	walk = func(x any, parent string) {
		switch t := x.(type) {
		case map[string]any:
			id := ""
			for _, key := range []string{"id", "nodeId", "nodeID"} {
				if s, ok := t[key].(string); ok && len(s) >= 8 {
					id = s
					break
				}
			}
			_, hasZone := t["zone"]
			_, hasCapacity := t["capacity"]
			_, hasTags := t["tags"]
			if id != "" && (hasZone || hasCapacity || hasTags || strings.Contains(strings.ToLower(parent), "role")) {
				ids[id] = true
			}
			for k, val := range t {
				walk(val, k)
			}
		case []any:
			for _, val := range t {
				walk(val, parent)
			}
		}
	}
	walk(v, "")
	return ids
}

func nextLayoutVersion(oldLayout, newLayout any) int64 {
	// Prefer a changed layout's highest version + 1 if possible; otherwise old highest + 1.
	newMax := maxVersion(newLayout)
	oldMax := maxVersion(oldLayout)
	if newMax > 0 {
		// Garage ApplyClusterLayout wants the new version. In normal flow this is current+1.
		// If GetClusterLayout exposes staged/next version, maxVersion(newLayout) is usually that value.
		if newMax > oldMax {
			return newMax
		}
		return newMax + 1
	}
	if oldMax > 0 {
		return oldMax + 1
	}
	return 0
}

func maxVersion(v any) int64 {
	var max int64
	var walk func(any, string)
	walk = func(x any, key string) {
		switch t := x.(type) {
		case map[string]any:
			for k, val := range t {
				walk(val, k)
			}
		case []any:
			for _, val := range t {
				walk(val, key)
			}
		case float64:
			if strings.Contains(strings.ToLower(key), "version") && int64(t) > max {
				max = int64(t)
			}
		case int64:
			if strings.Contains(strings.ToLower(key), "version") && t > max {
				max = t
			}
		}
	}
	walk(v, "")
	return max
}

func idsFromNodes(nodes []Node) []string {
	out := make([]string, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, n.ID)
	}
	sort.Strings(out)
	return out
}

func uniqueNodes(nodes []Node) []Node {
	seen := map[string]bool{}
	out := []Node{}
	for _, n := range nodes {
		if seen[n.ID] {
			continue
		}
		seen[n.ID] = true
		out = append(out, n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func excludePeer(peers []string, self string) []string {
	out := []string{}
	for _, p := range peers {
		if p != self {
			out = append(out, p)
		}
	}
	return out
}

func compactJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	if len(b) > 3000 {
		return string(b[:3000]) + "..."
	}
	return string(b)
}

func loadConfig() (Config, error) {
	cfg := Config{
		ServiceDNS:          env("GARAGE_SERVICE_DNS", "tasks.garage"),
		AdminPort:           env("GARAGE_ADMIN_PORT", "3903"),
		RPCPort:             env("GARAGE_RPC_PORT", "3901"),
		AdminToken:          env("GARAGE_ADMIN_TOKEN", ""),
		ExpectedNodes:       envInt("GARAGE_EXPECTED_NODES", 2),
		CapacityBytes:       envInt64("GARAGE_LAYOUT_CAPACITY_BYTES", 10_000_000_000),
		ZonePrefix:          env("GARAGE_ZONE_PREFIX", "swarm-zone-"),
		TagPrefix:           env("GARAGE_TAG_PREFIX", "garage-"),
		BucketName:          env("GARAGE_BUCKET", "docker-registry"),
		AccessKeyID:         env("GARAGE_S3_ACCESS_KEY_ID", ""),
		SecretKey:           env("GARAGE_S3_SECRET_KEY", ""),
		Interval:            envDuration("GARAGE_RECONCILE_INTERVAL", 30*time.Second),
		RequestTimeout:      envDuration("GARAGE_REQUEST_TIMEOUT", 5*time.Second),
		OfflineGrace:        envDuration("GARAGE_OFFLINE_GRACE", 5*time.Minute),
		CreateBucket:        envBool("GARAGE_CREATE_BUCKET", true),
		ImportKey:           envBool("GARAGE_IMPORT_KEY", true),
		ReplaceOfflineNodes: envBool("GARAGE_REPLACE_OFFLINE_NODES", true),
		RepairOnChange:      envBool("GARAGE_REPAIR_ON_CHANGE", true),
		DryRun:              envBool("GARAGE_DRY_RUN", false),
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
	return v == "1" || v == "true" || v == "yes" || v == "on"
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

func envInt64(key string, fallback int64) int64 {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	n, err := strconv.ParseInt(v, 10, 64)
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
