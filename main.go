package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
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
	"time"
)

type RuntimeConfig struct {
	Garages []GarageConfig
	DryRun  bool
}

type GarageConfig struct {
	Name                string
	GarageBin           string
	AdminPort           int
	RPCPort             int
	Interval            time.Duration
	RequestTimeout      time.Duration
	ExpectedNodes       int
	ReplicationFactor   int
	RPCSecret           SecretValue
	AdminToken          SecretValue
	ReplaceOfflineNodes bool
	Targets             []ConfiguredTarget
	AccessKeys          []AccessKeyConfig
	Buckets             []BucketConfig
	DryRun              bool
}

type ConfiguredTarget struct {
	Name           string
	Discovery      string
	Endpoint       string
	Endpoints      []string
	ExpectedCount  int
	Zone           string
	Capacity       string
	GarageBin      string
	AdminPort      int
	RPCPort        int
	RequestTimeout time.Duration
	AdminToken     SecretValue
	RPCSecret      SecretValue
}

type AccessKeyConfig struct {
	Key         string
	AccessKeyID SecretValue
	SecretKey   SecretValue
}

type BucketConfig struct {
	Name        string
	Key         string
	MaxSize     int64
	MaxObjects  int64
	AccessKeyID SecretValue
	SecretKey   SecretValue
}

type SecretValue struct {
	Kind   string
	Source string
	Value  string
}

type RawRoot struct {
	Garages []RawGarage `toml:"garages"`
}

type RawGarage struct {
	Name                *string `toml:"name"`
	GarageBin           *string `toml:"garage_bin"`
	AdminPort           *int    `toml:"admin_port"`
	RPCPort             *int    `toml:"rpc_port"`
	Interval            *string `toml:"interval"`
	Timeout             *string `toml:"timeout"`
	ReplicationFactor   *int    `toml:"replication_factor"`
	RPCSecret           *string `toml:"rpc_secret"`
	RPCSecretEnv        *string `toml:"rpc_secret_env"`
	RPCSecretFile       *string `toml:"rpc_secret_file"`
	AdminToken          *string `toml:"admin_token"`
	AdminTokenEnv       *string `toml:"admin_token_env"`
	AdminTokenFile      *string `toml:"admin_token_file"`
	ReplaceOfflineNodes *bool   `toml:"replace_offline_nodes"`
	Targets             []RawTarget
	AccessKeys          []RawAccessKey
	Buckets             []RawBucket
}

type RawTarget struct {
	Name           *string  `toml:"name"`
	Discovery      *string  `toml:"discovery"`
	Endpoint       *string  `toml:"endpoint"`
	Endpoints      []string `toml:"endpoints"`
	ExpectedCount  *int     `toml:"expected_count"`
	Zone           *string  `toml:"zone"`
	Capacity       *string  `toml:"capacity"`
	GarageBin      *string  `toml:"garage_bin"`
	AdminPort      *int     `toml:"admin_port"`
	RPCPort        *int     `toml:"rpc_port"`
	Timeout        *string  `toml:"timeout"`
	RPCSecret      *string  `toml:"rpc_secret"`
	RPCSecretEnv   *string  `toml:"rpc_secret_env"`
	RPCSecretFile  *string  `toml:"rpc_secret_file"`
	AdminToken     *string  `toml:"admin_token"`
	AdminTokenEnv  *string  `toml:"admin_token_env"`
	AdminTokenFile *string  `toml:"admin_token_file"`
}

type RawAccessKey struct {
	Key             *string `toml:"key"`
	AccessKeyID     *string `toml:"access_key_id"`
	AccessKeyIDEnv  *string `toml:"access_key_id_env"`
	AccessKeyIDFile *string `toml:"access_key_id_file"`
	SecretKey       *string `toml:"secret_key"`
	SecretKeyEnv    *string `toml:"secret_key_env"`
	SecretKeyFile   *string `toml:"secret_key_file"`
}

type RawBucket struct {
	Name            *string `toml:"name"`
	Key             *string `toml:"key"`
	MaxSize         *int64  `toml:"max_size"`
	MaxObjects      *int64  `toml:"max_objects"`
	AccessKeyID     *string `toml:"access_key_id"`
	AccessKeyIDEnv  *string `toml:"access_key_id_env"`
	AccessKeyIDFile *string `toml:"access_key_id_file"`
	SecretKey       *string `toml:"secret_key"`
	SecretKeyEnv    *string `toml:"secret_key_env"`
	SecretKeyFile   *string `toml:"secret_key_file"`
}

type Node struct {
	FullID string
	Short  string
	IP     string
	Peer   string
	Cfg    ConfiguredTarget
}

type APIClient struct {
	base  string
	token string
	hc    *http.Client
}

type ReconcileState struct {
	ClusterBootstrapped bool
	KeyBucketEnsured    bool
	LastNodeSet         string
}

type ValidationError []string

func (e ValidationError) Error() string {
	if len(e) == 0 {
		return "configuration validation failed"
	}
	var b strings.Builder
	b.WriteString("configuration validation failed:")
	for _, msg := range e {
		b.WriteString("\n  - ")
		b.WriteString(msg)
	}
	return b.String()
}

var (
	fullIDRe  = regexp.MustCompile(`(?i)\b[0-9a-f]{64}\b`)
	shortIDRe = regexp.MustCompile(`(?i)\b[0-9a-f]{16}\b`)
	versionRe = regexp.MustCompile(`(?m)Current cluster layout version:\s*(\d+)`)
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	configPath := flag.String("c", "", "path to reconciler TOML config file")
	dryRunOverride := flag.String("dry-run", "", "override dry-run mode: true or false")
	flag.Parse()

	cfg, err := loadRuntimeConfig(*configPath, *dryRunOverride)
	if cfg != nil {
		printRuntimeConfig(*cfg)
	}
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	log.Printf("garage-reconciler starting: garages=%d dry_run=%v", len(cfg.Garages), cfg.DryRun)

	for _, garage := range cfg.Garages {
		g := garage
		go runGarage(g)
	}

	select {}
}

func runGarage(cfg GarageConfig) {
	state := ReconcileState{}

	for {
		ctx, cancel := context.WithTimeout(context.Background(), maxDuration(6*cfg.RequestTimeout, 60*time.Second))
		err := reconcileGarage(ctx, cfg, &state)
		cancel()

		if err != nil {
			log.Printf("garage=%s reconcile failed: %v", cfg.Name, err)
		}

		time.Sleep(cfg.Interval)
	}
}

func reconcileGarage(ctx context.Context, cfg GarageConfig, state *ReconcileState) error {
	nodes, err := discoverLiveNodes(ctx, cfg)
	if err != nil {
		return err
	}
	if len(nodes) == 0 {
		return errors.New("no live garage nodes reachable through Admin API")
	}

	nodeSet := nodeSetKey(nodes)
	if state.LastNodeSet != nodeSet {
		state.ClusterBootstrapped = false
		state.KeyBucketEnsured = false
		state.LastNodeSet = nodeSet
	}

	if len(nodes) != cfg.ExpectedNodes {
		log.Printf("garage=%s visible garage node count does not match target topology: visible=%d target_nodes=%d; layout apply skipped", cfg.Name, len(nodes), cfg.ExpectedNodes)
		if !state.KeyBucketEnsured && len(cfg.Buckets) > 0 {
			client := newClient(endpointForIP(nodes[0].IP, nodes[0].Cfg.AdminPort), nodes[0].Cfg.AdminToken.Value, nodes[0].Cfg.RequestTimeout)
			if err := ensureBucketsAndKeys(ctx, cfg, client); err != nil {
				log.Printf("garage=%s bucket/key reconcile warning: %v", cfg.Name, err)
			} else {
				state.KeyBucketEnsured = true
			}
		}
		return nil
	}

	if !state.ClusterBootstrapped {
		for _, n := range nodes {
			for _, peer := range nodes {
				if peer.FullID == n.FullID {
					continue
				}

				if cfg.DryRun {
					log.Printf("garage=%s dry-run node connect on %s -> %s", cfg.Name, n.Short, peer.Short)
					continue
				}

				out, err := garageCLI(ctx, n.Cfg, n.Peer, "node", "connect", peer.Peer)
				if err != nil {
					log.Printf("garage=%s node connect on %s -> %s warning: %v output=%s", cfg.Name, n.Short, peer.Short, err, trimOutput(out))
				} else {
					log.Printf("garage=%s node connect on %s -> %s OK", cfg.Name, n.Short, peer.Short)
				}
			}
		}

		changed, err := reconcileLayout(ctx, cfg, nodes[0], nodes)
		if err != nil {
			return err
		}

		if changed {
			log.Printf("garage=%s layout changed/applied; data repair will be handled by Garage background workers", cfg.Name)
		}

		state.ClusterBootstrapped = true
	} else {
		log.Printf("garage=%s cluster layout already bootstrapped for %d nodes; no RPC action needed", cfg.Name, len(nodes))
	}

	if len(cfg.Buckets) > 0 && !state.KeyBucketEnsured {
		client := newClient(endpointForIP(nodes[0].IP, nodes[0].Cfg.AdminPort), nodes[0].Cfg.AdminToken.Value, nodes[0].Cfg.RequestTimeout)
		if err := ensureBucketsAndKeys(ctx, cfg, client); err != nil {
			log.Printf("garage=%s bucket/key reconcile warning: %v", cfg.Name, err)
		} else {
			state.KeyBucketEnsured = true
		}
	}

	return nil
}

func discoverLiveNodes(ctx context.Context, cfg GarageConfig) ([]Node, error) {
	var nodes []Node
	for _, nc := range cfg.Targets {
		ips, err := resolveTargetIPs(ctx, cfg.Name, nc)
		if err != nil {
			return nil, err
		}
		for _, ip := range ips {
			if n, ok := discoverOneNode(ctx, cfg, nc, ip); ok {
				nodes = append(nodes, n)
			}
		}
	}

	return uniqueNodes(nodes), nil
}

func discoverOneNode(ctx context.Context, garage GarageConfig, nc ConfiguredTarget, ip string) (Node, bool) {
	fullID, err := garageNodeIDV23(ctx, nc, ip)
	if err != nil {
		log.Printf("garage=%s node id not ready at %s:%d: %v", garage.Name, ip, nc.AdminPort, err)
		return Node{}, false
	}

	n := Node{
		FullID: strings.ToLower(fullID),
		Short:  shortID(fullID),
		IP:     ip,
		Peer:   fmt.Sprintf("%s@%s:%d", strings.ToLower(fullID), ip, nc.RPCPort),
		Cfg:    nc,
	}
	return n, true
}

func reconcileLayout(ctx context.Context, cfg GarageConfig, primary Node, nodes []Node) (bool, error) {
	layoutBefore, err := garageCLI(ctx, primary.Cfg, primary.Peer, "layout", "show")
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

	for _, n := range nodes {
		if roleIDs[n.Short] {
			continue
		}

		changed = true
		log.Printf("garage=%s layout assign %s zone=%s capacity=%s", cfg.Name, n.Short, n.Cfg.Zone, n.Cfg.Capacity)

		if !cfg.DryRun {
			out, err := garageCLI(ctx, primary.Cfg, primary.Peer, "layout", "assign", n.Short, "-z", n.Cfg.Zone, "-c", n.Cfg.Capacity)
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
			log.Printf("garage=%s layout remove offline node %s", cfg.Name, id)

			if !cfg.DryRun {
				out, err := garageCLI(ctx, primary.Cfg, primary.Peer, "layout", "remove", id)
				if err != nil {
					log.Printf("garage=%s layout remove %s warning: %v output=%s", cfg.Name, id, err, trimOutput(out))
				}
			}
		}
	}

	if !changed {
		log.Printf("garage=%s layout already contains all visible nodes; no layout update needed", cfg.Name)
		return false, nil
	}

	if cfg.DryRun {
		return true, nil
	}

	applyVersion := currentVersion + 1
	if applyVersion <= 0 {
		applyVersion = 1
	}

	log.Printf("garage=%s layout apply --version %d", cfg.Name, applyVersion)
	out, err := garageCLI(ctx, primary.Cfg, primary.Peer, "layout", "apply", "--version", strconv.FormatInt(applyVersion, 10))
	if err != nil {
		return false, fmt.Errorf("layout apply: %w output=%s", err, trimOutput(out))
	}

	return true, nil
}

func garageNodeIDV23(ctx context.Context, nc ConfiguredTarget, ip string) (string, error) {
	client := newClient(endpointForIP(ip, nc.AdminPort), nc.AdminToken.Value, nc.RequestTimeout)

	body, err := client.getRaw(ctx, "/v2/GetClusterStatus", nil)
	if err != nil {
		return "", err
	}

	var decoded any
	if err := json.Unmarshal(body, &decoded); err == nil {
		if id := findNodeIDForAddr(decoded, ip, strconv.Itoa(nc.RPCPort)); id != "" {
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

	return "", fmt.Errorf("GetClusterStatus returned multiple node ids and none matched %s:%d", ip, nc.RPCPort)
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

func garageCLI(ctx context.Context, nc ConfiguredTarget, remote string, args ...string) (string, error) {
	cliArgs := []string{"-h", remote, "-s", nc.RPCSecret.Value}
	cliArgs = append(cliArgs, args...)

	cmd := exec.CommandContext(ctx, nc.GarageBin, cliArgs...)
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

func ensureBucketsAndKeys(ctx context.Context, cfg GarageConfig, c APIClient) error {
	ensuredKeys := map[string]bool{}
	for _, accessKey := range cfg.AccessKeys {
		if err := ensureAccessKey(ctx, cfg, accessKey.Key, accessKey.AccessKeyID, accessKey.SecretKey, c); err != nil {
			return err
		}
		if accessKey.AccessKeyID.Value != "" {
			ensuredKeys[accessKey.AccessKeyID.Value] = true
		}
	}

	for _, bucket := range cfg.Buckets {
		if err := ensureBucketAndKey(ctx, cfg, bucket, ensuredKeys, c); err != nil {
			return err
		}
	}
	warnUndeclaredBuckets(ctx, cfg, c)
	return nil
}

func ensureAccessKey(ctx context.Context, cfg GarageConfig, name string, accessKeyID SecretValue, secretKey SecretValue, c APIClient) error {
	if accessKeyID.Value == "" || secretKey.Value == "" {
		return nil
	}

	if _, err := c.getQuery(ctx, "/v2/GetKeyInfo", map[string]string{"id": accessKeyID.Value}); err == nil {
		log.Printf("garage=%s key already exists: %s", cfg.Name, accessKeyID.Value)
		return nil
	}

	keyBody := map[string]any{
		"accessKeyId":     accessKeyID.Value,
		"secretAccessKey": secretKey.Value,
		"name":            name,
	}

	if cfg.DryRun {
		log.Printf("garage=%s ImportKey dry-run: %s name=%s", cfg.Name, accessKeyID.Value, name)
		return nil
	}

	if err := c.post(ctx, "/v2/ImportKey", keyBody, nil); err != nil {
		return fmt.Errorf("ImportKey %s: %w", accessKeyID.Value, err)
	}

	log.Printf("garage=%s key ensured: %s name=%s", cfg.Name, accessKeyID.Value, name)
	return nil
}

func ensureBucketAndKey(ctx context.Context, cfg GarageConfig, bucket BucketConfig, ensuredKeys map[string]bool, c APIClient) error {
	if bucket.AccessKeyID.Value != "" && bucket.SecretKey.Value != "" && !ensuredKeys[bucket.AccessKeyID.Value] {
		if err := ensureAccessKey(ctx, cfg, bucket.Key, bucket.AccessKeyID, bucket.SecretKey, c); err != nil {
			return err
		}
		ensuredKeys[bucket.AccessKeyID.Value] = true
	}

	if cfg.DryRun {
		log.Printf("garage=%s bucket/key dry-run: bucket=%s key=%s", cfg.Name, bucket.Name, bucket.AccessKeyID.Value)
		return nil
	}

	bucketID, err := ensureBucketID(ctx, cfg, bucket, c)
	if err != nil {
		return err
	}

	log.Printf("garage=%s bucket ensured: %s id=%s", cfg.Name, bucket.Name, bucketID)

	if err := ensureBucketQuota(ctx, cfg, bucket, bucketID, c); err != nil {
		return err
	}

	if bucket.AccessKeyID.Value != "" {
		allowBody := map[string]any{
			"bucketId":    bucketID,
			"accessKeyId": bucket.AccessKeyID.Value,
			"permissions": map[string]any{
				"read":  true,
				"write": true,
				"owner": true,
			},
		}

		if err := c.post(ctx, "/v2/AllowBucketKey", allowBody, nil); err != nil {
			return fmt.Errorf("AllowBucketKey bucket=%s id=%s key=%s: %w", bucket.Name, bucketID, bucket.AccessKeyID.Value, err)
		}

		log.Printf("garage=%s bucket permissions ensured: bucket=%s id=%s key=%s", cfg.Name, bucket.Name, bucketID, bucket.AccessKeyID.Value)
	}

	warnExtraBucketKeys(ctx, cfg, bucket, bucketID, c)
	return nil
}

func warnUndeclaredBuckets(ctx context.Context, cfg GarageConfig, c APIClient) {
	declared := map[string]bool{}
	for _, bucket := range cfg.Buckets {
		declared[bucket.Name] = true
	}

	names, err := listGarageBucketNames(ctx, c)
	if err != nil {
		log.Printf("WARNING garage=%s unable to list buckets for undeclared-bucket check: %v", cfg.Name, err)
		return
	}

	for _, name := range names {
		if name == "" || declared[name] {
			continue
		}
		log.Printf("WARNING garage=%s bucket %q exists in Garage but is not declared in TOML", cfg.Name, name)
	}
}

func warnExtraBucketKeys(ctx context.Context, cfg GarageConfig, bucket BucketConfig, bucketID string, c APIClient) {
	info, err := c.getQuery(ctx, "/v2/GetBucketInfo", map[string]string{"id": bucketID})
	if err != nil {
		log.Printf("WARNING garage=%s bucket=%s unable to inspect authorized keys: %v", cfg.Name, bucket.Name, err)
		return
	}

	ids := extractBucketAccessKeyIDs(info)
	for id := range ids {
		if id == "" || id == bucket.AccessKeyID.Value {
			continue
		}
		log.Printf("WARNING garage=%s bucket=%s has an extra authorized key not declared in TOML: %s", cfg.Name, bucket.Name, id)
	}
}

func listGarageBucketNames(ctx context.Context, c APIClient) ([]string, error) {
	v, err := c.getQuery(ctx, "/v2/ListBuckets", nil)
	if err != nil {
		return nil, err
	}

	seen := map[string]bool{}
	extractBucketNames(v, seen)
	out := make([]string, 0, len(seen))
	for name := range seen {
		out = append(out, name)
	}
	sort.Strings(out)
	return out, nil
}

func extractBucketNames(v any, out map[string]bool) {
	switch x := v.(type) {
	case []any:
		for _, it := range x {
			extractBucketNames(it, out)
		}
	case map[string]any:
		if s, ok := x["globalAlias"].(string); ok && s != "" {
			out[s] = true
		}
		if aliases, ok := x["globalAliases"]; ok {
			for _, alias := range extractStringList(aliases) {
				out[alias] = true
			}
		}
		if len(out) == 0 {
			if s, ok := x["name"].(string); ok && s != "" {
				out[s] = true
			}
		}
		for _, child := range x {
			extractBucketNames(child, out)
		}
	}
}

func extractStringList(v any) []string {
	out := []string{}
	switch x := v.(type) {
	case []any:
		for _, it := range x {
			if s, ok := it.(string); ok && s != "" {
				out = append(out, s)
			}
		}
	case []string:
		for _, s := range x {
			if s != "" {
				out = append(out, s)
			}
		}
	case string:
		if x != "" {
			out = append(out, x)
		}
	}
	return out
}

func extractBucketAccessKeyIDs(v any) map[string]bool {
	out := map[string]bool{}
	extractBucketAccessKeyIDsRec(v, false, out)
	return out
}

func extractBucketAccessKeyIDsRec(v any, inKeyList bool, out map[string]bool) {
	switch x := v.(type) {
	case []any:
		for _, it := range x {
			extractBucketAccessKeyIDsRec(it, inKeyList, out)
		}
	case map[string]any:
		if inKeyList {
			for _, field := range []string{"accessKeyId", "access_key_id", "keyId", "key_id"} {
				if s, ok := x[field].(string); ok && s != "" {
					out[s] = true
				}
			}
			if s, ok := x["id"].(string); ok && s != "" {
				out[s] = true
			}
		}
		for k, child := range x {
			lk := strings.ToLower(k)
			childInKeyList := inKeyList || lk == "keys" || lk == "authorizedkeys" || lk == "authorized_keys" || lk == "bucketkeys" || lk == "bucket_keys"
			extractBucketAccessKeyIDsRec(child, childInKeyList, out)
		}
	}
}

func ensureBucketID(ctx context.Context, cfg GarageConfig, bucket BucketConfig, c APIClient) (string, error) {
	id, err := getBucketIDByAlias(ctx, c, bucket.Name)
	if err == nil && id != "" {
		return id, nil
	}

	bucketBody := map[string]any{
		"globalAlias": bucket.Name,
	}

	var created any
	if err := c.post(ctx, "/v2/CreateBucket", bucketBody, &created); err != nil {
		return "", fmt.Errorf("CreateBucket %s: %w", bucket.Name, err)
	}

	if id := extractStringField(created, "id"); id != "" {
		return id, nil
	}

	id, err = getBucketIDByAlias(ctx, c, bucket.Name)
	if err != nil {
		return "", fmt.Errorf("CreateBucket %s succeeded but GetBucketInfo failed: %w", bucket.Name, err)
	}

	if id == "" {
		return "", fmt.Errorf("CreateBucket %s succeeded but no bucket id was returned", bucket.Name)
	}

	return id, nil
}

func ensureBucketQuota(ctx context.Context, cfg GarageConfig, bucket BucketConfig, bucketID string, c APIClient) error {
	quotas := map[string]any{}
	if bucket.MaxSize == 0 && bucket.MaxObjects == 0 {
		quotas["maxSize"] = nil
		quotas["maxObjects"] = nil
	} else {
		quotas["maxSize"] = bucket.MaxSize
		quotas["maxObjects"] = bucket.MaxObjects
	}

	body := map[string]any{"quotas": quotas}
	path := "/v2/UpdateBucket?id=" + url.QueryEscape(bucketID)
	if err := c.post(ctx, path, body, nil); err != nil {
		return fmt.Errorf("UpdateBucket quotas bucket=%s id=%s: %w", bucket.Name, bucketID, err)
	}

	log.Printf("garage=%s bucket quotas ensured: bucket=%s id=%s max_size=%d max_objects=%d", cfg.Name, bucket.Name, bucketID, bucket.MaxSize, bucket.MaxObjects)
	return nil
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
	for _, m := range shortIDRe.FindAllString(s, -1) {
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

func newClient(base string, token string, timeout time.Duration) APIClient {
	return APIClient{
		base:  strings.TrimRight(base, "/"),
		token: token,
		hc: &http.Client{
			Timeout: timeout,
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

func resolveTargetIPs(ctx context.Context, garageName string, target ConfiguredTarget) ([]string, error) {
	switch target.Discovery {
	case "static":
		seen := map[string]bool{}
		out := []string{}
		for _, endpoint := range target.Endpoints {
			ips, err := resolveIPs(ctx, endpoint)
			if err != nil {
				return nil, fmt.Errorf("garage=%s target=%s discovery=static endpoint=%s: %w", garageName, targetLabel(target), endpoint, err)
			}
			if len(ips) != 1 {
				return nil, fmt.Errorf("garage=%s target=%s discovery=static endpoint=%s resolved %d IPs, expected exactly 1", garageName, targetLabel(target), endpoint, len(ips))
			}
			if !seen[ips[0]] {
				seen[ips[0]] = true
				out = append(out, ips[0])
			}
		}
		if len(out) != target.ExpectedCount {
			return nil, fmt.Errorf("garage=%s target=%s discovery=static expected_count=%d discovered_count=%d", garageName, targetLabel(target), target.ExpectedCount, len(out))
		}
		sort.Strings(out)
		return out, nil
	case "dns":
		ips, err := resolveIPs(ctx, target.Endpoint)
		if err != nil {
			return nil, fmt.Errorf("garage=%s target=%s discovery=dns endpoint=%s: %w", garageName, targetLabel(target), target.Endpoint, err)
		}
		if len(ips) != target.ExpectedCount {
			return nil, fmt.Errorf("garage=%s target=%s discovery=dns endpoint=%s expected_count=%d discovered_count=%d resolved_ips=%s; refusing to reconcile layout", garageName, targetLabel(target), target.Endpoint, target.ExpectedCount, len(ips), strings.Join(ips, ","))
		}
		return ips, nil
	default:
		return nil, fmt.Errorf("garage=%s target=%s unsupported discovery=%q", garageName, targetLabel(target), target.Discovery)
	}
}

func targetLabel(t ConfiguredTarget) string {
	if t.Name != "" {
		return t.Name
	}
	if t.Endpoint != "" {
		return t.Endpoint
	}
	return strings.Join(t.Endpoints, ",")
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
		name = stripEndpointPort(name)

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

func stripEndpointPort(endpoint string) string {
	s := strings.TrimSpace(endpoint)
	if strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://") {
		if u, err := url.Parse(s); err == nil && u.Host != "" {
			s = u.Host
		}
	}
	if host, _, err := net.SplitHostPort(s); err == nil {
		return strings.Trim(host, "[]")
	}
	return strings.Trim(s, "[]")
}

func endpointForIP(ip string, port int) string {
	if strings.Contains(ip, ":") {
		return "http://[" + ip + "]:" + strconv.Itoa(port)
	}
	return "http://" + ip + ":" + strconv.Itoa(port)
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

type tomlScalar struct {
	Kind string
	S    string
	SA   []string
	I    int64
	B    bool
}

func parseRawConfigTOML(src string) (RawRoot, []string) {
	var root RawRoot
	var errs []string
	var current *RawGarage
	seenGarageKeys := map[int]map[string]bool{}

	lines := strings.Split(src, "\n")
	for i := 0; i < len(lines); i++ {
		lineNo := i + 1
		line := strings.TrimSpace(stripTOMLComment(lines[i]))
		if line == "" {
			continue
		}

		if line == "[[garages]]" {
			root.Garages = append(root.Garages, RawGarage{})
			current = &root.Garages[len(root.Garages)-1]
			seenGarageKeys[len(root.Garages)-1] = map[string]bool{}
			continue
		}

		if strings.HasPrefix(line, "[") {
			errs = append(errs, fmt.Sprintf("line %d: unsupported TOML section %q; only [[garages]] is supported", lineNo, line))
			continue
		}

		if current == nil {
			errs = append(errs, fmt.Sprintf("line %d: key outside [[garages]] block", lineNo))
			continue
		}

		key, value, ok := splitTOMLEqual(line)
		if !ok {
			errs = append(errs, fmt.Sprintf("line %d: expected key = value", lineNo))
			continue
		}

		value = strings.TrimSpace(value)
		if strings.HasPrefix(value, "[") && bracketDepth(value) > 0 {
			parts := []string{value}
			depth := bracketDepth(value)
			for depth > 0 && i+1 < len(lines) {
				i++
				next := strings.TrimSpace(stripTOMLComment(lines[i]))
				if next == "" {
					continue
				}
				parts = append(parts, next)
				depth += bracketDepthDelta(next)
			}
			value = strings.Join(parts, " ")
			if bracketDepth(value) != 0 {
				errs = append(errs, fmt.Sprintf("line %d: unterminated array for key %s", lineNo, key))
				continue
			}
		}

		idx := len(root.Garages) - 1
		seen := seenGarageKeys[idx]
		if seen[key] {
			errs = append(errs, fmt.Sprintf("line %d: duplicate key garages[%d].%s", lineNo, idx, key))
			continue
		}
		seen[key] = true

		path := fmt.Sprintf("garages[%d].%s", idx, key)
		assignRawGarageValue(current, key, value, path, lineNo, &errs)
	}

	return root, errs
}

func assignRawGarageValue(g *RawGarage, key string, raw string, path string, lineNo int, errs *[]string) {
	scalar := func() (tomlScalar, bool) {
		v, err := parseTOMLScalar(raw)
		if err != nil {
			*errs = append(*errs, fmt.Sprintf("line %d: %s: %v", lineNo, path, err))
			return tomlScalar{}, false
		}
		return v, true
	}

	setString := func(dst **string) {
		v, ok := scalar()
		if !ok {
			return
		}
		if v.Kind != "string" {
			*errs = append(*errs, fmt.Sprintf("line %d: %s must be a string", lineNo, path))
			return
		}
		*dst = &v.S
	}
	setInt := func(dst **int) {
		v, ok := scalar()
		if !ok {
			return
		}
		if v.Kind != "int" {
			*errs = append(*errs, fmt.Sprintf("line %d: %s must be an integer", lineNo, path))
			return
		}
		if v.I < int64(^uint(0)>>1)*-1-1 || v.I > int64(^uint(0)>>1) {
			*errs = append(*errs, fmt.Sprintf("line %d: %s integer is out of range", lineNo, path))
			return
		}
		n := int(v.I)
		*dst = &n
	}
	setBool := func(dst **bool) {
		v, ok := scalar()
		if !ok {
			return
		}
		if v.Kind != "bool" {
			*errs = append(*errs, fmt.Sprintf("line %d: %s must be a boolean", lineNo, path))
			return
		}
		b := v.B
		*dst = &b
	}

	switch key {
	case "name":
		setString(&g.Name)
	case "garage_bin":
		setString(&g.GarageBin)
	case "admin_port":
		setInt(&g.AdminPort)
	case "rpc_port":
		setInt(&g.RPCPort)
	case "interval":
		setString(&g.Interval)
	case "timeout":
		setString(&g.Timeout)
	case "replication_factor":
		setInt(&g.ReplicationFactor)
	case "rpc_secret":
		setString(&g.RPCSecret)
	case "rpc_secret_env":
		setString(&g.RPCSecretEnv)
	case "rpc_secret_file":
		setString(&g.RPCSecretFile)
	case "admin_token":
		setString(&g.AdminToken)
	case "admin_token_env":
		setString(&g.AdminTokenEnv)
	case "admin_token_file":
		setString(&g.AdminTokenFile)
	case "replace_offline_nodes":
		setBool(&g.ReplaceOfflineNodes)
	case "targets":
		targets, targetErrs := parseRawTargets(raw, path)
		*errs = append(*errs, targetErrs...)
		g.Targets = targets
	case "nodes":
		*errs = append(*errs, fmt.Sprintf("line %d: %s is deprecated/unsupported; use targets = [...] with discovery = \"static\" or discovery = \"dns\"", lineNo, path))
	case "access_keys":
		accessKeys, accessKeyErrs := parseRawAccessKeys(raw, path)
		*errs = append(*errs, accessKeyErrs...)
		g.AccessKeys = accessKeys
	case "buckets":
		buckets, bucketErrs := parseRawBuckets(raw, path)
		*errs = append(*errs, bucketErrs...)
		g.Buckets = buckets
	default:
		*errs = append(*errs, fmt.Sprintf("line %d: unknown key %s", lineNo, path))
	}
}

func parseRawTargets(raw string, path string) ([]RawTarget, []string) {
	tables, errs := parseInlineTablesArray(raw, path)
	out := make([]RawTarget, 0, len(tables))
	for i, tbl := range tables {
		p := fmt.Sprintf("%s[%d]", path, i)
		var n RawTarget
		for key, val := range tbl {
			switch key {
			case "name":
				setScalarString(p+".name", val, &n.Name, &errs)
			case "discovery":
				setScalarString(p+".discovery", val, &n.Discovery, &errs)
			case "endpoint":
				setScalarString(p+".endpoint", val, &n.Endpoint, &errs)
			case "endpoints":
				setScalarStringArray(p+".endpoints", val, &n.Endpoints, &errs)
			case "expected_count":
				setScalarInt(p+".expected_count", val, &n.ExpectedCount, &errs)
			case "zone":
				setScalarString(p+".zone", val, &n.Zone, &errs)
			case "capacity":
				setScalarString(p+".capacity", val, &n.Capacity, &errs)
			case "garage_bin":
				setScalarString(p+".garage_bin", val, &n.GarageBin, &errs)
			case "admin_port":
				setScalarInt(p+".admin_port", val, &n.AdminPort, &errs)
			case "rpc_port":
				setScalarInt(p+".rpc_port", val, &n.RPCPort, &errs)
			case "timeout":
				setScalarString(p+".timeout", val, &n.Timeout, &errs)
			case "rpc_secret":
				setScalarString(p+".rpc_secret", val, &n.RPCSecret, &errs)
			case "rpc_secret_env":
				setScalarString(p+".rpc_secret_env", val, &n.RPCSecretEnv, &errs)
			case "rpc_secret_file":
				setScalarString(p+".rpc_secret_file", val, &n.RPCSecretFile, &errs)
			case "admin_token":
				setScalarString(p+".admin_token", val, &n.AdminToken, &errs)
			case "admin_token_env":
				setScalarString(p+".admin_token_env", val, &n.AdminTokenEnv, &errs)
			case "admin_token_file":
				setScalarString(p+".admin_token_file", val, &n.AdminTokenFile, &errs)
			default:
				errs = append(errs, fmt.Sprintf("unknown key %s.%s", p, key))
			}
		}
		out = append(out, n)
	}
	return out, errs
}

func parseRawAccessKeys(raw string, path string) ([]RawAccessKey, []string) {
	tables, errs := parseInlineTablesArray(raw, path)
	out := make([]RawAccessKey, 0, len(tables))
	for i, tbl := range tables {
		p := fmt.Sprintf("%s[%d]", path, i)
		var ak RawAccessKey
		for key, val := range tbl {
			switch key {
			case "key":
				setScalarString(p+".key", val, &ak.Key, &errs)
			case "access_key_id":
				setScalarString(p+".access_key_id", val, &ak.AccessKeyID, &errs)
			case "access_key_id_env":
				setScalarString(p+".access_key_id_env", val, &ak.AccessKeyIDEnv, &errs)
			case "access_key_id_file":
				setScalarString(p+".access_key_id_file", val, &ak.AccessKeyIDFile, &errs)
			case "secret_key":
				setScalarString(p+".secret_key", val, &ak.SecretKey, &errs)
			case "secret_key_env":
				setScalarString(p+".secret_key_env", val, &ak.SecretKeyEnv, &errs)
			case "secret_key_file":
				setScalarString(p+".secret_key_file", val, &ak.SecretKeyFile, &errs)
			default:
				errs = append(errs, fmt.Sprintf("unknown key %s.%s", p, key))
			}
		}
		out = append(out, ak)
	}
	return out, errs
}

func parseRawBuckets(raw string, path string) ([]RawBucket, []string) {
	tables, errs := parseInlineTablesArray(raw, path)
	out := make([]RawBucket, 0, len(tables))
	for i, tbl := range tables {
		p := fmt.Sprintf("%s[%d]", path, i)
		var b RawBucket
		for key, val := range tbl {
			switch key {
			case "name":
				setScalarString(p+".name", val, &b.Name, &errs)
			case "key":
				setScalarString(p+".key", val, &b.Key, &errs)
			case "max_size":
				setScalarInt64(p+".max_size", val, &b.MaxSize, &errs)
			case "max_objects":
				setScalarInt64(p+".max_objects", val, &b.MaxObjects, &errs)
			case "access_key_id":
				setScalarString(p+".access_key_id", val, &b.AccessKeyID, &errs)
			case "access_key_id_env":
				setScalarString(p+".access_key_id_env", val, &b.AccessKeyIDEnv, &errs)
			case "access_key_id_file":
				setScalarString(p+".access_key_id_file", val, &b.AccessKeyIDFile, &errs)
			case "secret_key":
				setScalarString(p+".secret_key", val, &b.SecretKey, &errs)
			case "secret_key_env":
				setScalarString(p+".secret_key_env", val, &b.SecretKeyEnv, &errs)
			case "secret_key_file":
				setScalarString(p+".secret_key_file", val, &b.SecretKeyFile, &errs)
			default:
				errs = append(errs, fmt.Sprintf("unknown key %s.%s", p, key))
			}
		}
		out = append(out, b)
	}
	return out, errs
}

func setScalarString(path string, val tomlScalar, dst **string, errs *[]string) {
	if val.Kind != "string" {
		*errs = append(*errs, fmt.Sprintf("%s must be a string", path))
		return
	}
	s := val.S
	*dst = &s
}

func setScalarStringArray(path string, val tomlScalar, dst *[]string, errs *[]string) {
	if val.Kind != "string_array" {
		*errs = append(*errs, fmt.Sprintf("%s must be an array of strings", path))
		return
	}
	*dst = append((*dst)[:0], val.SA...)
}

func setScalarInt(path string, val tomlScalar, dst **int, errs *[]string) {
	if val.Kind != "int" {
		*errs = append(*errs, fmt.Sprintf("%s must be an integer", path))
		return
	}
	if val.I < int64(^uint(0)>>1)*-1-1 || val.I > int64(^uint(0)>>1) {
		*errs = append(*errs, fmt.Sprintf("%s integer is out of range", path))
		return
	}
	n := int(val.I)
	*dst = &n
}

func setScalarInt64(path string, val tomlScalar, dst **int64, errs *[]string) {
	if val.Kind != "int" {
		*errs = append(*errs, fmt.Sprintf("%s must be an integer", path))
		return
	}
	n := val.I
	*dst = &n
}

func parseInlineTablesArray(raw string, path string) ([]map[string]tomlScalar, []string) {
	var errs []string
	s := strings.TrimSpace(raw)
	if !strings.HasPrefix(s, "[") || !strings.HasSuffix(s, "]") {
		return nil, []string{fmt.Sprintf("%s must be an array of inline tables", path)}
	}
	s = strings.TrimSpace(s[1 : len(s)-1])
	if s == "" {
		return nil, nil
	}

	var out []map[string]tomlScalar
	inString := false
	escaped := false
	depth := 0
	start := -1
	for i, r := range s {
		if inString {
			if escaped {
				escaped = false
				continue
			}
			if r == '\\' {
				escaped = true
				continue
			}
			if r == '"' {
				inString = false
			}
			continue
		}

		switch r {
		case '"':
			inString = true
		case '{':
			if depth == 0 {
				start = i + len(string(r))
			}
			depth++
		case '}':
			depth--
			if depth < 0 {
				errs = append(errs, fmt.Sprintf("%s has unmatched }", path))
				return out, errs
			}
			if depth == 0 && start >= 0 {
				content := s[start:i]
				tbl, tblErrs := parseInlineTable(content, fmt.Sprintf("%s[%d]", path, len(out)))
				errs = append(errs, tblErrs...)
				out = append(out, tbl)
				start = -1
			}
		default:
			if depth == 0 && !isTOMLArraySeparator(r) {
				errs = append(errs, fmt.Sprintf("%s has invalid token %q outside inline table", path, string(r)))
				return out, errs
			}
		}
	}

	if inString {
		errs = append(errs, fmt.Sprintf("%s has unterminated string", path))
	}
	if depth != 0 {
		errs = append(errs, fmt.Sprintf("%s has unterminated inline table", path))
	}
	return out, errs
}

func parseInlineTable(content string, path string) (map[string]tomlScalar, []string) {
	out := map[string]tomlScalar{}
	var errs []string
	parts := splitTopLevelComma(content)
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		key, rawVal, ok := splitTOMLEqual(part)
		if !ok {
			errs = append(errs, fmt.Sprintf("%s: expected key = value in inline table item %q", path, part))
			continue
		}
		if out[key].Kind != "" {
			errs = append(errs, fmt.Sprintf("%s.%s is duplicated", path, key))
			continue
		}
		val, err := parseTOMLScalar(rawVal)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s.%s: %v", path, key, err))
			continue
		}
		out[key] = val
	}
	return out, errs
}

func parseTOMLScalar(raw string) (tomlScalar, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return tomlScalar{}, errors.New("empty value")
	}
	if strings.HasPrefix(s, "[") {
		arr, err := parseTOMLStringArray(s)
		if err != nil {
			return tomlScalar{}, err
		}
		return tomlScalar{Kind: "string_array", SA: arr}, nil
	}
	if strings.HasPrefix(s, "\"") {
		if !strings.HasSuffix(s, "\"") || len(s) < 2 {
			return tomlScalar{}, errors.New("unterminated string")
		}
		v, err := strconv.Unquote(s)
		if err != nil {
			return tomlScalar{}, err
		}
		return tomlScalar{Kind: "string", S: v}, nil
	}
	switch s {
	case "true":
		return tomlScalar{Kind: "bool", B: true}, nil
	case "false":
		return tomlScalar{Kind: "bool", B: false}, nil
	}
	if strings.ContainsAny(s, ".eE") {
		return tomlScalar{}, fmt.Errorf("unsupported scalar %q", s)
	}
	n, err := strconv.ParseInt(strings.ReplaceAll(s, "_", ""), 10, 64)
	if err != nil {
		return tomlScalar{}, fmt.Errorf("unsupported scalar %q", s)
	}
	return tomlScalar{Kind: "int", I: n}, nil
}

func parseTOMLStringArray(raw string) ([]string, error) {
	s := strings.TrimSpace(raw)
	if !strings.HasPrefix(s, "[") || !strings.HasSuffix(s, "]") {
		return nil, fmt.Errorf("unsupported scalar %q", raw)
	}
	inner := strings.TrimSpace(s[1 : len(s)-1])
	if inner == "" {
		return []string{}, nil
	}
	parts := splitTopLevelComma(inner)
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if !strings.HasPrefix(part, "\"") || !strings.HasSuffix(part, "\"") {
			return nil, fmt.Errorf("array items must be strings: %q", part)
		}
		v, err := strconv.Unquote(part)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

func splitTopLevelComma(s string) []string {
	var out []string
	inString := false
	escaped := false
	bracketDepth := 0
	start := 0
	for i, r := range s {
		if inString {
			if escaped {
				escaped = false
				continue
			}
			if r == '\\' {
				escaped = true
				continue
			}
			if r == '"' {
				inString = false
			}
			continue
		}
		switch r {
		case '"':
			inString = true
		case '[':
			bracketDepth++
		case ']':
			if bracketDepth > 0 {
				bracketDepth--
			}
		case ',':
			if bracketDepth == 0 {
				out = append(out, s[start:i])
				start = i + len(string(r))
			}
		}
	}
	out = append(out, s[start:])
	return out
}
func splitTOMLEqual(s string) (string, string, bool) {
	inString := false
	escaped := false
	for i, r := range s {
		if inString {
			if escaped {
				escaped = false
				continue
			}
			if r == '\\' {
				escaped = true
				continue
			}
			if r == '"' {
				inString = false
			}
			continue
		}
		if r == '"' {
			inString = true
			continue
		}
		if r == '=' {
			key := strings.TrimSpace(s[:i])
			val := strings.TrimSpace(s[i+len(string(r)):])
			if key == "" {
				return "", "", false
			}
			return key, val, true
		}
	}
	return "", "", false
}

func stripTOMLComment(s string) string {
	inString := false
	escaped := false
	for i, r := range s {
		if inString {
			if escaped {
				escaped = false
				continue
			}
			if r == '\\' {
				escaped = true
				continue
			}
			if r == '"' {
				inString = false
			}
			continue
		}
		if r == '"' {
			inString = true
			continue
		}
		if r == '#' {
			return s[:i]
		}
	}
	return s
}

func bracketDepth(s string) int {
	depth := 0
	inString := false
	escaped := false
	for _, r := range s {
		if inString {
			if escaped {
				escaped = false
				continue
			}
			if r == '\\' {
				escaped = true
				continue
			}
			if r == '"' {
				inString = false
			}
			continue
		}
		if r == '"' {
			inString = true
			continue
		}
		if r == '[' {
			depth++
		} else if r == ']' {
			depth--
		}
	}
	return depth
}

func bracketDepthDelta(s string) int {
	return bracketDepth(s)
}

func isTOMLArraySeparator(r rune) bool {
	return r == ',' || r == ' ' || r == '\t' || r == '\r' || r == '\n'
}

func loadRuntimeConfig(configPath string, dryRunOverride string) (*RuntimeConfig, error) {
	src, errs := loadConfigSource(configPath)
	if src == "" {
		errs = append(errs, "configuration is required: pass -c <path>, set GARAGE_RECONCILER_CONFIG_FILE, or set GARAGE_RECONCILER_CONFIG_TOML")
	}
	return loadTOMLRuntimeConfig(src, dryRunOverride, errs)
}

func loadConfigSource(configPath string) (string, []string) {
	var errs []string
	path := strings.TrimSpace(configPath)
	if path == "" {
		path = strings.TrimSpace(os.Getenv("GARAGE_RECONCILER_CONFIG_FILE"))
	}
	if path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			return "", []string{fmt.Sprintf("cannot read config file %s: %v", path, err)}
		}
		return strings.TrimSpace(string(b)), nil
	}
	return strings.TrimSpace(os.Getenv("GARAGE_RECONCILER_CONFIG_TOML")), errs
}

func loadTOMLRuntimeConfig(src string, dryRunOverride string, initialErrs []string) (*RuntimeConfig, error) {
	raw, errs := parseRawConfigTOML(src)
	errs = append(initialErrs, errs...)

	dryRun, ok, err := parseStrictBoolSetting("GARAGE_RECONCILER_DRY_RUN", dryRunOverride)
	if err != nil {
		errs = append(errs, err.Error())
	} else if !ok {
		errs = append(errs, "GARAGE_RECONCILER_DRY_RUN is required and must be true or false, unless -dry-run=true|false is passed")
	}

	cfg := RuntimeConfig{DryRun: dryRun}
	if len(raw.Garages) == 0 {
		errs = append(errs, "at least one [[garages]] block is required")
	}

	garageNames := map[string]bool{}
	for i, rg := range raw.Garages {
		path := fmt.Sprintf("garages[%d]", i)
		g := resolveGarage(path, rg, dryRun, &errs)
		if g.Name != "" {
			if garageNames[g.Name] {
				errs = append(errs, fmt.Sprintf("%s.name %q is duplicated", path, g.Name))
			}
			garageNames[g.Name] = true
		}
		cfg.Garages = append(cfg.Garages, g)
	}

	if len(errs) > 0 {
		return &cfg, ValidationError(errs)
	}
	return &cfg, nil
}

func resolveGarage(path string, rg RawGarage, dryRun bool, errs *[]string) GarageConfig {
	g := GarageConfig{DryRun: dryRun}
	g.Name = requiredString(path+".name", rg.Name, errs)
	g.GarageBin = requiredString(path+".garage_bin", rg.GarageBin, errs)
	g.AdminPort = requiredPort(path+".admin_port", rg.AdminPort, errs)
	g.RPCPort = requiredPort(path+".rpc_port", rg.RPCPort, errs)
	g.Interval = requiredDuration(path+".interval", rg.Interval, errs)
	g.RequestTimeout = requiredDuration(path+".timeout", rg.Timeout, errs)
	g.ReplicationFactor = requiredPositiveInt(path+".replication_factor", rg.ReplicationFactor, errs)
	g.RPCSecret = resolveSecret(path+".rpc_secret", rg.RPCSecret, rg.RPCSecretEnv, rg.RPCSecretFile, true, errs)
	g.AdminToken = resolveSecret(path+".admin_token", rg.AdminToken, rg.AdminTokenEnv, rg.AdminTokenFile, true, errs)
	g.ReplaceOfflineNodes = requiredBool(path+".replace_offline_nodes", rg.ReplaceOfflineNodes, errs)

	if len(rg.Targets) == 0 {
		*errs = append(*errs, fmt.Sprintf("%s.targets must contain at least one discovery target", path))
	}
	if len(rg.Buckets) == 0 {
		*errs = append(*errs, fmt.Sprintf("%s.buckets must contain at least one bucket", path))
	}

	declaredNodeCount := 0
	seenEndpoints := map[string]int{}
	for i, rn := range rg.Targets {
		targetPath := fmt.Sprintf("%s.targets[%d]", path, i)
		t := resolveTarget(targetPath, g, rn, errs)
		g.Targets = append(g.Targets, t)
		declaredNodeCount += t.ExpectedCount
		if t.Discovery == "dns" && t.Endpoint != "" {
			if _, exists := seenEndpoints[t.Endpoint]; exists {
				*errs = append(*errs, fmt.Sprintf("%s.endpoint %q is duplicated; use one DNS target per homogeneous zone/capacity group", targetPath, t.Endpoint))
			}
			seenEndpoints[t.Endpoint] = i
		}
	}
	g.ExpectedNodes = declaredNodeCount
	if g.ExpectedNodes > 0 && g.ReplicationFactor > g.ExpectedNodes {
		*errs = append(*errs, fmt.Sprintf("%s.replication_factor must be <= discovered topology size derived from targets: replication_factor=%d target_nodes=%d", path, g.ReplicationFactor, g.ExpectedNodes))
	}

	accessKeyByKey := map[string]AccessKeyConfig{}
	for i, rak := range rg.AccessKeys {
		accessKeyPath := fmt.Sprintf("%s.access_keys[%d]", path, i)
		ak := resolveAccessKey(accessKeyPath, rak, errs)
		g.AccessKeys = append(g.AccessKeys, ak)
		if ak.Key == "" {
			continue
		}
		if _, exists := accessKeyByKey[ak.Key]; exists {
			*errs = append(*errs, fmt.Sprintf("%s.access_keys[%d].key %q is duplicated", path, i, ak.Key))
			continue
		}
		accessKeyByKey[ak.Key] = ak
	}

	for i, rb := range rg.Buckets {
		bucketPath := fmt.Sprintf("%s.buckets[%d]", path, i)
		g.Buckets = append(g.Buckets, resolveBucket(bucketPath, rb, accessKeyByKey, errs))
	}

	bucketNames := map[string]bool{}
	for i, b := range g.Buckets {
		if b.Name == "" {
			continue
		}
		if bucketNames[b.Name] {
			*errs = append(*errs, fmt.Sprintf("%s.buckets[%d].name %q is duplicated", path, i, b.Name))
		}
		bucketNames[b.Name] = true
	}

	for i, n := range g.Targets {
		if n.RPCSecret.Value != "" && g.RPCSecret.Value != "" && n.RPCSecret.Value != g.RPCSecret.Value {
			*errs = append(*errs, fmt.Sprintf("%s.targets[%d].rpc_secret differs from garage rpc_secret; Garage requires the same rpc_secret on all targets in one cluster", path, i))
		}
	}

	return g
}

func resolveTarget(path string, g GarageConfig, rn RawTarget, errs *[]string) ConfiguredTarget {
	n := ConfiguredTarget{}
	n.Name = optionalString(rn.Name)
	n.Discovery = requiredString(path+".discovery", rn.Discovery, errs)
	n.Endpoint = optionalString(rn.Endpoint)
	n.Endpoints = trimStringList(rn.Endpoints)
	n.Zone = requiredString(path+".zone", rn.Zone, errs)
	n.Capacity = requiredString(path+".capacity", rn.Capacity, errs)
	n.GarageBin = inheritString(rn.GarageBin, g.GarageBin)
	n.AdminPort = inheritInt(rn.AdminPort, g.AdminPort)
	n.RPCPort = inheritInt(rn.RPCPort, g.RPCPort)
	if rn.Timeout != nil {
		n.RequestTimeout = requiredDuration(path+".timeout", rn.Timeout, errs)
	} else {
		n.RequestTimeout = g.RequestTimeout
	}
	if rn.AdminToken != nil || rn.AdminTokenEnv != nil || rn.AdminTokenFile != nil {
		n.AdminToken = resolveSecret(path+".admin_token", rn.AdminToken, rn.AdminTokenEnv, rn.AdminTokenFile, true, errs)
	} else {
		n.AdminToken = g.AdminToken
	}
	if rn.RPCSecret != nil || rn.RPCSecretEnv != nil || rn.RPCSecretFile != nil {
		n.RPCSecret = resolveSecret(path+".rpc_secret", rn.RPCSecret, rn.RPCSecretEnv, rn.RPCSecretFile, true, errs)
	} else {
		n.RPCSecret = g.RPCSecret
	}
	if n.AdminPort <= 0 || n.AdminPort > 65535 {
		*errs = append(*errs, fmt.Sprintf("%s effective admin_port must be between 1 and 65535", path))
	}
	if n.RPCPort <= 0 || n.RPCPort > 65535 {
		*errs = append(*errs, fmt.Sprintf("%s effective rpc_port must be between 1 and 65535", path))
	}
	if n.GarageBin == "" {
		*errs = append(*errs, fmt.Sprintf("%s effective garage_bin is required", path))
	}
	switch n.Discovery {
	case "static":
		if n.Endpoint != "" {
			*errs = append(*errs, fmt.Sprintf("%s.endpoint is not allowed with discovery=static; use endpoints=[...]", path))
		}
		if len(n.Endpoints) == 0 {
			*errs = append(*errs, fmt.Sprintf("%s.endpoints must contain at least one endpoint when discovery=static", path))
		}
		if rn.ExpectedCount != nil {
			n.ExpectedCount = optionalPositiveInt(path+".expected_count", rn.ExpectedCount, 0, errs)
			if n.ExpectedCount != len(n.Endpoints) {
				*errs = append(*errs, fmt.Sprintf("%s.expected_count must equal len(endpoints) when discovery=static: got %d, want %d", path, n.ExpectedCount, len(n.Endpoints)))
			}
		} else {
			n.ExpectedCount = len(n.Endpoints)
		}
	case "dns":
		if n.Endpoint == "" {
			*errs = append(*errs, fmt.Sprintf("%s.endpoint is required when discovery=dns", path))
		}
		if len(n.Endpoints) > 0 {
			*errs = append(*errs, fmt.Sprintf("%s.endpoints is not allowed with discovery=dns; use endpoint=...", path))
		}
		n.ExpectedCount = requiredPositiveInt(path+".expected_count", rn.ExpectedCount, errs)
	default:
		if n.Discovery != "" {
			*errs = append(*errs, fmt.Sprintf("%s.discovery must be either \"static\" or \"dns\", got %q", path, n.Discovery))
		}
	}

	return n
}

func resolveAccessKey(path string, rak RawAccessKey, errs *[]string) AccessKeyConfig {
	ak := AccessKeyConfig{}
	ak.Key = requiredString(path+".key", rak.Key, errs)
	ak.AccessKeyID = resolveSecret(path+".access_key_id", rak.AccessKeyID, rak.AccessKeyIDEnv, rak.AccessKeyIDFile, true, errs)
	ak.SecretKey = resolveSecret(path+".secret_key", rak.SecretKey, rak.SecretKeyEnv, rak.SecretKeyFile, true, errs)
	return ak
}

func resolveBucket(path string, rb RawBucket, inherited map[string]AccessKeyConfig, errs *[]string) BucketConfig {
	b := BucketConfig{}
	b.Name = requiredString(path+".name", rb.Name, errs)
	b.Key = requiredString(path+".key", rb.Key, errs)
	if rb.MaxSize == nil {
		*errs = append(*errs, fmt.Sprintf("%s.max_size is required", path))
	} else {
		b.MaxSize = *rb.MaxSize
	}
	if rb.MaxObjects == nil {
		*errs = append(*errs, fmt.Sprintf("%s.max_objects is required", path))
	} else {
		b.MaxObjects = *rb.MaxObjects
	}
	if b.MaxSize < 0 {
		*errs = append(*errs, fmt.Sprintf("%s.max_size must be >= 0", path))
	}
	if b.MaxObjects < 0 {
		*errs = append(*errs, fmt.Sprintf("%s.max_objects must be >= 0", path))
	}
	if (b.MaxSize == 0) != (b.MaxObjects == 0) {
		*errs = append(*errs, fmt.Sprintf("%s.max_size and max_objects must both be 0, or both be > 0", path))
	}

	parent, hasParent := inherited[b.Key]
	localAccessKeyID := hasSecretSource(rb.AccessKeyID, rb.AccessKeyIDEnv, rb.AccessKeyIDFile)
	localSecretKey := hasSecretSource(rb.SecretKey, rb.SecretKeyEnv, rb.SecretKeyFile)

	if localAccessKeyID != localSecretKey {
		*errs = append(*errs, fmt.Sprintf("%s bucket credential override must define both access_key_id and secret_key sources, or neither", path))
	}

	if localAccessKeyID && localSecretKey {
		b.AccessKeyID = resolveSecret(path+".access_key_id", rb.AccessKeyID, rb.AccessKeyIDEnv, rb.AccessKeyIDFile, true, errs)
		b.SecretKey = resolveSecret(path+".secret_key", rb.SecretKey, rb.SecretKeyEnv, rb.SecretKeyFile, true, errs)
		if hasParent && b.AccessKeyID.Value != "" && parent.AccessKeyID.Value == b.AccessKeyID.Value && parent.SecretKey.Value != "" && b.SecretKey.Value != "" && parent.SecretKey.Value != b.SecretKey.Value {
			*errs = append(*errs, fmt.Sprintf("%s overrides secret_key for inherited access_key_id %q; use a different access_key_id or keep the inherited secret_key", path, b.AccessKeyID.Value))
		}
	} else if hasParent {
		b.AccessKeyID = parent.AccessKeyID
		b.SecretKey = parent.SecretKey
	} else {
		*errs = append(*errs, fmt.Sprintf("%s.access_key_id, %s.access_key_id_env or %s.access_key_id_file is required because no access_keys entry exists for key %q", path, path, path, b.Key))
		*errs = append(*errs, fmt.Sprintf("%s.secret_key, %s.secret_key_env or %s.secret_key_file is required because no access_keys entry exists for key %q", path, path, path, b.Key))
		b.AccessKeyID = SecretValue{Kind: "missing", Source: "missing"}
		b.SecretKey = SecretValue{Kind: "missing", Source: "missing"}
	}

	return b
}

func hasSecretSource(valueRef *string, envRef *string, fileRef *string) bool {
	return valueRef != nil || envRef != nil || fileRef != nil
}

func requiredString(path string, v *string, errs *[]string) string {
	if v == nil {
		*errs = append(*errs, fmt.Sprintf("%s is required", path))
		return ""
	}
	s := strings.TrimSpace(*v)
	if s == "" {
		*errs = append(*errs, fmt.Sprintf("%s must not be empty", path))
	}
	return s
}

func optionalString(v *string) string {
	if v == nil {
		return ""
	}
	return strings.TrimSpace(*v)
}

func trimStringList(in []string) []string {
	out := []string{}
	seen := map[string]bool{}
	for _, raw := range in {
		s := strings.TrimSpace(raw)
		if s == "" || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}

func requiredBool(path string, v *bool, errs *[]string) bool {
	if v == nil {
		*errs = append(*errs, fmt.Sprintf("%s is required", path))
		return false
	}
	return *v
}

func requiredPositiveInt(path string, v *int, errs *[]string) int {
	if v == nil {
		*errs = append(*errs, fmt.Sprintf("%s is required", path))
		return 0
	}
	if *v <= 0 {
		*errs = append(*errs, fmt.Sprintf("%s must be > 0", path))
	}
	return *v
}

func optionalPositiveInt(path string, v *int, defaultValue int, errs *[]string) int {
	if v == nil {
		return defaultValue
	}
	if *v <= 0 {
		*errs = append(*errs, fmt.Sprintf("%s must be > 0", path))
	}
	return *v
}

func requiredPort(path string, v *int, errs *[]string) int {
	p := requiredPositiveInt(path, v, errs)
	if p > 65535 {
		*errs = append(*errs, fmt.Sprintf("%s must be <= 65535", path))
	}
	return p
}

func requiredDuration(path string, v *string, errs *[]string) time.Duration {
	if v == nil {
		*errs = append(*errs, fmt.Sprintf("%s is required", path))
		return 0
	}
	s := strings.TrimSpace(*v)
	if s == "" {
		*errs = append(*errs, fmt.Sprintf("%s must not be empty", path))
		return 0
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		*errs = append(*errs, fmt.Sprintf("%s must be a valid Go duration: %v", path, err))
		return 0
	}
	if d <= 0 {
		*errs = append(*errs, fmt.Sprintf("%s must be > 0", path))
	}
	return d
}

func resolveSecret(path string, valueRef *string, envRef *string, fileRef *string, required bool, errs *[]string) SecretValue {
	defined := []string{}
	if valueRef != nil {
		defined = append(defined, path)
	}
	if envRef != nil {
		defined = append(defined, path+"_env")
	}
	if fileRef != nil {
		defined = append(defined, path+"_file")
	}
	if len(defined) > 1 {
		*errs = append(*errs, fmt.Sprintf("%s cannot define more than one secret source: %s", path, strings.Join(defined, ", ")))
		return SecretValue{Kind: "invalid", Source: "conflict"}
	}

	if valueRef != nil {
		return resolveSecretValue(path, *valueRef, required, errs)
	}

	if envRef != nil {
		return resolveEnvSecret(path+"_env", *envRef, required, errs)
	}

	if fileRef != nil {
		return resolveSecretFile(path+"_file", *fileRef, required, errs)
	}

	if required {
		*errs = append(*errs, fmt.Sprintf("%s, %s_env or %s_file is required", path, path, path))
	}
	return SecretValue{Kind: "missing", Source: "missing"}
}

func resolveSecretValue(path string, raw string, required bool, errs *[]string) SecretValue {
	s := strings.TrimSpace(raw)
	if s == "" {
		if required {
			*errs = append(*errs, fmt.Sprintf("%s must not be empty", path))
		}
		return SecretValue{Kind: "empty", Source: "literal"}
	}
	return SecretValue{Kind: "literal", Source: "literal", Value: s}
}

func resolveEnvSecret(path string, envName string, required bool, errs *[]string) SecretValue {
	envName = strings.TrimSpace(envName)
	if envName == "" {
		if required {
			*errs = append(*errs, fmt.Sprintf("%s env reference must not be empty", path))
		}
		return SecretValue{Kind: "env", Source: "environment"}
	}
	v := strings.TrimSpace(os.Getenv(envName))
	if v == "" && required {
		*errs = append(*errs, fmt.Sprintf("%s references environment variable %s but it is empty or missing", path, envName))
	}
	return SecretValue{Kind: "env", Source: "$" + envName, Value: v}
}

func resolveSecretFile(path string, filePath string, required bool, errs *[]string) SecretValue {
	p := strings.TrimSpace(filePath)
	if p == "" {
		if required {
			*errs = append(*errs, fmt.Sprintf("%s must not be empty", path))
		}
		return SecretValue{Kind: "file", Source: "file:"}
	}
	b, err := os.ReadFile(p)
	if err != nil {
		if required {
			*errs = append(*errs, fmt.Sprintf("%s cannot be read: %v", path, err))
		}
		return SecretValue{Kind: "file", Source: "file:" + p}
	}
	v := strings.TrimSpace(string(b))
	if v == "" && required {
		*errs = append(*errs, fmt.Sprintf("%s file %s is empty", path, p))
	}
	return SecretValue{Kind: "file", Source: "file:" + p, Value: v}
}

func inheritString(v *string, parent string) string {
	if v == nil {
		return parent
	}
	return strings.TrimSpace(*v)
}

func inheritInt(v *int, parent int) int {
	if v == nil {
		return parent
	}
	return *v
}

func parseStrictBoolSetting(key string, override string) (bool, bool, error) {
	raw := strings.TrimSpace(override)
	exists := raw != ""
	if !exists {
		raw, exists = os.LookupEnv(key)
	}
	if !exists {
		return false, false, nil
	}
	v := strings.TrimSpace(strings.ToLower(raw))
	switch v {
	case "1", "true", "yes", "on":
		return true, true, nil
	case "0", "false", "no", "off":
		return false, true, nil
	default:
		return false, true, fmt.Errorf("%s must be true or false, got %q", key, raw)
	}
}

func printRuntimeConfig(cfg RuntimeConfig) {
	log.Printf("resolved configuration begin")
	log.Printf("  dry_run=%v", cfg.DryRun)
	for _, g := range cfg.Garages {
		log.Printf("  [[garages]] name=%q", g.Name)
		log.Printf("    garage_bin=%q admin_port=%d rpc_port=%d interval=%s timeout=%s", g.GarageBin, g.AdminPort, g.RPCPort, g.Interval, g.RequestTimeout)
		log.Printf("    target_nodes=%d replication_factor=%d replace_offline_nodes=%v", g.ExpectedNodes, g.ReplicationFactor, g.ReplaceOfflineNodes)
		log.Printf("    rpc_secret=%s admin_token=%s", redactSecret(g.RPCSecret), redactSecret(g.AdminToken))
		log.Printf("    targets:")
		for _, n := range g.Targets {
			log.Printf("      - name=%q discovery=%q endpoint=%q endpoints=%q expected_count=%d zone=%q capacity=%q garage_bin=%q admin_port=%d rpc_port=%d timeout=%s admin_token=%s rpc_secret=%s", n.Name, n.Discovery, n.Endpoint, strings.Join(n.Endpoints, ","), n.ExpectedCount, n.Zone, n.Capacity, n.GarageBin, n.AdminPort, n.RPCPort, n.RequestTimeout, redactSecret(n.AdminToken), redactSecret(n.RPCSecret))
		}
		log.Printf("    access_keys:")
		for _, ak := range g.AccessKeys {
			log.Printf("      - key=%q access_key_id=%s secret_key=%s", ak.Key, redactSecret(ak.AccessKeyID), redactSecret(ak.SecretKey))
		}
		log.Printf("    buckets:")
		for _, b := range g.Buckets {
			log.Printf("      - name=%q key=%q max_size=%d max_objects=%d access_key_id=%s secret_key=%s", b.Name, b.Key, b.MaxSize, b.MaxObjects, redactSecret(b.AccessKeyID), redactSecret(b.SecretKey))
		}
	}
	log.Printf("resolved configuration end")
}

func redactSecret(s SecretValue) string {
	if s.Kind == "" {
		return "<missing>"
	}
	status := "unset"
	if s.Value != "" {
		status = "********"
	}
	if s.Source == "" {
		return fmt.Sprintf("%s:%s", s.Kind, status)
	}
	return fmt.Sprintf("%s -> %s", s.Source, status)
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
