# garage-reconciler

Single-file Go reconciler for one or more Garage clusters used as S3 backends.

The reconciler is intentionally orchestrator-agnostic: it does **not** talk to the Docker API, the Docker Swarm API, or the Kubernetes API. It only knows how to obtain Garage endpoints, contact Garage's Admin API to read the real Garage node ID, then use the Garage CLI to reconcile the layout.

## Runtime

Preferred runtime form:

```bash
garage-reconciler -c /etc/garage-reconciler.toml -dry-run=false
```

Supported configuration sources, in priority order:

- `-c <path>`
- `GARAGE_RECONCILER_CONFIG_FILE=<path>`
- `GARAGE_RECONCILER_CONFIG_TOML=<full TOML>` for legacy deployments only

Dry-run can be set with `-dry-run=true|false` or `GARAGE_RECONCILER_DRY_RUN=true|false`.

## Discovery model

A Garage cluster contains `targets`, not Docker containers or Kubernetes pods.

Each target says:

- how to discover one or more Garage instances;
- which Garage layout role to apply to the discovered instances: `zone` and `capacity`;
- how many instances are expected behind that target.

Only two discovery modes exist:

```text
static

dns
```

There is deliberately no `docker_swarm` mode and no `kubernetes` mode.

### `static`

Use `static` when you already know each endpoint. This is the right mode for bare-metal, VPS, systemd services, Ansible/Terraform inventory, or any deployment with stable hostnames/IPs.

```toml
[not valid alone; see full example below]

targets = [
  { name = "paris-1", discovery = "static", endpoints = ["10.0.0.11"], zone = "paris", capacity = "2T" },
]
```

Rules:

- `endpoints = [...]` is required.
- `endpoint = "..."` is forbidden.
- `expected_count` is optional; if present, it must equal `len(endpoints)`.
- Each static endpoint must resolve to exactly one IP.

### `dns`

Use `dns` when one DNS name returns multiple Garage task/pod/instance IPs.

This covers Docker Swarm through `tasks.<service>` and Kubernetes through a headless service, without integrating either API.

```toml
[not valid alone; see full example below]

targets = [
  { name = "dc1", discovery = "dns", endpoint = "tasks.garage-dc1", expected_count = 2, zone = "dc1", capacity = "2T" },
]
```

Rules:

- `endpoint = "..."` is required.
- `endpoints = [...]` is forbidden.
- `expected_count` is required.
- DNS must resolve to exactly `expected_count` IPs.
- If DNS resolves fewer or more IPs than expected, the reconciler aborts and applies nothing.

This is fail-closed on purpose. If you declare `expected_count = 2` and DNS returns 3 IPs, the reconciler refuses to reconcile instead of picking two arbitrary nodes.

## Full example: Docker Swarm via DNS only

```toml
[[garages]]
name = "rf2"
garage_bin = "/garage"
admin_port = 3903
rpc_port = 3901
interval = "30s"
timeout = "10s"
replication_factor = 2
rpc_secret_env = "GARAGE_RF2_RPC_SECRET"
admin_token_env = "GARAGE_RF2_ADMIN_TOKEN"
replace_offline_nodes = true

targets = [
  # Docker Swarm replicated service: tasks.garage-rf2 resolves to all task IPs.
  # All discovered tasks get the same Garage zone/capacity.
  { name = "rf2-dc1", discovery = "dns", endpoint = "tasks.garage-rf2", expected_count = 2, zone = "dc1", capacity = "10G" },
]

access_keys = [
  { key = "registry", access_key_id_env = "GARAGE_REGISTRY_ACCESS_KEY_ID", secret_key_env = "GARAGE_REGISTRY_SECRET_KEY" },
  { key = "cache", access_key_id_env = "GARAGE_CACHE_ACCESS_KEY_ID", secret_key_env = "GARAGE_CACHE_SECRET_KEY" },
]

buckets = [
  { name = "docker-registry", key = "registry", max_size = 0, max_objects = 0 },
  { name = "registry-cache", key = "registry", max_size = 50000000000, max_objects = 1000000 },
  { name = "object-cache", key = "cache", max_size = 0, max_objects = 0 },
]
```

## Full example: systemd / bare-metal

```toml
[[garages]]
name = "rf3"
garage_bin = "/usr/local/bin/garage"
admin_port = 3903
rpc_port = 3901
interval = "30s"
timeout = "10s"
replication_factor = 3
rpc_secret_file = "/etc/garage/rpc_secret"
admin_token_file = "/etc/garage/admin_token"
replace_offline_nodes = true

targets = [
  { name = "paris-1", discovery = "static", endpoints = ["garage-paris-1.internal"], zone = "paris", capacity = "2T" },
  { name = "roubaix-1", discovery = "static", endpoints = ["garage-roubaix-1.internal"], zone = "roubaix", capacity = "2T" },
  { name = "frankfurt-1", discovery = "static", endpoints = ["garage-frankfurt-1.internal"], zone = "frankfurt", capacity = "2T" },
]

access_keys = [
  { key = "archive", access_key_id_env = "GARAGE_ARCHIVE_ACCESS_KEY_ID", secret_key_env = "GARAGE_ARCHIVE_SECRET_KEY" },
]

buckets = [
  { name = "archive", key = "archive", max_size = 1000000000000, max_objects = 10000000 },
]
```

## Target inheritance

Garage-level values are inherited by targets unless the target defines its own value:

- `garage_bin`
- `admin_port`
- `rpc_port`
- `timeout`
- `admin_token`, `admin_token_env`, or `admin_token_file`
- `rpc_secret`, `rpc_secret_env`, or `rpc_secret_file`

The final `rpc_secret` must resolve to the same value for every target in one Garage cluster.

## Secret sources

Secrets and tokens support three forms:

```toml
rpc_secret = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
rpc_secret_env = "GARAGE_RF2_RPC_SECRET"
rpc_secret_file = "/run/secrets/garage_rpc_secret"
```

The same pattern is supported for:

- `rpc_secret` / `rpc_secret_env` / `rpc_secret_file`
- `admin_token` / `admin_token_env` / `admin_token_file`
- `access_key_id` / `access_key_id_env` / `access_key_id_file`
- `secret_key` / `secret_key_env` / `secret_key_file`

Only one source may be defined for a given secret.

## Access keys and buckets

Access keys are declared once in `access_keys` and referenced by buckets through `key`.

```toml
access_keys = [
  { key = "registry", access_key_id_env = "GARAGE_REGISTRY_ACCESS_KEY_ID", secret_key_env = "GARAGE_REGISTRY_SECRET_KEY" },
]

buckets = [
  { name = "docker-registry", key = "registry", max_size = 0, max_objects = 0 },
]
```

A bucket may override the inherited access key locally, but the override must provide both sides of the credential pair.

`max_size = 0` and `max_objects = 0` means unlimited quotas. The reconciler sends `maxSize = null` and `maxObjects = null` to Garage when updating bucket quotas.

## Validation rules

The reconciler is fail-fast:

- Configuration must come from `-c`, `GARAGE_RECONCILER_CONFIG_FILE`, or `GARAGE_RECONCILER_CONFIG_TOML`.
- Dry-run must be explicit through `-dry-run=true|false` or `GARAGE_RECONCILER_DRY_RUN=true|false`.
- Every garage field is mandatory.
- `targets` must contain at least one discovery target.
- `discovery` must be either `static` or `dns`.
- `static` targets require `endpoints = [...]` and forbid `endpoint`.
- `dns` targets require `endpoint` and `expected_count`, and forbid `endpoints`.
- There is no global `expected_nodes`; the expected cluster size is computed from the declared targets.
- At runtime, each target must discover exactly its `expected_count`; too few or too many discovered IPs abort reconciliation.
- `replication_factor` must be less than or equal to the expected cluster size computed from the targets.
- `rpc_secret` must resolve to the same value on every target of one Garage cluster.
- Only one of direct value, `_env`, or `_file` may be defined for each secret.
- A referenced environment variable or secret file must exist and be non-empty.
- Every `access_keys` entry must have `key` and exactly one source for both `access_key_id` and `secret_key`.
- Every bucket must have `name`, `key`, `max_size`, and `max_objects`.
- A bucket must either reference a matching `access_keys[].key`, or define credentials locally.
- A bucket-local credential override must define both an access key ID source and a secret key source.
- If a bucket override uses the same inherited access key ID but a different secret key, the configuration is rejected.
- `max_size` and `max_objects` must both be `0`, or both be greater than `0`.

The old `nodes = [...]` field is rejected. Use `targets = [...]`.

## Reconciliation behavior

On every reconciliation loop for each configured Garage cluster, the binary:

1. discovers target IPs through `static` or `dns`;
2. refuses to continue unless each target returns exactly its declared count;
3. calls each discovered instance's Admin API to discover its real Garage node ID;
4. sends `ConnectClusterNodes` so the Garage nodes connect to each other;
5. calls `UpdateClusterLayout` for visible nodes that have no role;
6. if `replace_offline_nodes=true`, removes role IDs that are no longer visible once the expected target topology is fully visible;
7. applies the next layout version;
8. imports configured S3 keys, creates configured buckets, grants key permissions, and applies bucket quotas;
9. prints warnings for Garage buckets that exist but are not declared in TOML;
10. prints warnings when a declared bucket has extra authorized keys not declared for that bucket in TOML.

Garage layout changes are staged with role updates and applied once afterward. Bucket/key reconciliation is intentionally non-destructive: undeclared buckets and extra bucket-key grants are warned about, not deleted or revoked.

## Important deployment notes

The reconciler does **not** need Docker container names. It talks to Garage through:

- DNS or static hostnames/IPs, to discover reachable instances;
- Garage Admin API, to discover the real Garage node ID;
- Garage CLI, using `<node_id>@<ip>:<rpc_port>`, to connect nodes and update layout.

If multiple instances are behind one DNS target, they must share the same logical role: same `zone`, same `capacity`, same inherited secrets/ports. If you need different zones/capacities, use separate DNS names or separate static targets.

Garage data must be persistent per Garage node. A recreated container/pod/VM must keep the same Garage data directory if it is meant to keep the same Garage node ID.

## Build

```bash
go test ./...
make build
```
