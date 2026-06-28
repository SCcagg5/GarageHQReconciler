# garage-reconciler

Single-file Go reconciler for one or more Garage clusters used as S3 backends.

The binary is intended to be copied into the same `FROM scratch` image as the Garage server binary:

```dockerfile
FROM scratch
ADD garage.tgz /
ADD garage-reconciler.tgz /
COPY garage.toml /etc/garage.toml
ENTRYPOINT ["/garage"]
CMD ["server"]
```

## Configuration

The reconciler is configured with two mandatory environment variables:

- `GARAGE_RECONCILER_CONFIG_TOML`: full TOML configuration.
- `GARAGE_RECONCILER_DRY_RUN`: must be `true` or `false`.

The TOML configuration is explicit: every garage, node, access key, bucket, quota, and reconciliation option must be declared.

Example:

```toml
[[garages]]
name = "rf2"
garage_bin = "/garage"
admin_port = 3903
rpc_port = 3901
interval = "30s"
timeout = "10s"
expected_nodes = 2
replication_factor = 2
rpc_secret_env = "GARAGE_RF2_RPC_SECRET"
admin_token_env = "GARAGE_RF2_ADMIN_TOKEN"
replace_offline_nodes = true

nodes = [
  { endpoint = "tasks.garage-rf2", zone = "dc1", capacity = "10G" },
  { endpoint = "tasks.garage-rf2", zone = "dc2", capacity = "10G", admin_port = 4903, rpc_port = 4901 },
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

[[garages]]
name = "rf3"
garage_bin = "/garage"
admin_port = 3903
rpc_port = 3901
interval = "30s"
timeout = "10s"
expected_nodes = 3
replication_factor = 3
rpc_secret_file = "/run/secrets/garage_rf3_rpc_secret"
admin_token_file = "/run/secrets/garage_rf3_admin_token"
replace_offline_nodes = true

nodes = [
  { endpoint = "garage-rf3-1.internal", zone = "dc1", capacity = "100G" },
  { endpoint = "garage-rf3-2.internal", zone = "dc2", capacity = "100G" },
  { endpoint = "garage-rf3-3.internal", zone = "dc3", capacity = "100G" },
]

access_keys = [
  { key = "archive", access_key_id_env = "GARAGE_ARCHIVE_ACCESS_KEY_ID", secret_key_env = "GARAGE_ARCHIVE_SECRET_KEY" },
]

buckets = [
  { name = "archive", key = "archive", max_size = 1000000000000, max_objects = 10000000 },
]
```

Environment for the example above:

```env
GARAGE_RF2_RPC_SECRET=...
GARAGE_RF2_ADMIN_TOKEN=...
GARAGE_REGISTRY_ACCESS_KEY_ID=...
GARAGE_REGISTRY_SECRET_KEY=...
GARAGE_CACHE_ACCESS_KEY_ID=...
GARAGE_CACHE_SECRET_KEY=...
GARAGE_ARCHIVE_ACCESS_KEY_ID=...
GARAGE_ARCHIVE_SECRET_KEY=...
GARAGE_RECONCILER_DRY_RUN=false
```

## Inheritance

Garage-level values are inherited by nodes unless the node defines its own value:

- `garage_bin`
- `admin_port`
- `rpc_port`
- `timeout`
- `admin_token`, `admin_token_env`, or `admin_token_file`
- `rpc_secret`, `rpc_secret_env`, or `rpc_secret_file`

The final `rpc_secret` must resolve to the same value for every node in one garage cluster.

Access keys are declared once in `access_keys` and referenced by buckets through `key`:

```toml
access_keys = [
  { key = "registry", access_key_id_env = "GARAGE_REGISTRY_ACCESS_KEY_ID", secret_key_env = "GARAGE_REGISTRY_SECRET_KEY" },
]

buckets = [
  { name = "docker-registry", key = "registry", max_size = 0, max_objects = 0 },
  { name = "registry-cache", key = "registry", max_size = 0, max_objects = 0 },
]
```

A bucket may override the inherited access key locally, but the override must provide both sides of the credential pair:

```toml
access_keys = [
  { key = "registry", access_key_id_env = "GARAGE_REGISTRY_ACCESS_KEY_ID", secret_key_env = "GARAGE_REGISTRY_SECRET_KEY" },
]

buckets = [
  {
    name = "docker-registry"
    key = "registry"
    max_size = 0
    max_objects = 0
    access_key_id_file = "/run/secrets/custom_registry_access_key_id"
    secret_key_file = "/run/secrets/custom_registry_secret_key"
  },
]
```

A bucket may also declare both credentials directly without using `access_keys`:

```toml
buckets = [
  {
    name = "docker-registry"
    key = "registry"
    max_size = 0
    max_objects = 0
    access_key_id_file = "/run/secrets/registry_access_key_id"
    secret_key_file = "/run/secrets/registry_secret_key"
  },
]
```

## Secret sources

Secrets and tokens support three forms:

```toml
# direct literal value
rpc_secret = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

# environment variable reference
rpc_secret_env = "GARAGE_RF2_RPC_SECRET"

# file reference
rpc_secret_file = "/run/secrets/garage_rpc_secret"
```

The same pattern is supported for:

- `rpc_secret` / `rpc_secret_env` / `rpc_secret_file`
- `admin_token` / `admin_token_env` / `admin_token_file`
- `access_key_id` / `access_key_id_env` / `access_key_id_file`
- `secret_key` / `secret_key_env` / `secret_key_file`

Only one source may be defined for a given secret. For example, `admin_token` and `admin_token_file` together is a configuration error.

The reconciler always prints the fully resolved configuration with secrets redacted before exiting on configuration errors.

## Validation rules

The reconciler is fail-fast:

- `GARAGE_RECONCILER_CONFIG_TOML` is mandatory and must parse successfully.
- `GARAGE_RECONCILER_DRY_RUN` is mandatory and must be `true` or `false`.
- Every garage field is mandatory.
- Every node must have `endpoint`, `zone`, and `capacity`.
- Every `access_keys` entry must have `key` and exactly one source for both `access_key_id` and `secret_key`.
- Every bucket must have `name`, `key`, `max_size`, and `max_objects`.
- A bucket must either reference a matching `access_keys[].key`, or define credentials locally.
- A bucket-local credential override must define both an access key ID source and a secret key source.
- If a bucket override uses the same inherited access key ID but a different secret key, the configuration is rejected.
- `len(nodes)` must equal `expected_nodes`.
- `replication_factor` must be less than or equal to `expected_nodes`.
- `rpc_secret` must resolve to the same value on every node of one Garage cluster.
- Only one of direct value, `_env`, or `_file` may be defined for each secret.
- A referenced environment variable or secret file must exist and be non-empty.
- `max_size` and `max_objects` must both be `0`, or both be greater than `0`.

`max_size = 0` and `max_objects = 0` means unlimited quotas. The reconciler sends `maxSize = null` and `maxObjects = null` to Garage when updating bucket quotas.

## Reconciliation behavior

On every reconciliation loop for each configured Garage cluster, the binary:

1. resolves each node endpoint to get Garage task IPs;
2. calls each task's Admin API to discover its Garage node ID;
3. sends `ConnectClusterNodes` so the Garage nodes connect to each other;
4. calls `UpdateClusterLayout` for visible nodes that have no role;
5. if `replace_offline_nodes=true`, removes role IDs that are no longer visible once the expected number of replacement nodes is visible;
6. applies the next layout version;
7. imports configured S3 keys, creates configured buckets, grants key permissions, and applies bucket quotas;
8. prints warnings for Garage buckets that exist but are not declared in TOML;
9. prints warnings when a declared bucket has extra authorized keys not declared for that bucket in TOML.

Garage layout changes are staged with role updates and applied once afterward. Bucket/key reconciliation is intentionally non-destructive: undeclared buckets and extra bucket-key grants are warned about, not deleted or revoked.

## Build

```bash
go test ./...
make build
```

The release workflow builds static Linux binaries and publishes `.tgz` archives.
