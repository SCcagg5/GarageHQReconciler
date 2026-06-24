# garage-reconciler

Single-file Go reconciler for a Docker Swarm Garage cluster used as the S3 backend of a Docker Registry.

The binary is intended to be copied into the same `FROM scratch` image as the Garage server binary:

```dockerfile
FROM scratch
ADD garage.tgz /
ADD garage-reconciler.tgz /
COPY garage.toml /etc/garage.toml
ENTRYPOINT ["/garage"]
CMD ["server"]
```

Then the same image can run two Swarm services:

```yaml
services:
  garage:
    image: localhost:5000/garage:local
    command: ["server"]
    networks: [platform]
    volumes:
      - garage-meta:/var/lib/garage/meta
      - garage-data:/var/lib/garage/data
    deploy:
      replicas: 2
      placement:
        max_replicas_per_node: 1

  garage-reconciler:
    image: localhost:5000/garage:local
    command: ["/garage-reconciler"]
    networks: [platform]
    environment:
      GARAGE_SERVICE_DNS: tasks.garage
      GARAGE_EXPECTED_NODES: "2"
      GARAGE_ADMIN_TOKEN: ${GARAGE_ADMIN_TOKEN}
      GARAGE_S3_ACCESS_KEY_ID: ${GARAGE_S3_ACCESS_KEY_ID}
      GARAGE_S3_SECRET_KEY: ${GARAGE_S3_SECRET_KEY}
      GARAGE_BUCKET: docker-registry
    deploy:
      replicas: 1
      restart_policy:
        condition: any
```

## What it does

On every reconciliation loop, the binary:

1. resolves `tasks.garage` to get the current Garage task IPs;
2. calls each task's Admin API to discover its Garage node ID;
3. sends `ConnectClusterNodes` so the Garage nodes connect to each other;
4. calls `UpdateClusterLayout` for visible nodes that have no role;
5. if `GARAGE_REPLACE_OFFLINE_NODES=true`, removes role IDs that are no longer visible once the expected number of replacement nodes is visible;
6. applies the next layout version;
7. optionally imports the registry S3 key, creates the registry bucket, and grants key permissions;
8. optionally launches table/block repair after a layout change.

Garage's own documentation says layout changes must be staged with role updates and applied once afterward; this binary follows that model.

## Required environment

| Variable | Default | Description |
|---|---:|---|
| `GARAGE_ADMIN_TOKEN` | required | Bearer token for the Garage Admin API. |
| `GARAGE_SERVICE_DNS` | `tasks.garage` | Swarm DNS name used to resolve individual Garage tasks. |
| `GARAGE_ADMIN_PORT` | `3903` | Garage Admin API port. |
| `GARAGE_RPC_PORT` | `3901` | Garage RPC port used to build peer strings. |
| `GARAGE_EXPECTED_NODES` | `2` | Minimum visible Garage nodes before layout apply/remove. |
| `GARAGE_LAYOUT_CAPACITY_BYTES` | `10000000000` | Capacity assigned to each visible node. |
| `GARAGE_BUCKET` | `docker-registry` | Bucket to create for the registry backend. |
| `GARAGE_S3_ACCESS_KEY_ID` | empty | Access key ID to import/use for the registry. |
| `GARAGE_S3_SECRET_KEY` | empty | Secret key to import/use for the registry. |
| `GARAGE_RECONCILE_INTERVAL` | `30s` | Loop interval. |
| `GARAGE_REPLACE_OFFLINE_NODES` | `true` | Remove layout roles that are no longer visible when enough replacement nodes are visible. |
| `GARAGE_REPAIR_ON_CHANGE` | `true` | Launch Garage table/block repairs after a layout change. |
| `GARAGE_DRY_RUN` | `false` | Log intended changes without applying them. |

## Notes

This project intentionally has no third-party Go dependencies. The release workflow builds static Linux binaries and publishes `.tgz` archives.

The binary uses Garage Admin API v2 endpoints such as `GetClusterStatus`, `ConnectClusterNodes`, `UpdateClusterLayout`, `ApplyClusterLayout`, `CreateBucket`, `ImportKey`, `AllowBucketKey`, and `LaunchRepairOperation`.
