# ferryman

Mounts Google Drive shared drives through rclone, moves staged files up to
them, and rotates a pool of service-account keys when Drive's ~750G/day
upload cap hits. It is a standalone rewrite of the zsh `remote` tool from
the dotfiles.

- **Units:** each unit is one rclone process. A mount unit runs `rclone mount`; a transfer-only unit (`mount: false`) runs `rclone rcd`. Each process has its own rc socket. If a process dies, ferryman restarts it with backoff and detaches the dead FUSE endpoint it left behind.
- **Transfers:** a transfer is a `move`, `copy` or `sync` job that runs inside its unit's process, either on a cron schedule or on demand.
- **Pools:** a pool is an ordered list of service-account keys. When Drive reports `User rate limit exceeded.`, the pool advances to the next key. Every running unit on the pool switches with `backend set`, so nothing is remounted.
- **Health check:** it periodically confirms that each unit is mounted, that a time-limited `stat` on the mountpoint returns, and that the rc socket answers. A unit that fails any check is restarted.

## Usage

```sh
ferryman -c config.yaml verify          # offline: config, key files, rclone
ferryman -c config.yaml agent           # the daemon
ferryman status | stats | jobs [pattern]
ferryman sync <transfer> [--dry-run] [--no-wait]
ferryman restart <unit>
ferryman pool | rotate <pool>
ferryman throttle <pattern> <rate|off>
ferryman health
```

The CLI talks to the agent over `<paths.runtime>/api.sock`. To use a different socket, pass `--socket` or set `FERRYMAN_SOCKET`. The config file path can also come from `FERRYMAN_CONFIG`. Add `--json` to get raw output.

## Config

```yaml
paths:            # defaults shown
  runtime: /run/ferryman
  cache: /var/cache/ferryman
  state: /var/lib/ferryman
  logs: /var/log/ferryman
roots:
  local: /pool/local      # staged files, per unit mountpoint
  remote: /pool/remote    # mounts, per unit mountpoint
defaults:
  uid: 1000
  gid: 984
  cache_max_size: 10G
  cache_min_free_space: 100G
  warm: true      # load each mount's dir cache in bulk after it starts
  dir_mode: 0775  # for directories ferryman creates under roots.remote
pools:
  uploads:
    keys_dir: /etc/ferryman/keys     # <client_email>.json per key
    keys: [sa-01@proj.iam.gserviceaccount.com, sa-02@proj.iam.gserviceaccount.com]
units:
  media-tv:
    drive_id: 0AMN8oIdNrZNRUk9PVA
    mountpoint: media/tv
    pool: uploads
    cache_max_size: 150G
transfers:
  media-tv:
    unit: media-tv
    mode: move                       # move | copy | sync
    schedule: "0 */4 * * *"          # omit to run on demand only
```

Directories that ferryman creates under `roots.remote` (a mountpoint and any
missing parents) get the unit's `uid`/`gid` and `defaults.dir_mode`. Write
the mode as YAML octal (`0775`, `02775`), not `0o775`. Existing directories
are left alone.

`warm` runs a recursive `vfs/refresh` once a mount is ready. Without it, the
in-memory dir cache starts empty after every restart and the first walk of
the tree costs one Drive round trip per directory. `ferryman status` shows
each unit's last warm-up: running, done or failed, how long it took and
when it ended. It is retried once on failure.

Cron `schedule`s use the host's local timezone. Moving the agent into a UTC
container shifts every transfer by the host's UTC offset.

## Deployment

The agent runs as root (it mounts FUSE filesystems) under systemd, which it
tells about readiness and shutdown, and pings for its watchdog. To install it
into a venv from the lock file:

```sh
uv venv /opt/ferryman
uv export --frozen --no-dev --no-emit-project -o /tmp/ferryman-requirements.txt
uv pip install --python /opt/ferryman/bin/python -r /tmp/ferryman-requirements.txt .
ln -s /opt/ferryman/bin/ferryman /usr/local/bin/ferryman

install -Dm644 contrib/ferryman.service /etc/systemd/system/ferryman.service
# Set RequiresMountsFor= in the unit to your roots.local and paths.cache.
ferryman -c /etc/ferryman/config.yaml verify
systemctl daemon-reload && systemctl enable --now ferryman
```

`contrib/ferryman.service` has no sandboxing (`ProtectSystem=` and the
like): the agent needs `/dev/fuse`, `CAP_SYS_ADMIN` and write access to the
roots, and a hardened unit has not been tested yet.

## Coordinator

When several hosts upload with the same keys, the daily cap belongs to every
host using a key, so rotation has to be shared. `ferryman coordinator` owns
the keys and each pool's rotation state, and agents lease the current key
from it. It speaks plain HTTP, so put Traefik in front of it to terminate
TLS.

- **Auth:** every agent sends a bearer token. The coordinator config stores only the token's sha256, and each agent may use only the pools listed for it. `ferryman token` generates a token and its hash.
- **Leases:** an agent writes its leased key to a 0600 file under `<paths.runtime>/keys/` (rclone's `backend set` needs a key file) and deletes it on shutdown. It follows the pool's epoch with long polls, so a rotation triggered by one host moves every host's mounts without a remount.
- **Outages:** if the coordinator is down, agents keep their last lease and lose only rotation.
- **Claims:** a transfer on a coordinator pool must hold a claim before it runs. A `sync` mirrors its source and deletes everything else at its destination, so the coordinator refuses any claim whose destination overlaps another host's transfer on the same drive when either side is a `sync`. Moves and copies into the same place are fine. A claim granted earlier keeps working through a coordinator outage.

```yaml
# coordinator.yaml
listen: {host: 127.0.0.1, port: 8470, forwarded_allow_ips: 127.0.0.1}
state: /var/lib/ferryman-coordinator
pools:
  uploads:
    keys_dir: /etc/ferryman/keys
    keys: [sa-01@proj.iam.gserviceaccount.com, sa-02@proj.iam.gserviceaccount.com]
agents:
  pve-node-01: {token_sha256: <from ferryman token>, pools: [uploads]}
```

```yaml
# agent config
coordinator:
  url: https://ferryman.example.org
  token_file: /etc/ferryman/token
pools:
  uploads: {coordinator: true}
```

## Development

```sh
uv sync
uv run ruff check && uv run mypy && uv run pytest
uv run pytest -m integration   # needs rclone and FUSE
```
