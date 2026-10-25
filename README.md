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
ferryman run <transfer> [--dry-run] [--no-wait]   # `sync` still works
ferryman restart <unit>
ferryman pool | rotate <pool>
ferryman throttle <pattern> <rate|off>
ferryman health
ferryman events [--limit N]              # failures, recoveries, rotations
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
  backups:
    drive_id: 0ACZjQ...
    mountpoint: backups              # names the default source under roots.local
    pool: uploads
    mount: false                     # an `rclone rcd`: transfers only, no FUSE
transfers:
  media-tv:
    unit: media-tv
    mode: move                       # move | copy | sync
    schedule: "0 */4 * * *"          # omit to run on demand only
    min_age: 5m                      # skip files still being written
  pve-dump:                          # a backup: mirror a local dir to Drive
    unit: backups                    # a `mount: false` unit
    mode: sync
    source: /var/lib/vz/dump         # absolute, or relative to roots.local
    dest: proxmox/{hostname}/dump
    exclude: ["*.tmp", "*.dat"]
    max_delete: 20
    schedule: "30 3 * * *"
```

Transfer modes:

- **`move`** uploads the source and deletes each file from it once uploaded, removing directories it emptied. It is the staging flow: whatever lands under `roots.local/<mountpoint>` goes up.
- **`copy`** uploads new and changed files and deletes nothing on either side.
- **`sync`** makes the destination a mirror of the source, which includes deleting destination files the source no longer has. Two safety rails apply. A sync from an **empty** source is refused, since it would wipe the destination; set `allow_empty: true` if that is really what you mean. `max_delete` aborts the job after that many deletions. rclone has already deleted those files by the time the job aborts.

Every mode skips a source directory that does not exist (it reports nothing to do). `exclude` takes rclone filter patterns. `min_age` skips files modified more recently than the given age, so a backup still being written is left for the next run.

`{hostname}` in `dest` expands to the agent's hostname, so one config can serve every node and each node's backups land in their own folder. The value comes from the top-level `hostname:` key, else `$FERRYMAN_HOSTNAME`, else the system hostname. In a pod, set the env from the node name.

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

## Notifications

Problems that need a person go out by mail (through the host's `sendmail`), a JSON webhook, or both:

- **Problems:** a unit's process failed or was unhealthy, a transfer failed or was refused a claim, a pool ran out of keys, the coordinator went unreachable, or a warm-up failed twice.
- **Recoveries:** each problem's recovery is sent once the problem clears.
- **Information:** rotations and finished warm-ups are informational.

Events are batched for `batch` seconds and sent as one message, so a network blip that restarts every unit produces one mail, not twenty. A problem that stays open, such as a crash-looping unit, is sent once and then again every `repeat` seconds. Its recovery is sent only if the problem was. `ferryman events` shows the recent history whether or not anything was sent.

```yaml
notify:
  level: warning            # info | warning | error: the least severe to send
  batch: 60
  repeat: 21600
  mail: {to: [ops@example.org], from: ferryman@pve-01.example.org}
  webhook: {url: https://hooks.example.org/ferryman, headers: {Authorization: Bearer ...}}
```

The webhook receives a POST of `{host, title, text, events: [{kind, subject, message, severity, resolves, at}]}`.

## Metrics and probes

`api.listen` opens a TCP port with three endpoints and no control endpoints; control stays on the socket:

- **`/healthz`** answers while the process is alive.
- **`/readyz`** answers 200 once every unit is ready and 503 with the units still waiting before that, which is what a pod's readiness probe wants.
- **`/metrics`** serves Prometheus metrics.

```yaml
api:
  listen: {host: 0.0.0.0, port: 9470}
```

- **Units:** `ferryman_unit_ready`, `_state`, `_restarts_total`, `_warm_success`.
- **The rclone process:** `ferryman_rclone_speed_bytes`, `_bytes`, `_errors`.
- **Transfers:** `ferryman_transfer_runs_total{result}`, `_bytes_total`, `_last_success`, `_last_success_timestamp_seconds` and `_blocked`.
- **Pools:** `ferryman_pool_keys_available` and `_current_key`.
- **Coordinator pools:** `ferryman_coordinator_reachable`.

Alerting on backups, for instance, is `time() - ferryman_transfer_last_success_timestamp_seconds{transfer="pve-dump"} > 26*3600`. That timestamp resets when the agent restarts, so pair the rule with `ferryman_transfer_runs_total`.

The coordinator serves its own `/metrics`: pools, each agent's last report and ready units, and claims.

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
- **Reports and commands:** agents report their status every `report_interval` seconds (15 by default) and collect commands from the web UI with long polls. Set `commands: false` in an agent's `coordinator:` block to make it ignore them.
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

### Web UI

`ui: {enabled: true}` in the coordinator config serves a page at `/ui/`. It shows every agent's units, transfers, warm-ups, events and key pools, refreshing every 5s. Its buttons restart a unit, run a transfer or dry-run it, and rotate a shared pool. A restart or run is queued for the agent, picked up by its next long poll, and its result is shown under Commands.

The UI has no login of its own. Put it behind Traefik's forward-auth (authentik). The coordinator refuses UI requests that lack the auth proxy's user header (`ui.user_header`, by default `X-authentik-username`), and records that user on every command. That header is only as trustworthy as the network path, so nothing but Traefik may reach the coordinator's port.

Route the paths separately in Traefik:

| Paths | Who reaches them | Middleware |
| --- | --- | --- |
| `/ui/` and `/ui/api/` | people | forward-auth |
| `/v1/` | agents, which bring their own tokens | none |
| `/metrics` | Prometheus only | do not route it publicly |

A POST from the page must carry `X-Ferryman-UI: 1`. A cross-site form cannot set that header, which keeps the forward-auth cookie from being used for CSRF.

## Development

```sh
uv sync
uv run ruff check && uv run mypy && uv run pytest
uv run pytest -m integration   # needs rclone and FUSE
```
