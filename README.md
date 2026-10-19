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

## Development

```sh
uv sync
uv run ruff check && uv run mypy && uv run pytest
uv run pytest -m integration   # needs rclone and FUSE
```
