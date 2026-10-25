# ferryman

Google Drive for servers: ferryman keeps rclone mounts of your shared drives alive, moves staged files and backups up to them, and rotates a pool of service-account keys when Drive's ~750G/day upload cap hits — switching every running mount to the next key in place, without a remount.

```yaml
roots: {local: /pool/local, remote: /pool/remote}
pools:
  uploads:
    keys_dir: /etc/ferryman/keys
    keys: [sa-01@proj.iam.gserviceaccount.com, sa-02@proj.iam.gserviceaccount.com]
units:
  media-tv: {drive_id: 0AMN8oIdNrZNRUk9PVA, mountpoint: media/tv, pool: uploads}
  backups:  {drive_id: 0ACZjQ8dkr0XEUk9PVA, mountpoint: backups, pool: uploads, mount: false}
transfers:
  media-tv:                          # staged files move up every 4 hours
    unit: media-tv
    schedule: "0 */4 * * *"
  pve-dump:                          # each node mirrors its dumps, 7 rolling copies
    unit: backups
    mode: sync
    source: /var/lib/vz/dump
    dest: proxmox/{hostname}/{weekday}
    schedule: "30 4 * * *"
```

**📖 [Documentation lives on the wiki](https://github.com/andronics/ferryman/wiki)**

---

## What is ferryman?

rclone does the data path well, but running it as infrastructure leaves a lot to you: mounts that wedge and need detaching, a daily upload cap that silently stalls uploads, backups that a careless `sync` can wipe, and several hosts sharing keys without knowing what the others have used. ferryman is a small daemon that takes care of all of that, configured by one YAML file. It is a standalone rewrite of `remote`, a zsh tool that did the same job on a single workstation.

**Key Features:**

- **Supervised mounts:** one rclone process per drive, restarted with backoff. A health check catches wedged FUSE endpoints, not just dead ones, and detaches them ([docs](https://github.com/andronics/ferryman/wiki/Health-and-Supervision))
- **Live key rotation:** on `User rate limit exceeded.` the pool advances once and every mount switches key with `backend set`, keeping its caches ([docs](https://github.com/andronics/ferryman/wiki/Key-Pools-and-Rotation))
- **Move, copy and sync transfers:** these run inside the mount's own process. `sync` has safety rails: an empty source is refused, deletions can be capped, and date-folder mirrors are rejected ([docs](https://github.com/andronics/ferryman/wiki/Transfers))
- **Placeholders:** `{hostname}`, `{env:NAME}`, `{weekday}` and `{now:%Y/%m}` let one config serve every node ([docs](https://github.com/andronics/ferryman/wiki/Placeholders))
- **Coordinator:** a shared key pool for several hosts. A cap hit on one moves them all, and backup destinations that would overlap are refused ([docs](https://github.com/andronics/ferryman/wiki/Coordinator))
- **Observability:** Prometheus metrics, mail and webhook alerts, and a web UI with restart, run and rotate buttons ([metrics](https://github.com/andronics/ferryman/wiki/Metrics), [alerts](https://github.com/andronics/ferryman/wiki/Notifications), [UI](https://github.com/andronics/ferryman/wiki/Web-UI))

**How it works:** `ferryman agent` runs on each host under systemd. It supervises one `rclone mount` (or `rclone rcd`) per unit and drives each over its rc socket. It also schedules transfers as rc jobs, checks health, and serves a control API on a unix socket for the `ferryman` CLI. The optional `ferryman coordinator` sits behind Traefik and leases keys to agents.

---

## Quick Start

### Prerequisites

- Linux with FUSE, and root for the agent
- Python ≥ 3.13 and [uv](https://docs.astral.sh/uv/)
- rclone ≥ 1.75 on `PATH`. Distribution packages are often far older.

### Install

```bash
git clone https://github.com/andronics/ferryman.git && cd ferryman
export UV_PYTHON_INSTALL_DIR=/opt/uv-python   # if uv must download a Python: keep it out of /root
uv venv --python ">=3.13" /opt/ferryman
uv export --frozen --no-dev --no-emit-project -o /tmp/ferryman-requirements.txt
uv pip install --python /opt/ferryman/bin/python -r /tmp/ferryman-requirements.txt .
ln -s /opt/ferryman/bin/ferryman /usr/local/bin/ferryman
```

### Run it

Write `/etc/ferryman/config.yaml` (the example above, with your drive ids and keys), then:

```bash
ferryman verify                                    # offline: config, key files, rclone
install -Dm644 contrib/ferryman.service /etc/systemd/system/ferryman.service
systemctl daemon-reload && systemctl enable --now ferryman

ferryman status                                    # units and transfers
ferryman run media-tv --dry-run                    # what a transfer would do
ferryman pool                                      # keys: current, exhausted, file health
```

**Continue with the [Quick Start guide](https://github.com/andronics/ferryman/wiki/Quick-Start)** for a first run against a local folder, before any Drive key is involved. Or jump to [Examples](https://github.com/andronics/ferryman/wiki/Examples) for complete configs.

---

## Documentation

Everything beyond getting started lives on the **[wiki](https://github.com/andronics/ferryman/wiki)**:

**Core Concepts**
[Units](https://github.com/andronics/ferryman/wiki/Units) · [Transfers](https://github.com/andronics/ferryman/wiki/Transfers) · [Placeholders](https://github.com/andronics/ferryman/wiki/Placeholders) · [Key Pools and Rotation](https://github.com/andronics/ferryman/wiki/Key-Pools-and-Rotation) · [Coordinator](https://github.com/andronics/ferryman/wiki/Coordinator) · [Health and Supervision](https://github.com/andronics/ferryman/wiki/Health-and-Supervision)

**Control & Observability**
[CLI Reference](https://github.com/andronics/ferryman/wiki/CLI-Reference) · [HTTP API Reference](https://github.com/andronics/ferryman/wiki/HTTP-API-Reference) · [Web UI](https://github.com/andronics/ferryman/wiki/Web-UI) · [Notifications](https://github.com/andronics/ferryman/wiki/Notifications) · [Metrics](https://github.com/andronics/ferryman/wiki/Metrics)

**Security**
[Where keys live, socket and token auth, the coordinator's plain-HTTP hop, the web UI's trust model](https://github.com/andronics/ferryman/wiki/Security)

**Configuration & Deployment**
[In-Depth Configuration](https://github.com/andronics/ferryman/wiki/In-Depth-Configuration) · [Deployment](https://github.com/andronics/ferryman/wiki/Deployment) · [Examples](https://github.com/andronics/ferryman/wiki/Examples)

**Help**
[FAQ](https://github.com/andronics/ferryman/wiki/Frequently-Asked-Questions) · [Troubleshooting](https://github.com/andronics/ferryman/wiki/Troubleshooting)

**For Developers**
[Contributing](https://github.com/andronics/ferryman/wiki/Contributing) · [Architecture Internals](https://github.com/andronics/ferryman/wiki/Architecture-Internals) · [Testing Guide](https://github.com/andronics/ferryman/wiki/Testing-Guide)

---

## Requirements

- **Linux** with FUSE 3 (`/dev/fuse`, `fusermount3`)
- **Python** ≥ 3.13
- **rclone** ≥ 1.75
- **Google Cloud service accounts** that are members of your shared drives. More than one is needed to rotate past the daily cap.
- **Optional:** Traefik with forward-auth in front of the coordinator, sendmail for mail alerts, Prometheus

---

## Development

```bash
uv sync
uv run ruff check && uv run mypy && uv run pytest
uv run pytest -m integration   # needs rclone and FUSE
```

---

## Contributing

Contributions are welcome. See the wiki's [Contributing guide](https://github.com/andronics/ferryman/wiki/Contributing) for the workflow: dev setup, code style, testing, and how to report issues.
