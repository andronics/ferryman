# ferryman

Google Drive for servers: ferryman keeps rclone mounts of your shared drives alive, serves them over WebDAV, FTP and friends, moves staged files and backups up to them, and rotates a pool of service-account keys when Drive's ~750G/day upload cap hits — switching every running mount to the next key in place, without a remount. One controller holds the config for every host; an agent on each host runs its part.

```yaml
# controller.yaml: what each drive is, and how each host runs it
pools:
  uploads:
    keys_dir: /etc/ferryman/keys
    keys: [sa-01@proj.iam.gserviceaccount.com, sa-02@proj.iam.gserviceaccount.com]
    probe: {interval: 3600}          # test exhausted keys hourly; return them early
units:
  media-tv: {drive_id: 0AMN8oIdNrZNRUk9PVA, pool: uploads}
  backups:  {drive_id: 0ACZjQ8dkr0XEUk9PVA, pool: uploads}
agents:
  pve-01:                            # a Proxmox host: mounts, serves, uploads
    token_sha256: 3f1c…
    units:
      media-tv:
        mounts:                      # one process, one cache, any number of paths
          - path: /pool/google/media/tv
          - {path: /srv/emby/tv, read_only: true}
        serve: [{type: webdav, addr: "127.0.0.1:8081"}]
      backups: {}                    # no mounts: transfers only
    transfers:
      pve-dump:                      # 7 rolling copies, one per weekday
        unit: backups
        mode: sync
        source: /var/lib/vz/dump
        dest: proxmox/{hostname}/{weekday}
        schedule: "30 4 * * *"
  emby-01:                           # the same drive, read-only, inside a container
    token_sha256: 9a0e…
    units:
      media-tv: {mounts: [{path: /media/tv, read_only: true}], uid: 1001}
notify:
  channels: {ops: {smtp: {host: smtp.example.org, from: ferryman@example.org, to: [ops@example.org]}}}
  rules:
    - {name: crash-loop, match: {kind: unit.failed}, when: {count: 3, within: 15m}, send: [ops]}
```

**📖 [Documentation lives on the wiki](https://github.com/andronics/ferryman/wiki)**

---

## What is ferryman?

rclone does the data path well, but running it as infrastructure leaves a lot to you: mounts that wedge and need detaching, a daily upload cap that silently stalls uploads, backups that a careless `sync` can wipe, and several hosts sharing keys without knowing what the others have used. ferryman takes care of all of that: a controller holds one YAML config for every host, and an agent on each host runs its part of it. It began as a standalone rewrite of `remote`, a zsh tool that did the same job on a single workstation.

**Key Features:**

- **Supervised mounts:** one rclone process per drive, restarted with backoff, mounted at any number of absolute paths that share one cache, with per-path `allow_other` and kernel read-only binds. A health check catches wedged FUSE endpoints, not just dead ones, and re-mounts a lost path in place ([docs](https://github.com/andronics/ferryman/wiki/Units))
- **Serving:** WebDAV, HTTP, FTP(S), SFTP, S3, NFS, DLNA or restic from the drive's own process, sharing its cache and key; with auth required beyond loopback, and no FUSE needed ([docs](https://github.com/andronics/ferryman/wiki/Serving))
- **Live key rotation:** on `User rate limit exceeded.` the pool advances once and every mount switches key with `backend set`, keeping its caches ([docs](https://github.com/andronics/ferryman/wiki/Key-Pools-and-Rotation))
- **Move, copy and sync transfers:** these run inside the mount's own process. `sync` has safety rails: an empty source is refused, deletions can be capped, and date-folder mirrors are rejected ([docs](https://github.com/andronics/ferryman/wiki/Transfers))
- **Placeholders:** `{hostname}`, `{env:NAME}`, `{weekday}` and `{now:%Y/%m}` let one config serve every node ([docs](https://github.com/andronics/ferryman/wiki/Placeholders))
- **Upload tuning:** `ferryman tune` measures what the host's path to Drive sustains and picks the chunk size and parallel transfers from it, applied live; drift alerts and scheduled re-tunes keep it current ([docs](https://github.com/andronics/ferryman/wiki/Upload-Tuning))
- **One config, every host:** a unit says what a drive is; each host's binding says where and how it runs it (paths, uid, cache, servers). Edit the controller's config and every host applies its part live, restarting only what changed and never a running transfer. An agent keeps its last config and key on disk, so a controller outage doesn't take its mounts down ([docs](https://github.com/andronics/ferryman/wiki/Controller))
- **Shared keys:** one pool for every host; a cap hit on one moves them all, and exhausted keys are probed and returned early. Transfers that would overwrite each other are refused before they run ([docs](https://github.com/andronics/ferryman/wiki/Key-Pools-and-Rotation))
- **Rules that notify and act:** agents report every event to the controller, through outages; rules decide what is worth telling you (three crashes in 15 minutes, a unit down on every host) and send it by SMTP, sendmail or webhook, and can act on it: restart the unit, retry the transfer, rotate the pool, within limits that stop a rule making things worse ([docs](https://github.com/andronics/ferryman/wiki/Notifications))
- **Observability:** Prometheus metrics and a web UI with restart, run and rotate buttons ([metrics](https://github.com/andronics/ferryman/wiki/Metrics), [UI](https://github.com/andronics/ferryman/wiki/Web-UI))

**How it works:** `ferryman controller` holds the config, the keys and the events, and sits behind Traefik. `ferryman agent` runs on each host under systemd with a small `agent.yaml` (the controller's URL and a token): it fetches its slice of the config, supervises one `rclone mount` (or `rclone rcd`) per unit and drives each over its rc socket, where further mount paths and servers join that process and share its connection, cache and key. It schedules transfers as rc jobs, checks health, and serves a control API on a unix socket for the `ferryman` CLI.

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

Write the controller's config, `/etc/ferryman/controller.yaml`, from [`contrib/controller.example.yaml`](contrib/controller.example.yaml), and on each host the agent's own `/etc/ferryman/agent.yaml` from [`contrib/agent.example.yaml`](contrib/agent.example.yaml); both list every setting with its default. Then:

```bash
# the controller (one, anywhere the agents can reach)
install -Dm644 contrib/systemd/ferryman-controller.service /etc/systemd/system/
systemctl daemon-reload && systemctl enable --now ferryman-controller

# each host
ferryman token                                     # a token for agent.yaml; its sha256 for controller.yaml
ferryman verify                                    # this host: its config, the controller, its slice, paths, rclone
install -Dm644 contrib/systemd/ferryman.service /etc/systemd/system/ferryman.service
systemctl daemon-reload && systemctl enable --now ferryman

ferryman status                                    # units, mounts, servers, transfers
ferryman run media-tv --dry-run                    # what a transfer would do
ferryman pool                                      # keys: current, exhausted, file health
```

**Upgrading from 0.2 (or 0.1):** each host's config moves into the controller's. `ferryman migrate pve-01=/etc/ferryman/config.yaml emby-01=emby.yaml --coordinator coordinator.yaml` writes the controller's config, each host's `agent.yaml` and any new tokens, and says what to copy where; the coordinator is now the controller (`ferryman controller`, `/etc/ferryman/controller.yaml`). See [Upgrading](https://github.com/andronics/ferryman/wiki/Deployment#upgrading).

### Or with Docker

The image (`ghcr.io/andronics/ferryman`, amd64 and arm64) bundles a pinned rclone and FUSE. [`contrib/docker/docker-compose.yml`](contrib/docker/docker-compose.yml) runs the controller (as PUID:PGID) and the agent (privileged, on the host network, with your mount paths' parent bound `rshared` at the same path, so the mounts show on the host):

```bash
docker compose up -d                               # the controller and this host's agent; edit the volumes first
docker compose up -d ferryman                      # an agent alone, on another host
docker exec ferryman ferryman status
```

Tags: `latest` and `X.Y.Z`/`X.Y` for releases, `edge` for main, `next` for the next release in progress.

On Kubernetes, [`contrib/k8s`](contrib/k8s) has Kustomize manifests (the agent as a privileged DaemonSet, the controller holding every node's config), tested on Talos: `kubectl apply -k contrib/k8s`. See [Deployment](https://github.com/andronics/ferryman/wiki/Deployment#talos--kubernetes).

**Continue with the [Quick Start guide](https://github.com/andronics/ferryman/wiki/Quick-Start)** for a first run against a local folder, before any Drive key is involved. Or jump to [Examples](https://github.com/andronics/ferryman/wiki/Examples) for complete configs.

---

## Documentation

Everything beyond getting started lives on the **[wiki](https://github.com/andronics/ferryman/wiki)**:

**Core Concepts**
[Units](https://github.com/andronics/ferryman/wiki/Units) · [Serving](https://github.com/andronics/ferryman/wiki/Serving) · [Transfers](https://github.com/andronics/ferryman/wiki/Transfers) · [Placeholders](https://github.com/andronics/ferryman/wiki/Placeholders) · [Key Pools and Rotation](https://github.com/andronics/ferryman/wiki/Key-Pools-and-Rotation) · [Controller](https://github.com/andronics/ferryman/wiki/Controller) · [Health and Supervision](https://github.com/andronics/ferryman/wiki/Health-and-Supervision) · [Upload Tuning](https://github.com/andronics/ferryman/wiki/Upload-Tuning)

**Control & Observability**
[CLI Reference](https://github.com/andronics/ferryman/wiki/CLI-Reference) · [HTTP API Reference](https://github.com/andronics/ferryman/wiki/HTTP-API-Reference) · [Web UI](https://github.com/andronics/ferryman/wiki/Web-UI) · [Notifications](https://github.com/andronics/ferryman/wiki/Notifications) · [Metrics](https://github.com/andronics/ferryman/wiki/Metrics)

**Security**
[Where keys live, socket and token auth, the controller's plain-HTTP hop, the web UI's trust model](https://github.com/andronics/ferryman/wiki/Security)

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
- **Optional:** Traefik with forward-auth in front of the controller, an SMTP server for mail, Prometheus

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
