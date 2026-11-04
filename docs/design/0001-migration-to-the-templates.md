# 0001: Migration to the templates

<!-- Add a line for this record to the index in docs/design/README.md. -->

Status: Approved
Date: 2026-11-04
Authors: Stephen Cox, approved by the brain coordinator

## Context

ferryman is moving onto the stacked project templates (template-base, template-python, template-container),
as other services have. Today it is one package and one image with two long-running roles:

- `ferryman agent`, run as root: one rclone per unit (FUSE, bind mounts), cron transfers, key-pool rotation
  and tuning, all on asyncio. Its control API is Starlette on uvicorn over a Unix socket
  (`/run/ferryman/api.sock`), with an optional TCP listener for `/healthz`, `/readyz` and `/metrics`. It
  sends `READY=1`, `WATCHDOG=1` and `STOPPING=1` to systemd itself. The CLI (`status`, `jobs`, `run`,
  `restart`, `rotate`, `tune`, ...) talks to the socket.
- `ferryman coordinator` on main, renamed `ferryman controller` on `next`: Starlette on uvicorn on TCP 8470,
  a token-authenticated API, a web UI and background loops; runs as user `ferryman`, sandboxed.

Metrics are hand-written Prometheus text with `ferryman_*` names. Configuration is Pydantic models over a
YAML file of nested maps (units, pools, transfers, bindings, rules). Releases are manual (a bump script and a
tag push).

The `next` branch is the finished, green 0.3 line: 18 commits ahead of main, all breaking. Agents run from
their slice of the controller's configuration (`/etc/ferryman/agent.yaml` holds only the controller URL,
token file, paths and API settings), so an agent on 0.3 needs a controller. It adds `ferryman migrate` (0.1
and 0.2 configs to `controller.yaml` plus per-host `agent.yaml`), notifications, events, rules that act,
live config apply and the `/v2` API.

In production a single agent runs on nfs-01, from an early 0.1 commit, as `ferryman.service`
(`Type=notify`, `WatchdogSec=120`, `RequiresMountsFor`, root, `ExecStart=/usr/local/bin/ferryman -c
/etc/ferryman/config.yaml agent`), with a 0.1-format config, key files in `/etc/ferryman/keys` and its
cache volume at `/var/cache/ferryman`. No controller runs anywhere.

Where ferryman does not fit the templates:

- Web stack: the templates use FastAPI with one `serve` command on TCP; the agent's socket server and its
  in-loop uvicorn have no template equivalent.
- Background work: the template's `/readyz` watches threads; ferryman's loops are asyncio tasks.
- Metrics: the template has an interpose `Registry` at `/metrics`; ferryman renders its own text.
- Config: interpose settings are flat keys to environment variables; ferryman's file is nested records.
- Paths: the template has one `/etc/<project>/config.yaml` and `/var/lib/<project>`; ferryman 0.3 has two
  config files and two state directories, plus cache, log and runtime directories.
- Image: the Dockerfile is template-owned with no slot for ferryman's pinned, checksummed rclone stage, and
  its entrypoint drops privileges for every command, while the agent must stay root.
- systemd: the template's unit is `Type=exec`, non-root and strictly sandboxed; the agent's cannot be.
- Kubernetes: template-container has no DaemonSet shape; ferryman ships a privileged agent DaemonSet.
- CI and release: the templates bring their own CI, release-please and container cleanup in place of
  ferryman's workflows and bump script.

Nine questions (A to I below) had to be answered before any migration code. Stephen left them to the
brain coordinator's recommendation, which decided them on 2026-11-04.

## Decision

**A. `next` lands first, as 0.3.0.** Merge `next` into main and release it as 0.3.0 through
release-please, brought to ferryman ahead of the templates (see the ruling under Consequences), deploy 0.3
to nfs-01 (decision B), and only then apply the templates on top. The templated release is a separate,
later change.

**B. The controller runs on nfs-01, beside the agent.** A second, sandboxed unit,
`ferryman-controller.service` (as shipped in `contrib/systemd/`: `User=ferryman`,
`StateDirectory=ferryman-controller`, `ExecStart=/usr/local/bin/ferryman controller -c
/etc/ferryman/controller.yaml`), listening on TCP 8470. The agent on the same host reaches it over loopback
(`controller.url` in `/etc/ferryman/agent.yaml`). One host, no new network path, and the shared-filesystem
gateway does not depend on a cluster that itself mounts its export.

**C. Python 3.13 minimum.** `requires-python = ">=3.13"` and the template's `python_version: 3.13`; CI tests
3.13 and 3.14; the image stays on a 3.13 base. Before the deploy, the Python of nfs-01's venv is checked
read-only.

**D. The CLI keeps its commands.** `ferryman agent` and `ferryman controller` stay exactly as they are,
including the root option before the subcommand (`ferryman -c /etc/ferryman/config.yaml agent`). The
template's `serve` is added as an alias of `controller` (the HTTP service). Every other command (`status`,
`jobs`, `run`, `restart`, `rotate`, `tune`, `health`, `verify`, `migrate`, `token`, ...) is kept; the
template's `version` and `config` are added, and `health` takes the template's shape without losing what it
checks today. The systemd `ExecStart` lines, the image's `CMD ["agent"]`, the
Kubernetes args and existing habits keep working.

**E. Deployment shapes.** The templates are applied with `contrib: [docker, systemd]`. The controller takes
the template's systemd unit. Project-owned, outside the template:
- the agent's unit `contrib/systemd/ferryman.service` (root, `Type=notify`, `NotifyAccess=main`,
  `WatchdogSec=120`, `RequiresMountsFor`, `ExecStart=/usr/local/bin/ferryman ... agent`);
- `contrib/k8s/` (the privileged agent DaemonSet and the controller Deployment, Service, PVC,
  NetworkPolicy and namespace), whose render check moves to a `scripts/checks.d/` drop-in.

A template DaemonSet shape and root-agent option are raised with the templates only if a second FUSE or
host-level service appears.

**F. Config and state paths stay as in 0.3.** `/etc/ferryman/agent.yaml`, `/etc/ferryman/controller.yaml`,
`/var/lib/ferryman`, `/var/lib/ferryman-controller`, `/var/cache/ferryman`, `/var/log/ferryman` and
`/run/ferryman`; key files in `/etc/ferryman/keys`. interpose carries only the flat process settings
(listen host and port, log level, paths), keeping the `FERRYMAN_*` environment names (`FERRYMAN_CONFIG`,
`FERRYMAN_CONTROLLER_CONFIG`, `FERRYMAN_HOSTNAME`). The structured file (units, pools, transfers,
bindings, channels, rules) stays on Pydantic, with unknown keys still errors.

**G. The Drive tool stays out.** A separate personal Drive tool, out of scope: it is not folded into
ferryman and is not part of this migration. It may get its own repository later if it is kept.

**H. Release numbering.** release-please (run by the release App) numbers releases by its default from
the seeded version: with 0.3.0 shipped first, the templated release is 0.4.0, not 1.0.0.
`.release-please-manifest.json` is seeded at 0.2.0, and `release.yml` and `scripts/bump-version.sh` are
removed, when release-please comes to ferryman before 0.3.0 (see the ruling under Consequences).

**I. Metric names are kept.** Every `ferryman_*` metric name and label as released in 0.3.0 is kept
(for example `ferryman_unit_ready`, `ferryman_unit_mount_up`, `ferryman_transfer_last_success`,
`ferryman_transfer_runs_total`, `ferryman_pool_keys_available`, `ferryman_info`), so dashboards and alerts
keep working; the port onto interpose is checked against a golden capture of `/metrics`. The template's
`ferryman_build_info` is added beside `ferryman_info`.

## Alternatives considered

- **A2, template main (0.2) first, then rebase `next`:** ports the riskiest code, the Starlette apps, twice.
- **A3, template on top of `next` with no 0.3 release:** puts a config-format jump, a new required
  controller and a framework port into one production change.
- **B2, the controller on the Kubernetes cluster:** makes the shared-filesystem gateway depend on a cluster
  that mounts its export, and adds a network path.
- **B3, skip `next` and stay agent-only:** throws away finished 0.3 work.
- **C, 3.14 minimum:** nothing needs it, and it is not what ferryman or its image use today.
- **D2, clean break to `serve` and `agent` with `-c` after the subcommand:** breaks the production
  `ExecStart` and the image's `CMD` for no gain on a hand-built host.
- **D3, `serve --role agent|controller`:** the same breakage, and a less clear command.
- **E2, all three template shapes, adding a DaemonSet and a root-agent option to the templates:** template
  work for a need only ferryman has today.
- **E3, no template contrib:** loses the template's controller unit and compose file for nothing.
- **F2, one `/etc/ferryman/config.yaml` per role, chosen by the subcommand:** renames files that 0.3 and
  `ferryman migrate` already produce.
- **F3, template paths with a fallback rule:** two places for every file, and a fallback to keep forever.
- **G1, fold the Drive tool into ferryman:** a different job and a different credential class, which would
  then sit on the gateway.
- **G2, its own templated repository now:** not needed until it is kept.
- **H, 1.0.0 after the migration:** the templating is a framework port, not a statement of stability.
- **I, rename to the template's conventions:** breaks every dashboard and alert for no gain.

## Consequences

Rollout, in order:

1. Release 0.3.0 from `next` through release-please.
2. Deploy 0.3 on nfs-01: the controller as `ferryman-controller.service` and the agent moved from its 0.1
   config by `ferryman migrate`. Production needs the controller before the agent can leave 0.1.
3. Apply the templates on an integration branch, with the agent's socket API, probes, `/metrics`, the
   controller's `/v2` and UI, and the CLI checked against golden captures, then a side-by-side check on a
   scratch host (never nfs-01).
4. Release the templated version (0.4.0, decision H) through release-please, and deploy it to nfs-01.

Ruling, 2026-11-04 (the brain coordinator): 0.3.0 is cut by release-please (Tagwarden), brought to
ferryman before the templates, instead of the bump script and a hand-pushed tag, because coordinators and
agents don't push to main or create tags. `next` lands as one `feat!` squash commit.

Each change on nfs-01 (steps 2 and 4) goes ahead only on these conditions:

- it is done or watched by the homelab coordinator;
- a written rollback exists first, and the running commit, the unit files and the config are backed up
  before anything changes;
- no transfer is in flight when the agent restarts;
- afterwards it is verified: the mounts are up, a test read and write work, and the controller and agent are
  healthy (`ferryman status`, all units ready).

Through all of it nfs-01 keeps the unit name `ferryman.service`, the agent's `ExecStart` path,
`Type=notify` and the watchdog, root, `/etc/ferryman/keys`, `/var/cache/ferryman` with its caches, the
mount paths, and the transfer schedules.

Easier: one deployment shape on one host; no command, path or metric name changes for operators; the
templates bring CI, release automation and container cleanup.

Harder: ferryman keeps project-owned pieces beside the templates (the agent's unit, `contrib/k8s/`, the
agent's socket server, the Pydantic config file), and some template gaps (asyncio tasks in `/readyz`, a
Dockerfile stage for the pinned rclone, an entrypoint that keeps the agent root) need template changes or
further ferryman records before the templating lands.

Wiki: the 0.3 pages are published before the templates take over publishing the wiki from `docs/wiki/`.

## Open questions

None. Decided by the brain coordinator on 2026-11-04, on Stephen's behalf.
