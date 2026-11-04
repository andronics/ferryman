# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

> **This file is maintained by release-please ([`python-release.yml`](.github/workflows/python-release.yml))
> from conventional commit messages (`feat:`, `fix:`, `perf:`, `refactor:`, `docs:`): each release's entry
> is written on its Release PR. Don't hand-edit entries; change the Release PR before merging it.**

## [0.3.0](https://github.com/andronics/ferryman/compare/v0.2.0...v0.3.0) (2026-11-04)


### ⚠ BREAKING CHANGES

* an agent now needs a controller: it runs only from the slice of configuration its controller serves, and `/etc/ferryman/agent.yaml` holds just the controller URL, token file, paths and API settings. The coordinator is renamed the controller, with no aliases. Existing 0.1 and 0.2 configs must be converted with `ferryman migrate` before upgrading.

### ✨ Features

* coordinator probes exhausted keys and returns them early ([d70dd5e](https://github.com/andronics/ferryman/commit/d70dd5e22446d160c0bed67fda260dd7dcfcd1af))
* ferryman 0.3, controller-fed agents ([#32](https://github.com/andronics/ferryman/issues/32)) ([9ce2ec1](https://github.com/andronics/ferryman/commit/9ce2ec1d25d3046d02b08663dfee8fb9b1b41525))
* Kubernetes manifests in contrib/k8s; remove the Talos spike ([1faf229](https://github.com/andronics/ferryman/commit/1faf229cc366d63c761ff0d62e190b719c63e300))


### 🐛 Bug Fixes

* skip keys with bad files; rotating by hand keeps the key's quota ([5bb56a2](https://github.com/andronics/ferryman/commit/5bb56a2d3f81ce52a8f4f0fdd9554ed16e0781eb))


### 📚 Documentation

* **design:** record 0001, migration to the templates ([#29](https://github.com/andronics/ferryman/issues/29)) ([2124d8c](https://github.com/andronics/ferryman/commit/2124d8c82975b741dab10ddd8dd0ce20ceb65d7d))
* update CHANGELOG.md for v0.2.0 [skip ci] ([9b9db6c](https://github.com/andronics/ferryman/commit/9b9db6c5a4fd2f99ea45afb6e6a49ec91d4fc7f3))

## [0.2.0] - 2026-10-26

### ✨ New Features
- feat: sftp host_key; servers can't clash with api.listen; cache size is per unit
- feat: serve a unit over WebDAV, HTTP, FTP, SFTP, S3, NFS, DLNA or restic (mounts/serve phase 3)
- feat: read-only mounts as kernel bind mounts (mounts/serve phase 2)
- feat!: absolute mount paths, several mounts per unit, required sources (mounts/serve phase 1)
- feat: mount read settings in defaults and per unit
- feat: drift alerts, scheduled re-tunes and per-uplink tune locks (tuning phase 3)
- feat: apply tuning profiles to units and transfers (tuning phase 2)
- feat: measure Drive upload throughput with `ferryman tune` (tuning phase 1)

### 🐛 Bug Fixes
- fix: read a bare YAML `off` as vfs_cache_mode "off"


## [0.1.0] - 2026-10-25

### ✨ New Features
- First release: supervised rclone mounts, scheduled transfers, service-account key
  rotation, coordinator and web UI.
