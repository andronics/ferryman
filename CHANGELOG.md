# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

> **This file is maintained automatically by [`release.yml`](.github/workflows/release.yml)
> from conventional commit messages (`feat:`, `fix:`, `refactor:`, `chore:`) at tag time.
> Do not hand-edit entries — the release workflow doesn't check for an existing entry
> before inserting its own, so a manual entry for a version gets duplicated when that
> version is tagged.**

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
