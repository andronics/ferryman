# meta/media

The media centre's module for `meta`, the one front door to every knowledge centre (the brain's
design record 0019). The `meta` core in andronics/brain finds it through the media coordinator's actor
note, reads its manifest, and runs its verbs: `meta media status`, `meta media queue` and so on.

It is one executable, Python standard library only, independent of the ferryman package. It is
read-only: HTTP GETs, plus qBittorrent's login POST (which only opens a session) and LazyLibrarian's
read commands (sent as a POST body so its key stays out of URLs).

## Verbs

| Verb | Shows | Exits |
|---|---|---|
| `status` | A summing-up line, then one line per app: `ok`/`warn`/`fail`/`unknown`, the app, a detail. *arr apps: reachable, queue count, health warnings. qBittorrent: speeds, active and stalled counts. LazyLibrarian: reachable (its API gives no last-search time) | 0 all ok, 1 anything else, 4 no app answered |
| `list apps` | The app names, one per line, sorted | 0; any other `list` is 2 |
| `show <app>` | `key: value` lines: url host, version, reachable, state, queue, health warnings | 0; 4 unreachable; 5 no such app |
| `queue` | The download queue across qBittorrent and the *arrs: source, progress, state, size, name. Never paths, hashes or tracker URLs | 0; 1 some source failed; 4 none answered |

Every verb takes `--json` (or `META_JSON=1`) and prints one JSON value, and `--help`.
`meta/media --meta-manifest` prints the manifest.

## Config

| Variable | Means |
|---|---|
| `MEDIA_META_<APP>_URL` | The app's base URL, e.g. `MEDIA_META_SONARR_URL=http://sonarr.pvr:8989`. Default `https://<host>.andronics.io` (`qbt` for qBittorrent). `off` skips the app |
| `META_TIMEOUT` | The time budget in seconds (default 20). Apps are asked in parallel, 5 s per request at most |
| `META_JSON` | `1` is the same as `--json` |

Apps: bazarr, lazylibrarian, lidarr, prowlarr, qbittorrent, radarr, readarr, sonarr.

## Credentials

Read at run time with `pass show`, held in memory, sent by header or POST body; never in argv, the
environment, output or logs. A missing entry makes that app `unknown` ("no credential at <path>").

- `services/<app>/api-key` for sonarr, radarr, lidarr, readarr, prowlarr, bazarr, lazylibrarian
- `services/qbittorrent/webui-username` and `services/qbittorrent/webui-password`

## Tests

`tests/meta/test_media.py`: a fake core validates the manifest against record 0019's rules and runs
each verb against a fake HTTP server on 127.0.0.1 with a fake `pass`; no network.
