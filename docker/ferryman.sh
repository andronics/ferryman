#!/bin/sh
# Entrypoint and `docker exec` wrapper for ferryman.
#
# Installed as /opt/ferryman/bin/ferryman, ahead of the real one in
# /opt/venv/bin on PATH, so container start and `docker exec <c> ferryman ...`
# both come through here.
#
# The agent (and every CLI command) runs as root: FUSE mounts, kernel
# read-only binds and the agent's 0660 root-owned API socket need it.
#
# The coordinator needs no root. Started as root (the image default), it
# makes sure its state directory belongs to PUID:PGID (default 1000:1000,
# the ferryman user), then drops to that user. Started as anyone else
# (`--user`, Kubernetes runAsUser): run as-is, and leave ownership to
# whoever chose that user.
set -e

FERRYMAN_BIN=/opt/venv/bin/ferryman

# The subcommand: the first argument that isn't an option (or its value)
cmd=""
skip=""
for arg in "$@"; do
    if [ -n "$skip" ]; then skip=""; continue; fi
    case "$arg" in
        -c|--config|--socket) skip=1 ;;
        -*) ;;
        *) cmd="$arg"; break ;;
    esac
done

if [ "$cmd" != "coordinator" ] || [ "$(id -u)" != "0" ]; then
    exec "$FERRYMAN_BIN" "$@"
fi

PUID="${PUID:-1000}"
PGID="${PGID:-1000}"
case "$PUID$PGID" in
    *[!0-9]*) echo "ERROR: PUID and PGID must be numeric (got PUID=$PUID PGID=$PGID)" >&2; exit 1 ;;
esac

STATE_DIR="${FERRYMAN_COORDINATOR_STATE:-/var/lib/ferryman-coordinator}"
mkdir -p "$STATE_DIR"
if [ "$(stat -c %u:%g "$STATE_DIR")" != "$PUID:$PGID" ]; then
    echo "INFO: Setting ownership of $STATE_DIR to $PUID:$PGID" >&2
    chown -R "$PUID:$PGID" "$STATE_DIR"
fi
chmod 0700 "$STATE_DIR"

# setpriv doesn't change HOME, which would stay /root -- unreadable after
# the drop. Use the uid's home from /etc/passwd if it has a writable one
# (ferryman, 1000), otherwise /tmp
HOME_DIR="$(getent passwd "$PUID" | cut -d: -f6)"
if [ -n "$HOME_DIR" ] && [ -d "$HOME_DIR" ] && \
        setpriv --reuid="$PUID" --regid="$PGID" --clear-groups -- test -w "$HOME_DIR"; then
    export HOME="$HOME_DIR"
else
    export HOME=/tmp
fi

exec setpriv --reuid="$PUID" --regid="$PGID" --clear-groups -- "$FERRYMAN_BIN" "$@"
