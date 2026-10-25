"""ferryman: Google Drive mounts, uploads and key rotation via rclone."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("ferryman")
except PackageNotFoundError:  # running from a source tree
    __version__ = "0+unknown"
