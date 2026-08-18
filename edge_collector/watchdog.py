import os
import socket

from .logutil import logger


def sd_notify(message):
    sock_path = os.environ.get("NOTIFY_SOCKET")
    if not sock_path:
        return False
    try:
        if sock_path.startswith("@"):
            sock_path = "\0" + sock_path[1:]
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        try:
            sock.connect(sock_path)
            sock.sendall(message.encode("utf-8"))
        finally:
            sock.close()
        return True
    except Exception as exc:
        logger.warning("sd_notify failed: %s", exc)
        return False


class SystemdWatchdog:
    def __init__(self, config=None):
        self.config = config or {}
        self.enabled = bool(self.config.get("enable", True))
        self._ready = False

    def ready(self):
        if not self.enabled:
            return
        if sd_notify("READY=1"):
            self._ready = True
            logger.info("systemd notify READY=1")

    def ping(self):
        if not self.enabled:
            return
        sd_notify("WATCHDOG=1")
        if not self._ready:
            self.ready()

    def stopping(self):
        if not self.enabled:
            return
        sd_notify("STOPPING=1")
