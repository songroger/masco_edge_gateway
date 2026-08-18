from .logutil import logger

try:
    import gpiod
    from gpiod.line import Direction, Value
except Exception:  # pragma: no cover - optional on non-Linux hosts
    gpiod = None
    Direction = None
    Value = None


class GpioController:
    def __init__(self, config=None):
        self.config = config or {}
        self.chip_path = self.config.get("chip", "/dev/gpiochip0")
        self.consumer = self.config.get("consumer", "edge-collector")
        self._request = None
        self._owned_lines = set()

    def available(self):
        return gpiod is not None

    def close(self):
        if self._request is not None:
            try:
                self._request.release()
            except Exception:
                pass
            self._request = None
            self._owned_lines = set()

    def _ensure_line(self, line):
        if not self.available():
            logger.error("gpiod is not available on this system")
            return False
        if self._request is not None and line in self._owned_lines:
            return True
        self.close()
        try:
            self._request = gpiod.request_lines(
                self.chip_path,
                consumer=self.consumer,
                config={
                    line: gpiod.LineSettings(
                        direction=Direction.OUTPUT,
                        output_value=Value.INACTIVE,
                    )
                },
            )
            self._owned_lines = {line}
            logger.info("GPIO requested: chip=%s line=%s", self.chip_path, line)
            return True
        except Exception as exc:
            logger.error("GPIO request failed chip=%s line=%s: %s", self.chip_path, line, exc)
            return False

    def set_line(self, line, value):
        if not self._ensure_line(line):
            return False
        try:
            gpio_value = Value.ACTIVE if int(value) else Value.INACTIVE
            if hasattr(self._request, "set_value"):
                self._request.set_value(line, gpio_value)
            else:
                self._request.set_values({line: gpio_value})
            logger.critical("GPIO WRITE: chip=%s line=%s value=%s", self.chip_path, line, value)
            return True
        except Exception as exc:
            logger.error("GPIO write failed line=%s: %s", line, exc)
            return False
