import json
import time

from .collector import Collector
from .config import Config, ConfigError, unwrap_remote_payload
from .gpio_control import GpioController
from .logutil import logger
from .modbus_port import ModbusPort
from .mqtt_manager import MQTTManager
from .rules import RuleEngine
from .store import SQLiteStore
from .watchdog import SystemdWatchdog


class CollectorService:
    def __init__(self, config_path):
        self.config_path = config_path
        self.config = Config(config_path)
        self.running = True
        self.store = None
        self.mqtt = None
        self.ports = {}
        self.gpio = None
        self.rule_engine = None
        self.collector = None
        self.watchdog = None
        self._build()

    def _build(self, reuse_store=None, reuse_mqtt=None):
        self._close_runtime(keep_store=reuse_store is not None, keep_mqtt=reuse_mqtt is not None)

        self.store = reuse_store or SQLiteStore(self.config.database)
        self.ports = {}
        for port_config in self.config.serial_ports:
            timeout = port_config.get("timeout", self.config.collect.get("request_timeout", 2))
            merged = dict(port_config)
            merged["timeout"] = timeout
            port = ModbusPort(merged)
            self.ports[port_config["name"]] = port

        self.gpio = GpioController(self.config.gpio)
        self.rule_engine = RuleEngine(self.config.rules, self.ports, self.gpio)
        self.collector = Collector(self.config, self.ports)
        self.watchdog = SystemdWatchdog(self.config.watchdog)

        if reuse_mqtt is not None:
            reuse_mqtt.config = self.config.mqtt
            reuse_mqtt.store = self.store
            self.mqtt = reuse_mqtt
        else:
            self.mqtt = MQTTManager(self.config.mqtt, self.store)
            self.mqtt.start()

    def _close_runtime(self, keep_store=False, keep_mqtt=False):
        if self.collector:
            self.collector.close()
            self.collector = None
        for port in self.ports.values():
            port.close()
        self.ports = {}
        if self.gpio:
            self.gpio.close()
            self.gpio = None
        if self.mqtt and not keep_mqtt:
            self.mqtt.stop()
            self.mqtt = None
        if self.store and not keep_store:
            self.store.close()
            self.store = None

    def _maybe_apply_remote_config(self):
        payload = self.mqtt.poll_config() if self.mqtt else None
        if payload is None:
            return
        if isinstance(payload, dict) and payload.get("__error__"):
            self.mqtt.publish_ack(False, payload["__error__"], extra={"device_id": self.config.device_id})
            return
        try:
            data = unwrap_remote_payload(payload)
            old_mqtt_sig = MQTTManager.signature_from_config(self.mqtt.config)
            old_db_path = self.config.database.get("path")
            self.config.apply_dict(data)
            keep_store = self.config.database.get("path") == old_db_path
            keep_mqtt = MQTTManager.signature_from_config(self.config.mqtt) == old_mqtt_sig
            self._build(
                reuse_store=self.store if keep_store else None,
                reuse_mqtt=self.mqtt if keep_mqtt else None,
            )
            self.mqtt.publish_ack(
                True,
                "config applied",
                extra={"device_id": self.config.device_id, "ts": int(time.time())},
            )
            logger.info("Remote config applied")
        except (ConfigError, Exception) as exc:
            logger.error("Remote config rejected: %s", exc)
            if self.mqtt:
                self.mqtt.publish_ack(
                    False,
                    str(exc),
                    extra={"device_id": self.config.device_id},
                )

    def run(self):
        collect_interval = self.config.collect.get("interval", 10)
        upload_interval = self.config.collect.get("upload_interval", 60)
        last_upload = 0
        self.watchdog.ready()
        logger.info("Edge collector service started")

        while self.running:
            loop_start = time.time()
            try:
                self._maybe_apply_remote_config()
                collect_interval = self.config.collect.get("interval", 10)
                upload_interval = self.config.collect.get("upload_interval", 60)

                data, values, comm_status = self.collector.collect_once()
                self.rule_engine.process(values, comm_status)

                now = time.time()
                if now - last_upload >= upload_interval:
                    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
                    self.mqtt.send_data(self.config.mqtt["topic"], payload)
                    last_upload = now
                    self.mqtt.sync_offline_data()

                self.watchdog.ping()
            except Exception:
                logger.exception("Collector main loop error")
                self.watchdog.ping()

            elapsed = time.time() - loop_start
            time.sleep(max(0, collect_interval - elapsed))

    def stop(self):
        self.running = False
        if self.watchdog:
            self.watchdog.stopping()
        self._close_runtime()
        logger.info("Edge collector service stopped")
