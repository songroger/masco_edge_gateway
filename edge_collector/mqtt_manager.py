import json
import queue
import ssl

import paho.mqtt.client as mqtt

from .logutil import logger


class MQTTManager:
    def __init__(self, config, store):
        self.config = config
        self.store = store
        self.connected = False
        self._config_queue = queue.Queue()
        self.client = self._create_client()
        self._configure_auth()
        self._configure_tls()
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message

    def _create_client(self):
        client_id = self.config["client_id"]
        try:
            return mqtt.Client(
                mqtt.CallbackAPIVersion.VERSION2,
                client_id=client_id,
            )
        except AttributeError:
            return mqtt.Client(client_id=client_id)

    def _configure_auth(self):
        username = self.config.get("username")
        if username:
            self.client.username_pw_set(username, self.config.get("password"))

    def _configure_tls(self):
        tls = self.config.get("tls") or {}
        if not tls.get("enable"):
            return
        kwargs = {"tls_version": ssl.PROTOCOL_TLS_CLIENT}
        if tls.get("ca_certs"):
            kwargs["ca_certs"] = tls["ca_certs"]
        if tls.get("certfile"):
            kwargs["certfile"] = tls["certfile"]
        if tls.get("keyfile"):
            kwargs["keyfile"] = tls["keyfile"]
        self.client.tls_set(**kwargs)
        if tls.get("insecure"):
            self.client.tls_insecure_set(True)
        logger.info("MQTT TLS enabled")

    def on_connect(self, client, userdata, flags, reason_code, properties=None):
        rc = getattr(reason_code, "value", reason_code)
        if rc == 0:
            self.connected = True
            logger.info("MQTT connected")
            config_topic = self.config.get("config_topic")
            if config_topic:
                client.subscribe(config_topic, qos=1)
                logger.info("MQTT subscribed config topic: %s", config_topic)
        else:
            self.connected = False
            logger.error("MQTT connect failed: %s", reason_code)

    def on_disconnect(self, client, userdata, *args):
        self.connected = False
        reason_code = args[1] if len(args) >= 2 else (args[0] if args else None)
        logger.warning("MQTT disconnected: %s", reason_code)

    def on_message(self, client, userdata, message):
        config_topic = self.config.get("config_topic")
        if not config_topic or message.topic != config_topic:
            return
        try:
            payload = message.payload.decode("utf-8")
            data = json.loads(payload)
        except Exception as exc:
            logger.error("Invalid remote config payload: %s", exc)
            self._config_queue.put({"__error__": "invalid json: %s" % exc})
            return
        logger.info("Remote config received from %s", message.topic)
        self._config_queue.put(data)

    def poll_config(self):
        try:
            return self._config_queue.get_nowait()
        except queue.Empty:
            return None

    def drain_config_queue(self):
        items = []
        while True:
            item = self.poll_config()
            if item is None:
                break
            items.append(item)
        return items

    def restore_config_queue(self, items):
        for item in items or []:
            self._config_queue.put(item)

    def healthy(self):
        return bool(self.connected)

    def restart(self):
        pending = self.drain_config_queue()
        logger.warning("Restarting MQTT client")
        self.stop()
        self.connected = False
        self.client = self._create_client()
        self._configure_auth()
        self._configure_tls()
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message
        self.restore_config_queue(pending)
        self.start()

    def start(self):
        self.client.reconnect_delay_set(min_delay=1, max_delay=30)
        try:
            self.client.connect_async(
                self.config["host"],
                int(self.config["port"]),
                self.config.get("keepalive", 60),
            )
            self.client.loop_start()
        except Exception as exc:
            logger.error("MQTT start error: %s", exc)

    def stop(self):
        try:
            self.client.loop_stop()
            self.client.disconnect()
        except Exception:
            pass
        self.connected = False

    def publish(self, topic, payload):
        if not self.connected:
            return False
        try:
            info = self.client.publish(topic, payload, qos=1, retain=False)
            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                return False
            return True
        except Exception as exc:
            logger.error("MQTT publish error: %s", exc)
            self.connected = False
            return False

    def send_data(self, topic, payload):
        try:
            if self.publish(topic, payload):
                return True
        except Exception as exc:
            logger.error("MQTT publish exception: %s", exc)
            self.connected = False
        try:
            self.store.save(topic, payload)
            logger.warning("MQTT unavailable, data cached locally")
        except Exception as exc:
            logger.error("SQLite cache failed, dropping in-memory payload: %s", exc)
            return False
        return False

    def publish_alarm(self, event):
        topic = self.config.get("alarm_topic")
        if not topic:
            return False
        payload = json.dumps(event, ensure_ascii=False, separators=(",", ":"))
        return self.send_data(topic, payload)

    def publish_ack(self, ok, message, extra=None):
        topic = self.config.get("config_ack_topic")
        if not topic:
            return
        body = {"ok": ok, "message": message}
        if extra:
            body.update(extra)
        payload = json.dumps(body, ensure_ascii=False, separators=(",", ":"))
        if not self.publish(topic, payload):
            self.store.save(topic, payload)

    @staticmethod
    def signature_from_config(config):
        tls = config.get("tls") or {}
        return (
            config.get("host"),
            config.get("port"),
            config.get("client_id"),
            config.get("username"),
            bool(tls.get("enable")),
            tls.get("ca_certs"),
            tls.get("certfile"),
            tls.get("keyfile"),
            config.get("config_topic"),
        )

    def connection_signature(self):
        return self.signature_from_config(self.config)

    def sync_offline_data(self):
        if not self.connected:
            return
        try:
            batch_size = self.config.get("offline_sync_batch", 100)
            records = self.store.get_batch(batch_size)
        except Exception:
            logger.exception("SQLite outbox read failed")
            return
        if not records:
            return
        logger.info("Syncing %s offline records", len(records))
        for record in records:
            record_id, topic, payload = record[0], record[1], record[2]
            retry_count = record[3] if len(record) > 3 else 0
            if not self.connected:
                break
            try:
                if retry_count >= self.store.max_retry:
                    logger.error(
                        "Dropping outbox record id=%s after %s retries",
                        record_id,
                        retry_count,
                    )
                    self.store.delete(record_id)
                    continue
                if self.publish(topic, payload):
                    self.store.delete(record_id)
                else:
                    self.store.increase_retry(record_id)
                    break
            except Exception:
                logger.exception("Offline sync failed for id=%s", record_id)
                try:
                    self.store.increase_retry(record_id)
                except Exception:
                    pass
                break
