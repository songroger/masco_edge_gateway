import sqlite3
import threading
import time

from .logutil import logger


class SQLiteStore:
    def __init__(self, config):
        self.db_path = config["path"]
        self.max_records = config.get("max_records", 100000)
        self.max_retry = config.get("max_retry", 50)
        self.lock = threading.Lock()
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.create_tables()

    def create_tables(self):
        with self.lock:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS mqtt_outbox (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    topic TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    retry_count INTEGER DEFAULT 0
                )
                """
            )
            self.conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_outbox_created
                ON mqtt_outbox(created_at)
                """
            )
            self.conn.commit()

    def save(self, topic, payload):
        with self.lock:
            self.conn.execute(
                """
                INSERT INTO mqtt_outbox (topic, payload, created_at)
                VALUES (?, ?, ?)
                """,
                (topic, payload, int(time.time())),
            )
            self.conn.commit()
            self.cleanup()

    def get_batch(self, limit=100):
        with self.lock:
            cursor = self.conn.execute(
                """
                SELECT id, topic, payload, retry_count
                FROM mqtt_outbox
                ORDER BY id ASC
                LIMIT ?
                """,
                (limit,),
            )
            return cursor.fetchall()

    def delete(self, record_id):
        with self.lock:
            self.conn.execute("DELETE FROM mqtt_outbox WHERE id = ?", (record_id,))
            self.conn.commit()

    def increase_retry(self, record_id):
        with self.lock:
            self.conn.execute(
                """
                UPDATE mqtt_outbox
                SET retry_count = retry_count + 1
                WHERE id = ?
                """,
                (record_id,),
            )
            self.conn.commit()

    def count(self):
        with self.lock:
            cursor = self.conn.execute("SELECT COUNT(*) FROM mqtt_outbox")
            return cursor.fetchone()[0]

    def close(self):
        with self.lock:
            try:
                self.conn.close()
            except Exception:
                pass

    def cleanup(self):
        count = self.conn.execute("SELECT COUNT(*) FROM mqtt_outbox").fetchone()[0]
        if count <= self.max_records:
            return
        delete_count = count - self.max_records
        self.conn.execute(
            """
            DELETE FROM mqtt_outbox
            WHERE id IN (
                SELECT id FROM mqtt_outbox ORDER BY id ASC LIMIT ?
            )
            """,
            (delete_count,),
        )
        self.conn.commit()
        logger.warning(
            "SQLite cache exceeded limit, deleted %s old records", delete_count
        )
