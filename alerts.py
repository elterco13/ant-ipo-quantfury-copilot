"""Low-latency alert dispatcher (Telegram, Webhook/WhatsApp/n8n) and SQLite log."""

from datetime import datetime, timezone
import json
import sqlite3
import time
from typing import Any

import httpx

from config import DB_PATH


class AlertManager:
    """Manages alert deduplication, SQLite persistence, and Telegram/Webhook push delivery."""

    def __init__(self, db_path: Any = DB_PATH) -> None:
        self.db_path = str(db_path)
        self._last_sent: dict[str, float] = {}
        self.config: dict[str, Any] = {
            "telegram_enabled": False,
            "telegram_bot_token": "",
            "telegram_chat_id": "",
            "webhook_enabled": False,
            "webhook_url": "",
            "audio_enabled": True,
        }
        self._init_db()
        self._load_config()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS alerts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    category TEXT NOT NULL,
                    title TEXT NOT NULL,
                    message TEXT NOT NULL,
                    payload_json TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )

    def _load_config(self) -> None:
        with self._connect() as conn:
            row = conn.execute("SELECT value FROM settings WHERE key = 'alert_config'").fetchone()
            if row:
                try:
                    loaded = json.loads(row["value"])
                    self.config.update(loaded)
                except json.JSONDecodeError:
                    pass

    def update_config(self, updates: dict[str, Any]) -> dict[str, Any]:
        """Update and persist notification channel settings."""
        for key in self.config:
            if key in updates and updates[key] is not None:
                self.config[key] = updates[key]
        with self._connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES ('alert_config', ?)",
                (json.dumps(self.config),),
            )
        return self.get_public_config()

    def get_public_config(self) -> dict[str, Any]:
        """Return config with masked Telegram token for safe UI display."""
        token = self.config.get("telegram_bot_token", "")
        masked = f"{token[:6]}...{token[-4:]}" if len(token) > 12 else ("***" if token else "")
        return {
            "telegram_enabled": bool(self.config.get("telegram_enabled")),
            "telegram_bot_token_masked": masked,
            "telegram_has_token": bool(token),
            "telegram_chat_id": self.config.get("telegram_chat_id", ""),
            "webhook_enabled": bool(self.config.get("webhook_enabled")),
            "webhook_url": self.config.get("webhook_url", ""),
            "audio_enabled": bool(self.config.get("audio_enabled", True)),
        }

    def list_alerts(self, limit: int = 40) -> list[dict[str, Any]]:
        """Retrieve recent alerts from SQLite."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT id, created_at, severity, category, title, message, payload_json "
                "FROM alerts ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        result: list[dict[str, Any]] = []
        for r in rows:
            payload = {}
            if r["payload_json"]:
                try:
                    payload = json.loads(r["payload_json"])
                except json.JSONDecodeError:
                    payload = {}
            result.append(
                {
                    "id": r["id"],
                    "created_at": r["created_at"],
                    "severity": r["severity"],
                    "category": r["category"],
                    "title": r["title"],
                    "message": r["message"],
                    "payload": payload,
                }
            )
        return result

    async def emit(
        self,
        severity: str,
        category: str,
        title: str,
        message: str,
        dedup_key: str | None = None,
        cooldown_seconds: float = 60.0,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """Record an alert and push to configured external channels if not in cooldown."""
        now_mono = time.monotonic()
        if dedup_key:
            last = self._last_sent.get(dedup_key)
            if last is not None and (now_mono - last) < cooldown_seconds:
                return None
            self._last_sent[dedup_key] = now_mono

        now_iso = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            cur = conn.execute(
                "INSERT INTO alerts (created_at, severity, category, title, message, payload_json) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (now_iso, severity, category, title, message, json.dumps(payload or {})),
            )
            alert_id = cur.lastrowid

        alert_record = {
            "id": alert_id,
            "created_at": now_iso,
            "severity": severity,
            "category": category,
            "title": title,
            "message": message,
            "payload": payload or {},
            "delivery": await self._dispatch_external(severity, category, title, message, payload),
        }
        return alert_record

    async def _dispatch_external(
        self,
        severity: str,
        category: str,
        title: str,
        message: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, str]:
        """Deliver alert via Telegram Bot API and/or generic webhook."""
        status: dict[str, str] = {}
        text = f"[{severity}] {title}\nCategoria: {category}\n{message}"

        if self.config.get("telegram_enabled") and self.config.get("telegram_bot_token") and self.config.get("telegram_chat_id"):
            tg_url = f"https://api.telegram.org/bot{self.config['telegram_bot_token']}/sendMessage"
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    resp = await client.post(
                        tg_url,
                        json={"chat_id": self.config["telegram_chat_id"], "text": text},
                    )
                    status["telegram"] = "SENT" if resp.status_code == 200 else f"HTTP_{resp.status_code}"
            except Exception as exc:
                status["telegram"] = f"ERROR: {exc}"

        if self.config.get("webhook_enabled") and self.config.get("webhook_url"):
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    resp = await client.post(
                        self.config["webhook_url"],
                        json={
                            "severity": severity,
                            "category": category,
                            "title": title,
                            "message": message,
                            "payload": payload or {},
                            "text": text,
                        },
                    )
                    status["webhook"] = "SENT" if 200 <= resp.status_code < 300 else f"HTTP_{resp.status_code}"
            except Exception as exc:
                status["webhook"] = f"ERROR: {exc}"

        return status

