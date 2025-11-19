from __future__ import annotations

import json
import logging
import os
import time
import urllib.error
import urllib.request
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 5  # seconds
MAX_RETRIES = 3
RETRY_SLEEP = 1.0


def _post_json(url: str, payload: dict[str, Any]) -> tuple[int, str]:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Content-Type": "application/json",
            "User-Agent": "tiki-data-pipeline/monitoring",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=DEFAULT_TIMEOUT) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
            return resp.getcode(), body
    except urllib.error.HTTPError as e:  # pragma: no cover - passthrough
        body = e.read().decode("utf-8", errors="ignore") if e.fp else str(e)
        return e.code, body
    except Exception as e:  # pragma: no cover - network issues
        return 0, str(e)


class DiscordNotifier:
    """Simple Discord Webhook notifier with convenience params.

    Accepts either direct content or (title, message, color) which will be
    transformed into an embed to match existing DAG usage.
    """

    def __init__(self, webhook_url: str | None = None) -> None:
        if webhook_url is None:
            webhook_url = os.getenv("DISCORD_MONITORING_WEBHOOK_URL") or os.getenv(
                "DISCORD_WEBHOOK_URL"
            )
        self.webhook_url = webhook_url

    def send_alert(
        self,
        content: str | None = None,
        *,
        title: str | None = None,
        message: str | None = None,
        color: int | None = None,
        embeds: list[dict[str, Any]] | None = None,
        username: str = "Tiki Monitor",
    ) -> bool:
        """Send an alert to Discord.

        Parameters:
            content: Plain message content (optional if title/message provided).
            title/message/color: Convenience params; if provided will build an embed.
            embeds: Custom embeds override convenience embed creation if present.
        """
        if not self.webhook_url:
            logger.warning("DiscordNotifier: Missing webhook URL; skip send.")
            return False

        # Auto-build embed if title/message provided and no embeds passed.
        if embeds is None and (title or message):
            built = self.build_embed(
                title=title or "Alert",
                description=message or (content or "No details"),
                color=color or 0x3498DB,
            )
            embeds = [built]
            # If content not explicitly set, use title as content for mention.
            if content is None:
                content = f"{title or 'Alert'}"

        # Fallback content if still None.
        if content is None:
            content = "(no content)"

        payload: dict[str, Any] = {"content": content, "username": username}
        if embeds:
            payload["embeds"] = embeds

        for attempt in range(1, MAX_RETRIES + 1):
            status, body = _post_json(self.webhook_url, payload)
            if 200 <= status < 300:
                logger.info(
                    "DiscordNotifier: Alert sent successfully (status=%s, attempt=%s)",
                    status,
                    attempt,
                )
                return True
            logger.warning(
                "DiscordNotifier: Failed to send (status=%s, attempt=%s, body=%s)",
                status,
                attempt,
                body[:300],
            )
            time.sleep(RETRY_SLEEP)
        logger.error("DiscordNotifier: Exhausted retries sending alert.")
        return False

    @staticmethod
    def build_embed(title: str, description: str, color: int = 0x3498DB) -> dict[str, Any]:
        return {
            "title": title,
            "description": description,
            "color": color,
        }
