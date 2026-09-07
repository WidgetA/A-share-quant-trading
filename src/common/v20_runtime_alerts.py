"""Operational Feishu alerts independent of the strategy DB, relay and outbox.

Only the V20 supervisor calls this transport. Strategy decisions retain their
durable outbox. Credentials are never included in exceptions or alert text.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

import httpx


@dataclass(frozen=True)
class V20RuntimeAlerts:
    app_id: str
    app_secret: str = field(repr=False)
    chat_id: str

    async def send(self, message: str, notification_id: str) -> None:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(
                "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
                json={"app_id": self.app_id, "app_secret": self.app_secret},
            )
            response.raise_for_status()
            token_result = response.json()
            token = token_result.get("tenant_access_token")
            if token_result.get("code") != 0 or not isinstance(token, str) or not token:
                raise RuntimeError("V20 operational Feishu token request failed")
            response = await client.post(
                "https://open.feishu.cn/open-apis/im/v1/messages",
                params={"receive_id_type": "chat_id"},
                headers={"Authorization": f"Bearer {token}"},
                json={
                    "receive_id": self.chat_id,
                    "msg_type": "text",
                    "content": json.dumps({"text": message}, ensure_ascii=False),
                    "uuid": notification_id,
                },
            )
            response.raise_for_status()
            receipt = response.json()
            if receipt.get("code") != 0 or not receipt.get("data", {}).get("message_id"):
                raise RuntimeError("V20 operational Feishu message was not acknowledged")
