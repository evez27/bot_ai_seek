import aiohttp
import logging
import os

logger = logging.getLogger("discord_notifier")

class DiscordNotifier:
    def __init__(self):
        self.webhook_url = os.getenv("DISCORD_WEBHOOK_URL", "")
        self.session = None

    async def _init_session(self):
        if not self.session or self.session.closed:
            self.session = aiohttp.ClientSession()

    async def send(self, message: str):
        if not self.webhook_url:
            return
        await self._init_session()
        try:
            async with self.session.post(self.webhook_url, json={"content": message}) as resp:
                if resp.status >= 300:
                    logger.warning(f"Échec envoi Discord: {resp.status}")
        except Exception as e:
            logger.warning(f"Erreur Discord: {e}")

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

notifier = DiscordNotifier()