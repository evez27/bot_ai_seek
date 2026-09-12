#news_calendar.py
import aiohttp
import json
import logging
from datetime import datetime, timedelta, timezone

logger = logging.getLogger("news_calendar")

# URL du flux hebdomadaire Forex Factory au format JSON
NEWS_JSON_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"

class NewsCalendar:
    def __init__(self):
        self.events = []  # Liste des événements à fort impact
        self.last_update = None
        self.session = None

    async def _init_session(self):
        if not self.session or self.session.closed:
            self.session = aiohttp.ClientSession()

    async def fetch_events(self):
        """Récupère le calendrier et filtre les événements à fort impact."""
        await self._init_session()
        try:
            async with self.session.get(NEWS_JSON_URL) as resp:
                if resp.status != 200:
                    logger.warning(f"⚠️ Échec récupération calendrier: {resp.status}")
                    return
                raw_data = await resp.json()
        except Exception as e:
            logger.warning(f"⚠️ Erreur réseau calendrier: {e}")
            return

        high_impact_events = []
        for event in raw_data:
            # Forex Factory utilise "High" pour l'impact élevé
            if event.get("impact") == "High":
                try:
                    # Le JSON fournit une date et une heure séparées
                    date_str = event.get("date")  # Format: "2026-09-12T12:30:00-04:00" (souvent ISO)
                    if not date_str:
                        continue
                    # Parser la date/heure ISO
                    event_time = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                    # Convertir en UTC pour comparaison
                    event_time_utc = event_time.astimezone(timezone.utc)
                    
                    high_impact_events.append({
                        "title": event.get("title", "Unknown"),
                        "country": event.get("country", ""),
                        "currency": event.get("currency", ""),
                        "time_utc": event_time_utc,
                        "impact": event.get("impact")
                    })
                except Exception as e:
                    logger.debug(f"Erreur parsing event: {e}")
                    continue

        self.events = high_impact_events
        self.last_update = datetime.now(timezone.utc)
        logger.info(f"📅 Calendrier mis à jour: {len(self.events)} événements à fort impact")

    def is_trade_blocked(self, buffer_minutes=15):
        """
        Vérifie si on est dans une fenêtre de blocage autour d'une annonce.
        Retourne (blocked: bool, event_info: dict ou None)
        """
        if not self.events:
            return False, None

        now_utc = datetime.now(timezone.utc)
        window = timedelta(minutes=buffer_minutes)

        for event in self.events:
            event_time = event["time_utc"]
            # Vérifier si l'heure actuelle est dans la fenêtre [T-15min, T+15min]
            if event_time - window <= now_utc <= event_time + window:
                return True, event

        return False, None

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

# Instance globale
news_calendar = NewsCalendar()