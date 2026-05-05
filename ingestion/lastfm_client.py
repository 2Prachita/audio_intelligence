"""
lastfm_client.py
─────────────────
Last.fm API client.
- 100% free, no OAuth needed — just an API key
- No geo-restrictions (works from India)
- Sign up: https://www.last.fm/api/account/create
- Docs:    https://www.last.fm/api

Data available:
  chart.getTopTracks      → globally trending tracks right now
  chart.getTopArtists     → globally trending artists
  tag.getTopTracks        → top tracks per genre tag
  artist.getTopTracks     → top tracks per artist
  track.getInfo           → full track metadata + play counts
"""

import time
import logging
import requests
import os
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

BASE_URL    = "https://ws.audioscrobbler.com/2.0/"
MAX_RETRIES = 4
BACKOFF     = 2.0


class LastFMError(Exception):
    pass


class LastFMClient:
    def __init__(self):
        self.api_key = os.getenv("LASTFM_API_KEY")
        if not self.api_key:
            raise LastFMError(
                "LASTFM_API_KEY not set in .env\n"
                "Get a free key at: https://www.last.fm/api/account/create"
            )

    def _get(self, method: str, extra_params: dict = None) -> dict:
        """Core GET with retry. Last.fm always returns JSON with format=json."""
        params = {
            "method":  method,
            "api_key": self.api_key,
            "format":  "json",
            **(extra_params or {}),
        }
        for attempt in range(MAX_RETRIES):
            try:
                resp = requests.get(BASE_URL, params=params, timeout=15)
                logger.debug("GET %s | status=%s", method, resp.status_code)

                if resp.status_code == 429:
                    wait = BACKOFF ** attempt
                    logger.warning("Rate limited. Waiting %.1fs", wait)
                    time.sleep(wait)
                    continue

                if resp.status_code >= 500:
                    wait = BACKOFF ** attempt
                    logger.warning("Server error %s. Retrying in %.1fs", resp.status_code, wait)
                    time.sleep(wait)
                    continue

                resp.raise_for_status()
                data = resp.json()

                # Last.fm wraps errors as {"error": <code>, "message": "..."}
                if "error" in data:
                    raise LastFMError(
                        f"Last.fm API error {data['error']}: {data.get('message')}"
                    )

                return data

            except requests.exceptions.RequestException as e:
                logger.error("Request failed (attempt %d/%d): %s", attempt+1, MAX_RETRIES, e)
                if attempt == MAX_RETRIES - 1:
                    raise
                time.sleep(BACKOFF ** attempt)

        raise LastFMError(f"Max retries exceeded for method={method}")

    # ── Public methods ─────────────────────────────────────────────────

    def get_top_tracks(self, limit: int = 100, page: int = 1) -> list[dict]:
        """Global top tracks chart right now."""
        data = self._get("chart.getTopTracks", {"limit": limit, "page": page})
        tracks = data.get("tracks", {}).get("track", [])
        logger.info("chart.getTopTracks page=%d → %d tracks", page, len(tracks))
        return tracks

    def get_top_artists(self, limit: int = 100, page: int = 1) -> list[dict]:
        """Global top artists chart."""
        data = self._get("chart.getTopArtists", {"limit": limit, "page": page})
        return data.get("artists", {}).get("artist", [])

    def get_tag_top_tracks(self, tag: str, limit: int = 50) -> list[dict]:
        """Top tracks for a genre tag (e.g. 'pop', 'hip-hop', 'electronic')."""
        data = self._get("tag.getTopTracks", {"tag": tag, "limit": limit})
        tracks = data.get("tracks", {}).get("track", [])
        logger.info("tag.getTopTracks tag='%s' → %d tracks", tag, len(tracks))
        return tracks

    def get_track_info(self, artist: str, track: str) -> dict:
        """Full track info: duration, play count, listeners, tags."""
        data = self._get("track.getInfo", {"artist": artist, "track": track})
        return data.get("track", {})

    def get_artist_info(self, artist_name: str) -> dict:
        """Full artist info: bio, tags, similar artists, listener count."""
        data = self._get("artist.getInfo", {"artist": artist_name})
        return data.get("artist", {})

    def get_artist_top_tracks(self, artist_name: str, limit: int = 10) -> list[dict]:
        """Top tracks for a specific artist."""
        data = self._get("artist.getTopTracks", {"artist": artist_name, "limit": limit})
        return data.get("toptracks", {}).get("track", [])

    def get_similar_tracks(self, artist: str, track: str, limit: int = 10) -> list[dict]:
        """Tracks similar to a given track — good for recommendation data."""
        data = self._get("track.getSimilar",
                         {"artist": artist, "track": track, "limit": limit})
        return data.get("similartracks", {}).get("track", [])

    def diagnose(self) -> dict:
        """Quick connectivity check. Run this first."""
        report = {}
        try:
            tracks = self.get_top_tracks(limit=5)
            report["chart_ok"]    = True
            report["chart_count"] = len(tracks)
            report["sample"]      = tracks[0].get("name") if tracks else "N/A"
        except Exception as e:
            report["chart_ok"]    = False
            report["chart_error"] = str(e)
        return report