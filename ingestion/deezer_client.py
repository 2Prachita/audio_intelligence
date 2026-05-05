"""
deezer_client.py
─────────────────
Deezer API client — no auth required for public endpoints.
Handles: rate limiting, retries, full response logging for debugging.
"""

import time
import logging
import requests
from typing import Optional

logger = logging.getLogger(__name__)

BASE_URL = "https://api.deezer.com"
MAX_RETRIES = 5
BACKOFF_BASE = 2


class DeezerAPIError(Exception):
    pass


class DeezerEmptyResponseError(Exception):
    pass


def _get(url: str, params: dict = None) -> dict:
    """
    Core GET with retry + FULL response logging.
    Deezer returns HTTP 200 even for errors — we check the JSON body too.
    """
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, params=params, timeout=15)
            logger.debug("GET %s | params=%s | status=%s", url, params, resp.status_code)

            # ── Log raw response for debugging (first 500 chars) ─────
            raw_text = resp.text[:500]
            logger.debug("Raw response: %s", raw_text)

            if resp.status_code == 429:
                wait = BACKOFF_BASE ** attempt
                logger.warning("Rate limited. Waiting %.1fs (attempt %d/%d)", wait, attempt+1, MAX_RETRIES)
                time.sleep(wait)
                continue

            if resp.status_code >= 500:
                wait = BACKOFF_BASE ** attempt
                logger.warning("Server error %s. Retrying in %.1fs", resp.status_code, wait)
                time.sleep(wait)
                continue

            resp.raise_for_status()
            data = resp.json()

            # ── Deezer wraps errors in JSON even on 200 ──────────────
            if "error" in data:
                err = data["error"]
                logger.error(
                    "Deezer API error: code=%s type=%s message=%s",
                    err.get("code"), err.get("type"), err.get("message")
                )
                raise DeezerAPIError(
                    f"Deezer error {err.get('code')}: {err.get('message')}"
                )

            return data

        except requests.exceptions.ConnectionError as e:
            logger.error("Connection error (attempt %d/%d): %s", attempt+1, MAX_RETRIES, e)
            if attempt == MAX_RETRIES - 1:
                raise
            time.sleep(BACKOFF_BASE ** attempt)

        except requests.exceptions.Timeout:
            logger.error("Timeout (attempt %d/%d)", attempt+1, MAX_RETRIES)
            if attempt == MAX_RETRIES - 1:
                raise
            time.sleep(BACKOFF_BASE ** attempt)

    raise DeezerAPIError(f"Max retries exceeded for {url}")


def search_tracks(query: str, limit: int = 50) -> list[dict]:
    """
    Search tracks. Returns flat list of track dicts.
    Logs FULL response structure so you can see exactly what Deezer returns.
    """
    logger.info("Searching Deezer tracks: '%s' (limit=%d)", query, limit)
    url = f"{BASE_URL}/search"
    all_tracks = []
    index = 0

    while True:
        params = {"q": query, "limit": min(limit, 50), "index": index}
        data = _get(url, params=params)

        # ── Log full structure on first call for debugging ────────────
        if index == 0:
            logger.info(
                "Deezer search response keys: %s | total=%s | data_count=%s",
                list(data.keys()),
                data.get("total", "N/A"),
                len(data.get("data", []))
            )

        items = data.get("data", [])

        if not items:
            if index == 0:
                logger.warning(
                    "Query '%s' returned 0 results. "
                    "Full response: %s", query, str(data)[:300]
                )
            break

        all_tracks.extend(items)
        index += len(items)

        # Deezer signals last page when 'next' is absent
        if not data.get("next") or len(items) < limit:
            break

        time.sleep(0.3)  # gentle throttle

    logger.info("Query '%s': %d tracks found", query, len(all_tracks))
    return all_tracks


def get_artist(artist_id: int) -> dict:
    """Fetch single artist by Deezer artist ID."""
    return _get(f"{BASE_URL}/artist/{artist_id}")


def get_track(track_id: int) -> dict:
    """Fetch single track — includes more fields than search result."""
    return _get(f"{BASE_URL}/track/{track_id}")


def get_chart_tracks(limit: int = 100) -> list[dict]:
    """
    Fetch global chart tracks — reliable fallback when search returns 0.
    Chart endpoint never returns empty for Deezer.
    """
    logger.info("Fetching Deezer global chart tracks (limit=%d)", limit)
    data = _get(f"{BASE_URL}/chart/0/tracks", params={"limit": limit})
    items = data.get("data", [])
    logger.info("Chart returned %d tracks", len(items))
    return items


def get_genre_chart(genre_id: int, limit: int = 50) -> list[dict]:
    """Fetch top tracks for a specific genre."""
    logger.info("Fetching genre chart: genre_id=%d", genre_id)
    data = _get(f"{BASE_URL}/chart/{genre_id}/tracks", params={"limit": limit})
    items = data.get("data", [])
    logger.info("Genre %d chart: %d tracks", genre_id, len(items))
    return items


def get_playlist_tracks(playlist_id: int) -> list[dict]:
    """Fetch tracks from a public Deezer playlist."""
    logger.info("Fetching playlist %d", playlist_id)
    data = _get(f"{BASE_URL}/playlist/{playlist_id}/tracks")
    items = data.get("data", [])
    logger.info("Playlist %d: %d tracks", playlist_id, len(items))
    return items


def diagnose_connection() -> dict:
    """
    Call this first to verify Deezer is reachable from your machine.
    Returns a diagnostic dict — log it and share the output when debugging.
    """
    report = {}
    try:
        # Simplest possible call — no params
        data = _get(f"{BASE_URL}/chart/0/tracks", params={"limit": 5})
        report["chart_ok"] = True
        report["chart_count"] = len(data.get("data", []))
        report["sample_track"] = data.get("data", [{}])[0].get("title", "N/A")
    except Exception as e:
        report["chart_ok"] = False
        report["chart_error"] = str(e)

    try:
        data = _get(f"{BASE_URL}/search", params={"q": "eminem", "limit": 5})
        report["search_ok"] = True
        report["search_count"] = len(data.get("data", []))
        report["search_total"] = data.get("total", 0)
        report["response_keys"] = list(data.keys())
    except Exception as e:
        report["search_ok"] = False
        report["search_error"] = str(e)

    return report