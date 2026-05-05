"""
spotify_client.py
─────────────────
Production-grade Spotify API client.
Handles: OAuth2 token refresh, rate limiting, pagination, retries.
"""

import os
import time
import logging
import requests
from typing import Optional
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


class SpotifyAuthError(Exception):
    pass


class SpotifyRateLimitError(Exception):
    pass


class SpotifyClient:
    """
    Wraps the Spotify Web API.
    Automatically handles:
      - Client Credentials OAuth2 flow
      - Token expiry + refresh
      - 429 rate limit back-off (Retry-After header)
      - Exponential back-off on 5xx errors
      - Pagination via next-URL following
    """

    BASE_URL = "https://api.spotify.com/v1"
    AUTH_URL = "https://accounts.spotify.com/api/token"
    MAX_RETRIES = 5
    BACKOFF_BASE = 1.5  # seconds

    def __init__(self):
        self.client_id     = os.getenv("SPOTIFY_CLIENT_ID")
        self.client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")
        if not self.client_id or not self.client_secret:
            raise SpotifyAuthError(
                "SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET must be set in .env"
            )
        self._token: Optional[str] = None
        self._token_expiry: Optional[datetime] = None

    # ── Auth ──────────────────────────────────────────────────────────

    def _get_token(self) -> str:
        """Fetch or refresh the bearer token."""
        if self._token and datetime.utcnow() < self._token_expiry:
            return self._token

        logger.info("Fetching new Spotify access token")
        resp = requests.post(
            self.AUTH_URL,
            data={"grant_type": "client_credentials"},
            auth=(self.client_id, self.client_secret),
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        self._token = data["access_token"]
        # Subtract 30s buffer so we refresh before actual expiry
        self._token_expiry = datetime.utcnow() + timedelta(seconds=data["expires_in"] - 30)
        logger.info("Token acquired, expires at %s", self._token_expiry)
        return self._token

    def _headers(self) -> dict:
        return {"Authorization": f"Bearer {self._get_token()}"}

    # ── Core request with retry ───────────────────────────────────────

    def _get(self, url: str, params: dict = None) -> dict:
        """
        GET with retry logic:
          - 429 → wait Retry-After seconds
          - 5xx → exponential back-off
          - 401 → force token refresh once, then re-raise
        """
        token_refreshed = False
        for attempt in range(self.MAX_RETRIES):
            resp = requests.get(url, headers=self._headers(),
                                params=params, timeout=15)

            if resp.status_code == 200:
                return resp.json()

            elif resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 5))
                logger.warning("Rate limited. Waiting %ss (attempt %d/%d)",
                               retry_after, attempt + 1, self.MAX_RETRIES)
                time.sleep(retry_after)

            elif resp.status_code == 401 and not token_refreshed:
                logger.warning("Token expired mid-flight, refreshing...")
                self._token = None
                token_refreshed = True

            elif resp.status_code >= 500:
                wait = self.BACKOFF_BASE ** attempt
                logger.warning("Server error %s, retrying in %.1fs", resp.status_code, wait)
                time.sleep(wait)

            else:
                resp.raise_for_status()

        raise SpotifyRateLimitError(f"Max retries ({self.MAX_RETRIES}) exceeded for {url}")

    # ── Pagination helper ─────────────────────────────────────────────

    def _paginate(self, url: str, params: dict = None, max_items: int = 500) -> list:
        """Follow Spotify's next-URL pagination, collecting all items."""
        items = []
        while url and len(items) < max_items:
            data = self._get(url, params)
            # Spotify wraps results differently per endpoint
            if "items" in data:
                batch = data["items"]
                url = data.get("next")
            else:
                # Some endpoints wrap in a sub-key (e.g. tracks.items)
                break
            items.extend([i for i in batch if i])  # filter None items
            params = None  # params only on first request; next URL has them baked in
            logger.debug("Paginated: %d items collected so far", len(items))

        return items[:max_items]

    # ── Public API methods ────────────────────────────────────────────

    def search_tracks(self, query: str, limit: int = 50) -> list[dict]:
        """
        Search for tracks. Returns a flat list of track objects.
        `limit` per page is max 50 (Spotify enforces this).
        """
        logger.info("Searching tracks: '%s'", query)
        url = f"{self.BASE_URL}/search"
        all_tracks = []
        offset = 0
        while offset < 200:  # cap at 200 per search term
            data = self._get(url, params={
                "q": query, "type": "track",
                "limit": min(limit, 50), "offset": offset
            })
            tracks = data.get("tracks", {})
            batch = [t for t in tracks.get("items", []) if t]
            if not batch:
                break
            all_tracks.extend(batch)
            offset += len(batch)
            if not tracks.get("next"):
                break
        logger.info("Found %d tracks for query '%s'", len(all_tracks), query)
        return all_tracks

    def get_audio_features(self, track_ids: list[str]) -> list[dict]:
        """
        Fetch audio features in batches of 100 (Spotify's batch limit).
        Returns list of feature dicts.
        """
        all_features = []
        for i in range(0, len(track_ids), 100):
            batch = track_ids[i:i + 100]
            logger.info("Fetching audio features: batch %d-%d", i, i + len(batch))
            data = self._get(
                f"{self.BASE_URL}/audio-features",
                params={"ids": ",".join(batch)}
            )
            features = [f for f in data.get("audio_features", []) if f]
            all_features.extend(features)
            time.sleep(0.2)  # gentle throttle between batch calls
        return all_features

    def get_artist(self, artist_id: str) -> dict:
        """Fetch full artist object including genres and popularity."""
        return self._get(f"{self.BASE_URL}/artists/{artist_id}")

    def get_artists_batch(self, artist_ids: list[str]) -> list[dict]:
        """Batch-fetch up to 50 artists per call."""
        all_artists = []
        unique_ids = list(set(artist_ids))
        for i in range(0, len(unique_ids), 50):
            batch = unique_ids[i:i + 50]
            data = self._get(
                f"{self.BASE_URL}/artists",
                params={"ids": ",".join(batch)}
            )
            all_artists.extend(data.get("artists", []))
            time.sleep(0.2)
        logger.info("Fetched %d artists", len(all_artists))
        return all_artists

    def get_new_releases(self, country: str = "US", limit: int = 50) -> list[dict]:
        """Fetch new album releases, then extract their tracks."""
        data = self._get(
            f"{self.BASE_URL}/browse/new-releases",
            params={"country": country, "limit": limit}
        )
        albums = data.get("albums", {}).get("items", [])
        logger.info("Fetched %d new release albums", len(albums))
        return albums

    def get_playlist_tracks(self, playlist_id: str) -> list[dict]:
        """Fetch all tracks from a public playlist (paginated)."""
        url = f"{self.BASE_URL}/playlists/{playlist_id}/tracks"
        raw = self._paginate(url, params={"limit": 100}, max_items=500)
        # Unwrap track objects (playlist items wrap actual track in .track key)
        return [item["track"] for item in raw if item and item.get("track")]
