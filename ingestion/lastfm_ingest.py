"""
lastfm_ingest.py
────────────────
Extraction layer for Last.fm API.

Normalizes Last.fm responses to the same raw schema used by spotify_ingest.py
and deezer_ingest.py so Snowflake loader + dbt models can stay unchanged.
"""

import json
import logging
from datetime import date
from pathlib import Path

from ingestion.lastfm_client import LastFMClient

logger = logging.getLogger(__name__)

RAW_DIR = Path("data/raw")
TOP_TRACK_LIMIT = 200
TOP_ARTIST_LIMIT = 150


def _raw_path(entity: str) -> Path:
    today = date.today().isoformat()
    p = RAW_DIR / entity / today
    p.mkdir(parents=True, exist_ok=True)
    return p


def _save_json(data: list[dict], entity: str, filename: str) -> Path:
    path = _raw_path(entity)
    filepath = path / filename
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info("Saved %d records -> %s", len(data), filepath)
    return filepath


def _dedupe(items: list[dict], key: str) -> list[dict]:
    seen = set()
    unique = []
    for item in items:
        value = item.get(key)
        if value and value not in seen:
            seen.add(value)
            unique.append(item)
    return unique


def _safe_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _track_id(track: dict) -> str:
    mbid = (track.get("mbid") or "").strip()
    if mbid:
        return mbid

    artist_name = (track.get("artist", {}).get("name") or "").strip().lower()
    track_name = (track.get("name") or "").strip().lower()
    return f"lfm:{artist_name}:{track_name}"


def extract_tracks_lastfm(client: LastFMClient) -> list[dict]:
    """Extract and normalize Last.fm top tracks."""
    logger.info("=== Last.fm track extraction ===")
    raw_tracks = client.get_top_tracks(limit=TOP_TRACK_LIMIT)
    normalized = []

    for t in raw_tracks:
        artist = t.get("artist", {}) or {}
        normalized.append(
            {
                "track_id": _track_id(t),
                "track_name": t.get("name", ""),
                "duration_ms": _safe_int(t.get("duration")),
                "explicit": False,
                "popularity": _safe_int(t.get("playcount")),
                "track_number": None,
                "preview_url": None,
                "album_id": None,
                "album_name": None,
                "album_type": None,
                "release_date": None,
                "primary_artist_id": (artist.get("mbid") or "").strip() or None,
                "primary_artist_name": artist.get("name"),
                "all_artist_ids": [artist.get("mbid")] if artist.get("mbid") else [],
                "source": "lastfm",
                "ingested_at": date.today().isoformat(),
            }
        )

    unique = _dedupe(normalized, key="track_id")
    logger.info("Tracks: %d raw -> %d unique", len(raw_tracks), len(unique))
    _save_json(unique, "tracks", "tracks.json")
    return unique


def extract_artists_lastfm(client: LastFMClient, tracks: list[dict]) -> list[dict]:
    """Extract and normalize artist records from top artists + track artists."""
    logger.info("=== Last.fm artist extraction ===")
    top_artists = client.get_top_artists(limit=TOP_ARTIST_LIMIT)

    artist_rows = []
    for a in top_artists:
        mbid = (a.get("mbid") or "").strip()
        artist_id = mbid or f"lfm:{(a.get('name') or '').strip().lower()}"
        artist_rows.append(
            {
                "artist_id": artist_id,
                "artist_name": a.get("name", ""),
                "genres": [],
                "popularity": _safe_int(a.get("playcount")),
                "followers": _safe_int(a.get("listeners")),
                "image_url": None,
                "source": "lastfm",
                "ingested_at": date.today().isoformat(),
            }
        )

    # Backfill artists that appear in tracks but not in top artist chart payload.
    for t in tracks:
        artist_name = t.get("primary_artist_name")
        if not artist_name:
            continue
        artist_rows.append(
            {
                "artist_id": t.get("primary_artist_id") or f"lfm:{artist_name.strip().lower()}",
                "artist_name": artist_name,
                "genres": [],
                "popularity": 0,
                "followers": 0,
                "image_url": None,
                "source": "lastfm",
                "ingested_at": date.today().isoformat(),
            }
        )

    unique = _dedupe(artist_rows, key="artist_id")
    logger.info("Artists: %d raw -> %d unique", len(artist_rows), len(unique))
    _save_json(unique, "artists", "artists.json")
    return unique


def extract_audio_features_lastfm(tracks: list[dict]) -> list[dict]:
    """Last.fm has no Spotify-style audio features; keep schema-compatible nulls."""
    logger.info("=== Last.fm audio features (schema-compatible placeholders) ===")
    features = []
    for t in tracks:
        features.append(
            {
                "track_id": t["track_id"],
                "danceability": None,
                "energy": None,
                "loudness": None,
                "speechiness": None,
                "acousticness": None,
                "instrumentalness": None,
                "liveness": None,
                "valence": None,
                "tempo": None,
                "duration_ms": t.get("duration_ms"),
                "time_signature": None,
                "key": None,
                "mode": None,
                "source": "lastfm",
                "ingested_at": date.today().isoformat(),
            }
        )

    _save_json(features, "audio_features", "audio_features.json")
    logger.info("Audio features: %d rows written", len(features))
    return features


def extract_all() -> dict:
    """Master extract function for Last.fm provider."""
    logger.info("Starting Last.fm extraction - %s", date.today().isoformat())
    client = LastFMClient()

    tracks = extract_tracks_lastfm(client)
    artists = extract_artists_lastfm(client, tracks)
    features = extract_audio_features_lastfm(tracks)

    summary = {
        "run_date": date.today().isoformat(),
        "source": "lastfm",
        "tracks": len(tracks),
        "artists": len(artists),
        "audio_features": len(features),
    }
    logger.info("Extraction complete: %s", summary)
    return summary


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    print(extract_all())
