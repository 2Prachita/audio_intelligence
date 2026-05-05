"""
spotify_ingest.py
─────────────────
Extraction layer: pulls from Spotify API and lands raw JSON to disk.

Folder structure (mirrors S3 medallion pattern):
  data/
    raw/
      tracks/YYYY-MM-DD/tracks.json
      artists/YYYY-MM-DD/artists.json
      audio_features/YYYY-MM-DD/audio_features.json
    staging/       ← dbt writes here (Snowflake)
    marts/         ← dbt writes here (Snowflake)

Run directly:  python -m ingestion.spotify_ingest
Or via Prefect: orchestration/pipeline.py calls extract_all()
"""

import json
import logging
from datetime import date
from pathlib import Path
from typing import Any

from ingestion.spotify_client import SpotifyClient

logger = logging.getLogger(__name__)

# ── Search terms — mix of genres to get diverse data ─────────────────
SEARCH_QUERIES = [
    "pop 2024",
    "hip hop 2024",
    "electronic music",
    "indie rock",
    "bollywood hits",
    "jazz classics",
    "k-pop",
    "latin music",
]

# ── Well-known public playlists to supplement search ─────────────────
PLAYLIST_IDS = [
    "37i9dQZEVXbMDoHDwVN2tF",  # Global Top 50
    "37i9dQZEVXbLiRSasKsNU9",  # Global Viral 50
    "37i9dQZF1DXcBWIGoYBM5M",  # Today's Top Hits
]

RAW_DIR = Path("data/raw")


def _raw_path(entity: str) -> Path:
    """Returns date-partitioned path: data/raw/{entity}/YYYY-MM-DD/"""
    today = date.today().isoformat()
    p = RAW_DIR / entity / today
    p.mkdir(parents=True, exist_ok=True)
    return p


def _save_json(data: Any, path: Path, filename: str) -> Path:
    """Write JSON with pretty-print. Returns full file path."""
    filepath = path / filename
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info("Saved %d records → %s", len(data) if isinstance(data, list) else 1, filepath)
    return filepath


def _deduplicate(items: list[dict], key: str) -> list[dict]:
    """Remove duplicate dicts by a unique key field."""
    seen = set()
    unique = []
    for item in items:
        val = item.get(key)
        if val and val not in seen:
            seen.add(val)
            unique.append(item)
    logger.info("Deduplication: %d → %d unique records (key='%s')", len(items), len(unique), key)
    return unique


def _validate_track(track: dict) -> bool:
    """Basic schema validation — filter out malformed API responses."""
    required = ["id", "name", "artists", "album", "duration_ms", "popularity"]
    return all(track.get(f) for f in required)


def extract_tracks(client: SpotifyClient) -> list[dict]:
    """
    Pull tracks from search + playlists, deduplicate, validate.
    Returns clean list of track dicts.
    """
    logger.info("=== Extracting tracks ===")
    all_tracks = []

    # From search queries
    for query in SEARCH_QUERIES:
        try:
            tracks = client.search_tracks(query, limit=50)
            all_tracks.extend(tracks)
        except Exception as e:
            logger.error("Failed to search '%s': %s", query, e)
            continue

    # From playlists
    for pid in PLAYLIST_IDS:
        try:
            tracks = client.get_playlist_tracks(pid)
            all_tracks.extend(tracks)
        except Exception as e:
            logger.error("Failed to fetch playlist %s: %s", pid, e)
            continue

    # Clean up
    valid_tracks = [t for t in all_tracks if _validate_track(t)]
    unique_tracks = _deduplicate(valid_tracks, key="id")
    logger.info("Tracks: %d raw → %d valid → %d unique", len(all_tracks), len(valid_tracks), len(unique_tracks))

    # Flatten: we only keep the fields we need downstream
    flattened = []
    for t in unique_tracks:
        flattened.append({
            "track_id":       t["id"],
            "track_name":     t["name"],
            "duration_ms":    t["duration_ms"],
            "explicit":       t.get("explicit", False),
            "popularity":     t.get("popularity", 0),
            "track_number":   t.get("track_number"),
            "preview_url":    t.get("preview_url"),
            "album_id":       t["album"]["id"],
            "album_name":     t["album"]["name"],
            "album_type":     t["album"]["album_type"],
            "release_date":   t["album"].get("release_date"),
            # Take first artist (primary) — others captured in dim_artists
            "primary_artist_id":   t["artists"][0]["id"] if t.get("artists") else None,
            "primary_artist_name": t["artists"][0]["name"] if t.get("artists") else None,
            "all_artist_ids":      [a["id"] for a in t.get("artists", [])],
            "ingested_at":    date.today().isoformat(),
        })

    path = _raw_path("tracks")
    _save_json(flattened, path, "tracks.json")
    return flattened


def extract_artists(client: SpotifyClient, tracks: list[dict]) -> list[dict]:
    """
    Collect all unique artist IDs from extracted tracks,
    then batch-fetch full artist objects (includes genres + follower count).
    """
    logger.info("=== Extracting artists ===")
    # Collect ALL artist IDs (primary + featured)
    all_ids = set()
    for t in tracks:
        if t.get("primary_artist_id"):
            all_ids.add(t["primary_artist_id"])
        for aid in t.get("all_artist_ids", []):
            all_ids.add(aid)

    artists = client.get_artists_batch(list(all_ids))

    flattened = []
    for a in artists:
        if not a:
            continue
        flattened.append({
            "artist_id":      a["id"],
            "artist_name":    a["name"],
            "genres":         a.get("genres", []),
            "popularity":     a.get("popularity", 0),
            "followers":      a.get("followers", {}).get("total", 0),
            "image_url":      a["images"][0]["url"] if a.get("images") else None,
            "ingested_at":    date.today().isoformat(),
        })

    path = _raw_path("artists")
    _save_json(flattened, path, "artists.json")
    return flattened


def extract_audio_features(client: SpotifyClient, tracks: list[dict]) -> list[dict]:
    """
    Batch-fetch audio features for all extracted track IDs.
    Audio features: tempo, key, energy, danceability, valence, etc.
    These are the ML-relevant numeric features.
    """
    logger.info("=== Extracting audio features ===")
    track_ids = [t["track_id"] for t in tracks]
    features  = client.get_audio_features(track_ids)

    flattened = []
    for f in features:
        if not f:
            continue
        flattened.append({
            "track_id":          f["id"],
            "danceability":      f.get("danceability"),
            "energy":            f.get("energy"),
            "loudness":          f.get("loudness"),
            "speechiness":       f.get("speechiness"),
            "acousticness":      f.get("acousticness"),
            "instrumentalness":  f.get("instrumentalness"),
            "liveness":          f.get("liveness"),
            "valence":           f.get("valence"),
            "tempo":             f.get("tempo"),
            "duration_ms":       f.get("duration_ms"),
            "time_signature":    f.get("time_signature"),
            "key":               f.get("key"),
            "mode":              f.get("mode"),  # 1=major, 0=minor
            "ingested_at":       date.today().isoformat(),
        })

    path = _raw_path("audio_features")
    _save_json(flattened, path, "audio_features.json")
    return flattened


def extract_all() -> dict:
    """
    Master extract function — called by Prefect pipeline.
    Returns summary of records extracted per entity.
    """
    logger.info("Starting full extraction run — %s", date.today().isoformat())
    client = SpotifyClient()

    tracks   = extract_tracks(client)
    artists  = extract_artists(client, tracks)
    features = extract_audio_features(client, tracks)

    summary = {
        "run_date":      date.today().isoformat(),
        "tracks":        len(tracks),
        "artists":       len(artists),
        "audio_features": len(features),
    }
    logger.info("Extraction complete: %s", summary)
    return summary


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    result = extract_all()
    print("\nDone:", result)
