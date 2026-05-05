"""
deezer_ingest.py
─────────────────
Extraction layer for Deezer API.

Strategy (ordered — falls back if previous returns 0):
  1. Global chart       ← most reliable, always has data
  2. Genre charts       ← pop / hip-hop / electronic / rock
  3. Curated playlists  ← editorial playlists
  4. Search queries     ← broader terms (can be empty in some regions)

Normalises everything to the SAME schema as spotify_ingest.py so
snowflake_loader.py and dbt models don't care which source ran.
"""

import json
import logging
from datetime import date
from pathlib import Path

from ingestion.deezer_client import (
    search_tracks, get_chart_tracks, get_genre_chart,
    get_playlist_tracks, diagnose_connection, DeezerAPIError
)

logger = logging.getLogger(__name__)

# ── Deezer Genre IDs (from https://api.deezer.com/genre) ─────────────
GENRE_IDS = {
    "pop":         132,
    "hip_hop":     116,
    "electronic":  106,
    "rock":        152,
    "jazz":        129,
    "rnb":         165,
}

# ── Popular Deezer playlist IDs (public, editorial) ───────────────────
PLAYLIST_IDS = [
    1313621735,   # Top Global
    1116190381,   # Hot Right Now
    1282516355,   # New Music Friday
]

# ── Search terms (broader = better for regional availability) ─────────
SEARCH_QUERIES = [
    "top hits 2024",
    "pop music",
    "hip hop",
    "electronic",
    "rock",
]

RAW_DIR = Path("data/raw")
MIN_RECORDS_REQUIRED = 50  # pipeline fails if we get fewer than this


def _raw_path(entity: str) -> Path:
    today = date.today().isoformat()
    p = RAW_DIR / entity / today
    p.mkdir(parents=True, exist_ok=True)
    return p


def _save_json(data: list, entity: str, filename: str) -> Path:
    path = _raw_path(entity)
    filepath = path / filename
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info("Saved %d records → %s", len(data), filepath)
    return filepath


def _dedupe(items: list[dict], key: str) -> list[dict]:
    seen, unique = set(), []
    for item in items:
        v = item.get(key)
        if v and v not in seen:
            seen.add(v)
            unique.append(item)
    return unique


def _normalise_track(t: dict) -> dict | None:
    """
    Flatten a raw Deezer track object into our standard schema.
    Returns None if the track is missing critical fields.
    """
    if not t.get("id") or not t.get("title"):
        return None

    artist = t.get("artist") or {}
    album  = t.get("album")  or {}

    return {
        "track_id":            str(t["id"]),
        "track_name":          t.get("title", ""),
        "duration_ms":         (t.get("duration", 0) or 0) * 1000,  # Deezer gives seconds
        "explicit":            bool(t.get("explicit_lyrics", False)),
        "popularity":          t.get("rank", 0),
        "preview_url":         t.get("preview"),
        "album_id":            str(album.get("id", "")),
        "album_name":          album.get("title", ""),
        "album_type":          "album",
        "release_date":        album.get("release_date"),
        "primary_artist_id":   str(artist.get("id", "")),
        "primary_artist_name": artist.get("name", ""),
        "all_artist_ids":      [str(artist.get("id", ""))],
        "source":              "deezer",
        "ingested_at":         date.today().isoformat(),
    }


def _normalise_artist(a: dict) -> dict | None:
    if not a.get("id") or not a.get("name"):
        return None
    return {
        "artist_id":    str(a["id"]),
        "artist_name":  a.get("name", ""),
        "genres":       [],           # Deezer search result doesn't include genres
        "popularity":   a.get("nb_fan", 0),
        "followers":    a.get("nb_fan", 0),
        "image_url":    a.get("picture_medium") or a.get("picture"),
        "source":       "deezer",
        "ingested_at":  date.today().isoformat(),
    }


def _derive_audio_features(t: dict) -> dict | None:
    """
    Deezer doesn't have audio features like Spotify.
    We derive proxies from available fields so our schema stays consistent.
    dbt staging model handles the NULLs gracefully.
    """
    if not t.get("track_id"):
        return None
    return {
        "track_id":         t["track_id"],
        "danceability":     None,
        "energy":           None,
        "loudness":         None,
        "speechiness":      None,
        "acousticness":     None,
        "instrumentalness": None,
        "liveness":         None,
        "valence":          None,
        "tempo":            None,
        "duration_ms":      t.get("duration_ms"),
        "time_signature":   None,
        "key":              None,
        "mode":             None,
        "bpm":              t.get("bpm"),          # Deezer sometimes provides BPM
        "explicit":         t.get("explicit"),
        "rank":             t.get("popularity"),   # Deezer's popularity proxy
        "source":           "deezer",
        "ingested_at":      date.today().isoformat(),
    }


# ═══════════════════════════════════════════════════════════════════════
# EXTRACTION FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════

def extract_tracks_deezer() -> list[dict]:
    """
    Pull tracks using fallback strategy.
    Raises ValueError if total < MIN_RECORDS_REQUIRED.
    """
    logger.info("=== Deezer track extraction (with fallback strategy) ===")
    raw_tracks: list[dict] = []

    # ── Strategy 1: Global chart (most reliable) ──────────────────────
    logger.info("Strategy 1: Global chart")
    try:
        chart = get_chart_tracks(limit=100)
        raw_tracks.extend(chart)
        logger.info("Chart: +%d tracks (total so far: %d)", len(chart), len(raw_tracks))
    except Exception as e:
        logger.error("Chart failed: %s", e)

    # ── Strategy 2: Genre charts ──────────────────────────────────────
    logger.info("Strategy 2: Genre charts")
    for genre_name, genre_id in GENRE_IDS.items():
        try:
            genre_tracks = get_genre_chart(genre_id, limit=50)
            raw_tracks.extend(genre_tracks)
            logger.info("Genre '%s': +%d tracks", genre_name, len(genre_tracks))
        except Exception as e:
            logger.warning("Genre chart '%s' failed: %s", genre_name, e)

    # ── Strategy 3: Playlists ─────────────────────────────────────────
    logger.info("Strategy 3: Playlists")
    for pid in PLAYLIST_IDS:
        try:
            playlist_tracks = get_playlist_tracks(pid)
            raw_tracks.extend(playlist_tracks)
            logger.info("Playlist %d: +%d tracks", pid, len(playlist_tracks))
        except Exception as e:
            logger.warning("Playlist %d failed: %s", pid, e)

    # ── Strategy 4: Search (optional — may fail in some regions) ──────
    logger.info("Strategy 4: Search queries")
    for query in SEARCH_QUERIES:
        try:
            results = search_tracks(query, limit=50)
            if results:
                raw_tracks.extend(results)
                logger.info("Search '%s': +%d tracks", query, len(results))
            else:
                logger.warning("Search '%s': 0 results (region/API issue)", query)
        except Exception as e:
            logger.warning("Search '%s' failed: %s", query, e)

    # ── Normalise + deduplicate ───────────────────────────────────────
    normalised = [_normalise_track(t) for t in raw_tracks]
    normalised = [t for t in normalised if t]
    unique     = _dedupe(normalised, key="track_id")

    logger.info(
        "Tracks: %d raw → %d normalised → %d unique",
        len(raw_tracks), len(normalised), len(unique)
    )

    # ── GUARDRAIL: fail loudly if too few records ─────────────────────
    if len(unique) < MIN_RECORDS_REQUIRED:
        raise ValueError(
            f"GUARDRAIL FAILED: Only {len(unique)} tracks extracted "
            f"(minimum required: {MIN_RECORDS_REQUIRED}). "
            "Check Deezer connectivity — run: python -m ingestion.deezer_client diagnose"
        )

    _save_json(unique, "tracks", "tracks.json")
    return unique


def extract_artists_deezer(tracks: list[dict]) -> list[dict]:
    """Extract unique artists from normalised track list."""
    logger.info("=== Deezer artist extraction ===")
    seen_ids: set = set()
    artists: list[dict] = []

    for t in tracks:
        aid  = t.get("primary_artist_id")
        name = t.get("primary_artist_name")
        if aid and aid not in seen_ids:
            seen_ids.add(aid)
            artists.append({
                "id":   aid,
                "name": name,
            })

    normalised = [_normalise_artist(a) for a in artists]
    normalised = [a for a in normalised if a]

    logger.info("Artists: %d unique extracted", len(normalised))
    _save_json(normalised, "artists", "artists.json")
    return normalised


def extract_audio_features_deezer(tracks: list[dict]) -> list[dict]:
    """Derive audio feature proxies for all tracks."""
    logger.info("=== Deezer audio features (derived) ===")
    features = [_derive_audio_features(t) for t in tracks]
    features = [f for f in features if f]
    _save_json(features, "audio_features", "audio_features.json")
    logger.info("Audio features: %d records written", len(features))
    return features


def extract_all_deezer() -> dict:
    """
    Master extract function.
    Run diagnose first if you're getting 0 records.
    """
    logger.info("Starting Deezer extraction — %s", date.today().isoformat())

    tracks   = extract_tracks_deezer()
    artists  = extract_artists_deezer(tracks)
    features = extract_audio_features_deezer(tracks)

    summary = {
        "run_date":        date.today().isoformat(),
        "source":          "deezer",
        "tracks":          len(tracks),
        "artists":         len(artists),
        "audio_features":  len(features),
    }
    logger.info("Extraction complete: %s", summary)
    return summary


def extract_all() -> dict:
    """Compatibility wrapper used by orchestration flow."""
    return extract_all_deezer()


# ── Run diagnose if called directly ──────────────────────────────────
if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    )
    if len(sys.argv) > 1 and sys.argv[1] == "diagnose":
        print("\n🔍 Running Deezer connection diagnostics...\n")
        report = diagnose_connection()
        for k, v in report.items():
            print(f"  {k}: {v}")
        print()
        if report.get("chart_ok") and report.get("chart_count", 0) > 0:
            print("Deezer is reachable. Chart works.")
        elif report.get("search_ok") and report.get("search_count", 0) > 0:
            print("Chart failed but search works.")
        else:
            print("Deezer unreachable or returning 0 records.")
            print("   Possible causes:")
            print("   - Network/firewall blocking api.deezer.com")
            print("   - Regional restriction (Deezer not available in your country)")
            print("   - Try: curl https://api.deezer.com/chart/0/tracks?limit=5")
    else:
        result = extract_all_deezer()
        print("\nDone:", result)