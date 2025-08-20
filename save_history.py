import os
import time
import json
import sqlite3
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyOAuth

pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

print("🎵 Fetching Spotify Data")
print("=" * 50)

# 0) Environment
load_dotenv()
CLIENT_ID = os.getenv('SPOTIFY_CLIENT_ID')
CLIENT_SECRET = os.getenv('SPOTIFY_CLIENT_SECRET')
REDIRECT_URI = os.getenv('SPOTIFY_REDIRECT_URI')
SCOPE = "user-read-recently-played user-read-private user-top-read"

if not all([CLIENT_ID, CLIENT_SECRET, REDIRECT_URI]):
    raise RuntimeError("Missing SPOTIFY_* environment variables (.env)!")

# 1) Spotify client (headless-friendly)
sp = spotipy.Spotify(
    auth_manager=SpotifyOAuth(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        redirect_uri=REDIRECT_URI,
        scope=SCOPE,
        cache_path=".cache-ec2",        # separate cache on server
        open_browser=False              # don't try to open a browser
    )
)
print("✅ Spotify client initialized")

# 2) SQLite – database
DB_PATH = "spotify.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    # Unique key: played_at + track_id → avoid duplicates
    cur.execute("""
        CREATE TABLE IF NOT EXISTS plays (
            played_at TEXT NOT NULL,
            track_id TEXT NOT NULL,
            track_name TEXT,
            artist_name TEXT,
            album_name TEXT,
            album_type TEXT,
            release_date TEXT,
            duration_ms INTEGER,
            popularity INTEGER,
            spotify_url TEXT,
            cover_url TEXT,
            track_href TEXT,
            PRIMARY KEY (played_at, track_id)
        );
    """)
    conn.commit()
    conn.close()

def ensure_schema():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # plays table – add new columns if missing
    wanted_cols = [
        ("explicit", "INTEGER"),
        ("track_number", "INTEGER"),
        ("disc_number", "INTEGER"),
        ("is_local", "INTEGER"),
        ("isrc", "TEXT"),
        ("available_markets_count", "INTEGER"),
        ("context_type", "TEXT"),
        ("context_uri", "TEXT"),
        ("context_url", "TEXT"),
    ]

    # fetch existing columns
    cur.execute("PRAGMA table_info(plays);")
    existing = {row[1] for row in cur.fetchall()}

    for col, ctype in wanted_cols:
        if col not in existing:
            try:
                cur.execute(f"ALTER TABLE plays ADD COLUMN {col} {ctype};")
            except Exception:
                pass  # if it already exists or due to SQLite quirks — skip

    # mapping table: track_artists (track_id, artist_id, artist_name)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS track_artists (
            track_id   TEXT NOT NULL,
            artist_id  TEXT NOT NULL,
            artist_name TEXT,
            PRIMARY KEY (track_id, artist_id)
        );
    """)

    # artists dim table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS artists (
            artist_id TEXT PRIMARY KEY,
            name TEXT,
            genres TEXT,               -- JSON list string
            followers_total INTEGER,
            popularity INTEGER,
            url TEXT,
            href TEXT,
            image_url TEXT
        );
    """)

    conn.commit()
    conn.close()


# --- Artist enrichment helpers ---
def get_missing_artist_ids():
    """Return artist_ids that appear in track_artists but are missing from the artists dimension table."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT ta.artist_id
        FROM track_artists ta
        LEFT JOIN artists a ON a.artist_id = ta.artist_id
        WHERE a.artist_id IS NULL
    """)
    ids = [row[0] for row in cur.fetchall()]
    conn.close()
    return ids

def chunked(seq, size):
    """Simple chunker: split a sequence into chunks of size N."""
    for i in range(0, len(seq), size):
        yield seq[i:i + size]

def enrich_artists(sp):
    """Populate the artists dimension table for missing artist_ids found in track_artists."""
    ids = get_missing_artist_ids()
    if not ids:
        return 0

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    inserted = 0

    for chunk in chunked(ids, 50):  # Spotify artists() max 50 ID in one go
        try:
            resp = sp.artists(chunk) or {}
            for a in (resp.get("artists") or []):
                if not a:
                    continue
                try:
                    cur.execute("""
                        INSERT OR IGNORE INTO artists
                        (artist_id, name, genres, followers_total, popularity, url, href, image_url)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        a.get("id"),
                        a.get("name"),
                        json.dumps(a.get("genres", [])),
                        (a.get("followers") or {}).get("total"),
                        a.get("popularity"),
                        (a.get("external_urls") or {}).get("spotify"),
                        a.get("href"),
                        (a.get("images") or [{}])[0].get("url")
                    ))
                    if cur.rowcount == 1:
                        inserted += 1
                except Exception:
                    # on per-record error, skip and continue
                    pass
            conn.commit()
        except Exception:
            # on request error, continue with next chunk
            continue

    conn.close()
    return inserted

def save_batch(rows):
    if not rows:
        return 0

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # plays insert
    cur.executemany("""
        INSERT OR IGNORE INTO plays
        (played_at, track_id, track_name, artist_name, album_name, album_type,
         release_date, duration_ms, popularity, spotify_url, cover_url, track_href,
         explicit, track_number, disc_number, is_local, isrc, available_markets_count,
         context_type, context_uri, context_url)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """, [
        (
            r["played_at"], r["track_id"], r["track_name"], r["artist_name"],
            r["album_name"], r["album_type"], r["release_date"], r["duration_ms"],
            r["popularity"], r["spotify_url"], r["cover_url"], r["track_href"],
            r["explicit"], r["track_number"], r["disc_number"], r["is_local"],
            r["isrc"], r["available_markets_count"],
            r["context_type"], r["context_uri"], r["context_url"]
        )
        for r in rows
    ])
    inserted_plays = cur.rowcount

    # track_artists mapping insert
    mapping_rows = []
    for r in rows:
        for aid, aname in zip(r.get("artist_ids") or [], r.get("artist_names_list") or []):
            mapping_rows.append((r["track_id"], aid, aname))

    if mapping_rows:
        cur.executemany("""
            INSERT OR IGNORE INTO track_artists (track_id, artist_id, artist_name)
            VALUES (?, ?, ?);
        """, mapping_rows)

    conn.commit()
    conn.close()
    return inserted_plays

def to_ms(ts_iso: str) -> int:
    # '2025-07-27T11:13:43.874Z' -> epoch ms
    return int(datetime.strptime(ts_iso, "%Y-%m-%dT%H:%M:%S.%fZ").timestamp() * 1000)

def pick_cover_300(images):
    if not images:
        return None
    for img in images:
        if img.get("height") == 300:
            return img.get("url")
    # if no 300px image, fall back to the first
    return images[0].get("url")

def fetch_recent_paginated(sp, limit=50, max_loops=10):
    """ Fetch recently played tracks in pages (approx last 24h per run). """
    all_items = []
    before = int(time.time() * 1000)  # now (ms)

    for _ in range(max_loops):
        res = sp.current_user_recently_played(limit=limit, before=before)
        items = res.get("items", [])
        if not items:
            break
        all_items.extend(items)
        # set 'before' to the oldest play timestamp minus 1ms
        oldest = items[-1]["played_at"]
        before = to_ms(oldest) - 1

    return all_items

def normalize_items(items):
    rows = []
    for it in items:
        tr = it["track"]
        al = tr["album"]
        artists = tr.get("artists", [])
        ctx = it.get("context") or {}

        rows.append({
            # keys for plays table
            "played_at":  it["played_at"],
            "track_id":   tr["id"],
            "track_name": tr["name"],
            "artist_name": ", ".join(a["name"] for a in artists) if artists else None,
            "album_name": al.get("name"),
            "album_type": al.get("album_type"),
            "release_date": al.get("release_date"),
            "duration_ms": tr.get("duration_ms"),
            "popularity": tr.get("popularity"),
            "spotify_url": tr.get("external_urls", {}).get("spotify"),
            "cover_url": pick_cover_300(al.get("images")),
            "track_href": tr.get("href"),

            # extended fields
            "explicit": 1 if tr.get("explicit") else 0,
            "track_number": tr.get("track_number"),
            "disc_number": tr.get("disc_number"),
            "is_local": 1 if tr.get("is_local") else 0,
            "isrc": tr.get("external_ids", {}).get("isrc"),
            "available_markets_count": len(tr.get("available_markets") or []),

            # context (may be None)
            "context_type": ctx.get("type"),
            "context_uri": ctx.get("uri"),
            "context_url": (ctx.get("external_urls") or {}).get("spotify"),

            # artists for mapping table
            "artist_ids": [a.get("id") for a in artists if a.get("id")],
            "artist_names_list": [a.get("name") for a in artists if a.get("name")],
        })
    return rows

# ---- run ----
init_db()
ensure_schema()

print("\n📥 Fetching (paginated)...")
items = fetch_recent_paginated(sp, limit=50, max_loops=10)
print(f"🔁 Retrieved items from API: {len(items)}")

rows = normalize_items(items)
inserted = save_batch(rows)
print(f"💾 Inserted into DB (new): {inserted} rows")
new_artists = enrich_artists(sp)
print(f"👤 Inserted into artists dim (new): {new_artists} rows")

# Optional: quick validation summary
if rows:
    df = pd.DataFrame(rows)
    print("\n📊 Quick summary of the current fetch:")
    print(f" - Most recent play at: {df['played_at'].max()}")
    print(f" - Oldest play in this batch: {df['played_at'].min()}")
    print(f" - Unique tracks: {df['track_id'].nunique()}")
    print(f" - Unique artists: {df['artist_name'].nunique()}")
    print("\n - Last 10 plays (chronological):")
    last_10 = df.sort_values('played_at').tail(10)
    for _, row in last_10.iterrows():
        print(f"   * {row['track_name']} - {row['artist_name']}")

print("\n🎉 Done. The full history is being accumulated in spotify.db -> plays table.")