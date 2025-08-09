import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime

print("🎵 Spotify Adatok Lekérdezése")
print("=" * 50)

# ===============================================
# 1. KAPCSOLAT LÉTREHOZÁSA
# ===============================================

load_dotenv()
CLIENT_ID = os.getenv('SPOTIFY_CLIENT_ID')
CLIENT_SECRET = os.getenv('SPOTIFY_CLIENT_SECRET')
REDIRECT_URI = os.getenv('SPOTIFY_REDIRECT_URI')

# Scope meghatározása, hogy milyen adatokat kérünk le (https://developer.spotify.com/documentation/web-api/concepts/scopes)
SCOPE = "user-read-recently-played user-read-private user-top-read"

# Spotify kliens létrehozása
sp = spotipy.Spotify(
    auth_manager=SpotifyOAuth(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        redirect_uri=REDIRECT_URI,
        scope=SCOPE,
        cache_path=".cache-test"
    )
)

print("✅ Spotify kapcsolat létrehozva")

# ===============================================
# 2. TOP 50 DAL LEKÉRDEZÉSE
# ===============================================

print("\n📥 Kedvenc dalok lekérdezése...")

# Lekérdezés
top_tracks = sp.current_user_top_tracks(limit=3, time_range='short_term')
track_ids = [track['id'] for track in top_tracks['items']]
print(track_ids)

