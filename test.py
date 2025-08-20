import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
pd.set_option('display.max_columns', None) # Minden oszlop látszódjon
pd.set_option('display.width', None) # A sor teljes szélességben jelenjen meg (ne tördelje)
pd.set_option('display.max_colwidth', None) # Ne rövidítse le a cellák tartalmát
import os
from dotenv import load_dotenv
from datetime import datetime
import time
import json

print("🎵 Spotify Adatok Lekérdezése")
print("=" * 50)

# ===============================================
# 1. KAPCSOLAT LÉTREHOZÁSA (ugyanaz mint előbb)
# ===============================================

load_dotenv()
CLIENT_ID = os.getenv('SPOTIFY_CLIENT_ID')
CLIENT_SECRET = os.getenv('SPOTIFY_CLIENT_SECRET')
REDIRECT_URI = os.getenv('SPOTIFY_REDIRECT_URI')

# Most több engedélyt kérünk, hogy több adatot tudjunk lekérdezni
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
# 2. LEGUTÓBBI 50 DAL LEKÉRDEZÉSE
# ===============================================

print("\n📥 Legutóbbi dalok lekérdezése...")

# Lekérdezés
results = sp.current_user_recently_played(limit=50)

# 1. TELJES VÁLASZ MEGNÉZÉSE
#print("🔍 TELJES API VÁLASZ STRUKTÚRA:")
#print("="*50)
#print(results)
#print(type(results))  # <class 'dict'>
#print(results.keys())  # dict_keys(['items', 'next', 'cursors', 'limit', 'href'])
#print(json.dumps(results, indent=4)) #json formátum indentált visszakapása

items=results['items']

# Kiolvasunk minden fontos mezőt egy list comprehension segítségével
tracks_data=[]

for item in items:
    track=item['track']
    album=track['album']
    artists=track['artists']

    tracks_data.append(
        {
            'played_at': item['played_at'],
            'track_name': track['name'],
            'track_id': track['id'],
            'track_link': track['href'],
            'artist_name': ', '.join([a['name'] for a in artists]),
            'album_name': album['name'],
            'album_type': album['album_type'],
            'release_date': album['release_date'],
            'duration_ms': track['duration_ms'],
            'popularity': track.get('popularity'),
            'spotify_url': track['external_urls']['spotify'],
            #'cover': album['images']
            'cover': next((img['url'] for img in album['images'] if img['height'] == 300), None) #visszaadja az első olyan img['url'] értéket, ahol a height == 300. Ha nincs ilyen (extrém ritka), akkor None kerül be
        }
    )

# Átalakítás DataFrame-be
df = pd.DataFrame(tracks_data)

# Megnézheted a végeredményt
print(df)

all_tracks = []
limit = 50 # Ennyi dalt kérünk le egy körben (max 50)
max_loops = 10 # Ennyi körben kérdezzük le (max 500 dal)
before = int(time.time()*1000) # mostani idő, ezredmásodpercben

for i in range(max_loops):
    results = sp.current_user_recently_played(limit=limit, before=before)
    items = results.get('items',[])

    if not items:
        break

    all_tracks.extend(items)

    # Before paraméter frissítése a legrégebbi dat időpontjára
    oldest_played_at = items[-1]['played_at']
    before = int(datetime.strptime(oldest_played_at, "%Y-%m-%dT%H:%M:%S.%fZ").timestamp() * 1000)

    print(f"🔁 Lekérdezve {len(items)} dal (összesen eddig: {len(all_tracks)})")

print(f"\n✅ Összesen {len(all_tracks)} dalt sikerült lekérdezni.")

tracks_data_long = []

for item in all_tracks:
    track = item['track']
    album = track['album']
    artists = track['artists']

    tracks_data_long.append(
        {
            'played_at': item['played_at'],
            'track_name': track['name'],
            'track_id': track['id'],
            'track_link': track['href'],
            'artist_name': ', '.join([a['name'] for a in artists]),
            'album_name': album['name'],
            'album_type': album['album_type'],
            'release_date': album['release_date'],
            'duration_ms': track['duration_ms'],
            'popularity': track.get('popularity'),
            'spotify_url': track['external_urls']['spotify'],
            #'cover': album['images']
            'cover': next((img['url'] for img in album['images'] if img['height'] == 300), None) #visszaadja az első olyan img['url'] értéket, ahol a height == 300. Ha nincs ilyen (extrém ritka), akkor None kerül be
        }
    )

df_long = pd.DataFrame(tracks_data_long)

print(df_long)