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
SCOPE = "user-read-recently-played user-read-private user-top-read "

# Spotify kliens létrehozása
sp = spotipy.Spotify(
    auth_manager=SpotifyOAuth(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        redirect_uri=REDIRECT_URI,
        scope=SCOPE,
        cache_path="../.cache-test"
    )
)

print("✅ Spotify kapcsolat létrehozva")

# ===============================================
# 2. LEGUTÓBBI 50 DAL LEKÉRDEZÉSE
# ===============================================

print("\n📥 Legutóbbi dalok lekérdezése...")

try:
    # A Spotify API hívás - ez adja vissza a legutóbb hallgatott dalokat
    results = sp.current_user_recently_played(limit=50)

    print(f"✅ Sikeresen lekérdezve {len(results['items'])} dal")

    # ===============================================
    # 3. ADATOK FELDOLGOZÁSA TÁBLÁZATHOZ
    # ===============================================

    print("\n🔄 Adatok feldolgozása táblázatos formára...")

    # Üres lista létrehozása - ide fogjuk gyűjteni az adatokat
    songs_data = []

    # Végigmegyünk minden dalón és kigyűjtjük a fontos adatokat
    for item in results['items']:
        # Az 'item' egy komplex objektum, amiből kiválasztjuk a szükséges részeket
        track = item['track']  # A dal információi

        # Egy sor adatot készítünk minden dalhoz
        song_info = {
            'name': track['name'],  # A dal címe
            'artist': track['artists'][0]['name'],  # Az első előadó neve
            'album': track['album']['name'],  # Album név
            'played_at': item['played_at'],  # Mikor hallgattad meg
            'duration_ms': track['duration_ms'],  # Dal hossza milliszekundumban
            'popularity': track['popularity'],  # Népszerűség 0-100 skálán
            'explicit': track['explicit'],  # Van-e káromkodás benne
            'song_id': track['id']  # Egyedi azonosító
        }

        # Hozzáadjuk ezt a sort az adataink listájához
        songs_data.append(song_info)

    print(f"✅ {len(songs_data)} dal adatai feldolgozva")

    # ===============================================
    # 4. TÁBLÁZAT (DATAFRAME) LÉTREHOZÁSA
    # ===============================================

    print("\n📊 Táblázat létrehozása...")

    # A pandas DataFrame egy olyan táblázat, amivel könnyen tudsz dolgozni
    df = pd.DataFrame(songs_data)

    # Néhány hasznos átalakítás:

    # A) Dátum oszlop olvashatóbb formában
    df['played_at'] = pd.to_datetime(df['played_at'])
    df['date'] = df['played_at'].dt.strftime('%Y-%m-%d %H:%M')

    # B) Dal hossza percben és másodpercben
    df['duration_mins'] = (df['duration_ms'] / 1000 / 60).round(2)

    print("✅ Táblázat létrehozva!")

    # ===============================================
    # 5. TÁBLÁZAT MEGJELENÍTÉSE ÉS ELEMZÉSE
    # ===============================================

    print("\n📋 TÁBLÁZAT MEGJELENÍTÉSE")
    print("=" * 50)

    # Az első 10 dal megjelenítése
    print("🎵 ELSŐ 10 LEGUTÓBB HALLGATOTT DAL:")
    print("-" * 80)

    # Csak a fontosabb oszlopokat jelenítjük meg
    display_columns = ['name', 'artist', 'date', 'duration_mins', 'popularity']
    print(df[display_columns].head(10).to_string(index=False))

    print("\n📊 ALAPSTATISZTIKÁK:")
    print("-" * 30)
    print(f"📈 Összesen hallgatott dalok: {len(df)}")
    print(f"🎤 Különböző előadók: {df['artist'].nunique()}")
    print(f"💿 Különböző albumok: {df['album'].nunique()}")
    print(f"⏱️  Átlagos dal hossz: {df['duration_mins'].mean():.2f} perc")
    print(f"🌟 Átlagos népszerűség: {df['popularity'].mean():.1f}/100")

    print("\n🏆 TOP 5 ELŐADÓ (leggyakrabban hallgatott):")
    top_artists = df['artist'].value_counts().head(5)
    for i, (artist, count) in enumerate(top_artists.items(), 1):
        print(f"   {i}. {artist}: {count} dal")

    print("\n🎵 LEGUTÓBBI 5 DAL:")
    latest_songs = df.head(5)
    for i, row in latest_songs.iterrows():
        print(f"   • {row['name']} - {row['artist']} ({row['date']})")

    # ===============================================
    # 6. ADATOK MENTÉSE FÁJLBA
    # ===============================================

    print(f"\n💾 ADATOK MENTÉSE")
    print("-" * 30)

    # CSV fájlba mentés (Excel-ben is megnyitható)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"spotify_recent_tracks_{timestamp}.csv"

    # Mappa létrehozása ha nem létezik
    os.makedirs('../data', exist_ok=True)

    # Fájlba írás
    df.to_csv(f'data/{filename}', index=False, encoding='utf-8')
    print(f"✅ Adatok mentve: data/{filename}")

    # Excel formátumba is (ha telepítve van az openpyxl)
    try:
        excel_filename = f"spotify_recent_tracks_{timestamp}.xlsx"
        df.to_excel(f'data/{excel_filename}', index=False)
        print(f"✅ Excel fájl is mentve: data/{excel_filename}")
    except ImportError:
        print("ℹ️  Excel mentéshez telepítsd: pip install openpyxl")

    # ===============================================
    # 7. TÁBLÁZAT BÖNGÉSZÉSE ÉS KERESÉS
    # ===============================================

    print(f"\n🔍 TÁBLÁZAT BÖNGÉSZÉSE")
    print("-" * 30)

    # Példa: Adott előadó dalainak keresése
    print("Példa keresések:")

    # Legnépszerűbb dalok (80+ népszerűség)
    popular_songs = df[df['popularity'] >= 80]
    if not popular_songs.empty:
        print(f"\n🌟 Nagy népszerűségű dalok (80+): {len(popular_songs)} db")
        print(popular_songs[['name', 'artist', 'popularity']].head(3).to_string(index=False))

    # Hosszú dalok (5+ perc)
    long_songs = df[df['duration_mins'] >= 5.0]
    if not long_songs.empty:
        print(f"\n⏱️  Hosszú dalok (5+ perc): {len(long_songs)} db")
        print(long_songs[['name', 'artist', 'duration_mins']].head(3).to_string(index=False))

    print(f"\n🎉 KÉSZ! Az adatok sikeresen lekérdezve és feldolgozva!")
    print(f"📁 A fájlok itt találhatók: data/ mappa")
    print(f"📊 A 'df' változóban van a teljes táblázat - ezzel tovább tudsz dolgozni")

except Exception as e:
    print(f"❌ Hiba történt: {e}")
    print("Ellenőrizd, hogy hallgattál-e dalokat az utóbbi időben!")

# ===============================================
# 8. KÖVETKEZŐ LÉPÉSEK ÉS TIPPEK
# ===============================================

print(f"\n💡 KÖVETKEZŐ LÉPÉSEK ÉS TIPPEK")
print("=" * 50)
print("""
PYCHARM HASZNÁLAT:
1. A 'df' változó tartalmazza az adatokat
2. Új cellában írd be: print(df.head()) - első 5 sor
3. Oszlopok listája: print(df.columns.tolist())
4. Adott oszlop: print(df['dal_neve'])

HASZNOS PANDAS PARANCSOK:
• df.info() - táblázat általános infói
• df.describe() - számszerű statisztikák
• df.shape - sorok és oszlopok száma
• df['oszlop_neve'].unique() - egyedi értékek

KÖVETKEZŐ PROJEKTRÉSZEK:
1. Audio features lekérdezése (danceability, energy, stb.)
2. Top tracks különböző időszakokra
3. Automatikus napi lekérdezés
4. Power BI kapcsolat kialakítása

FÁJLOK:
• CSV: Excel-ben megnyitható, Power BI-ban importálható
• data/ mappában találod a mentett fájlokat
""")

print("🚀 Most már készen állsz a következő lépésre!")

