# Spotify Logger

A simple Python project that fetches recently played tracks from the Spotify Web API, stores them in a local SQLite database, and enriches the data with artist information.

## Features
- Fetches recently played tracks (up to 500 per run, paginated).
- Saves play history into `spotify.db` (`plays` table).
- Creates mapping of tracks and artists (`track_artists` table).
- Enriches artist data (`artists` table) with genres, followers, popularity, and images.
- Avoids duplicates by using composite primary keys.

## Requirements
- Python 3.9+ recommended
- Spotify developer credentials:
  - `SPOTIFY_CLIENT_ID`
  - `SPOTIFY_CLIENT_SECRET`
  - `SPOTIFY_REDIRECT_URI`

These should be stored in a local `.env` file.

## Setup
```bash
git clone https://github.com/<your-username>/spotify-logger.git
cd spotify-logger
python -m venv .venv
source .venv/bin/activate   # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Usage
Add your credentials to a .env file:
```bash
SPOTIFY_CLIENT_ID=xxxx
SPOTIFY_CLIENT_SECRET=xxxx
SPOTIFY_REDIRECT_URI=http://127.0.0.1:8080/callback
```
Run the script:
```bash
python save_history.py
```

The script will:
- Authenticate with Spotify
- Fetch your recently played tracks
- Save results into spotify.db

## Database Schema
- plays: play history with track metadata
- track_artists: mapping between tracks and artists
- artists: enriched artist information

## Example output
📥 Fetching (paginated)...

🔁 Retrieved items from API: 50

💾 Inserted into DB (new): 2 rows

👤 Inserted into artists dim (new): 2 rows