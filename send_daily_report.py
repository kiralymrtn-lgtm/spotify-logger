import os
import sqlite3
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
import smtplib
import ssl

DB_PATH = "spotify.db"

def query_stats():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Összes darab
    cur.execute("SELECT COUNT(*) FROM plays;")
    total_plays = cur.fetchone()[0]

    cur.execute("SELECT COUNT(DISTINCT track_id) FROM plays;")
    total_unique_tracks = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM artists;")
    total_artists = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM track_artists;")
    total_track_artists = cur.fetchone()[0]

    # Utolsó 24 óra (UTC) – a played_at 'YYYY-MM-DDTHH:MM:SS.sssZ' formátum,
    # ezért substr+replace, majd strftime('%s') unix epochra alakítva
    cur.execute("""
        SELECT COUNT(*)
        FROM plays
        WHERE strftime('%s', replace(substr(played_at,1,19),'T',' ')) >= strftime('%s','now','-1 day')
    """)
    last24h_plays = cur.fetchone()[0]

    # Legutóbbi és legrégebbi play időpont
    cur.execute("SELECT MAX(played_at), MIN(played_at) FROM plays;")
    most_recent, oldest = cur.fetchone()

    # Legutóbbi 10 lejátszás (időrendben)
    cur.execute("""
        SELECT played_at, track_name, artist_name
        FROM plays
        ORDER BY strftime('%s', replace(substr(played_at,1,19),'T',' ')) DESC
        LIMIT 10
    """)
    last10 = cur.fetchall()
    last10 = list(reversed(last10))  # kronológiai sorrend (régebbitől az újabb felé)

    conn.close()
    return {
        "total_plays": total_plays,
        "total_unique_tracks": total_unique_tracks,
        "total_artists": total_artists,
        "total_track_artists": total_track_artists,
        "last24h_plays": last24h_plays,
        "most_recent": most_recent,
        "oldest": oldest,
        "last10": last10,
    }

def build_email_body(stats):
    lines = []
    lines.append("📬 Spotify Logger – Daily Report")
    lines.append("=" * 50)
    lines.append("")
    lines.append("Totals so far:")
    lines.append(f"- plays: {stats['total_plays']}")
    lines.append(f"- unique tracks: {stats['total_unique_tracks']}")
    lines.append(f"- artists (dim): {stats['total_artists']}")
    lines.append(f"- track_artists (map): {stats['total_track_artists']}")
    lines.append("")
    lines.append(f"Plays in last 24h: {stats['last24h_plays']}")
    lines.append(f"Oldest play in DB: {stats['oldest'] or '-'}")
    lines.append(f"Most recent play: {stats['most_recent'] or '-'}")
    lines.append("")
    lines.append("Last 10 plays (chronological):")
    for played_at, track, artist in stats["last10"]:
        lines.append(f"  * {played_at} — {track} — {artist}")
    lines.append("")
    lines.append("– spotify-logger")
    return "\n".join(lines)

def send_email(subject, body):
    user = os.getenv("GMAIL_USER")
    app_password = os.getenv("GMAIL_APP_PASSWORD")
    to_addr = os.getenv("MAIL_TO") or user

    if not user or not app_password:
        raise RuntimeError("Missing GMAIL_USER / GMAIL_APP_PASSWORD env vars (.env.mail).")

    msg = EmailMessage()
    msg["From"] = user
    msg["To"] = to_addr
    msg["Subject"] = subject
    msg.set_content(body)

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as smtp:
        smtp.login(user, app_password)
        smtp.send_message(msg)

if __name__ == "__main__":
    stats = query_stats()
    body = build_email_body(stats)
    # subjectben dátum (UTC)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    subject = f"Spotify Logger – Daily Report – {today} (UTC)"
    send_email(subject, body)
    print("✅ Daily report email sent.")