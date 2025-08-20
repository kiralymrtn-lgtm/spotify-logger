set -e
# Simple DB health line
sqlite3 spotify.db "SELECT 'Last played at: ' || IFNULL(MAX(played_at),'(none)') || ' | Plays: ' || COUNT(*) FROM plays;" \
&& sqlite3 spotify.db "SELECT 'Artists: ' || COUNT(*) FROM artists;" \
&& sqlite3 spotify.db "SELECT 'Track-Artists: ' || COUNT(*) FROM track_artists;"
