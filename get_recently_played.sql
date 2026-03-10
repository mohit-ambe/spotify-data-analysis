SELECT
te.played_at,

t.id AS track_id,
t.name AS track_name,
t.disc_number,
t.duration_ms,
t.explicit,
t.track_number,
t.track_url,

a.id AS album_id,
a.name AS album_name,
a.album_type,
a.release_date,
a.total_tracks,
a.image_url,
a.album_url,

ta.artist_order AS track_artist_order,
ar_t.id AS track_artist_id,
ar_t.name AS track_artist_name,
ar_t.artist_url AS track_artist_url,

aa.artist_order AS album_artist_order,
ar_a.id AS album_artist_id,
ar_a.name AS album_artist_name,
ar_a.artist_url AS album_artist_url
FROM TrackEvents te
JOIN Tracks t
    ON te.track_id = t.id
LEFT JOIN Albums a
    ON t.album_id = a.id
LEFT JOIN TrackArtists ta
    ON t.id = ta.track_id
LEFT JOIN Artists ar_t
    ON ta.artist_id = ar_t.id
LEFT JOIN AlbumArtists aa
    ON a.id = aa.album_id
LEFT JOIN Artists ar_a
    ON aa.artist_id = ar_a.id

ORDER BY te.played_at DESC, t.id, ta.artist_order, aa.artist_order;