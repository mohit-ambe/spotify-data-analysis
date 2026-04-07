import os
import time

from .extracts import (
    extract_playlist_items,
    extract_recently_played,
    extract_track_features,
    extract_users_playlist,
)
from .loads import load
from .reads import read_new_playlists, read_new_tracks
from .transforms import (
    transform_playlist_items,
    transform_recently_played,
    transform_track_features,
    transform_users_playlist,
)


def recently_played(api):
    TIME = time.time()

    content = extract_recently_played(api)
    for i, track in enumerate(transform_recently_played(content)):
        t, t_art, art_t, t_e, a, a_art, art_a = track
        load('Tracks', t)
        for track_artist in t_art:
            load('TrackArtists', track_artist)
        for artist in art_t:
            load('Artists', artist)
        load('TrackEvents', t_e)

        load('Albums', a)
        for album_artist in a_art:
            load('AlbumArtists', album_artist)
        for artist in art_a:
            load('Artists', artist)

    print(f"Loaded - Recently Played ({round(time.time() - TIME, 3)}s)")


def users_playlists(api, current_user_id):
    content = extract_users_playlist(api)
    playlists = []
    for tp in transform_users_playlist(content, current_user_id):
        load('Playlists', tp)
        playlists.append((tp['playlist_id'], tp['name']))
    print(f"Loaded - {current_user_id}'s Playlists")
    return playlists


def playlist_items(api, playlist_id, playlist_name, offset=0):
    TIME = time.time()

    content = extract_playlist_items(api, playlist_id, offset=offset)
    for i, track in enumerate(transform_playlist_items(content, playlist_id, offset=offset)):
        t, t_art, art_t, p_t, a, a_art, art_a = track

        load('Tracks', t)
        for track_artist in t_art:
            load('TrackArtists', track_artist)
        for artist in art_t:
            load('Artists', artist)
        load('PlaylistTracks', p_t)

        load('Albums', a)
        for album_artist in a_art:
            load('AlbumArtists', album_artist)
        for artist in art_a:
            load('Artists', artist)

    runtime = f"({round(time.time() - TIME, 3)}s)"
    if offset + 50 < content['total']:
        print(f"Loaded - Batch {1 + offset // 50} {runtime}")
        playlist_items(api, playlist_id, playlist_name, offset=offset + 50)
    else:
        print(f"{playlist_name} Completed <= {content['total']} items {runtime}")


def track_features(offset=0, limit=None, workers=os.cpu_count()):
    TIME = time.time()

    track_ids = read_new_tracks()
    limit = limit if limit is not None else len(track_ids)
    content = extract_track_features(track_ids[offset:offset + limit], workers)

    for t_f in transform_track_features(content):
        load('TrackFeatures', t_f)

    runtime = f"({round(time.time() - TIME, 3)}s)"
    print(f"Loaded Track Features - <= {limit} items {runtime}")


def get_all_playlist_items(api, current_user_id):
    playlist_ids = users_playlists(api, current_user_id)
    seen = read_new_playlists()
    for pid, pname in playlist_ids:
        if pid in seen:
            continue
        playlist_items(api, pid, pname)