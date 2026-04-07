import multiprocessing
import os
import sqlite3
import time

import spotipy
from spotipy.oauth2 import SpotifyOAuth

import songdata_tf

DB_FILENAME = "music.sqlite"
CURRENT_USER_ID = ""


def extract_recently_played(api):
    return api.current_user_recently_played()


def extract_users_playlist(api):
    return api.current_user_playlists()


def extract_playlist_items(api, playlist_id, offset=0):
    return api.playlist_items(playlist_id=playlist_id, offset=offset)


def extract_track(api, track_id):
    return api.track(track_id=track_id, market="US")


def extract_track_features(track_ids, workers=os.cpu_count()):
    bsize = max(4, len(track_ids) // workers)
    track_ids_batched = [track_ids[i:i + bsize] for i in range(0, len(track_ids), bsize)]

    with multiprocessing.Pool(processes=workers) as pool:
        batches = pool.map(songdata_tf.track_features, track_ids_batched)

    if batches:
        result = []
        for batch in batches:
            result.extend(batch)
        return result
    else:
        return [dict() for _ in range(len(track_ids))]


def transform_recently_played(content):
    for i in range(len(content['items'])):
        item = content['items'][i]
        track = content['items'][i]['track']
        album = track['album']

        t = transform_track(track)
        t_art = transform_track_artists(track)
        art_t = transform_artists(track)
        t_e = transform_track_events(item)
        a = transform_album(album)
        a_art = transform_album_artists(album)
        art_a = transform_artists(album)

        yield t, t_art, art_t, t_e, a, a_art, art_a


def transform_users_playlist(content):
    for i in range(len(content['items'])):
        playlist = content['items'][i]
        if playlist['owner']['id'] != CURRENT_USER_ID:
            continue
        tp = transform_playlist(playlist)
        yield tp


def transform_playlist_items(content, playlist_id, offset=0):
    for i in range(len(content['items'])):
        item = content['items'][i]
        track = item['item']
        if track is None or not track['is_playable'] or not track['track']:
            continue
        album = track['album']

        t = transform_track(track)
        t_art = transform_track_artists(track)
        art_t = transform_artists(track)
        p_t = transform_playlist_track(track, playlist_id, offset + i)
        a = transform_album(album)
        a_art = transform_album_artists(album)
        art_a = transform_artists(album)

        yield t, t_art, art_t, p_t, a, a_art, art_a


def transform_playlist_track(track, playlist_id, playlist_order):
    data = dict()
    data['playlist_id'] = playlist_id
    data['playlist_order'] = playlist_order
    data['track_id'] = track['id']
    return data


def transform_track_query(track_content):
    data = dict()
    data['track_id'] = track_content['id']
    data['isrc'] = track_content['external_ids']['isrc']
    data['query'] = track_content['name'] + " - " + track_content['artists'][0]['name']
    return data


def transform_track_features(track_features_content):
    for item in track_features_content:
        data = dict()
        for k, v in item.items():
            if k == 'key':
                v = v.replace("♭", "b").replace("♯", "#")
            if k == 'mode':
                v = v.lower()
            data[k.lower()] = v
        yield data


def transform_playlist(playlist):
    data = dict()
    data['playlist_id'] = playlist['id']
    data['name'] = playlist['name']
    data['tracks'] = playlist['items']['total']
    data['playlist_url'] = playlist['external_urls']['spotify']
    data['image_url'] = playlist['images'][0]['url']
    return data


def transform_artists(object):
    artists = object['artists']
    for i, artist in enumerate(artists):
        data = dict()
        data['id'] = artist['id']
        data['name'] = artist['name']
        data['artist_url'] = artist['external_urls']['spotify']
        yield data


def transform_album(album):
    data = dict()
    data['id'] = album['id']
    data['name'] = album['name']
    data['album_type'] = album['album_type']
    data['release_date'] = album['release_date']
    data['total_tracks'] = album['total_tracks']
    data['image_url'] = album['images'][0]['url']
    data['album_url'] = album['external_urls']['spotify']
    return data


def transform_album_artists(album):
    album_id = album['id']
    artists = album['artists']
    for i, artist in enumerate(artists):
        data = dict()
        data['album_id'] = album_id
        data['artist_id'] = artist['id']
        data['artist_order'] = i
        yield data


def transform_track(track):
    data = dict()
    data['id'] = track['id']
    data['name'] = track['name']
    data['album_id'] = track['album']['id']
    data['disc_number'] = track['disc_number']
    data['duration_ms'] = track['duration_ms']
    data['explicit'] = track['explicit']
    data['track_number'] = track['track_number']
    data['track_url'] = track['external_urls']['spotify']
    return data


def transform_track_artists(track):
    track_id = track['id']
    artists = track['artists']
    for i, artist in enumerate(artists):
        data = dict()
        data['track_id'] = track_id
        data['artist_id'] = artist['id']
        data['artist_order'] = i
        yield data


def transform_track_events(item):
    track = item['track']
    data = dict()
    data['track_id'] = track['id']
    data['played_at'] = item['played_at']
    return data


def load(table, data, db=DB_FILENAME):
    schema = str(tuple(data.keys()))
    values = str(tuple(data.values()))
    try:
        with sqlite3.connect(db) as connection:
            cursor = connection.cursor()
            query = f'''INSERT INTO {table} {schema} VALUES {values}'''
            cursor.execute(query)
    except:
        pass


def read_new_tracks(db=DB_FILENAME):
    try:
        with sqlite3.connect(db) as connection:
            cursor = connection.cursor()
            query = f'''SELECT T.id
                        FROM Tracks T
                                LEFT OUTER JOIN TrackFeatures TF
                                                 on T.id = TF.track_id
                        WHERE TF.track_id IS NULL
                        UNION
                        SELECT T.id
                        FROM TrackFeatures TF
                                 LEFT JOIN main.Tracks T
                                    on T.id = TF.track_id
                        WHERE TF.popularity IS NULL
                           OR TF.loudness IS NULL
                           OR TF.acousticness IS NULL
                           OR TF.danceability IS NULL
                           OR TF.energy IS NULL
                           OR TF.instrumentalness IS NULL
                           OR TF.liveness IS NULL
                           OR TF.speechiness IS NULL
                           OR TF.valence IS NULL'''
            cursor.execute(query)
            return [row[0] for row in cursor]
    except:
        pass
    return []


def read_new_playlists(db=DB_FILENAME):
    try:
        with sqlite3.connect(db) as connection:
            cursor = connection.cursor()
            query = f'''SELECT P.playlist_id
                        FROM Playlists P
                                 LEFT JOIN main.PlaylistTracks PT
                                    on P.playlist_id = PT.playlist_id
                        WHERE PT.playlist_order IS NOT NULL'''
            cursor.execute(query)
            return {row[0] for row in cursor}
    except:
        pass
    return set()


def recently_played(api):
    TIME = time.time()

    content = extract_recently_played(api)
    for i, track in enumerate(transform_recently_played(content)):
        t, t_art, art_t, t_e, a, a_art, art_a = track
        load("Tracks", t)
        for track_artist in t_art:
            load("TrackArtists", track_artist)
        for artist in art_t:
            load("Artists", artist)
        load("TrackEvents", t_e)

        load("Albums", a)
        for album_artist in a_art:
            load("AlbumArtists", album_artist)
        for artist in art_a:
            load("Artists", artist)

    print(f"Loaded - Recently Played ({round(time.time() - TIME, 3)}s)")


def users_playlists(api):
    content = extract_users_playlist(api)
    playlists = []
    for tp in transform_users_playlist(content):
        load("Playlists", tp)
        playlists.append((tp['playlist_id'], tp['name']))
    print(f"Loaded - {CURRENT_USER_ID}'s Playlists")
    return playlists


def playlist_items(api, playlist_id, playlist_name, offset=0):
    TIME = time.time()

    content = extract_playlist_items(api, playlist_id, offset=offset)
    for i, track in enumerate(transform_playlist_items(content, playlist_id, offset=offset)):
        t, t_art, art_t, p_t, a, a_art, art_a = track

        load("Tracks", t)
        for track_artist in t_art:
            load("TrackArtists", track_artist)
        for artist in art_t:
            load("Artists", artist)
        load("PlaylistTracks", p_t)

        load("Albums", a)
        for album_artist in a_art:
            load("AlbumArtists", album_artist)
        for artist in art_a:
            load("Artists", artist)

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
        load("TrackFeatures", t_f)

    runtime = f"({round(time.time() - TIME, 3)}s)"
    print(f"Loaded Track Features - <= {limit} items {runtime}")


def main():
    global CURRENT_USER_ID
    with open("scopes.txt", 'r') as scopes:
        ALL_SCOPES = " ".join([scope.strip() for scope in scopes.readlines()])

    api = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=ALL_SCOPES, open_browser=True, cache_path=".spotify_cache"))
    CURRENT_USER_ID = api.current_user()['id']

    recently_played(api)
    playlist_ids = users_playlists(api)
    seen = read_new_playlists()
    for pid, pname in playlist_ids:
        if pid in seen:
            continue
        playlist_items(api, pid, pname)
    track_features()


if __name__ == '__main__':
    main()