import os
import sqlite3
import time

import spotipy
from spotipy.oauth2 import SpotifyOAuth

import dl
import tf

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


def extract_track_features(isrc, query):
    url = dl.download_track(isrc, query)
    if not url:
        return dict()
    print(f"Downloaded: {query}")
    filename = f"track_downloads/{url[url.index('=') + 1:]}.mp3"
    content = tf.track_features(filename)
    print(f"Features: {query}")
    try:
        os.remove(filename)
        print(f"Removed: {query}")
    except Exception:
        pass
    return content


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

        track_content = extract_track(api, track['id'])
        t_q = transform_track_query(track_content)
        track_features_content = extract_track_features(t_q['isrc'], t_q['query'])
        t_f = transform_track_features(track['id'], track_features_content)
        t_g = transform_track_genres(track['id'], track_features_content)

        yield t, t_art, art_t, t_e, t_q, t_f, t_g, a, a_art, art_a


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

        track_content = extract_track(api, track['id'])
        t_q = transform_track_query(track_content)
        track_features_content = extract_track_features(t_q['isrc'], t_q['query'])
        t_f = transform_track_features(track['id'], track_features_content)
        t_g = transform_track_genres(track['id'], track_features_content)

        yield t, t_art, art_t, p_t, t_q, t_f, t_g, a, a_art, art_a


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


def transform_track_features(track_id, track_features_content):
    data = dict()
    data['track_id'] = track_id
    for k, v in track_features_content.items():
        if k == 'genres':
            continue
        data[k] = v
    return data


def transform_track_genres(track_id, track_features_content):
    for i, genre in enumerate(track_features_content['genres']):
        data = dict()
        data['track_id'] = track_id
        data['genre'] = genre
        data['genre_order'] = i
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


def recently_played(api):
    content = extract_recently_played(api)
    TIME = time.time()
    for i, track in enumerate(transform_recently_played(content)):
        t, t_art, art_t, t_e, t_q, t_f, t_g, a, a_art, art_a = track
        load("Tracks", t)
        for track_artist in t_art:
            load("TrackArtists", track_artist)
        for artist in art_t:
            load("Artists", artist)
        load("TrackEvents", t_e)

        load("TrackQueries", t_q)
        load("TrackFeatures", t_f)
        for genre in t_g:
            load("TrackGenres", genre)

        load("Albums", a)
        for album_artist in a_art:
            load("AlbumArtists", album_artist)
        for artist in art_a:
            load("Artists", artist)

        print(f"Track {i + 1} - Loaded ({round(time.time() - TIME, 3)}s)")
        TIME = time.time()

    print("Loaded - Recently Played")


def users_playlists(api):
    content = extract_users_playlist(api)
    playlists = []
    for tp in transform_users_playlist(content):
        load("Playlists", tp)
        playlists.append(tp['playlist_id'])
    print(f"Loaded - {CURRENT_USER_ID}'s Playlists")
    return playlists


def playlist_items(api, playlist_id, offset=0):
    content = extract_playlist_items(api, playlist_id, offset=offset)
    TIME = time.time()
    for i, track in enumerate(transform_playlist_items(content, playlist_id, offset=offset)):
        t, t_art, art_t, p_t, t_q, t_f, t_g, a, a_art, art_a = track

        load("Tracks", t)
        for track_artist in t_art:
            load("TrackArtists", track_artist)
        for artist in art_t:
            load("Artists", artist)
        load("PlaylistTracks", p_t)

        load("TrackQueries", t_q)
        load("TrackFeatures", t_f)
        for genre in t_g:
            load("TrackGenres", genre)

        load("Albums", a)
        for album_artist in a_art:
            load("AlbumArtists", album_artist)
        for artist in art_a:
            load("Artists", artist)

        print(f"Track {i + offset + 1} - Loaded ({round(time.time() - TIME, 3)}s)")
        TIME = time.time()

    if offset + 50 < content['total']:
        print(f"Loaded - Batch {1 + offset // 50}")
        playlist_items(api, playlist_id, offset=offset + 50)
    else:
        print(f"Completed - {content['total']} (potential) items")


if __name__ == '__main__':
    with open("scopes.txt", 'r') as scopes:
        ALL_SCOPES = " ".join([scope.strip() for scope in scopes.readlines()])

    api = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=ALL_SCOPES, open_browser=True, cache_path=".spotify_cache"))
    CURRENT_USER_ID = api.current_user()['id']