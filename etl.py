import sqlite3

import spotipy
from spotipy.oauth2 import SpotifyOAuth


def extract_recently_played(api):
    return api.current_user_recently_played()


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


def load(table, data, db="music.sqlite"):
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
    for t, t_art, art_t, t_e, a, a_art, art_a in transform_recently_played(content):
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


if __name__ == '__main__':
    with open("scopes.txt", 'r') as scopes:
        ALL_SCOPES = " ".join([scope.strip() for scope in scopes.readlines()])

    api = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=ALL_SCOPES, open_browser=True, cache_path=".spotify_cache"))