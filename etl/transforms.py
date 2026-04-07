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


def transform_users_playlist(content, current_user_id):
    for i in range(len(content['items'])):
        playlist = content['items'][i]
        if playlist['owner']['id'] != current_user_id:
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
                v = v.replace("â™­", "b").replace("â™¯", "#")
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
