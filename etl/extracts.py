import multiprocessing
import os

import songdata_tf


def extract_recently_played(api):
    return api.current_user_recently_played()


def extract_users_playlist(api):
    return api.current_user_playlists()


def extract_playlist_items(api, playlist_id, offset=0):
    return api.playlist_items(playlist_id=playlist_id, offset=offset)


def extract_track(api, track_id):
    return api.track(track_id=track_id, market='US')


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
