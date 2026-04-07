import sqlite3


DB_FILENAME = 'music.sqlite'


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
