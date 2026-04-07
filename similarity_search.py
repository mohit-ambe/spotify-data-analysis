import json
import math
import re
import sqlite3
from pathlib import Path

import faiss
import numpy as np
import pandas as pd

DB_FILENAME = "music.sqlite"
INDEX_DIRNAME = "vector_db"
INDEX_FILENAME = "tracks.faiss"
METADATA_FILENAME = "tracks.csv"
CONFIG_FILENAME = "config.json"

NUMERIC_FEATURES = ["bpm", "popularity", "loudness", "acousticness", "danceability", "energy", "instrumentalness",
                    "liveness", "speechiness", "valence", ]

NOTE_TO_PITCH_CLASS = {
    "C": 0,
    "C#": 1,
    "Db": 1,
    "D": 2,
    "D#": 3,
    "Eb": 3,
    "E": 4,
    "F": 5,
    "F#": 6,
    "Gb": 6,
    "G": 7,
    "G#": 8,
    "Ab": 8,
    "A": 9,
    "A#": 10,
    "Bb": 10,
    "B": 11,
}


def _parse_mode(value):
    if value is None:
        return None
    normalized = str(value).strip().lower()
    if normalized == "major":
        return 1.0
    if normalized == "minor":
        return 0.0
    return None


def _parse_camelot(value):
    if value is None:
        return None, None
    match = re.match(r"^\s*(\d{1,2})\s*([ABab])\s*$", str(value))
    if not match:
        return None, None

    number = int(match.group(1))
    if not 1 <= number <= 12:
        return None, None

    letter = 1.0 if match.group(2).upper() == "B" else 0.0
    return number - 1, letter


def _load_tracks(db_path):
    query = """
        SELECT
            tf.track_id,
            t.name AS track_name,
            GROUP_CONCAT(ar.name, ', ') AS artist_names,
            al.name AS album_name,
            al.image_url AS album_image_url,
            tf.bpm,
            tf.popularity,
            tf.loudness,
            tf.acousticness,
            tf.danceability,
            tf.energy,
            tf.instrumentalness,
            tf.liveness,
            tf.speechiness,
            tf.valence,
            tf."key" AS key,
            tf.mode,
            tf.camelot
        FROM TrackFeatures tf
        LEFT JOIN Tracks t
            ON t.id = tf.track_id
        LEFT JOIN Albums al
            ON al.id = t.album_id
        LEFT JOIN TrackArtists ta
            ON ta.track_id = tf.track_id
        LEFT JOIN Artists ar
            ON ar.id = ta.artist_id
        GROUP BY
            tf.track_id,
            t.name,
            al.name,
            al.image_url,
            tf.bpm,
            tf.popularity,
            tf.loudness,
            tf.acousticness,
            tf.danceability,
            tf.energy,
            tf.instrumentalness,
            tf.liveness,
            tf.speechiness,
            tf.valence,
            tf."key",
            tf.mode,
            tf.camelot
        ORDER BY t.name, tf.track_id
    """

    with sqlite3.connect(db_path) as connection:
        df = pd.read_sql_query(query, connection)

    return df.drop_duplicates(subset=["track_id"]).reset_index(drop=True)


def _build_defaults(df):
    key_default = df["key"].dropna().mode().iloc[0]
    camelot_default = df["camelot"].dropna().mode().iloc[0]
    camelot_number_default, camelot_letter_default = _parse_camelot(camelot_default)
    mode_default = _parse_mode(df["mode"].dropna().mode().iloc[0])

    return {
        "numeric_defaults": {feature: float(df[feature].median()) for feature in NUMERIC_FEATURES},
        "key_default": key_default,
        "pitch_class_default": NOTE_TO_PITCH_CLASS.get(key_default, 0),
        "mode_default": 0.0 if mode_default is None else float(mode_default),
        "camelot_number_default": 0 if camelot_number_default is None else int(camelot_number_default),
        "camelot_letter_default": 0.0 if camelot_letter_default is None else float(camelot_letter_default),
    }


def _row_to_vector(values, defaults):
    vector = []

    for feature in NUMERIC_FEATURES:
        raw_value = values.get(feature, defaults["numeric_defaults"][feature])
        if raw_value is None or pd.isna(raw_value):
            raw_value = defaults["numeric_defaults"][feature]
        vector.append(float(raw_value))

    key_value = values.get("key", defaults["key_default"])
    pitch_class = NOTE_TO_PITCH_CLASS.get(str(key_value).strip(), defaults["pitch_class_default"])
    vector.extend([math.sin(2 * math.pi * pitch_class / 12.0), math.cos(2 * math.pi * pitch_class / 12.0), ])

    mode_value = _parse_mode(values.get("mode"))
    if mode_value is None:
        mode_value = defaults["mode_default"]
    vector.append(float(mode_value))

    camelot_number, camelot_letter = _parse_camelot(values.get("camelot"))
    if camelot_number is None:
        camelot_number = defaults["camelot_number_default"]
    if camelot_letter is None:
        camelot_letter = defaults["camelot_letter_default"]
    vector.extend([math.sin(2 * math.pi * camelot_number / 12.0), math.cos(2 * math.pi * camelot_number / 12.0),
                   float(camelot_letter), ])

    return np.asarray(vector, dtype=np.float32)


def _build_matrix(df):
    defaults = _build_defaults(df)
    raw_matrix = np.vstack([_row_to_vector(row.to_dict(), defaults) for _, row in df.iterrows()]).astype(np.float32)

    means = raw_matrix.mean(axis=0).astype(np.float32)
    stds = raw_matrix.std(axis=0).astype(np.float32)
    stds[stds == 0] = 1.0

    normalized = ((raw_matrix - means) / stds).astype(np.float32)
    faiss.normalize_L2(normalized)

    config = {
        "dimension": int(normalized.shape[1]),
        "means": means.tolist(),
        "stds": stds.tolist(), **defaults,
    }
    return normalized, config


def _query_to_vector(feature_values, config):
    raw = _row_to_vector(feature_values, config)
    means = np.asarray(config["means"], dtype=np.float32)
    stds = np.asarray(config["stds"], dtype=np.float32)
    vector = ((raw - means) / stds).reshape(1, -1).astype(np.float32)
    faiss.normalize_L2(vector)
    return vector


def _save_index(index_dir, index, metadata, config):
    index_dir.mkdir(parents=True, exist_ok=True)
    faiss.write_index(index, str(index_dir / INDEX_FILENAME))
    metadata.to_csv(index_dir / METADATA_FILENAME, index=False)
    with open(index_dir / CONFIG_FILENAME, "w", encoding="utf-8") as handle:
        json.dump(config, handle, indent=2)


def _load_saved_index(index_dir):
    index = faiss.read_index(str(index_dir / INDEX_FILENAME))
    metadata = pd.read_csv(index_dir / METADATA_FILENAME)
    with open(index_dir / CONFIG_FILENAME, "r", encoding="utf-8") as handle:
        config = json.load(handle)
    return index, metadata, config


def build(db=DB_FILENAME, output_dir=INDEX_DIRNAME):
    db_path = Path(db)
    index_dir = Path(output_dir)

    tracks = _load_tracks(db_path)
    if tracks.empty:
        raise RuntimeError("TrackFeatures is empty. Populate music.sqlite before building the FAISS index.")

    matrix, config = _build_matrix(tracks)
    index = faiss.IndexFlatIP(matrix.shape[1])
    index.add(matrix)
    _save_index(index_dir, index, tracks, config)

    return {
        "tracks_indexed": len(tracks),
        "index_path": str(index_dir / INDEX_FILENAME),
        "metadata_path": str(index_dir / METADATA_FILENAME),
    }


def _ensure_index(db=DB_FILENAME, output_dir=INDEX_DIRNAME):
    db_path = Path(db)
    index_dir = Path(output_dir)

    required_files = [index_dir / INDEX_FILENAME, index_dir / METADATA_FILENAME, index_dir / CONFIG_FILENAME, ]

    if not all(path.exists() for path in required_files):
        build(db=db, output_dir=output_dir)

    index, metadata, config = _load_saved_index(index_dir)
    if index.ntotal != len(metadata) or "album_image_url" not in metadata.columns:
        build(db=db, output_dir=output_dir)
        index, metadata, config = _load_saved_index(index_dir)

    return index, metadata, config


def _format_results(metadata, distances, indices, k, exclude_track_id=None):
    results = []
    seen = set()

    for score, idx in zip(distances[0], indices[0]):
        if idx < 0:
            continue

        row = metadata.iloc[int(idx)]
        track_id = row["track_id"]

        if exclude_track_id is not None and track_id == exclude_track_id:
            continue
        if track_id in seen:
            continue

        seen.add(track_id)
        results.append({
            "rank": len(results) + 1,
            "track_id": track_id,
            "track_name": row["track_name"],
            "artist_names": row["artist_names"],
            "album_name": row["album_name"],
            "album_image_url": row.get("album_image_url"),
            "score": float(score),
        })

        if len(results) >= k:
            break

    return results


def search_by_track_id(track_id, k=5, db=DB_FILENAME, output_dir=INDEX_DIRNAME):
    index, metadata, config = _ensure_index(db=db, output_dir=output_dir)

    matches = metadata[metadata["track_id"] == track_id]
    if matches.empty:
        fresh_tracks = _load_tracks(Path(db))
        fresh_matches = fresh_tracks[fresh_tracks["track_id"] == track_id]
        if fresh_matches.empty:
            raise ValueError(f"Track id not found in music.sqlite: {track_id}")
        query_values = fresh_matches.iloc[0].to_dict()
    else:
        query_values = matches.iloc[0].to_dict()

    query_vector = _query_to_vector(query_values, config)
    candidate_count = min(max(k * 10, k + 10), index.ntotal)
    distances, indices = index.search(query_vector, candidate_count)
    return _format_results(metadata, distances, indices, k, exclude_track_id=track_id)


def search_by_features(bpm=None, popularity=None, loudness=None, acousticness=None, danceability=None, energy=None,
                       instrumentalness=None, liveness=None, speechiness=None, valence=None, key=None, mode=None,
                       camelot=None, k=5, db=DB_FILENAME, output_dir=INDEX_DIRNAME, ):
    index, metadata, config = _ensure_index(db=db, output_dir=output_dir)

    query_values = {
        "bpm": bpm,
        "popularity": popularity,
        "loudness": loudness,
        "acousticness": acousticness,
        "danceability": danceability,
        "energy": energy,
        "instrumentalness": instrumentalness,
        "liveness": liveness,
        "speechiness": speechiness,
        "valence": valence,
        "key": key,
        "mode": mode,
        "camelot": camelot,
    }
    query_values = {name: value for name, value in query_values.items() if value is not None}

    if not query_values:
        raise ValueError("Provide at least one feature value.")

    query_vector = _query_to_vector(query_values, config)
    candidate_count = min(max(k * 10, k + 10), index.ntotal)
    distances, indices = index.search(query_vector, candidate_count)
    return _format_results(metadata, distances, indices, k)