import argparse
import json
import sqlite3
from pathlib import Path

import faiss
import numpy as np
import pandas as pd
from sklearn.cluster import AgglomerativeClustering, Birch, DBSCAN, KMeans
from sklearn.compose import ColumnTransformer
from sklearn.decomposition import PCA
from sklearn.impute import SimpleImputer
from sklearn.metrics import calinski_harabasz_score, davies_bouldin_score, silhouette_score
from sklearn.mixture import GaussianMixture
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

DB_FILENAME = "music.sqlite"
OUTPUT_DIRNAME = "clustering"

NUMERIC_FEATURES = ["bpm", "popularity", "loudness", "acousticness", "danceability", "energy", "instrumentalness",
                    "liveness", "speechiness", "valence", ]

CATEGORICAL_FEATURES = ["key", "mode", "camelot", ]

MODEL_FEATURES = NUMERIC_FEATURES + CATEGORICAL_FEATURES
RANDOM_STATE = 42


def make_one_hot_encoder():
    try:
        return OneHotEncoder(handle_unknown="ignore", sparse_output=False)
    except TypeError:
        return OneHotEncoder(handle_unknown="ignore", sparse=False)


def load_dataset(db_path: Path) -> pd.DataFrame:
    print(f"[cluster_tracks] Loading TrackFeatures dataset from {db_path}...")
    query = """
        SELECT
            tf.track_id,
            t.name AS track_name,
            al.name AS album_name,
            al.image_url AS album_image_url,
            GROUP_CONCAT(ar.name, ', ') AS artist_names,
            tf.bpm,
            tf."key" AS "key",
            tf.mode,
            tf.camelot,
            tf.popularity,
            tf.loudness,
            tf.acousticness,
            tf.danceability,
            tf.energy,
            tf.instrumentalness,
            tf.liveness,
            tf.speechiness,
            tf.valence
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
            tf."key",
            tf.mode,
            tf.camelot,
            tf.popularity,
            tf.loudness,
            tf.acousticness,
            tf.danceability,
            tf.energy,
            tf.instrumentalness,
            tf.liveness,
            tf.speechiness,
            tf.valence
        ORDER BY t.name, tf.track_id
    """

    with sqlite3.connect(db_path) as connection:
        df = pd.read_sql_query(query, connection)
    print(f"[cluster_tracks] Loaded {len(df)} raw rows from SQLite.")

    raw_feature_columns = NUMERIC_FEATURES + CATEGORICAL_FEATURES
    df["available_feature_count"] = df[raw_feature_columns].notna().sum(axis=1)
    df["missing_feature_count"] = len(raw_feature_columns) - df["available_feature_count"]
    df = df[df["missing_feature_count"] == 0]
    print(f"[cluster_tracks] Retained {len(df)} rows after removing incomplete feature rows.")
    return df


def build_preprocessor() -> ColumnTransformer:
    numeric_pipeline = Pipeline(steps=[("imputer", SimpleImputer(strategy="median")), ("scaler", StandardScaler()), ])
    categorical_pipeline = Pipeline(
        steps=[("imputer", SimpleImputer(strategy="most_frequent")), ("one_hot", make_one_hot_encoder()), ])

    return ColumnTransformer(transformers=[("numeric", numeric_pipeline, NUMERIC_FEATURES),
                                           ("categorical", categorical_pipeline, CATEGORICAL_FEATURES), ],
                             sparse_threshold=0.0, )


def reduce_features(df: pd.DataFrame):
    print("[cluster_tracks] Building preprocessing pipeline...")
    preprocessor = build_preprocessor()
    print("[cluster_tracks] Encoding and scaling features...")
    encoded = preprocessor.fit_transform(df[MODEL_FEATURES])
    print(f"[cluster_tracks] Encoded feature matrix shape: {encoded.shape}")

    max_components = min(encoded.shape[0], encoded.shape[1])
    if max_components > 1:
        print("[cluster_tracks] Running PCA dimensionality reduction...")
        reducer = PCA(n_components=0.95, random_state=RANDOM_STATE)
        reduced = reducer.fit_transform(encoded)
        explained_variance = float(np.sum(reducer.explained_variance_ratio_))
        n_components = int(reducer.n_components_)
        print("[cluster_tracks] PCA complete: "
              f"{encoded.shape[1]} -> {n_components} dimensions "
              f"(explained_variance={explained_variance:.4f})")
    else:
        reduced = encoded
        explained_variance = 1.0
        n_components = max_components
        print("[cluster_tracks] Skipping PCA because the matrix has one effective component.")

    return reduced, {
        "rows": int(encoded.shape[0]),
        "encoded_dimensions": int(encoded.shape[1]),
        "reduced_dimensions": int(n_components),
        "explained_variance": explained_variance,
    }


def score_labels(matrix: np.ndarray, labels: np.ndarray):
    labels = np.asarray(labels)
    clustered_mask = labels != -1
    clustered_labels = labels[clustered_mask]
    assigned_points = int(np.sum(clustered_mask))
    total_points = int(labels.shape[0])

    unique_labels = np.unique(clustered_labels)
    if unique_labels.size < 2:
        return None

    clustered_matrix = matrix[clustered_mask]
    if clustered_matrix.shape[0] <= unique_labels.size:
        return None

    return {
        "n_clusters": int(unique_labels.size),
        "assigned_points": assigned_points,
        "coverage_ratio": float(assigned_points / total_points),
        "noise_points": int(np.sum(labels == -1)),
        "silhouette": float(silhouette_score(clustered_matrix, clustered_labels)),
        "calinski_harabasz": float(calinski_harabasz_score(clustered_matrix, clustered_labels)),
        "davies_bouldin": float(davies_bouldin_score(clustered_matrix, clustered_labels)),
    }


def evaluate_partitioning_algorithms(matrix: np.ndarray):
    print("[cluster_tracks] Evaluating partitioning algorithms (KMeans, Agglomerative, Birch, GMM)...")
    results = []
    upper_k = min(8, matrix.shape[0] - 1)

    for k in range(2, upper_k + 1):
        print(f"[cluster_tracks] Testing partitioning algorithms with k={k}...")
        candidates = [(f"kmeans_k{k}", KMeans(n_clusters=k, n_init=20, random_state=RANDOM_STATE),),
                      (f"agglomerative_k{k}", AgglomerativeClustering(n_clusters=k),),
                      (f"birch_k{k}", Birch(n_clusters=k),), (f"gmm_k{k}",
                                                              GaussianMixture(n_components=k, covariance_type="full",
                                                                              random_state=RANDOM_STATE),), ]

        for name, estimator in candidates:
            print(f"[cluster_tracks] Running {name}...")
            labels = estimator.fit_predict(matrix)
            metrics = score_labels(matrix, labels)
            if metrics is None:
                print(f"[cluster_tracks] {name} did not produce a valid multi-cluster result.")
                continue
            print(f"[cluster_tracks] {name} complete: "
                  f"clusters={metrics['n_clusters']}, "
                  f"coverage={metrics['coverage_ratio']:.3f}, "
                  f"silhouette={metrics['silhouette']:.4f}")
            results.append({
                "algorithm": name,
                "labels": labels,
                "metrics": metrics,
            })

    return results


def evaluate_dbscan(matrix: np.ndarray):
    print("[cluster_tracks] Evaluating DBSCAN parameter sweep...")
    results = []
    for eps in (0.7, 0.9, 1.1, 1.3, 1.5, 1.8, 2.1):
        for min_samples in (4, 6, 8, 10):
            name = f"dbscan_eps{eps}_min{min_samples}"
            print(f"[cluster_tracks] Running {name}...")
            estimator = DBSCAN(eps=eps, min_samples=min_samples)
            labels = estimator.fit_predict(matrix)
            metrics = score_labels(matrix, labels)
            if metrics is None:
                print(f"[cluster_tracks] {name} did not produce a valid multi-cluster result.")
                continue
            print(f"[cluster_tracks] {name} complete: "
                  f"clusters={metrics['n_clusters']}, "
                  f"coverage={metrics['coverage_ratio']:.3f}, "
                  f"silhouette={metrics['silhouette']:.4f}")
            results.append({
                "algorithm": name,
                "labels": labels,
                "metrics": metrics,
            })
    return results


def choose_best_result(results):
    print(f"[cluster_tracks] Choosing the best result from {len(results)} valid clustering runs...")
    if not results:
        raise RuntimeError("No clustering algorithm produced a valid multi-cluster result.")

    practical_results = [item for item in results if item["metrics"]["coverage_ratio"] >= 0.6]
    candidate_pool = practical_results if practical_results else results

    best = max(candidate_pool, key=lambda item: (
        item["metrics"]["silhouette"], item["metrics"]["calinski_harabasz"], -item["metrics"]["davies_bouldin"],), )
    print(f"[cluster_tracks] Best result is {best['algorithm']} "
          f"with silhouette={best['metrics']['silhouette']:.4f} "
          f"and {best['metrics']['n_clusters']} clusters.")
    return best


def build_assignments(df: pd.DataFrame, best_result):
    assignments = df[
        ["track_id", "track_name", "artist_names", "album_name", "album_image_url"] + MODEL_FEATURES].copy()
    assignments["cluster"] = best_result["labels"]
    assignments["algorithm"] = best_result["algorithm"]
    return assignments


def build_assignments_for_result(df: pd.DataFrame, result):
    assignments = df[
        ["track_id", "track_name", "artist_names", "album_name", "album_image_url"] + MODEL_FEATURES].copy()
    assignments["cluster"] = result["labels"]
    assignments["algorithm"] = result["algorithm"]
    return assignments


def build_cluster_profiles(assignments: pd.DataFrame) -> pd.DataFrame:
    clustered = assignments[assignments["cluster"] != -1].copy()
    if clustered.empty:
        return pd.DataFrame()

    aggregations = {feature: "mean" for feature in NUMERIC_FEATURES}
    aggregations["track_id"] = "count"
    profiles = clustered.groupby("cluster", as_index=False).agg(aggregations)
    profiles = profiles.rename(columns={
        "track_id": "track_count"
    })
    return profiles.sort_values("cluster")


def serialize_results(all_results, preprocessing_summary, best_result):
    score_table = []
    for item in all_results:
        score_table.append({
            "algorithm": item["algorithm"], **item["metrics"],
        })

    score_table.sort(key=lambda row: (row["silhouette"], row["calinski_harabasz"], -row["davies_bouldin"],),
                     reverse=True, )

    return {
        "preprocessing": preprocessing_summary,
        "best_algorithm": {
            "algorithm": best_result["algorithm"], **best_result["metrics"],
        },
        "algorithm_scores": score_table,
    }


def ensure_output_dir(output_dir: Path) -> Path:
    print(f"[cluster_tracks] Ensuring output directory exists at {output_dir}...")
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def save_all_algorithm_assignments(df: pd.DataFrame, all_results, output_dir: Path):
    print("[cluster_tracks] Writing per-algorithm assignment files...")
    manifest_rows = []
    all_assignments = []

    for result in all_results:
        assignments = build_assignments_for_result(df, result)
        filename = f"{result['algorithm']}_assignments.csv"
        print(f"[cluster_tracks] Writing {filename}...")
        assignments.to_csv(output_dir / filename, index=False)
        all_assignments.append(assignments)
        manifest_rows.append({
            "algorithm": result["algorithm"],
            "file": filename, **result["metrics"],
        })

    print("[cluster_tracks] Writing algorithm_manifest.csv...")
    pd.DataFrame(manifest_rows).sort_values(by=["silhouette", "calinski_harabasz", "davies_bouldin"],
                                            ascending=[False, False, True], ).to_csv(
        output_dir / "algorithm_manifest.csv", index=False)

    print("[cluster_tracks] Writing all_algorithm_assignments.csv...")
    pd.concat(all_assignments, ignore_index=True).to_csv(output_dir / "all_algorithm_assignments.csv", index=False, )


def main():
    print("[cluster_tracks] Starting clustering pipeline...")
    parser = argparse.ArgumentParser(
        description="Cluster songs from the TrackFeatures table using multiple algorithms.")
    parser.add_argument("--db", default=DB_FILENAME, help="Path to the SQLite database.")
    parser.add_argument("--output-dir", default=OUTPUT_DIRNAME,
                        help="Directory where clustering artifacts will be written.", )
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    dataset = load_dataset(db_path)
    if dataset.empty:
        raise RuntimeError("TrackFeatures is empty. Run the ETL before clustering.")

    print("[cluster_tracks] Reducing features for clustering...")
    matrix, preprocessing_summary = reduce_features(dataset)
    print("[cluster_tracks] Running clustering algorithms...")
    all_results = evaluate_partitioning_algorithms(matrix) + evaluate_dbscan(matrix)
    best_result = choose_best_result(all_results)

    print("[cluster_tracks] Building output tables...")
    assignments = build_assignments(dataset, best_result)
    profiles = build_cluster_profiles(assignments)
    summary = serialize_results(all_results, preprocessing_summary, best_result)

    output_dir = ensure_output_dir(Path(args.output_dir))
    print("[cluster_tracks] Writing all_tracks.csv...")
    dataset.to_csv(output_dir / "all_tracks.csv", index=False)
    save_all_algorithm_assignments(dataset, all_results, output_dir)
    print("[cluster_tracks] Writing track_cluster_assignments.csv...")
    assignments.to_csv(output_dir / "track_cluster_assignments.csv", index=False)
    print("[cluster_tracks] Writing cluster_profiles.csv...")
    profiles.to_csv(output_dir / "cluster_profiles.csv", index=False)
    print("[cluster_tracks] Writing clustering_summary.json...")
    with open(output_dir / "clustering_summary.json", "w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2)

    print(f"Loaded {len(dataset)} tracks from {db_path}")
    print(f"Best algorithm: {summary['best_algorithm']['algorithm']} "
          f"(clusters={summary['best_algorithm']['n_clusters']}, "
          f"silhouette={summary['best_algorithm']['silhouette']:.4f})")
    print(f"Wrote per-algorithm assignment files to {output_dir}")
    print(f"Wrote assignments to {output_dir / 'track_cluster_assignments.csv'}")
    print(f"Wrote profiles to {output_dir / 'cluster_profiles.csv'}")
    print(f"Wrote summary to {output_dir / 'clustering_summary.json'}")


if __name__ == "__main__":
    main()