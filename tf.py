import json
import os

# suppress TF logs
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"
os.environ["TF_ENABLE_ONEDNN_OPTS"] = "0"

import essentia
import essentia.standard as es

essentia.log.debugActive = False
essentia.log.infoActive = False
essentia.log.warningActive = False
essentia.log.errorActive = False


def extract_labels_from_metadata(meta):
    candidates = [["classes"], ["schema", "classes"], ["classes", "names"], ["model", "classes"], ["annotations"]]

    for path in candidates:
        cur = meta
        ok = True
        for key in path:
            if isinstance(cur, dict) and key in cur:
                cur = cur[key]
            else:
                ok = False
                break
        if not ok:
            continue

        if isinstance(cur, list) and cur:
            if all(isinstance(x, str) for x in cur):
                return cur
            if all(isinstance(x, dict) and "name" in x for x in cur):
                return [x["name"] for x in cur]


def load_labels(metadata_json_path: str) -> list[str]:
    with open(metadata_json_path, "r", encoding="utf-8") as f:
        metadata_json = json.load(f)
        return extract_labels_from_metadata(metadata_json)


def mean_columns(matrix) -> list[float]:
    cols = len(matrix[0])
    total = [0.0] * cols

    for row in matrix:
        for i, v in enumerate(row):
            total[i] += float(v)

    return [v / len(matrix) for v in total]


def top_k(scores, k=5):
    return sorted(scores.items(), key=lambda kv: kv[1], reverse=True)[:k]


def extract_musicnn_embeddings(audio_path):
    audio = es.MonoLoader(filename=audio_path, sampleRate=16000, resampleQuality=4)()
    model = es.TensorflowPredictMusiCNN(graphFilename="models/msd-musicnn-1.pb", output="model/dense/BiasAdd")
    return model(audio)


def extract_discogs_effnet_embeddings(audio_path):
    audio = es.MonoLoader(filename=audio_path, sampleRate=16000, resampleQuality=4)()
    model = es.TensorflowPredictEffnetDiscogs(graphFilename="models/discogs-effnet-bs64-1.pb",
                                              output="PartitionedCall:1")
    return model(audio)


def classify(embeddings, graph_filename, metadata_json, output_node="model/Softmax"):
    graph_filename = "models/" + graph_filename
    metadata_json = "models/" + metadata_json
    model = es.TensorflowPredict2D(graphFilename=graph_filename, output=output_node)
    preds = model(embeddings)
    avg = mean_columns(preds)
    labels = load_labels(metadata_json)

    return dict(zip(labels, avg))


def multilabel(embeddings, graph_filename, metadata_json, output_node="model/Sigmoid"):
    graph_filename = "models/" + graph_filename
    metadata_json = "models/" + metadata_json
    model = es.TensorflowPredict2D(graphFilename=graph_filename, output=output_node)
    preds = model(embeddings)
    avg = mean_columns(preds)
    labels = load_labels(metadata_json)

    return dict(zip(labels, avg))


def identity_reg(embeddings, graph_filename, output_node="model/Identity"):
    graph_filename = "models/" + graph_filename
    model = es.TensorflowPredict2D(graphFilename=graph_filename, output=output_node)
    preds = model(embeddings)
    return mean_columns(preds)


def normalize_note_name(note):
    enharmonic = {
        "A#": "Bb",
        "D#": "Eb",
        "G#": "Ab",
        "C#": "Db",
    }
    return enharmonic.get(note, note)


def extract_key_features(features):
    key_name = normalize_note_name(str(features["tonal.key_edma.key"]))
    mode = str(features["tonal.key_edma.scale"])
    score = float(features["tonal.key_edma.strength"])

    with open("camelot.txt", 'r') as c:
        lines = "".join([line.strip() for line in c.readlines()])
    PITCH_CLASS_NUM, CAMELOT_MINOR, CAMELOT_MAJOR = eval(lines)

    key_num = PITCH_CLASS_NUM[key_name]

    if mode == "minor":
        camelot = CAMELOT_MINOR[key_name]
    else:
        camelot = CAMELOT_MAJOR[key_name]

    return {
        "key": key_num,
        "note": key_name,
        "mode": mode,
        "camelot": camelot,
        "score": round(score, 4),
    }


def track_features(audio_path):
    try:
        # Low Level Features (BPM, Key, Scale)
        features, frame_features = es.MusicExtractor()(audio_path)
        result = extract_key_features(features)
        result["bpm"] = float(features["rhythm.bpm"])

        # Embeddings
        m_e = extract_musicnn_embeddings(audio_path)
        d_e = extract_discogs_effnet_embeddings(audio_path)

        # High Level Features (Mood, Vocals, Genre)
        danceability = classify(m_e, "danceability-msd-musicnn-1.pb", "danceability-msd-musicnn-1.json")
        aggressive = classify(m_e, "mood_aggressive-msd-musicnn-1.pb", "mood_aggressive-msd-musicnn-1.json")
        happy = classify(m_e, "mood_happy-msd-musicnn-1.pb", "mood_happy-msd-musicnn-1.json")
        party = classify(m_e, "mood_party-msd-musicnn-1.pb", "mood_party-msd-musicnn-1.json")
        relaxed = classify(m_e, "mood_relaxed-msd-musicnn-1.pb", "mood_relaxed-msd-musicnn-1.json")
        sad = classify(m_e, "mood_sad-msd-musicnn-1.pb", "mood_sad-msd-musicnn-1.json")
        acoustic = classify(m_e, "mood_acoustic-msd-musicnn-1.pb", "mood_acoustic-msd-musicnn-1.json")

        voice_instrumental = classify(m_e, "voice_instrumental-msd-musicnn-1.pb",
                                      "voice_instrumental-msd-musicnn-1.json")

        valence_arousal_raw = identity_reg(m_e, "deam-msd-musicnn-2.pb")
        scale = lambda x: max(0.0, min(1.0, (float(x) - 1.0) / 8.0))  # [1,9] --> [0,1]
        valence, arousal = tuple(map(scale, valence_arousal_raw))

        genre = multilabel(d_e, "mtg_jamendo_genre-discogs-effnet-1.pb", "mtg_jamendo_genre-discogs-effnet-1.json")

        # Pack and return
        result["danceability"] = round(danceability.get("danceable", 0.0), 4)
        result["energy"] = round(arousal, 4)
        result["acousticness"] = round(acoustic.get("acoustic", 0.0), 4)
        result["instrumentalness"] = round(voice_instrumental.get("instrumental", 0.0), 4)
        result["valence"] = round(valence, 4)

        result["dance_floor"] = round(0.6 * result["danceability"] + 0.4 * arousal, 4)
        result["happy"] = round(happy.get("happy", 0.0), 4)
        result["sad"] = round(sad.get("sad", 0.0), 4)
        result["chill"] = round(relaxed.get("relaxed", 0.0), 4)
        result["aggressive"] = round(aggressive.get("aggressive", 0.0), 4)
        result["hype"] = round(0.5 * party.get("party", 0.0) + 0.5 * arousal, 4)
        result["groove"] = round(0.6 * result["danceability"] + 0.4 * result["dance_floor"], 4)
        result["warmup"] = round(0.5 * result["chill"] + 0.5 * (1.0 - result["aggressive"]), 4)
        result["peak_time"] = round(result["hype"], 4)
        result["blendability"] = round(0.5 * result["score"] + 0.5 * (1.0 - result["aggressive"]), 4)
        result["vocal_risk"] = round(voice_instrumental.get("voice", 0.0), 4)

        result["genres"] = [name for name, score in top_k(genre, k=5) if score >= 0.15]

        return result

    finally:
        pass