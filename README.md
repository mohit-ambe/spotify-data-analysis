# Spotify Data Analysis

A local Spotify analysis app for importing Spotify listening data, clustering tracks in feature space, and generating
mood-based playlists from similarity search.

## Features

- Import current Spotify data into `music.sqlite` from the landing page.
- Run ETL and clustering from the web app with a live import status flow.
- Explore generated clustering outputs in a 3D interactive viewer.
- Switch clustering methods and parameters from the UI without uploading CSV files manually.
- Build playlists from mood prompts like `study`, `workout`, `sleep`, and `party`.
- Generate larger playlists from 5 seed picks using FAISS-based similarity search.
- Show album art in playlist picks and generated playlist results.

## Current Architecture

- `server.py`
  Local HTTP server, import job runner, and API endpoints.
- `frontend/`
  Landing page, clustering page, playlist page, shared styles, and browser logic.
- `etl/`
  Refactored ETL package split into:
    - `api.py`
    - `extracts.py`
    - `transforms.py`
    - `loads.py`
    - `reads.py`
    - `stages.py`
    - `main.py`
- `etl.py`
  Original legacy ETL script kept in the repo unchanged.
- `clustering.py`
  Multi-algorithm clustering pipeline that writes outputs to `clustering/`.
- `similarity_search.py`
  Builds and queries the FAISS similarity index stored in `vector_db/`.
- `music.sqlite`
  Local SQLite database used by ETL, clustering, and similarity search.
- `clustering/`
  Generated clustering CSVs and summaries used directly by the frontend.
- `vector_db/`
  Saved FAISS index and metadata used by playlist generation.

## Tools Used

- Python
- SQLite
- Spotipy
- pandas / numpy
- scikit-learn
- FAISS
- Selenium

## Requirements

The main Python dependencies are listed
in [requirements.txt](C:\Users\mohit\PycharmProjects\spotify-data-analysis\requirements.txt):

- `numpy`
- `pandas`
- `scikit-learn`
- `faiss-cpu`
- `spotipy`
- `selenium`

## Spotify Setup

Before running imports:

1. Create a Spotify app in the Spotify Developer Dashboard.
2. Configure your Spotify client credentials for Spotipy.
3. Make sure `scopes.txt` exists in the project root.
4. Keep `music.sqlite` in the project root.

## How To Use

### 1. Start the server

```powershell
py server.py
```

### 2. Import music

On the landing page:

- click `Import From Spotify`
- wait for the ETL + clustering pipeline to finish

This updates:

- `music.sqlite`
- files in `clustering/`
- files in `vector_db/`

### 3. Explore clustering

Go to `/clustering` and:

- choose a clustering method
- adjust its parameters
- drag to orbit
- zoom to inspect spatial neighborhoods
- focus clusters and inspect tracks

The clustering page reads generated assignment files directly from `clustering/`.

### 4. Generate playlists

Go to `/playlist` and:

- choose a mood
- pick 1 of 3 songs for 5 rounds
- choose a playlist length
- generate the final playlist

The playlist page reads track data from `clustering/all_tracks.csv`