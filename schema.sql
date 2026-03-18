CREATE TABLE Artists
(
    id         VARCHAR(255) PRIMARY KEY,
    name       VARCHAR(255),
    artist_url VARCHAR(255)
);

CREATE TABLE Albums
(
    id           VARCHAR(255) PRIMARY KEY,
    name         VARCHAR(255),
    album_type   VARCHAR(50),
    release_date VARCHAR(20),
    total_tracks INT,
    image_url    VARCHAR(255),
    album_url    VARCHAR(255)
);

CREATE TABLE AlbumArtists
(
    album_id     VARCHAR(255),
    artist_id    VARCHAR(255),
    artist_order INT,
    PRIMARY KEY (album_id, artist_id),
    FOREIGN KEY (album_id) REFERENCES Albums (id),
    FOREIGN KEY (artist_id) REFERENCES Artists (id)
);

CREATE TABLE Tracks
(
    id           VARCHAR(255) PRIMARY KEY,
    name         VARCHAR(255),
    album_id     VARCHAR(255),
    disc_number  INT,
    duration_ms  INT,
    explicit     BOOLEAN,
    track_number INT,
    track_url    VARCHAR(255),
    FOREIGN KEY (album_id) REFERENCES Albums (id)
);

CREATE TABLE TrackArtists
(
    track_id     VARCHAR(255),
    artist_id    VARCHAR(255),
    artist_order INT,
    PRIMARY KEY (track_id, artist_id),
    FOREIGN KEY (track_id) REFERENCES Tracks (id),
    FOREIGN KEY (artist_id) REFERENCES Artists (id)
);

CREATE TABLE TrackEvents
(
    track_id  VARCHAR(255),
    played_at DATETIME,
    PRIMARY KEY (track_id, played_at),
    FOREIGN KEY (track_id) REFERENCES Tracks (id)
);

CREATE TABLE TrackQueries
(
    track_id VARCHAR(255) PRIMARY KEY,
    isrc     VARCHAR(255),
    query    VARCHAR(255),
    FOREIGN KEY (track_id) REFERENCES Tracks (id)
);

CREATE TABLE Playlists
(
    playlist_id  VARCHAR(255) PRIMARY KEY,
    name         VARCHAR(255),
    tracks       INT,
    playlist_url VARCHAR(255),
    image_url    VARCHAR(255)
);

CREATE TABLE PlaylistTracks
(
    playlist_id    VARCHAR(255),
    playlist_order INT,
    track_id       VARCHAR(255),
    PRIMARY KEY (playlist_id, playlist_order)
);