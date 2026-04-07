import spotipy
from spotipy.oauth2 import SpotifyOAuth


def build_api():
    with open('scopes.txt', 'r') as scopes:
        all_scopes = " ".join([scope.strip() for scope in scopes.readlines()])

    return spotipy.Spotify(
        auth_manager=SpotifyOAuth(
            scope=all_scopes,
            open_browser=True,
            cache_path='.spotify_cache',
        )
    )
