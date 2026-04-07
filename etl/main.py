from .api import build_api
from .stages import recently_played, get_all_playlist_items, track_features


def main():
    api = build_api()
    current_user_id = api.current_user()['id']

    recently_played(api)
    get_all_playlist_items(api, current_user_id)
    track_features()


if __name__ == "__main__":
    main()