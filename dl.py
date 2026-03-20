import yt_dlp


def download_track(isrc, query):
    ydl_opts = {
        "quiet": True,
        "no_warnings": True,

        "format": "bestaudio/best",
        "outtmpl": "track_downloads/%(id)s",

        "extract_flat": True,
        "playlist_items": "3",

        "download-sections": "*0:00-5:00",
        "match-filter": "duration < 300",
        "max-filesize": "2M",

        "noplaylist": True,
        "extract_audio": True,
        "ignore-errors": True,

        "audio_format": "mp3",
        "audio_quality": "0",

        "postprocessors": [{
            "key": "FFmpegExtractAudio",
            "preferredcodec": "mp3",
            "preferredquality": "0",
        }],
    }

    def download(ydl, url, song, artist):
        info = ydl.extract_info(url, download=False)
        entries = info.get("entries", [])
        if not entries:
            return ""
        print(entries[0])
        title = entries[0].get("title", "").lower()
        desc = entries[0].get("description", "").lower()
        if song in title or artist in title or song in desc or artist in desc:
            url = entries[0].get("url", "")
            try:
                if url:
                    error_code = ydl.download([url])
                    if not error_code:
                        return url
            except Exception:
                print("Exception found")
                return ""

        return ""

    song = query[:query.rfind("-") - 1].lower()
    artist = query[query.rfind("-") + 2:].lower()

    search_urls = ["https://music.youtube.com/search?q={} - {}".format(isrc, query),
                   "https://music.youtube.com/search?q={}".format(query),
                   "https://youtube.com/search?q={} - {}".format(isrc, query),
                   "https://youtube.com/search?q={}".format(query)]

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        for search_url in search_urls:
            url = download(ydl, search_url, song, artist)
            if url:
                return url

    return ""