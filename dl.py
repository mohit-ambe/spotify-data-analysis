import yt_dlp


def download_track(isrc, query):
    ydl_opts = {
        "quiet": True,
        "no_warnings": True,

        'format': 'bestaudio/best',
        'outtmpl': 'track_downloads/%(id)s',

        'noplaylist': True,
        'extract_audio': True,

        'audio_format': 'mp3',
        'audio_quality': '0',

        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '0',
        }, ],
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        try:
            info = ydl.extract_info(f"ytsearch1:{isrc} - {query}", download=False)
            url = info['entries'][0]['webpage_url']
            ydl.download([url])
            return url
        except Exception:
            return ""