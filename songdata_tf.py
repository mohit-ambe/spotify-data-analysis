import re
import time

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

options = Options()

options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("--window-size=1,1")

extract = lambda s: s[s.index(">") + 1:s.index("</")].replace("%", "")
parse = lambda s: extract(s) if not extract(s).isdigit() else int(extract(s))

STYLE_HTML = r"""<dt style="color:var(--dark-gray);font-size:1.25rem">"""
FEATURE_TAG_HTML = r"""<dt>[a-zA-Z]+</dt>"""
PAGE_TITLE = r"""Analysis | SongData.io"""


def track_features(track_ids):
    try:
        driver = webdriver.Chrome(options=options)
        driver.get(f"https://songdata.io/")
        driver.implicitly_wait(10)
        time.sleep(5)
        items = []

        for track_id in track_ids:
            driver.implicitly_wait(10)
            data = {
                "track_id": track_id
            }

            driver.get(f"https://songdata.io/track/{track_id}")
            html = driver.page_source

            # wait for data to load
            while PAGE_TITLE not in html:
                driver.refresh()
                driver.implicitly_wait(5)
                time.sleep(3)
                html = driver.page_source

            lines = list(map(str.strip, html.split("\n")))
            for i, line in enumerate(lines):
                if STYLE_HTML in line or re.match(FEATURE_TAG_HTML, line):
                    # <dt> tags enclose features - use this marker to extract
                    # also perform special formatting for key (note and mode)
                    if parse(line).lower() == 'key':
                        value = parse(lines[i + 1]).split(" ")
                        data['key'] = value[0]
                        data['mode'] = value[1]
                    else:
                        data[parse(line)] = parse(lines[i + 1])

            items.append(data)

        driver.quit()
        return items

    except Exception:
        driver.quit()
        return [dict() for _ in range(len(track_ids))]