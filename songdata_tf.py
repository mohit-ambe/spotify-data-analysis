import re
import time

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

options = Options()

options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("--window-size=1,1")

driver = webdriver.Chrome(options=options)

try:
    driver.get(f"https://songdata.io/")
    driver.implicitly_wait(10)
    time.sleep(5)
except Exception:
    pass

extract = lambda s: s[s.index(">") + 1:s.index("</")].replace("%", "")
parse = lambda s: extract(s) if not extract(s).isdigit() else int(extract(s))

STYLE_HTML = r"""<dt style="color:var(--dark-gray);font-size:1.25rem">"""
FEATURE_TAG_HTML = r"""<dt>[a-zA-Z]+</dt>"""
ALBUM_ART_CLASS = r"""'class=\"album-art-container\"'"""


def track_features(track_id):
    try:
        driver.implicitly_wait(10)
        data = {
            "track_id": track_id
        }

        driver.get(f"https://songdata.io/track/{track_id}")
        html = driver.page_source

        while ALBUM_ART_CLASS not in html:
            driver.refresh()
            time.sleep(1)
            html = driver.page_source

        lines = list(map(str.strip, html.split("\n")))
        for i, line in enumerate(lines):
            if STYLE_HTML in line or re.match(FEATURE_TAG_HTML, line):
                if parse(line).lower() == 'key':
                    value = parse(lines[i + 1]).split(" ")
                    data['key'] = value[0].replace("♭", "b")
                    data['mode'] = value[1].lower()
                else:
                    data[parse(line).lower()] = parse(lines[i + 1])

        return data

    except Exception:
        return dict()