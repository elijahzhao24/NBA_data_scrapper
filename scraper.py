import os, psycopg2, requests
from dotenv import load_dotenv
from bs4 import BeautifulSoup


load_dotenv()  
URL = 'https://www.spotrac.com/nba/rankings/player/_/year/2025/sort/cash_total'
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/117.0.0.0 Safari/537.36"),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.spotrac.com/",
    "Connection": "keep-alive",
}


def fetch_page(url, session=None, tries=3):
    s = session or requests.Session()
    s.headers.update(HEADERS)
    for attempt in range(tries):
        resp = s.get(url, timeout=20)
        print("status:", resp.status_code)
        if resp.status_code == 200:
            return resp.text
        # simple backoff
        time.sleep(1 + attempt)
    raise RuntimeError(f"Failed to fetch {url} (last status {resp.status_code})")

session = requests.Session()
html = fetch_page(URL, session=session)
print(html[:800])
# html = requests.get(url).text
# soup = BeautifulSoup(html, "lxml")
# body1 = soup.find("body")



DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL not set")

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

cur.execute("SELECT NOW();")
print("Connected to Postgres at:", cur.fetchone())
cur.close()
conn.close()