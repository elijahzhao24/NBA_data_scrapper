import os, psycopg2, requests
import pandas as pd
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import csv
import hashlib
from datetime import datetime
import difflib
from psycopg2.extras import execute_values
import re
import time
import unicodedata
from urllib.parse import urljoin, unquote
import json
import mimetypes
from rembg import new_session, remove
from PIL import Image
import io




class Player:
    def __init__(self, name, team, salary, code):
        self.name = name
        self.team = team
        self.salary = salary
        self.code = code
    
    def __str__(self):
        return f"({self.name})({self.team})({self.salary})({self.code})"


# given a list of li items, get the player contents, make a player object, and append to a list
def listPlayerObjects(soup):
    temp = []

    # iterate through all li items
    for p in soup:

        # make sure its a list item with a player
        a = p.find("a")
        if not a:
            continue

        # get all the required fields from the soup for player class
        name = a.get_text(strip=True)
        code = a.get('href') # example: "https://www.spotrac.com/redirect/player/84769"

        # get the unique number code for each player
        for i in range(len(code) - 1, -1, -1):
            if (code[i] == "/"):
                code = code[i+1:len(code)]
                break
        
        team = p.small.get_text(strip=True)[0:3]
        salary = p.find("span", class_="medium").get_text(strip=True)

        # create player and add to list
        nbaplayer = Player(name, team, salary, code) 
        temp.append(nbaplayer)

        #debugging
        print(nbaplayer)

    return temp

# Not used as we are directly uploading to supabase db 
def export_csvs(players, season_year, teams_filename='teams.csv', players_filename='players.csv'):
    teams_seen = {}

    for p in players:
        # ormalize the team code to uppercase and strip whitespace
        team_code = (p.team or "").strip().upper()

        if not team_code:
            continue

        # Comment: get full name if available
        team_name = NBA_TEAMS.get(team_code, "Error")
        teams_seen[team_code] = {"code": team_code, "name": team_name}
    

    with open(teams_filename, 'w', newline='', encoding='utf-8') as tf:

        # columns stored in postgres table
        fieldnames = ['code', 'name']

        writer = csv.DictWriter(tf, fieldnames=fieldnames)
        writer.writeheader()

        # use sorted so SERIAL PRIMARY KEY is easier to map into player column
        for tcode in sorted(teams_seen.keys()):
            writer.writerow(teams_seen[tcode])

        now_iso = datetime.now().isoformat()
        with open(players_filename, 'w', newline='', encoding='utf-8') as pf:

            # columns stored in postgres table
            fieldnames = ['site_player_id', 'name', 'team', 'year', 'salary', 'row_hash', 'last_scrape']

            writer = csv.DictWriter(pf, fieldnames=fieldnames)
            writer.writeheader()

            for p in players:
                team_code = (p.team or "").strip().upper()
                if not team_code:
                    #Skip or handle players missing team codes. Here we skip.
                    continue

                # remove '$' and ',' and cast to int
                salary_text = str(p.salary) if p.salary is not None else ""

                # Remove dollar sign and thousands separators, then trim whitespace
                salary_text = salary_text.replace("$", "").replace(",", "").strip()

                # If after cleaning we have something, convert to int; otherwise leave as None (empty CSV cell)
                try:
                    salary_val = int(salary_text) if salary_text != "" else None
                except ValueError:
                    salary_val = None

                # Create a hash for the row for duplicate detection
                hash_source = f"{p.code}|{p.name}|{team_code}|{season_year}|{salary_val}"
                row_hash = hashlib.sha256(hash_source.encode('utf-8')).hexdigest()

                writer.writerow({
                    'site_player_id': p.code,
                    'name': p.name,
                    'team': team_code,
                    'year': int(season_year),
                    'salary': salary_val,
                    'row_hash': row_hash,
                    'last_scrape': now_iso
                })
        
        print(f"Exported {len(teams_seen)} unique teams -> {teams_filename}")
        print(f"Exported {sum(1 for _ in players)} players -> {players_filename}")


def build_teams_from_players(players):
    # Return a dict mapping team_code -> {'code': code, 'name': fullname}
    teams_seen = {}

    for p in players:
        code = (p.team or "").strip().upper()
        if not code:
            continue
        name = NBA_TEAMS.get(code, "ERROR TEAM NOT FOUND")  
        teams_seen[code] = {"code": code, "name": name}
    return teams_seen

def upsert_teams(conn, teams_seen):
    
    # Upsert teams into the teams table and return a mapping: code -> id


    # convert teams_seen dict to list of tuples for bulk insert
    rows = [(t['code'], t['name']) for t in teams_seen.values()]

    sql = """
    INSERT INTO teams (code, name)
    VALUES %s
    ON CONFLICT (code) DO UPDATE
      SET name = EXCLUDED.name   --  When there is duplicate, update team name
    RETURNING id, code;         --  return tuple with team code and team.id
    """

    with conn.cursor() as cur:

        # execute_values expands VALUES %s with many rows efficiently
        execute_values(cur, sql, rows, template="(%s, %s)")

        # fetchall returns list of (id, code) for inserted/updated rows
        returned = cur.fetchall()

    conn.commit()

    # build mapping code -> id for player one to many relation
    code_to_id = {row[1]: row[0] for row in returned}
    return code_to_id


def upsert_players(conn, players, code_to_id, season_year):

    # Upsert players into the players table.

    now_iso = datetime.now().isoformat()

    # Convert Player objects to list of tuples for bulk insert
    rows = []
    nplayers = 0

    for p in players:
        nplayers +=1
        team_code = (p.team or "").strip().upper()
        if not team_code or team_code not in code_to_id:
            continue  # Skip if not recognized

        # remove "$" and "," then cast to int from salary
        salary_val = int(p.salary.replace("$", "").replace(",", ""))

        # Create hash string
        hash_source = f"{p.code}|{p.name}|{team_code}|{season_year}|{salary_val}"
        row_hash = hashlib.sha256(hash_source.encode("utf-8")).hexdigest()

        rows.append((
            p.code,                 # site_player_id
            p.name,                 # name
            team_code,              # team (short code)
            code_to_id[team_code],  # team_id from teams table
            season_year,            # year
            salary_val,             # salary
            row_hash,               # row_hash
            now_iso                 # last_scrape
        ))

    sql = """
    INSERT INTO players
      (site_player_id, name, team, team_id, year, salary, row_hash, last_scrape)
    VALUES %s
    ON CONFLICT (site_player_id, year) DO UPDATE     -- if a player id and the same year exists, replace according data
      SET name = EXCLUDED.name,
          team = EXCLUDED.team,
          team_id = EXCLUDED.team_id,
          salary = EXCLUDED.salary,
          row_hash = EXCLUDED.row_hash,
          last_scrape = EXCLUDED.last_scrape;
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows,
            template="(%s, %s, %s, %s, %s, %s, %s, %s)"
        )
    conn.commit()
    return nplayers

def normalize_name_for_match(name: str) -> str:
    if not name:
        return ""
    # de-accent
    nfkd = unicodedata.normalize('NFKD', name)
    only_ascii = "".join([c for c in nfkd if not unicodedata.combining(c)])

    # lowercase, remove punctuation except hyphen and space
    s = re.sub(r"[^a-zA-Z0-9\-\s]", "", only_ascii).strip().lower()

    # collapse multiple spaces
    s = re.sub(r"\s+", " ", s)
    return s

def scrape_realgm_player_links(index_url='https://basketball.realgm.com/nba/players'):
 
    resp = requests.get(index_url, timeout=20)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.content, 'html.parser')
    td_elements = soup.find_all('td', {'data-th': 'Player'})
    mapping = {}
    duplicates = {}
    for td in td_elements:
        a = td.find('a')
        if not a:
            continue
        raw_name = a.get_text(strip=True)
        href = a.get('href')
        key = normalize_name_for_match(raw_name)
        if key in mapping:

            # handle duplicates by keeping first and logging
            duplicates.setdefault(key, []).append(href)
            continue
        mapping[key] = href
    if duplicates:
        print(f"[WARN] {len(duplicates)} duplicate normalized names found in RealGM index (kept first occurrence).")
    print(f"[OK] scraped RealGM index: {len(mapping)} players")
    return mapping


def get_realgm_headshot_info(relative_link):
    from urllib.parse import urljoin, unquote
    import re, unicodedata

    base = "https://basketball.realgm.com"
    full = urljoin(base, relative_link)

    r = requests.get(full, timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.content, "html.parser")

    image = None
    container = soup.find("div", class_="player_profile_headshot")
    if container:
        image = container.find("img")
    if not image:
        return "could not find"

    # Handle lazy-load attributes
    src = image.get("src") or image.get("data-src") or image.get("data-original")
    if not src:
        srcset = image.get("srcset")
        if srcset:
            # take the first candidate url
            src = srcset.split(",")[0].split()[0]

    if not src:
        return None

    src = urljoin(base, src)
    filename = unquote(src.split("/")[-1])

    # Prefer the RealGM player id from the link itself (e.g., .../Summary/104915)
    m = re.search(r"/Summary/(\d+)", relative_link)
    site_id = m.group(1) if m else None

    # Page title name (nice to have)
    h1 = soup.find("h1")
    player_name = h1.get_text(strip=True) if h1 else None

    return {"url": src, "filename": filename, "site_id": site_id, "player_name": player_name}

def download_image_bytes(url, timeout=20):
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.content

def upload_to_supabase_storage(remote_path, file_bytes, content_type=None):
    #Upload raw bytes to Supabase Storage using the REST endpoint.

    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
    SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET", "player-headshots")

    # determine content type if not provided
    if not content_type:
        content_type = mimetypes.guess_type(remote_path)[0] or "application/octet-stream"


    upload_url = f"{SUPABASE_URL}/storage/v1/object/{SUPABASE_BUCKET}/{remote_path}"
    headers = {
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "apikey": SUPABASE_SERVICE_KEY,
        "Content-Type": content_type
    }
    resp = requests.post(upload_url, headers=headers, data=file_bytes, timeout=60)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"Supabase upload failed ({resp.status_code}): {resp.text}")

    public_url = f"{SUPABASE_URL}/storage/v1/object/public/{SUPABASE_BUCKET}/{remote_path}"
    return public_url

def update_player_headshot_url(conn, site_player_id, season_year, headshot_url):

    sql = """
    UPDATE players
       SET headshot_url = COALESCE(%s, headshot_url)
     WHERE site_player_id = %s AND year = %s;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (headshot_url, str(site_player_id), int(season_year)))
    conn.commit()
    return True

# def remove_bg(image_bytes: bytes) -> bytes:

#     output_bytes = remove(image_bytes)
#     img = Image.open(io.BytesIO(output_bytes)).convert("RGBA")
#     buffer = io.BytesIO()
#     img.save(buffer, format="PNG")
#     return buffer.getvalue()

def upload_headshot_for_player(conn, player_obj, headshot_info, season_year=2025, sleep_after=0.25):

    """
    Given a Player object and headshot_info, upload the image and update DB.
    Returns the public_url on success, or None on failure.
    """
    
    if not headshot_info or not isinstance(headshot_info, dict) or not headshot_info.get("url"):
        print(f"[SKIP] invalid headshot_info for {player_obj.name}")
        return None

    try:
        # Download
        img_bytes = download_image_bytes(headshot_info["url"])
        # img_bytes = remove_bg(img_bytes)

        # 2) Choose extension and remote path
        raw_fname = headshot_info.get("filename") or ""
        ext = raw_fname.split(".")[-1].lower() if "." in raw_fname else "jpg"
        remote_path = f"headshots/{player_obj.code}.{ext}"

        # 3) Content-Type
        content_type = mimetypes.guess_type(remote_path)[0] or "image/jpeg"

        # 4) Upload
        public_url = upload_to_supabase_storage(remote_path, img_bytes, content_type=content_type)
        print(f"[OK] uploaded headshot for {player_obj.name} -> {public_url}")

        # 5) Update DB
        update_player_headshot_url(conn, player_obj.code, season_year, public_url)

        # polite pause
        time.sleep(sleep_after)
        return public_url

    except Exception as e:
        print(f"[ERROR] uploading headshot for {player_obj.name} (id={player_obj.code}): {e}")
        return None
    

def test_upload_one_player(conn, playerlist, realgm_index, season_year=2025):
    """
    Upload headshot for the first player in playerlist (safe test).
    Prints each step and returns (player, public_url) on success or (player, None) on failure.
    """
    if not playerlist:
        print("No players available in playerlist.")
        return None, None

    player = playerlist[0]  # change index if you want a different player
    print(f"TEST: trying player {player.name} (id={player.code})")

    # 1) find RealGM link
    norm = normalize_name_for_match(player.name)
    rel_link = realgm_index.get(norm)
    if not rel_link:
        print(f"[NO MATCH] RealGM index has no page for {player.name} (normalized='{norm}').")
        return player, None

    # 2) get headshot info
    info = get_realgm_headshot_info(rel_link)
    if not info or info == "could not find" or not info.get("url"):
        print(f"[NO IMAGE] Could not extract headshot info for {player.name} from {rel_link}")
        return player, None

    print(f"Found headshot URL: {info['url']} (filename={info.get('filename')})")

    # 3) download image bytes
    try:
        img_bytes = download_image_bytes(info['url'])
    except Exception as e:
        print(f"[ERROR] downloading image: {e}")
        return player, None

    # 4) pick extension and remote path (use player's site id)
    fname = info.get("filename") or ""
    ext = fname.split(".")[-1].lower() if "." in fname else "jpg"
    remote_path = f"headshots/{player.code}.{ext}"
    print(f"Uploading to Supabase as: {remote_path}")

    # 5) upload
    try:
        public_url = upload_to_supabase_storage(remote_path, img_bytes, content_type=None)
    except Exception as e:
        print(f"[ERROR] upload failed: {e}")
        return player, None

    print(f"[OK] uploaded -> {public_url}")

    # 6) update DB
    try:
        update_player_headshot_url(conn, player.code, season_year, public_url)
        print(f"[OK] DB updated: players.headshot_url for site_player_id={player.code}, year={season_year}")
    except Exception as e:
        print(f"[ERROR] DB update failed: {e}")
        return player, public_url

    return player, public_url

NBA_TEAMS = {
    "ATL": "Atlanta Hawks",
    "BOS": "Boston Celtics",
    "BKN": "Brooklyn Nets",
    "CHA": "Charlotte Hornets",
    "CHI": "Chicago Bulls",
    "CLE": "Cleveland Cavaliers",
    "DAL": "Dallas Mavericks",
    "DEN": "Denver Nuggets",
    "DET": "Detroit Pistons",
    "GSW": "Golden State Warriors",
    "HOU": "Houston Rockets",
    "IND": "Indiana Pacers",
    "LAC": "Los Angeles Clippers",
    "LAL": "Los Angeles Lakers",
    "MEM": "Memphis Grizzlies",
    "MIA": "Miami Heat",
    "MIL": "Milwaukee Bucks",
    "MIN": "Minnesota Timberwolves",
    "NOP": "New Orleans Pelicans",
    "NYK": "New York Knicks",
    "OKC": "Oklahoma City Thunder",
    "ORL": "Orlando Magic",
    "PHI": "Philadelphia 76ers",
    "PHX": "Phoenix Suns",
    "POR": "Portland Trail Blazers",
    "SAC": "Sacramento Kings",
    "SAS": "San Antonio Spurs",
    "TOR": "Toronto Raptors",
    "UTA": "Utah Jazz",
    "WAS": "Washington Wizards",
}
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET", "player-headshots")
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

realgm_index = scrape_realgm_player_links() 

load_dotenv()  

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
soup = BeautifulSoup(html, "lxml")
body1 = soup.find("body")
main1 = body1.find("main")
ul1 = main1.find("ul", class_=["list-group", "mb-4", "not-premium"])
lis = ul1.find_all(class_ = ["list-group-item"])

playerlist = listPlayerObjects(lis)
#export_csvs(playerlist, season_year=2025)





DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL not set")

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor() 

cur.execute("SELECT NOW();")
print("Connected to Postgres at:", cur.fetchone())

teams_seen = build_teams_from_players(playerlist)
# Comment: upsert_teams returns a dict mapping team_code -> teams.id
code_to_id = upsert_teams(conn, teams_seen)
n_players = upsert_players(conn, playerlist, code_to_id, season_year=2025)

print(f"Upserted {len(code_to_id)} teams and processed {n_players} player rows.")

# pl, url = test_upload_one_player(conn, playerlist, realgm_index, season_year=2025)
# if url:
#     print("Test upload succeeded:", url)
# else:
#     print("Test upload failed for player:", pl.name if pl else "(none)")

for p in playerlist:
    norm = normalize_name_for_match(p.name)
    rel_link = realgm_index.get(norm)
    if not rel_link:
        # optional: you can add difflib fuzzy fallback here if you want
        print(f"[NO MATCH] RealGM page not found for {p.name}")
        continue

    info = get_realgm_headshot_info(rel_link)
    if not info or info == "could not find":
        print(f"[NO IMAGE] RealGM page has no headshot for {p.name}")
        continue

    upload_headshot_for_player(conn, p, info, season_year=2025)

cur.close()
conn.close()