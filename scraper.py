import os, psycopg2, requests
import pandas as pd
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import csv
import hashlib
from datetime import datetime
import difflib
from psycopg2.extras import execute_values



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

cur.close()
conn.close()