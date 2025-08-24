import os, psycopg2, requests
import pandas as pd
from dotenv import load_dotenv
from bs4 import BeautifulSoup

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
soup = BeautifulSoup(html, "lxml")
body1 = soup.find("body")
main1 = body1.find("main")
ul1 = main1.find("ul", class_=["list-group", "mb-4", "not-premium"])
lis = ul1.find_all(class_ = ["list-group-item"])

playerlist = listPlayerObjects(lis)




DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL not set")

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

cur.execute("SELECT NOW();")
print("Connected to Postgres at:", cur.fetchone())
cur.close()
conn.close()