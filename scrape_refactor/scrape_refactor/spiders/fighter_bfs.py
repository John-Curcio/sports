import os
import scrapy
import sqlite3
import datetime as dt
# from scrape_refactor.items import FighterBFSWrite
import urllib.parse

## worry about this later
# db_mem = sqlite3.connect("file::memory:?cache=shared")
# db_file.backup(db_mem)
## when you're done...
# TODO: Move db stuff to new utility

class EventScrape(scrapy.Spider):
    name = "espn_fight_scrape"
    db_path = "/Users/jai/Documents/code/sports/db/espn_fighters.db"
    db = db_file = sqlite3.connect(db_path)
    db_cur = db.cursor()
    to_crawl_query_count = "SELECT COUNT(fighter_url) FROM espn_fighters WHERE last_crawled_iso_ts IS NULL;"
    to_crawl_query = "SELECT fighter_url FROM espn_fighters WHERE last_crawled_iso_ts IS NULL;"
    ## fighter_url

    def start_requests(self):
        counter = self.db_cur.execute(self.to_crawl_query_count)
        counter_result = counter.fetchone()
        print(f"TO SCRAPE: {counter_result[0]}")
        query = self.db_cur.execute(self.to_crawl_query)
        for row in query.fetchall():
            url = row[0]
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        self.db_cur.execute("UPDATE espn_fighters SET last_crawled_iso_ts = ?,  page_html = ? WHERE fighter_url = ?", (dt.datetime.now().isoformat(), response.body, urllib.parse.unquote(response.url)))
        self.db.commit()

class ESPNSearch(scrapy.Spider):
    name = "espn_fighter_search"
    db_path = "/Users/jai/Documents/code/sports/db/espn_fighters.db"
    db = db_file = sqlite3.connect(db_path)
    db_cur = db.cursor()    

    def db_exists(self):
        is_created = os.path.isfile(self.db_path)
        if not is_created:
            return False
        check_table = self.db_cur.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='espn_fighters';")
        check_table_result = check_table.fetchone()
        return check_table_result[0] != 0

    def setup_db(self):
        espn_fighter_db = sqlite3.connect(self.db_path)
        cur = espn_fighter_db.cursor()
        cur.execute("""
        CREATE TABLE espn_fighters (
            fighter_url text NOT NULL,
            added_iso_ts text NOT NULL, --this is when it's added to the queue
            last_crawled_iso_ts text, --this is when we started crawling
            page_html text
        );
        """)
        cur.execute(
            """
            CREATE UNIQUE INDEX idx_fighter_last_crawled_iso_ts 
            ON espn_fighters (last_crawled_iso_ts);
            """
        )
        # Create index to check if fighter has been added
        cur.execute("""
            CREATE UNIQUE INDEX idx_fighter_url 
            ON espn_fighters (fighter_url);
        """    
        )

    def start_requests(self):
        if not self.db_exists():
            self.setup_db()
        urls = [
        "https://www.espn.com/mma/fighters?search=a",
        "https://www.espn.com/mma/fighters?search=b",
        "https://www.espn.com/mma/fighters?search=c",
        "https://www.espn.com/mma/fighters?search=d",
        "https://www.espn.com/mma/fighters?search=e",
        "https://www.espn.com/mma/fighters?search=f",
        "https://www.espn.com/mma/fighters?search=g",
        "https://www.espn.com/mma/fighters?search=h",
        "https://www.espn.com/mma/fighters?search=i",
        "https://www.espn.com/mma/fighters?search=j",
        "https://www.espn.com/mma/fighters?search=k",
        "https://www.espn.com/mma/fighters?search=l",
        "https://www.espn.com/mma/fighters?search=m",
        "https://www.espn.com/mma/fighters?search=n",
        "https://www.espn.com/mma/fighters?search=o",
        "https://www.espn.com/mma/fighters?search=p",
        "https://www.espn.com/mma/fighters?search=q",
        "https://www.espn.com/mma/fighters?search=r",
        "https://www.espn.com/mma/fighters?search=s",
        "https://www.espn.com/mma/fighters?search=t",
        "https://www.espn.com/mma/fighters?search=u",
        "https://www.espn.com/mma/fighters?search=v",
        "https://www.espn.com/mma/fighters?search=w",
        "https://www.espn.com/mma/fighters?search=x",
        "https://www.espn.com/mma/fighters?search=y",
        "https://www.espn.com/mma/fighters?search=z" 
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        all_urls = [_url.get() for _url in response.css('a::attr(href)')]
        fighter_urls = [_url for _url in all_urls if "/mma/fighter/_/id/" in _url]
        fighter_data = [
            (f"https://www.espn.com{_url}", dt.datetime.now().isoformat(), None, None) for _url in fighter_urls
        ]
        self.db_cur.executemany("INSERT OR IGNORE INTO espn_fighters VALUES (?, ?, ?, ?)", fighter_data)
        self.db.commit()

class EventBFS(scrapy.Spider):
    name = "event_scrape"
    db_path = "/Users/jai/Documents/code/sports/db/fighter_bfs.db"
    db = db_file = sqlite3.connect(db_path)
    db_cur = db.cursor()
    to_crawl_query_count = "SELECT COUNT(event_url) FROM event_bfs WHERE last_crawled_iso_ts IS NULL;"
    to_crawl_query = "SELECT event_url FROM event_bfs WHERE last_crawled_iso_ts IS NULL;"
    ## fighter_url

    def start_requests(self):
        counter = self.db_cur.execute(self.to_crawl_query_count)
        counter_result = counter.fetchone()
        print(f"TO SCRAPE: {counter_result[0]}")
        query = self.db_cur.execute(self.to_crawl_query)
        for row in query.fetchall():
            url = row[0]
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        self.db_cur.execute("UPDATE event_bfs SET last_crawled_iso_ts = ?,  page_html = ? WHERE event_url = ?", (dt.datetime.now().isoformat(), response.body, response.url))
        self.db.commit()

class FighterBFS(scrapy.Spider):
    name = "fighter_bfs"
    db_path = "/Users/jai/Documents/code/sports/db/fighter_bfs.db"
    db = db_file = sqlite3.connect(db_path)
    db_cur = db.cursor()
    to_crawl_query_count = "SELECT COUNT(fighter_url) FROM fighter_bfs WHERE last_crawled_iso_ts IS NULL;"
    to_crawl_query = "SELECT fighter_url FROM fighter_bfs WHERE last_crawled_iso_ts IS NULL;"
    max_bfs_iter = 20 # Took 70 iterations in testing
    ## fighter_url

    def db_exists(self):
        is_created = os.path.isfile(self.db_path)
        if not is_created:
            return False
        check_table = self.db_cur.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='fighter_bfs';")
        check_table_result = check_table.fetchone()
        return check_table_result[0] != 0

    
    def seed_database(self):
        fighter_bfs_db = sqlite3.connect(self.db_path)
        cur = fighter_bfs_db.cursor()
        # Create this table for BFS ## DO I JUST ADD THE HTML HERE?
        cur.execute("""
        CREATE TABLE fighter_bfs (
            fighter_url text NOT NULL,
            added_iso_ts text NOT NULL, --this is when it's added to the queue
            last_crawled_iso_ts text, --this is when we started crawling
            page_html text
        );
        """)
        # Create index to check if fighter has been queued up for searching
        cur.execute(
            """
            CREATE UNIQUE INDEX idx_fighter_last_crawled_iso_ts 
            ON fighter_bfs (last_crawled_iso_ts);
            """
        )
        # Create index to check if fighter has been added
        cur.execute("""
            CREATE UNIQUE INDEX idx_fighter_url 
            ON fighter_bfs (fighter_url);
        """    
        )
        cur.execute("""
        CREATE TABLE event_bfs (
            event_url text NOT NULL,
            added_iso_ts text NOT NULL, --this is when it's added to the queue
            last_crawled_iso_ts text,
            page_html text
        );
        """)
        # Create index to check if fighter has been queued up for searching
        cur.execute(
            """
            CREATE UNIQUE INDEX idx_event_last_crawled_iso_ts 
            ON event_bfs (last_crawled_iso_ts);
            """
        )
        # Create index to check if fighter has been added
        cur.execute("""
            CREATE UNIQUE INDEX idx_event_url 
            ON event_bfs (event_url);
        """    
        )        
        now = dt.datetime.now().isoformat()
        urls = [
                "https://www.bestfightodds.com/fighters/Alex-Pereira-10463",
                "https://www.bestfightodds.com/fighters/Alex-Perez-2459",
                "https://www.bestfightodds.com/fighters/Alex-Volkanovski-6723",
                "https://www.bestfightodds.com/fighters/Alexa-Grasso-5096",
                "https://www.bestfightodds.com/fighters/Alexander-Gustafsson-1559",
                "https://www.bestfightodds.com/fighters/Alexander-Volkanovski-9523",
                "https://www.bestfightodds.com/fighters/Alexander-Yakovlev-2434",
                "https://www.bestfightodds.com/fighters/Alexandre-Pantoja-5118",
                "https://www.bestfightodds.com/fighters/Alexis-Davis-2693",
                "https://www.bestfightodds.com/fighters/Alistair-Overeem-246",
                "https://www.bestfightodds.com/fighters/Aljamain-Sterling-4688",
                "https://www.bestfightodds.com/fighters/Amanda-Nunes-2225",
                "https://www.bestfightodds.com/fighters/Anderson-Silva-38",
                "https://www.bestfightodds.com/fighters/Andrei-Arlovski-487",
                "https://www.bestfightodds.com/fighters/Andy-Ogle-3308",
                "https://www.bestfightodds.com/fighters/Angela-Hill-5348",
                "https://www.bestfightodds.com/fighters/Anthony-Hamilton-3127",
                "https://www.bestfightodds.com/fighters/Anthony-Johnson-135",
                "https://www.bestfightodds.com/fighters/Anthony-Pettis-1171",
                "https://www.bestfightodds.com/fighters/Anthony-Smith-1829",
                "https://www.bestfightodds.com/fighters/Augusto-Mendes-5296",
                "https://www.bestfightodds.com/fighters/Beneil-Dariush-4633",
                "https://www.bestfightodds.com/fighters/Bojan-Mihajlovic-6191",
                "https://www.bestfightodds.com/fighters/Brad-Tavares-1959",
                "https://www.bestfightodds.com/fighters/Brandon-Moreno-6681",
                "https://www.bestfightodds.com/fighters/Brett-Johns-3667",
                "https://www.bestfightodds.com/fighters/Brian-Ortega-4290",
                "https://www.bestfightodds.com/fighters/Bryan-Caraway-1704",
                "https://www.bestfightodds.com/fighters/Cain-Velasquez-464",
                "https://www.bestfightodds.com/fighters/Carla-Esparza-2043",
                "https://www.bestfightodds.com/fighters/Cat-Zingano-3549",
                "https://www.bestfightodds.com/fighters/Chan-Sung-Jung-1150",
                "https://www.bestfightodds.com/fighters/Charles-Oliveira-1893",
                "https://www.bestfightodds.com/fighters/Christos-Giagos-5071",
                "https://www.bestfightodds.com/fighters/Ciryl-Gane-9273",
                "https://www.bestfightodds.com/fighters/Clay-Guida-23",
                "https://www.bestfightodds.com/fighters/Cody-Gibson-1915",
                "https://www.bestfightodds.com/fighters/Cody-Stamann-7352",
                "https://www.bestfightodds.com/fighters/Colby-Covington-5049",
                "https://www.bestfightodds.com/fighters/Corey-Anderson-5008",
                "https://www.bestfightodds.com/fighters/Cory-Sandhagen-6127",
                "https://www.bestfightodds.com/fighters/Cristiane-Justino-617",
                "https://www.bestfightodds.com/fighters/Cub-Swanson-387",
                "https://www.bestfightodds.com/fighters/Curtis-Blaydes-5732",
                "https://www.bestfightodds.com/fighters/Darren-Elkins-1768",
                "https://www.bestfightodds.com/fighters/David-Teymur-6105",
                "https://www.bestfightodds.com/fighters/Deiveson-Figueiredo-7514",
                "https://www.bestfightodds.com/fighters/Demian-Maia-213",
                "https://www.bestfightodds.com/fighters/Derek-Brunson-2594",
                "https://www.bestfightodds.com/fighters/Derrick-Lewis-2610",
                "https://www.bestfightodds.com/fighters/Donald-Cerrone-220",
                "https://www.bestfightodds.com/fighters/Dustin-Poirier-2034",
                "https://www.bestfightodds.com/fighters/Efrain-Escudero-984",
                "https://www.bestfightodds.com/fighters/Emil-Meek-4104",
                "https://www.bestfightodds.com/fighters/Emily-Kagan-3802",
                "https://www.bestfightodds.com/fighters/Eric-Wisely-2510",
                "https://www.bestfightodds.com/fighters/Fabio-Maldonado-1479",
                "https://www.bestfightodds.com/fighters/Felicia-Spencer-7052",
                "https://www.bestfightodds.com/fighters/Francis-Ngannou-5847",
                "https://www.bestfightodds.com/fighters/Frankie-Edgar-54",
                "https://www.bestfightodds.com/fighters/Germaine-de-Randamie-2269",
                "https://www.bestfightodds.com/fighters/Gilbert-Burns-4747",
                "https://www.bestfightodds.com/fighters/Gillian-Robertson-7784",
                "https://www.bestfightodds.com/fighters/Glover-Teixeira-1477",
                "https://www.bestfightodds.com/fighters/Hatsu-Hioki-1147",
                "https://www.bestfightodds.com/fighters/Hayder-Hassan-1714",
                "https://www.bestfightodds.com/fighters/Henry-Cejudo-4438",
                "https://www.bestfightodds.com/fighters/Holly-Holm-3908",
                "https://www.bestfightodds.com/fighters/Hugo-Viana-3402",
                "https://www.bestfightodds.com/fighters/Ion-Cutelaba-4315",
                "https://www.bestfightodds.com/fighters/Islam-Makhachev-5541",
                "https://www.bestfightodds.com/fighters/Israel-Adesanya-7845",
                "https://www.bestfightodds.com/fighters/Jairzinho-Rozenstruik-8218",
                "https://www.bestfightodds.com/fighters/Jamahal-Hill-9288",
                "https://www.bestfightodds.com/fighters/James-Te-Huna-1657",
                "https://www.bestfightodds.com/fighters/Jan-Blachowicz-2371",
                "https://www.bestfightodds.com/fighters/Jan-Finney-1894",
                "https://www.bestfightodds.com/fighters/Jared-Cannonier-5333",
                "https://www.bestfightodds.com/fighters/Jared-Gordon-2971",
                "https://www.bestfightodds.com/fighters/Jarred-Brooks-7262",
                "https://www.bestfightodds.com/fighters/Jeff-Monson-175",
                "https://www.bestfightodds.com/fighters/Jennifer-Maia-3198",
                "https://www.bestfightodds.com/fighters/Jeremy-Stephens-210",
                "https://www.bestfightodds.com/fighters/Jessica-Andrade-4201",
                "https://www.bestfightodds.com/fighters/Jessica-Eye-2817",
                "https://www.bestfightodds.com/fighters/Jessica-Rakozczy-4566",
                "https://www.bestfightodds.com/fighters/Jim-Miller-265",
                "https://www.bestfightodds.com/fighters/Jimmie-Rivera-1379",
                "https://www.bestfightodds.com/fighters/Jiri-Prochazka-6058",
                "https://www.bestfightodds.com/fighters/Joanna-Jedrzejczyk-4939",
                "https://www.bestfightodds.com/fighters/John-Moraga-2173",
                "https://www.bestfightodds.com/fighters/Johnny-Eduardo-2692",
                "https://www.bestfightodds.com/fighters/Jon-Jones-819",
                "https://www.bestfightodds.com/fighters/Jonathan-Brookins-914",
                "https://www.bestfightodds.com/fighters/Jorge-Masvidal-99",
                "https://www.bestfightodds.com/fighters/Joseph-Benavidez-938",
                "https://www.bestfightodds.com/fighters/Joseph-Morales-5383",
                "https://www.bestfightodds.com/fighters/Julia-Budd-2154",
                "https://www.bestfightodds.com/fighters/Julianna-Pena-1816",
                "https://www.bestfightodds.com/fighters/Junior-Dos-Santos-851",
                "https://www.bestfightodds.com/fighters/Jussier-Formiga-4673",
                "https://www.bestfightodds.com/fighters/Justin-Gaethje-3964",
                "https://www.bestfightodds.com/fighters/Kalindra-Faria-5314",
                "https://www.bestfightodds.com/fighters/Kamaru-Usman-4664",
                "https://www.bestfightodds.com/fighters/Karl-Roberson-7389",
                "https://www.bestfightodds.com/fighters/Karolina-Kowalkiewicz-3745",
                "https://www.bestfightodds.com/fighters/Kathina-Catron-3950",
                "https://www.bestfightodds.com/fighters/Katlyn-Chookagian-5133",
                "https://www.bestfightodds.com/fighters/Kelvin-Gastelum-4061",
                "https://www.bestfightodds.com/fighters/Kevin-Lee-4643",
                "https://www.bestfightodds.com/fighters/Khamzat-Chimaev-10189",
                "https://www.bestfightodds.com/fighters/Kyle-Kingsbury-993",
                "https://www.bestfightodds.com/fighters/Lauren-Murphy-4580",
                "https://www.bestfightodds.com/fighters/Lenny-Lovato-1912",
                "https://www.bestfightodds.com/fighters/Leon-Edwards-4608",
                "https://www.bestfightodds.com/fighters/Leonardo-Lucio-Nascimento-1478",
                "https://www.bestfightodds.com/fighters/Liz-Carmouche-2047",
                "https://www.bestfightodds.com/fighters/Luiz-Henrique-5848",
                "https://www.bestfightodds.com/fighters/Marcus-Hicks-204",
                "https://www.bestfightodds.com/fighters/Marko-Peselj-1984",
                "https://www.bestfightodds.com/fighters/Marlon-Moraes-3248",
                "https://www.bestfightodds.com/fighters/Marvin-Vettori-6541",
                "https://www.bestfightodds.com/fighters/Max-Holloway-3090",
                "https://www.bestfightodds.com/fighters/Megan-Anderson-5790",
                "https://www.bestfightodds.com/fighters/Michael-Chandler-2158",
                "https://www.bestfightodds.com/fighters/Michelle-Waterson-3944",
                "https://www.bestfightodds.com/fighters/Miesha-Tate-783",
                "https://www.bestfightodds.com/fighters/Milana-Dudieva-4945",
                "https://www.bestfightodds.com/fighters/Misha-Cirkunov-5741",
                "https://www.bestfightodds.com/fighters/Myles-Jury-3222",
                "https://www.bestfightodds.com/fighters/Nicco-Montano-7777",
                "https://www.bestfightodds.com/fighters/Nik-Lentz-1542",
                "https://www.bestfightodds.com/fighters/Nikita-Krylov-4188",
                "https://www.bestfightodds.com/fighters/Ovince-St-Preux-1788",
                "https://www.bestfightodds.com/fighters/Paige-Vanzant-3804",
                "https://www.bestfightodds.com/fighters/Patrick-Cummins-2205",
                "https://www.bestfightodds.com/fighters/Paul-Felder-5116",
                "https://www.bestfightodds.com/fighters/Paulo-Costa-7991",
                "https://www.bestfightodds.com/fighters/Pedro-Munhoz-3750",
                "https://www.bestfightodds.com/fighters/Petr-Yan-7578",
                "https://www.bestfightodds.com/fighters/Phil-Davis-1045",
                "https://www.bestfightodds.com/fighters/Priscila-Cachoeira-7754",
                "https://www.bestfightodds.com/fighters/Quinton-Jackson-57",
                "https://www.bestfightodds.com/fighters/Rafael-Dos-Anjos-895",
                "https://www.bestfightodds.com/fighters/Raphael-Assuncao-2390",
                "https://www.bestfightodds.com/fighters/Raquel-Paaluhi-2813",
                "https://www.bestfightodds.com/fighters/Raquel-Pennington-3353",
                "https://www.bestfightodds.com/fighters/Rashad-Evans-42",
                "https://www.bestfightodds.com/fighters/Renan-Barao-1857",
                "https://www.bestfightodds.com/fighters/Ricardo-Lamas-1130",
                "https://www.bestfightodds.com/fighters/Rob-Wilkinson-7451",
                "https://www.bestfightodds.com/fighters/Robert-Whittaker-3761",
                "https://www.bestfightodds.com/fighters/Ronda-Rousey-2741",
                "https://www.bestfightodds.com/fighters/Rose-Namajunas-3803",
                "https://www.bestfightodds.com/fighters/Ryan-Bader-987",
                "https://www.bestfightodds.com/fighters/Sara-McMann-2704",
                "https://www.bestfightodds.com/fighters/Sarah-Dalelio-3455",
                "https://www.bestfightodds.com/fighters/Sarah-Kaufman-1228",
                "https://www.bestfightodds.com/fighters/Sean-Strickland-4733",
                "https://www.bestfightodds.com/fighters/Sergio-Moraes-707",
                "https://www.bestfightodds.com/fighters/Shayna-Baszler-803",
                "https://www.bestfightodds.com/fighters/Sheila-Gaff-2466",
                "https://www.bestfightodds.com/fighters/Steven-Rodriguez-4517",
                "https://www.bestfightodds.com/fighters/Stipe-Miocic-1869",
                "https://www.bestfightodds.com/fighters/Taila-Santos-8541",
                "https://www.bestfightodds.com/fighters/Takeya-Mizugaki-1129",
                "https://www.bestfightodds.com/fighters/Tecia-Torres-3557",
                "https://www.bestfightodds.com/fighters/Thiago-Santos-2526",
                "https://www.bestfightodds.com/fighters/Tim-Elliott-3288",
                "https://www.bestfightodds.com/fighters/Tj-Dillashaw-13965",
                "https://www.bestfightodds.com/fighters/Tony-Ferguson-2568",
                "https://www.bestfightodds.com/fighters/Tyron-Woodley-1299",
                "https://www.bestfightodds.com/fighters/Valentina-Shevchenko-5475",
                "https://www.bestfightodds.com/fighters/Warlley-Alves-4924",
                "https://www.bestfightodds.com/fighters/Weili-Zhang-7955",
                "https://www.bestfightodds.com/fighters/Will-Brooks-3787",
                "https://www.bestfightodds.com/fighters/Yoel-Romero-2778",
                "https://www.bestfightodds.com/fighters/Zhang-Weili-9795"
        ]
        data = [
            (_url, dt.datetime.now().isoformat(), None, None) for _url in urls
        ]
        # cur.execute(f"INSERT INTO fighter_bfs VALUES ('https://www.bestfightodds.com/fighters/Zhang-Weili-9795', '{dt.datetime.now().isoformat()}', NULL);")
        cur.executemany("INSERT INTO fighter_bfs VALUES (?, ?, ?, ?)", data)
        fighter_bfs_db.commit()

    def start_requests(self):
        if not self.db_exists():
            self.seed_database()
        counter = self.db_cur.execute(self.to_crawl_query_count)
        counter_result = counter.fetchone()
        print(f"TO CRAWL: {counter_result[0]}")
        query = self.db_cur.execute(self.to_crawl_query)
        bfs_iter = 0
        while (bfs_iter < self.max_bfs_iter) and (counter_result[0] != 0):
            print(f"STARTING ITER: {bfs_iter}")
            for row in query.fetchall():
                url = row[0]
                yield scrapy.Request(url=url, callback=self.parse)
            bfs_iter += 1
            counter = self.db_cur.execute(self.to_crawl_query_count)
            counter_result = counter.fetchone()
            query = self.db_cur.execute(self.to_crawl_query)
            print(f"TO CRAWL: {counter_result[0]}")

    def parse(self, response):
        ## get urls
        all_urls = [_url.get() for _url in response.css('a::attr(href)')]
        fighter_urls = [_url for _url in all_urls if "/fighters/" in _url and _url not in response.url]
        event_urls = [_url for _url in all_urls if "/events/" in _url and _url not in response.url]
        fighter_data = [
            (f"https://www.bestfightodds.com{_url}", dt.datetime.now().isoformat(), None, None) for _url in fighter_urls
        ]
        event_data = [
            (f"https://www.bestfightodds.com{_url}", dt.datetime.now().isoformat(), None, None) for _url in event_urls
        ]
        # this write can be done asynchronously
        self.db_cur.executemany("INSERT OR IGNORE INTO fighter_bfs VALUES (?, ?, ?, ?)", fighter_data)
        self.db_cur.executemany("INSERT OR IGNORE INTO event_bfs VALUES (?, ?, ?, ?)", event_data)
        self.db.commit()
        # fbfs_item = FighterBFSWrite()
        # fbfs_item["urls"] = fighter_urls
        ## this write NEEDS to happen to prevent us from crawling the page again
        self.db_cur.execute("UPDATE fighter_bfs SET last_crawled_iso_ts = ?,  page_html = ? WHERE fighter_url = ?", (dt.datetime.now().isoformat(), response.body, response.url))
        self.db.commit()