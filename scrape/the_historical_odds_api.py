import requests
from db import base_db_interface
import time
import pandas as pd
from tqdm import tqdm 

# Define the API endpoint URL and parameters
url = 'https://api.the-odds-api.com/v4/sports/mma_mixed_martial_arts/odds-history/'
api_key = 'a3fcf93edd65c14e9b93374d03ad53ff'
regions = 'us'
markets = 'h2h'

class TheOddsScraper(object):

    def __init__(self):
        self.create_table()

    def get_historical_odds_response(self, date_formatted):
        headers = {'Content-Type': 'application/json'}
        params = {'apiKey': api_key, 'regions': regions, 'markets': markets, 'date': date_formatted}

        # Send the API request and retrieve the response
        response = requests.get(url, headers=headers, params=params)

        # Check if the API request was successful
        if response.status_code == 200:
            return response # don't even need to convert to json yet
        else:
            # Print an error message if the API request failed
            print('Error: API request failed with status code', response.status_code)
            return None
        
    def create_table(self):
        base_db_interface.execute(
            """create table if not exists the_historical_odds_raw (
                timestamp text primary key, 
                data text
            )"""
        )
        
    def get_and_write_historical_odds(self, date):
        date_formatted = date.isoformat() + "Z"
        raw_response = self.get_historical_odds_response(date_formatted)
        if raw_response is not None:
            base_db_interface.execute(
                "insert into the_historical_odds_raw (timestamp, data) values (?, ?)", 
                (str(date.date()), raw_response.text)
            )
        else:
            print("No odds found for date: ", date_formatted)
        
    def get_and_write_historical_odds_over_range(self, start_date, end_date):
        for date in tqdm(pd.date_range(start_date, end_date)):
            self.get_and_write_historical_odds(date)
            time.sleep(1)
        base_db_interface.commit()

def main():
    scraper = TheOddsScraper()
    scraper.get_and_write_historical_odds_over_range("2020-06-10", "2023-03-29")