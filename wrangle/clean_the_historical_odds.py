from db import base_db_interface
import pandas as pd
import json
from tqdm import tqdm

class TheOddsFormatter(object):

    def __init__(self, raw_df:pd.DataFrame):
        self.raw_df = raw_df

    def format_response(self, response):
        """
        Format the response from the Odds API into a dataframe
        Response is for odds at a single timestamp
        """
        df_list = []
        timestamp = response['timestamp']
        for fight in response['data']:
            commence_time = fight['commence_time']
            home_team = fight['home_team']
            away_team = fight['away_team']
            for bookmaker in fight['bookmakers']:
                bookmaker_name = bookmaker['title']
                for market in bookmaker['markets']:
                    market_name = market['key']
                    for outcome in market['outcomes']:
                        outcome_name = outcome['name']
                        price = outcome['price']
                        df_list.append([
                            timestamp, commence_time, home_team, 
                            away_team, bookmaker_name, market_name, 
                            outcome_name, price
                        ])
        return pd.DataFrame(df_list, columns=[
            'timestamp', 'commence_time', 'home_team', 
            'away_team', 'bookmaker_name', 'market_name', 
            'outcome_name', 'price'
        ])
    
    def format_all_responses(self):
        """
        Format all responses from the Odds API into a dataframe
        """
        df_list = []
        for _, row in tqdm(self.raw_df.iterrows(), total=len(self.raw_df)):
            json_data = json.loads(row['data'])
            response = self.format_response(json_data)
            df_list.append(response)
        # Have to format this in a way that is compatible with my other data
        df = pd.concat(df_list, ignore_index=True)
        # Unambiguous fighter names
        df["fighter_name"] = df[["home_team", "away_team"]].min(axis=1)
        df["opponent_name"] = df[["home_team", "away_team"]].max(axis=1)
        # For each fight, timestamp, bookmaker, and market_name, there are two rows
        # One row is for the outcome where home_team (fighter_name) wins
        # The other row is for the outcome where away_team (opponent_name) wins
        outcome_is_fighter = df['outcome_name'] == df['fighter_name']
        fighter_rows = df.loc[outcome_is_fighter].rename(columns={
            'price': 'fighter_decimal_odds',
        }).drop(columns=['outcome_name'])
        opponent_rows = df.loc[~outcome_is_fighter].rename(columns={
            'price': 'opponent_decimal_odds',
        }).drop(columns=['home_team', 'away_team', 'outcome_name'])

        df = fighter_rows.merge(opponent_rows, on=[
            'timestamp', 'commence_time', 'fighter_name',
            'opponent_name', 'bookmaker_name', 'market_name'
        ])
        return df
    
    def write_replace_formatted_responses(self):
        """
        Write the formatted responses to the database
        """
        df = self.format_all_responses()
        return base_db_interface.write_replace('the_historical_odds_clean', df)
    
    def write_update_formatted_responses(self):
        """
        Write the formatted responses to the database
        """
        # TODO untested!!!!
        df = self.format_all_responses()
        return base_db_interface.write_update('the_historical_odds_clean', df)

    def write_open_and_close_odds(self):
        """
        Write the opening odds to the database
        """
        df = self.format_all_responses()
        open_df = df.sort_values(["timestamp"], ascending=True).groupby([
            "fighter_name", "opponent_name", 
            "bookmaker_name", "market_name",
            "commence_time",
        ]).first().reset_index()
        close_df = df.sort_values(["timestamp"], ascending=True).groupby([
            "fighter_name", "opponent_name", 
            "bookmaker_name", "market_name",
            "commence_time",
        ]).last().reset_index()
        open_df = open_df.rename(columns={
            'fighter_decimal_odds': 'open_fighter_decimal_odds',
            'opponent_decimal_odds': 'open_opponent_decimal_odds',
            'timestamp': 'open_timestamp',
        })
        close_df = close_df.rename(columns={
            'fighter_decimal_odds': 'close_fighter_decimal_odds',
            'opponent_decimal_odds': 'close_opponent_decimal_odds',
            'timestamp': 'close_timestamp',
        }).drop(columns=['home_team', 'away_team'])
        open_close_df = open_df.merge(close_df, on=[
            'fighter_name', 'opponent_name', 
            'bookmaker_name', 'market_name',
            'commence_time',
        ])
        return base_db_interface.write_replace('the_historical_odds_open_close', open_close_df)
        # base_db_interface.write_replace('the_historical_odds_open', open_df)
        # base_db_interface.write_replace('the_historical_odds_close', close_df)

def main():
    raw_df = base_db_interface.read('the_historical_odds_raw')
    formatter = TheOddsFormatter(raw_df)
    formatter.write_replace_formatted_responses()
    formatter.write_open_and_close_odds()
    # formatter.write_update_formatted_responses()