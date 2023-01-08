import numpy as np
import pandas as pd
from db import base_db_interface


class UfcDataCleaner(object):
    # I still havent' figured out what to do with round stats!

    def __init__(self):
        self.totals_df = base_db_interface.read("ufc_totals")
        self.strikes_df = base_db_interface.read("ufc_strikes")
        self.events_df = base_db_interface.read("ufc_events")
        self.desc_df = base_db_interface.read("ufc_fight_description")

        self.clean_totals_df = None 
        self.clean_strikes_df = None 
        self.clean_events_df = None 
        self.clean_desc_df = None

    @staticmethod
    def get_clean_fighter_id(fighter_id_vec):
        return fighter_id_vec.str.split("/fighter-details/").str[1]
        
    def _parse_totals(self):
        """
        Get clean_totals_df, a cleaned version of totals_df
        """
        totals_clean_df = self.totals_df.rename(columns={
            "Sub. att": "SM",
            "Rev.": "RV",
        })
        totals_clean_df["SM"] = totals_clean_df["SM"].astype(int)
        totals_clean_df["RV"] = totals_clean_df["RV"].astype(int)

        totals_clean_df["FighterID"] = self.get_clean_fighter_id(totals_clean_df["FighterID"])
        totals_clean_df["SSL"] = np.nan
        # inds = totals_clean_df["Sig. str."].str.contains("of")
        totals_clean_df["SSL"] = totals_clean_df["Sig. str."].str.split("of").str[0].astype(int)
        totals_clean_df["SSA"] = totals_clean_df["Sig. str."].str.split("of").str[1].astype(int)
        totals_clean_df["TSL"] = totals_clean_df["Total str."].str.split("of").str[0].astype(int)
        totals_clean_df["TSA"] = totals_clean_df["Total str."].str.split("of").str[1].astype(int)
        totals_clean_df["TDL"] = totals_clean_df["Td"].str.split("of").str[0].astype(int)
        totals_clean_df["TDA"] = totals_clean_df["Td"].str.split("of").str[1].astype(int)

        ctrl_inds = totals_clean_df["Ctrl"].str.contains(":")
        totals_clean_df["ctrl_seconds"] = 0 #np.nan
        totals_clean_df.loc[ctrl_inds, "ctrl_seconds"] = (
            totals_clean_df.loc[ctrl_inds, "Ctrl"].str.split(":").str[0].astype(int) * 60 +
            totals_clean_df.loc[ctrl_inds, "Ctrl"].str.split(":").str[1].astype(int)
        )
        self.clean_totals_df = totals_clean_df.drop(columns=[
            "Sig. str.", "Sig. str. %", "Total str.", "Td", "Td %", "Ctrl",
            "Fighter",
        ])
        return self.clean_totals_df

    def _parse_strikes(self):
        """
        Get clean_strikes_df, a cleaned version of the ufc_strikes table.
        """
        strikes_clean_df = self.strikes_df.copy()
        strikes_clean_df["FighterID"] = self.get_clean_fighter_id(strikes_clean_df["FighterID"])
        strikes_clean_df["SHL"] = strikes_clean_df["Head"].str.split("of").str[0].astype(int)
        strikes_clean_df["SHA"] = strikes_clean_df["Head"].str.split("of").str[1].astype(int)

        strikes_clean_df["SBL"] = strikes_clean_df["Body"].str.split("of").str[0].astype(int)
        strikes_clean_df["SBA"] = strikes_clean_df["Body"].str.split("of").str[1].astype(int)

        strikes_clean_df["SLL"] = strikes_clean_df["Leg"].str.split("of").str[0].astype(int)
        strikes_clean_df["SLA"] = strikes_clean_df["Leg"].str.split("of").str[1].astype(int)

        strikes_clean_df["SDL"] = strikes_clean_df["Distance"].str.split("of").str[0].astype(int)
        strikes_clean_df["SDA"] = strikes_clean_df["Distance"].str.split("of").str[1].astype(int)

        strikes_clean_df["SCL"] = strikes_clean_df["Clinch"].str.split("of").str[0].astype(int)
        strikes_clean_df["SCA"] = strikes_clean_df["Clinch"].str.split("of").str[1].astype(int)

        strikes_clean_df["SGL"] = strikes_clean_df["Ground"].str.split("of").str[0].astype(int)
        strikes_clean_df["SGA"] = strikes_clean_df["Ground"].str.split("of").str[1].astype(int)

        # dropping Sig. str because it's already in clean_totals_df
        # Sig. str. % is derived from Sig. str, and imo just a bad statistic,
        # so it's not worth keeping. Head, Body, etc have been parsed into
        # SHL, SHA, SBL, SBA, etc.
        self.clean_strikes_df = strikes_clean_df.drop(columns=[
            "Sig. str", "Sig. str. %", "Head", "Body", "Leg", "Distance", "Clinch", "Ground",
            "Fighter",
        ])

    def _parse_events(self):
        """
        Get clean_events_df, a cleaned version of events_df, which is 
        from the ufc_events table in mma.db
        """
        event_clean_df = self.events_df.rename(columns={
            "Weight class": "weight_class",
            "Method": "method",
            "Round": "round", 
            "Location": "location",
            "FighterUrl": "FighterID",
            "OpponentUrl": "OpponentID",
            "Kd": "KD_event_str",
            "Str": "Str_event_str",
            "Td": "Td_event_str",
            "Sub": "Sub_event_str",
        })
        event_clean_df["FighterID"] = self.get_clean_fighter_id(event_clean_df["FighterID"])
        event_clean_df["OpponentID"] = self.get_clean_fighter_id(event_clean_df["OpponentID"])
        event_clean_df["time_seconds"] = (
            event_clean_df["Time"].str.split(":").str[0].astype(int) * 60 +
            event_clean_df["Time"].str.split(":").str[1].astype(int)
        )
        event_clean_df = event_clean_df.drop(columns=["Time"])
        self.clean_events_df = event_clean_df

    def _get_doubled_event_df(self):
        """
        Each event is represented once in the ufc_events table. However, it is 
        useful to have each event represented twice, once for each fighter as the 
        Fighter, and once as Opponent. This function returns a dataframe with
        self.clean_events_df concat'ed with a complement of self.clean_events_df.
        """
        if self.clean_events_df is None:
            self._parse_events()
        # okay, let's get the complement of event_clean_df
        event_complement_df = self.clean_events_df.rename(columns={
            "FighterID": "OpponentID",
            "OpponentID": "FighterID",
            "FighterName": "OpponentName",
            "OpponentName": "FighterName",
        })
        event_complement_df["W/L"] = event_complement_df["W/L"].replace("win", "loss")
        drop_cols = [
            "KD_event_str", "Str_event_str", "Td_event_str", "Sub_event_str",
            "Fighter",
        ]
        event_clean_df = self.clean_events_df.drop(columns=drop_cols)
        event_complement_df = event_complement_df.drop(columns=drop_cols)
        return pd.concat([event_clean_df, event_complement_df], axis=0)\
            .reset_index(drop=True)
        

    def _parse_descriptions(self):
        df = self.desc_df.rename(columns={
            "Weight": "weight_bout",
            "Method": "method_description",
            "Round": "round_description",
            "Time": "time_description",
            "Time Format": "time_format",
            "Referee": "referee",
            "Details": "details_description",
        }).copy()
        # need to do some serious work to figure out the duration of the fight
        max_time = pd.Series(np.nan, index=df.index)
        time_dur = pd.Series(np.nan, index=df.index)
        
        time_into_round = df["time_description"].replace("-", "0:0").fillna("0:0").str.split(":")
        seconds_into_round = (
            time_into_round.str[0].astype(int) * 60 +
            time_into_round.str[1].astype(int)
        )
        # parsing the time format
        # prep by overwriting some special cases
        time_format = df["time_format"].str.lower()
        time_format.loc[time_format == "unlimited rnd"] = "unlimited rnd (15)"
        time_format.loc[time_format == "no time limit"] = "1 round (999999)"
        unlimited_rnd_inds = time_format.str.startswith("unlimited rnd")
        unlimited_rnd_lens = time_format.loc[unlimited_rnd_inds].str.split("(").str[1].str[:-1]
        # just impute some really big number of rounds lol
        time_format.loc[unlimited_rnd_inds] = unlimited_rnd_lens.apply(
            lambda x: "50 rnd (" + "-".join(50 * [x]) + ")"
        )
        round_formats = time_format.str.split("(").str[1].str[:-1]\
            .apply(lambda x: [int(s) for s in x.split("-")])
        max_time = round_formats.apply(np.sum) * 60
        round_index = (df["round_description"].astype(int) - 1)
        time_dur = pd.Series(
            [np.sum(round_format[:final_round]) * 60 for final_round, round_format 
            in zip(round_index, round_formats)]
        ) + seconds_into_round
        self.clean_desc_df = df.assign(max_time=max_time, time_dur=time_dur)


    def parse_all(self):
        """
        Parse ufc_events, ufc_strikes, ufc_totals, ufc_fight_description,
        and merge them into a single dataframe, ufc_df. 
        ufc_df should have two rows per fight, and columns for all the information
        about the fight.
        """
        self._parse_events()
        self._parse_strikes()
        self._parse_totals()
        self._parse_descriptions()

        doubled_events_df = self._get_doubled_event_df()
        # merge events with descriptions
        ufc_df = doubled_events_df.merge(
            self.clean_desc_df,
            on=["FightID"],
            how="left",
        )
        # add totals for fighter and opponent
        ufc_df = ufc_df.merge(
            self.clean_totals_df,
            on=["FighterID", "FightID"],
            how="left",
        ).merge(
            self.clean_totals_df.rename(columns={
                "FighterID": "OpponentID",
            }),
            on=["OpponentID", "FightID"],
            how="left",
            suffixes=("", "_opp")
        )
        # add strikes for fighter and opponent
        ufc_df = ufc_df.merge(
            self.clean_strikes_df,
            on=["FighterID", "FightID"],
            how="left",
        ).merge(
            self.clean_strikes_df.rename(columns={
                "FighterID": "OpponentID",
            }),
            on=["OpponentID", "FightID"],
            how="left",
            suffixes=("", "_opp")
        )
        ufc_df["Date"] = pd.to_datetime(ufc_df["Date"])
        self.ufc_df = ufc_df
        return self.ufc_df



