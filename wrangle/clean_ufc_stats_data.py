import numpy as np
import pandas as pd


class UfcDataCleaner(object):
    # I still havent' figured out what to do with round stats!

    def __init__(self, totals_path, strikes_path, events_path, desc_path):
        self.totals_df = pd.read_csv(totals_path)
        self.strikes_df = pd.read_csv(strikes_path)
        self.event_df = pd.read_csv(events_path)
        self.desc_df = pd.read_csv(desc_path)

        self.clean_totals_df = None 
        self.clean_strikes_df = None 
        self.clean_events_df = None 
        self.clean_desc_df = None
        
    def _parse_totals(self):
        totals_clean_df = self.totals_df.rename(columns={
            "Sub. att": "SM",
            "Rev.": "RV",
        })
        totals_clean_df["SSL"] = np.nan
        inds = totals_clean_df["Sig. str."].str.contains("of")
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
        ])
        return self.clean_totals_df

    def _parse_strikes(self):
        strikes_clean_df = self.strikes_df.copy()
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

        self.clean_strikes_df = strikes_clean_df.drop(columns=[
            "Sig. str", "Sig. str. %", "Head", "Body", "Leg", "Distance", "Clinch", "Ground"
        ])

    def _parse_events(self):
        event_clean_df = self.event_df.rename(columns={
            "Weight class": "weight_class",
            "Method": "method",
            "Round": "round", 
            "Location": "location",
        })[["weight_class", "method", "round", "location", "img_png_url",
            "Time", "is_title_fight", "FightID", "EventUrl", "Date"]]
        event_clean_df["time_seconds"] = (
            event_clean_df["Time"].str.split(":").str[0].astype(int) * 60 +
            event_clean_df["Time"].str.split(":").str[1].astype(int)
        )
        self.clean_events_df = event_clean_df.drop(columns=["Time"])

    def _parse_descriptions(self):
        self.clean_desc_df = self.desc_df.rename(columns={
            "Weight": "weight_bout",
            "Method": "method_description",
            "Round": "round_description",
            "Time": "time_description",
            "Time Format": "time_format",
            "Referee": "referee",
            "Details": "details_description",
        })


    def parse_all(self):
        self._parse_events()
        self._parse_strikes()
        self._parse_totals()
        self._parse_descriptions()
        temp_ufc_df = self.clean_totals_df.merge(
            self.clean_strikes_df.drop(columns=["Fighter"]),
            on=["FighterID", "FightID"],
            how="inner",
        ).merge(
            self.clean_events_df,
            on=["FightID"],
            how="inner",
        ).merge(
            self.clean_desc_df,
            on=["FightID"],
            how="left",
        ).rename(columns={
            "Fighter": "FighterName",
        })

        fighter_rows = temp_ufc_df.groupby("FightID").first()
        opponent_rows = temp_ufc_df.groupby("FightID").last()
        # Drop columns about the event or the fight description
        # from the opponent rows because they're redundant. Still
        # need FightID though
        drop_cols = (
            set(self.clean_events_df.columns) | 
            set(self.clean_desc_df.columns)
        ) - {"FightID"}
        self.ufc_df = fighter_rows.join(
            opponent_rows.drop(columns=drop_cols),
            lsuffix="", rsuffix="_opp"
        ).reset_index()
        return self.ufc_df 



