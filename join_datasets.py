import pandas as pd
import numpy as np


class IsomorphismFinder(object):
    """
    Learn mapping btw FighterIDs in df1 and FighterIDs in df2
    """
    
    def __init__(self, df1, df2, manual_mapping=None):
        self.df1 = self.get_double_df(df1)
        self.df2 = self.get_double_df(df2)
        self.frontier_fighter_id1_vals = pd.concat([self.df1["FighterID"], 
                                                    self.df1["OpponentID"]])
        self.frontier_fighter_id2_vals = pd.concat([self.df2["FighterID"], 
                                                    self.df2["OpponentID"]])
        for df in [self.df1, self.df2]:
            for col in ["FighterName", "OpponentName"]:
                df[col] = self.clean_names(df[col])
        self.fighter_id_map = pd.Series(dtype='object')
        if manual_mapping is not None:
            index, vals = zip(*manual_mapping.items()) # gets keys and values of dict respectively
            self.fighter_id_map = pd.Series(vals, index=index, dtype='object')
        self.conflict_fights = None
        
    @staticmethod
    def get_double_df(df):
        # edges are bidirectional
        fight_id = IsomorphismFinder.get_fight_id(df)
        df = df[["Date", "FighterID", "OpponentID", "FighterName", "OpponentName"]]\
            .assign(fight_id = fight_id)\
            .drop_duplicates("fight_id")
        df_complement = df.rename(columns={
            "FighterID":"OpponentID", "OpponentID":"FighterID",
            "FighterName":"OpponentName", "OpponentName":"FighterName",
        })
        df_doubled = pd.concat([df, df_complement]).reset_index(drop=True)
        return df_doubled

    @staticmethod
    def get_fight_id(df):
        max_id = np.maximum(df["FighterID"], df["OpponentID"])
        min_id = np.minimum(df["FighterID"], df["OpponentID"])
        return df["Date"].astype(str) + "_" + min_id + "_" + max_id

    
    @staticmethod
    def get_fight_id(df):
        max_id = np.maximum(df["FighterID"], df["OpponentID"])
        min_id = np.minimum(df["FighterID"], df["OpponentID"])
        return df["Date"].astype(str) + "_" + min_id + "_" + max_id
    
    def _catch_conflicts_in_merge(self, df):
        counts = df.groupby("FighterID1")["FighterID2"].nunique()
        if any(counts > 1):
            print(f"Found {sum(counts > 1)} conflicts")
            conflict_fighter_id1s = counts[counts > 1].index
            conflict_fighter_names = df["FighterName"]\
                .loc[df["FighterID1"].isin(conflict_fighter_id1s)]\
                .unique()
            print(f"fighter names with conflicts: {conflict_fighter_names}")
            self.conflict_fights = df.loc[df["FighterID1"].isin(conflict_fighter_id1s)]\
                .sort_values("Date")
            print(self.conflict_fights)
            raise Exception("Clean up conflicts with these FighterIDs in df2, then try again")
        return None
    
    def find_base_map(self):
        cols = ["Date", "FighterName", "OpponentName", "FighterID", "OpponentID"]
        overlapping_fights = self.df1[cols].merge(
            self.df2[cols],
            how="inner", 
            on=["Date", "FighterName", "OpponentName"],
            suffixes=("1", "2"),
        ) 
        ####
        overlapping_fights2 = self.df1[cols].merge(
            self.df2[cols],
            how="inner",
            left_on=["Date", "FighterName", "OpponentName"],
            right_on=["Date", "OpponentName", "FighterName"],
        )
        overlapping_fights = pd.concat([overlapping_fights, overlapping_fights2])
        ####
        self._catch_conflicts_in_merge(overlapping_fights)
        temp_map = overlapping_fights.groupby("FighterID1")["FighterID2"].first()
        self.fighter_id_map = self.fighter_id_map.combine_first(temp_map) 
    
    def find_isomorphism(self, n_iters=3):
        self.find_base_map()
        for _ in range(n_iters):            
            # update mapping greedily
            # find missing fighter_id1 with most fights with known opponent_id1
            # okay, find fighter_id1s with missing fighter_id2s
            # then for each of these, find # fights with known opponent_id2s
            missing_fighter_id1 = ~self.df1["FighterID"].isin(self.fighter_id_map.index)
            known_opponent_id2 = self.df1["OpponentID"].isin(self.fighter_id_map.index)
            df1_sub = self.df1.loc[missing_fighter_id1 & known_opponent_id2]
            # okay, let's figure out what df2 is calling this fighter
            df1_sub = df1_sub.rename(columns={"OpponentID":"OpponentID1"})
            df1_sub["OpponentID2"] = df1_sub["OpponentID1"].map(self.fighter_id_map)
            df_inner = df1_sub.merge(
                self.df2, how="inner", 
                left_on=["Date", "OpponentID2"],
                right_on=["Date", "OpponentID"],
                suffixes=("1", "2"),
            ).rename(columns={"FighterName1": "FighterName", "OpponentName1": "OpponentName"})
            self._catch_conflicts_in_merge(df_inner)
            temp_map = df_inner.groupby("FighterID1")["FighterID2"].first()
            self.fighter_id_map = self.fighter_id_map.combine_first(temp_map)
            if len(df_inner) == 0:
                self.stray_fights = df1_sub
                break
    
    @staticmethod
    def clean_names(names):
        replace_dict = {
            "julianna peña": "julianna pena",
            "marco polo reyes": "polo reyes",
            "brad scott": "bradley scott",
            "nicholas musoke": "nico musoke",
            "paddy holohan": "patrick holohan",
            "alatengheili": "alateng heili",
            "ode osbourne": "ode' osbourne",
            "zhang tiequan": "tiequan zhang",
            "aleksandra albu": "alexandra albu",
            "alvaro herrera mendoza": "alvaro herrera",
            "sumudaerji": "su mudaerji",
            "mark madsen": "mark o. madsen",
            "pingyuan liu": "liu pingyuan",
            "robert mcdaniel": "bubba mcdaniel",
            "aoriqileng": "aori qileng",
            "robert sanchez": "roberto sanchez",
            "patrick smith": "patrick trey smith",
        }
        to_replace, value = zip(*replace_dict.items()) # gets keys and values of dict respectively
        names = names.fillna("").str.strip().str.lower()\
                .replace(to_replace=to_replace, value=value)
        return names

manual_ufc_espn_mapping = {
    # chris brennan
    "http://ufcstats.com/fighter-details/b19fc66613dc75b9": "2500426",
    # courtney turner
    "http://ufcstats.com/fighter-details/56f4b81ec4db61af": "2951489",
    # patrick trey smith
    "http://ufcstats.com/fighter-details/46c8ec317aff28ac": "2335742",
    # ray wizard
    "http://ufcstats.com/fighter-details/ea0ad155451ed1f5": "2951510",
    # karine silva
    'http://ufcstats.com/fighter-details/9d62c2d8ee151f08': '3309918',
    # rinat fakhretdinov
    'http://ufcstats.com/fighter-details/8f765fd5775a8873': '4712980',
    # jason guida
    'http://ufcstats.com/fighter-details/ce25b4ed82b1811b': '2354107',
    # andrew martinez
    'http://ufcstats.com/fighter-details/f8c2aba4815876b5': '3162579',
    # maheshate
    'http://ufcstats.com/fighter-details/8c1ca54b5089d199': '4895360',
    # kyle driscoll
    'http://ufcstats.com/fighter-details/e5e148d4363deff8': '4246527',
    # JR/Marty Coughran
    'http://ufcstats.com/fighter-details/8112c9a23dccc759': '4354427',
    # Drew dimanlig
    'http://ufcstats.com/fighter-details/cc2ad11b1f9d818b': '2559902',
    # damon jackson
    'http://ufcstats.com/fighter-details/29af297d9f1de0f8': '3099187',
    # dan argueta
    'http://ufcstats.com/fighter-details/e4ba58725825412d': '4815973',
    # askar mozharov
    'http://ufcstats.com/fighter-details/e92901944ce91909': '4217396',
    # mabelly lima
    'http://ufcstats.com/fighter-details/6135fd9665fbb74e': '4372190',
    # mario rivera
    'http://ufcstats.com/fighter-details/0f7210aa8d61af8d': '2951376',
    # dan molina
    'http://ufcstats.com/fighter-details/606136dee6f6ecea': '2556050',
    # jeremy freitag
    'http://ufcstats.com/fighter-details/a47e9ec288c91067': '2556758',
    # naoki matsushita
    'http://ufcstats.com/fighter-details/990060b2a68a7b82': '2553054',
    # luciano azevedo
    'http://ufcstats.com/fighter-details/9bcfb40dbcd50568': '2381679',
    # thiago moises
    'http://ufcstats.com/fighter-details/d945aae53e3e54e6': '3955778',
    # gleidson cutis
    'http://ufcstats.com/fighter-details/44a94bbde42246e4': '4372295',
    # dayana silva
    'http://ufcstats.com/fighter-details/b19aecbfbb5508cc': '3971629',
    # gisele moreira
    'http://ufcstats.com/fighter-details/6a125ba3ec37e27e': '4030631',
    # patrick murphy
    'http://ufcstats.com/fighter-details/eca7e064746c161a': '3039036',
    # josh mcdonald
    'http://ufcstats.com/fighter-details/b507a76087e3ed9f': '2527951',
    # rafael de real
    'http://ufcstats.com/fighter-details/e82b2adcaeff71ec': '2500780',
    # trevor harris
    'http://ufcstats.com/fighter-details/0e98b05d3cf6d271': '2969478',
    # kenny ento
    'http://ufcstats.com/fighter-details/daf9be103c1edbbd': '2965044',
}

manual_espn_bfo_mapping = {
    # 3041602/brianna-fortino
    '3041602': '/fighters/Brianna-Fortino-13884',
    # /3153355/uyran-carlos
    '3153355': '/fighters/Uyran-Carlos-11754',
    # 3146349/carlos-leal
    '3146349': '/fighters/Carlos-Leal-Miranda-7744',
    # 3153355/uyran-carlos
    '3153355': '/fighters/Uyran-Carlos-11754',
    # 4916590/diego-dias
    '4916590': '/fighters/Diego-Dias-11750',
    # 2431314/jacare-souza
    '2431314': '/fighters/Ronaldo-Souza-725',
    # 2555633/jj-ambrose
#     '2555633': '/fighters/J-J-Ambrose-459',
    # /2558487/tony-johnson-jr
    '2558487': '/fighters/Tony-Johnson-918',
    # 2504175/zachary-micklewright
    '2504175': '/fighters/Zach-Micklewright-1651',
    # rodrigo de lima
    '3110330': '/fighters/Rodrigo-Goiana-de-Lima-4992',
    # /4030644/marcelo-rojo
    '4030644': '/fighters/Marcelo-Rojo-7706',
    # 3083639/mike-erosa
    '3083639': '/fighters/Mikey-Erosa-7707',
#     '4335927/levy-saul-marroquin-salazar'
    '4335927': '/fighters/Levy-Saul-Marroquin-7713',
    '4063869': '/fighters/John-Castaneda-7396',
    
    '4423264': 'fighters/Tofiq-Musaev-9177',
    '4306125': '/fighters/Gabe-Green-10506',
    '4914568': '/fighters/Pete-Rodrigue-13104',
    '3091146': '/fighters/Toninho-Gavinho-11224',
}

# def join_on_fighter_id_mapping(df1, df2, fighter_id_map, lsuffix="_ufc", rsuffix="_espn"):
#     """
#     fighter_id_map: mapping btw FighterID1 and FighterID2
#     """
#     def get_fight_id(df):
#         max_id = np.maximum(df["FighterID"].fillna("unknown"), 
#                             df["OpponentID"].fillna("unknown"))
#         min_id = np.minimum(df["FighterID"].fillna("unknown"), 
#                             df["OpponentID"].fillna("unknown"))
#         return df["Date"].astype(str) + "_" + min_id + "_" + max_id
#     # okay, let's just create a fight_id for each, then join on fight_id
#     df1 = df1.assign(
#         FighterID=df1["FighterID"].map(fighter_id_map),
#         OpponentID=df1["OpponentID"].map(fighter_id_map),
#     )
#     print(df1.loc[df1[["FighterID", "OpponentID"]].isnull().any(1), 
#                     ["FighterName", "FighterID", "OpponentName", "OpponentID", "Date"]].sort_values("Date"))
#     print(df1.loc[df1[["FighterID", "OpponentID"]].isnull().any(1), ["Date"]].value_counts())
#     df1 = df1.assign(fight_id=get_fight_id(df1))
#     df2 = df2.assign(fight_id=get_fight_id(df2))
#     # df1 = df1.drop(columns=['Date', 'FighterID', 'OpponentID'])
#     df1 = df1.drop(columns=['FighterID', 'OpponentID'])\
#         .rename(columns={"Date": "Date"+lsuffix})
#     def get_deduped(df):
#         # remove duplicates, keeping the row with the fewest missing values
#         return df.assign(n_missing=df.isnull().sum(1))\
#             .sort_values("n_missing", ascending=True)\
#             .drop_duplicates(subset="fight_id", keep="first")
#     df1 = get_deduped(df1)
#     df2 = get_deduped(df2)
#     return df1.merge(df2, on="fight_id", how="outer", suffixes=(lsuffix, rsuffix))

def join_ufc_and_espn(ufc_df, espn_df, ufc_espn_fighter_id_map):
    """
    fighter_id_map: mapping UFC ID --> ESPN ID
    """
    def get_fight_id(df):
        max_id = np.maximum(df["FighterID"].fillna("unknown"), 
                            df["OpponentID"].fillna("unknown"))
        min_id = np.minimum(df["FighterID"].fillna("unknown"), 
                            df["OpponentID"].fillna("unknown"))
        return df["Date"].astype(str) + "_" + min_id + "_" + max_id
    # okay, let's just create a fight_id for each, then join on fight_id
    ufc_df = ufc_df.assign(
        FighterID=ufc_df["FighterID"].map(ufc_espn_fighter_id_map),
        OpponentID=ufc_df["OpponentID"].map(ufc_espn_fighter_id_map),
    )
    print(ufc_df.loc[ufc_df[["FighterID", "OpponentID"]].isnull().any(1), 
                    ["FighterName", "FighterID", "OpponentName", "OpponentID", "Date"]].sort_values("Date"))
    print(ufc_df.loc[ufc_df[["FighterID", "OpponentID"]].isnull().any(1), ["Date"]].value_counts())
    ufc_df = ufc_df.assign(fight_id=get_fight_id(ufc_df))
    espn_df = espn_df.assign(fight_id=get_fight_id(espn_df))
    # ufc_df = ufc_df.drop(columns=['Date', 'FighterID', 'OpponentID'])
    # ufc_df = ufc_df.drop(columns=['FighterID', 'OpponentID'])\
    #     .rename(columns={"Date": "Date_ufc"})

    def get_deduped(df):
        # remove duplicates, keeping the row with the fewest missing values
        return df.assign(n_missing=df.isnull().sum(1))\
            .sort_values("n_missing", ascending=True)\
            .drop_duplicates(subset="fight_id", keep="first")
    espn_df = get_deduped(espn_df)
    ufc_df = get_deduped(ufc_df)
    # deliberately add duplicates to ufc df: want to make sure fighter
    # and opponent stats are matched up
    col_map = dict()
    for col in ufc_df.columns:
        if col.startswith("Fighter"):
            fighter_col, opp_col = col, "Opponent" + col[len("Fighter"):]
            col_map[fighter_col] = opp_col
            col_map[opp_col] = fighter_col
        if col.endswith("_opp"):
            opp_col, fighter_col = col, col[:-len("_opp")]
            col_map[fighter_col] = opp_col
            col_map[opp_col] = fighter_col
    ufc_df2 = ufc_df.rename(columns=col_map)
    ufc_df = pd.concat([ufc_df, ufc_df2]).reset_index(drop=True)
    return espn_df.merge(ufc_df[["fight_id", *col_map.keys()]], 
        on=["fight_id", "FighterID", "OpponentID"], how="left", 
        suffixes=("_espn", "_ufc"))

def join_espn_and_bfo(espn_df, bfo_df, espn_bfo_fighter_id_map):
    def get_fight_id(df):
        max_id = np.maximum(df["FighterID"].fillna("unknown"), 
                            df["OpponentID"].fillna("unknown"))
        min_id = np.minimum(df["FighterID"].fillna("unknown"), 
                            df["OpponentID"].fillna("unknown"))
        return df["Date"].astype(str) + "_" + min_id + "_" + max_id
    # okay, let's just create a fight_id for each, then join on fight_id
    espn_df = espn_df.assign(
        espn_fight_id=espn_df["fight_id"],
        espn_fighter_id=espn_df["FighterID"],
        espn_opponent_id=espn_df["OpponentID"],
        FighterID=espn_df["FighterID"].map(espn_bfo_fighter_id_map),
        OpponentID=espn_df["OpponentID"].map(espn_bfo_fighter_id_map),
    )
    espn_df = espn_df.assign(fight_id=get_fight_id(espn_df))
    bfo_df = bfo_df.assign(fight_id=get_fight_id(bfo_df))\
        .drop(columns=["FighterName", "OpponentName", "Event"])
    # remove duplicates in BFO, keeping the row with the fewest missing values
    bfo_df = bfo_df.assign(n_missing=bfo_df.isnull().sum(1))\
        .sort_values("n_missing", ascending=True)\
        .drop_duplicates(subset="fight_id", keep="first")\
        .drop(columns=["n_missing"])
    # okay, now i deliberately add duplicates. Want to make sure the fighter
    # and opponent are matched with their respective odds!
    bfo_df2 = bfo_df.rename(columns={
        "FighterID": "OpponentID",
        "FighterOpen": "OpponentOpen",
        "FighterCloseLeft": "OpponentCloseLeft",
        "FighterCloseRight": "OpponentCloseRight",
        "p_fighter_open_implied": "p_opponent_open_implied",
        "OpponentID": "FighterID",
        "OpponentOpen": "FighterOpen",
        "OpponentCloseLeft": "FighterCloseLeft",
        "OpponentCloseRight": "FighterCloseRight",
        "p_opponent_open_implied": "p_fighter_open_implied",
    })
    bfo_duped_df = pd.concat([bfo_df, bfo_df2]).reset_index(drop=True)
    return espn_df.merge(bfo_duped_df, 
                         on=["fight_id", "Date", "FighterID", "OpponentID"], 
                         how="left")

if __name__ == "__main__":
    espn_df = pd.read_csv("data/espn_data.csv").rename(columns={
        "Name": "FighterName",
        "Name_opp": "OpponentName",
    })
    for col in ["FighterID", "OpponentID"]:
        espn_df[col] = espn_df[col].str.split("/").str[0]
    ufc_df = pd.read_csv("data/ufc_stats_df.csv").rename(columns={
        "FighterID_opp": "OpponentID",
        "FighterName_opp": "OpponentName",
    })
    bfo_df = pd.read_csv("data/bfo_fighter_odds.csv")
    for df in [espn_df, ufc_df, bfo_df]:
        df["Date"] = pd.to_datetime(df["Date"])
        for col in ["FighterName", "OpponentName"]:
            df[col] = df[col].str.lower().str.strip()

    # 2561001/bruno-carvalho didn't fight 2488370/eiji-mitsuoka on Jul 16, 2011
    # that was a different bruno carvalho, who is already in the dataset
    drop_pair = ("2561001", "2488370")
    drop_fight = espn_df["FighterID"].isin(drop_pair) & espn_df["OpponentID"].isin(drop_pair)
    espn_df_clean = espn_df.loc[~drop_fight]

    # These espn IDs correspond to the same guy - they have to be merged
    fighter_id_map = {
        "2583704": "2613376",
        # "2583704/luis-ramos": "2613376/luis-ramos",
    }
    espn_df_clean = espn_df_clean.assign(
        FighterID=espn_df["FighterID"].replace(to_replace=fighter_id_map),
        OpponentID=espn_df["OpponentID"].replace(to_replace=fighter_id_map),
    )
    # find mapping btw ufc IDs and espn IDs
    iso_finder = IsomorphismFinder(ufc_df, espn_df_clean, manual_ufc_espn_mapping)
    iso_finder.find_isomorphism(n_iters=20)
    
    # okay great, now that we have the mapping, let's join ufc data and espn data
    ufc_espn_df = join_ufc_and_espn(ufc_df, espn_df_clean, iso_finder.fighter_id_map)

    # okay, let's proceed to join with bestfightodds data
    # lots of redundant BFO pages
    bfo_fighter_id_map = {
        '/fighters/Paddy-Holohan-2786': '/fighters/Patrick-Holohan-4991',
        '/fighters/Robert-McDaniel-4064': '/fighters/Bubba-McDaniel-744',
        '/fighters/Nicholas-Musoke-4199': '/fighters/Nico-Musoke-2144',
        '/fighters/Marco-Polo-Reyes-6679': '/fighters/Polo-Reyes-5991',
        '/fighters/Pingyuan-Liu-7732': '/fighters/Liu-Pingyuan-8739',
        '/fighters/Luis-Luna-7785': '/fighters/Anselmo-Luis-Luna-Jr-4330',
        '/fighters/Jung-Bu-Kyung-670': '/fighters/Bukyung-Jung-445',
        '/fighters/Brianna-van-Buren-4076': '/fighters/Brianna-Fortino-13884',
        '/fighters/J-J-Ambrose-12683': '/fighters/J-J-Ambrose-459',
        '/fighters/Anthony-Waldburger-1564': '/fighters/T-J-Waldburger-2156',
        '/fighters/Jadamba-Narantungalag-2028': '/fighters/Narantungalag-Jadambaa-6335', 
    #     '/fighters/Narantungalag-Jadambaa-6335': '/fighters/Jadamba-Narantungalag-2028',
        '/fighters/Raquel-Paaluhi-2813': '/fighters/Raquel-Pa-aluhi-5257',
        '/fighters/Rodrigo-Cavalheiro-Correia-5516': '/fighters/Rodrigo-Cavalheiro-4743',
        '/fighters/Jesse-Miele-5797': '/fighters/Jessy-Miele-8855',
        '/fighters/Jp-Buys-12275': '/fighters/J-P-Buys-7455',
        '/fighters/Levy-Marroquin-9617': '/fighters/Levy-Saul-Marroquin-7713',
        '/fighters/Guilherme-Faria-8090': '/fighters/Guillerme-Faria-12163',
        '/fighters/Gabriel-Green-6587': '/fighters/Gabe-Green-10506',
        '/fighters/Philip-Rowe-9379': '/fighters/Phil-Rowe-9898',
        '/fighters/Phillip-Rowe-11319': '/fighters/Phil-Rowe-9898',
    }

    bfo_df_clean = bfo_df.assign(
        FighterID=bfo_df["FighterID"].replace(to_replace=bfo_fighter_id_map),
        OpponentID=bfo_df["OpponentID"].replace(to_replace=bfo_fighter_id_map),
    )
    # these fights didn't end up happening
    drop_pairs = [
        ('/fighters/Gabriel-Bonfim-11752', '/fighters/Carlos-Leal-Miranda-7744'),
        ('/fighters/Gabriel-Bonfim-11752', '/fighters/Diego-Dias-11750'),
    ]
    drop_inds = np.any([
        bfo_df_clean["FighterID"].isin(drop_pair) & bfo_df_clean["OpponentID"].isin(drop_pair)
        for drop_pair in drop_pairs
    ], axis=0)
    bfo_df_clean = bfo_df_clean.loc[~drop_inds]

    bfo_iso_finder = IsomorphismFinder(espn_df_clean, bfo_df_clean, manual_espn_bfo_mapping)
    bfo_iso_finder.find_isomorphism(n_iters=20)

    bfo_ufc_espn_df = join_espn_and_bfo(ufc_espn_df, bfo_df_clean, bfo_iso_finder.fighter_id_map)
    print(bfo_ufc_espn_df.shape)
    bfo_ufc_espn_df.to_csv("data/full_bfo_ufc_espn_data.csv", index=False)