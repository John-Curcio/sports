import sqlite3
import pandas as pd

class DbInterface(object):
    """
    Provide an interface to read from and write to a sqlite database
    """

    def __init__(self, db_name="deleteme.db"):
        self.db_name = db_name 
        self._con = None
        self.connect()

    def connect(self):
        # connects to the database and creates it if it doesn't exist
        self._con = sqlite3.connect(self.db_name)
        self._cursor = self._con.cursor()

    def close(self):
        self._con.close()
        
    def read(self, table_name):
        """
        read all rows from table_name
        """
        return pd.read_sql(f"select * from {table_name}", con=self._con)
    
    def write_replace(self, table_name, df:pd.DataFrame):
        """
        write df to table_name, replacing any existing table
        """
        return df.to_sql(
            table_name, 
            con=self._con, 
            if_exists="replace", 
            index=False
        )
    
    def write_update(self, table_name, df:pd.DataFrame):
        """
        Write df to table_name, updating according to index.
        * If a row in df has the same index as a row in table_name, then 
        table_name's row will be updated with the values in df's row
        * Else, df's row will be inserted into table_name
        """
        # I should probably set the index of df appropriately before passing in df
        try:
            # if the table doesn't already exist, create it from scratch
            return df.to_sql(
                table_name, 
                con=self._con, 
                if_exists="fail", 
                index=False
            )
        except ValueError:
            # print("okay, apparently the table already exists. let's append new rows")
            # if the table already exists, append new rows
            # I do this in a hacky way: I read all the data into memory, 
            # then concat old data with new data. then i check the result 
            # for duplicates and drop them, favoring new data. Then I 
            # overwrite all the data in the original table. 
            #print(df.iloc[0])
            df_old = self.read(table_name)
            # print("df_old shape: ", df_old.shape)
            # print(df_old.iloc[0])
            # print("----")
            # print(df_old.head())
            df_new = pd.concat([
                df_old.set_index(["FighterHref", "OpponentHref", "Date"]),
                df.set_index(["FighterHref", "OpponentHref", "Date"])
            ])
            # print("df_new shape: ", df_new.shape)
            # in the case of duplicate rows, use the result from df, which is more recent
            df_new = df_new.loc[~df_new.index.duplicated(keep="last")]
            df_new = df_new.reset_index()
            # print("df_new shape after dropping duplicates: ", df_new.shape)
            return df_new.to_sql(
                table_name, 
                con=self._con, 
                if_exists="replace",
                index=False
            )

base_db_interface = DbInterface(db_name="mma.db")