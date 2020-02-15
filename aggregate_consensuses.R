# Yeah uh here we take in all the sbr_data_{year}.csv files, 
# stratify by league, take the market consensuses, and save them to 
# {league}_consensuses.csv

# columns to save: game_id, date, home_team, away_team
# home_score, away_score
# avg_spread (just average of spread_home)
# avg_spread_money_home
# avg_spread_money_away
# avg_money_home
# avg_money_away

df <- data.frame()
for(curr_year in c(2011:2018)){
  path <- paste("sports/data/sbr_data_", curr_year, ".csv", sep="")
  print(path)
  curr_df <- read.csv(path, check.names = FALSE)
  df <- rbind(df, curr_df)
}
print("Done loading sbr_data. On to some type juggling")

markets <- c("Pinnacle Sports",
             "5Dimes",
             "Bookmaker",
             "BetOnline",
             "Bovada",
             "Heritage",
             "Intertops",
             "YouWager",
             "JustBet",
             "Sportsbetting")

col_suffixes <- c("spread_home", 
                  "spread_money_home", 
                  "spread_money_away",
                  "money_home", 
                  "money_away")
new_cols <- paste("avg", col_suffixes, sep="_")

# Reformat market data to numeric type
for(market in markets){
  for(col_suff in col_suffixes){
    col_name <- paste(market, col_suff, sep="_")
    df[col_name] <- as.numeric(unlist(df[col_name]))
  }
}

print("Done with type conversion character --> numeric. On to aggregation")

for(curr_league in unique(df$league)){
  print(paste("aggregating data for", curr_league))
  league_df <- df[df$league == curr_league,]
  for(col_suff in col_suffixes){
    old_cols <- paste(markets, col_suff, sep="_")
    new_col <- paste("avg", col_suff, sep="_")
    # Have to convert file type to numeric
    league_df[new_col] <- rowMeans(league_df[,old_cols], 
                                   na.rm=TRUE)
  }
  
  cols_to_keep <- c("game_id", "date", "home_team", "away_team",
                    "home_score", "away_score", new_cols)
  curr_path <- paste("C:/Users/John/Documents/sports/data/", 
                         paste(curr_league, "consensuses.csv", sep="_"),
                         sep="")
  write.csv(league_df[,cols_to_keep], file=curr_path)
}

print("Finished!")
