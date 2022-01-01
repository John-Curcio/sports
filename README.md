# READ ME

work in progress for sure. including this README

* `data` contains *cleaned* data (you're welcome). It's small enough to fit in version control, luckily.
    * `clean_bios.csv` is from [ESPN fighter bios](https://www.espn.com/mma/fighter/bio/_/id/4426000/ciryl-gane)
    * `clean_matches.csv` is from [ESPN fighter fight histories](https://www.espn.com/mma/fighter/history/_/id/2560713/derrick-lewis). There are many duplicate rows in here, because the opponent has a fight history page, too. But I think this is easy to filter and possibly useful.
    * `clean_stats.csv` is from [ESPN fighter stats](https://www.espn.com/mma/fighter/stats/_/id/4350762/zhang-weili). The column names are pretty cryptic, but **every fighter stats page has a glossary at the bottom.** Duplicates should absolutely be kept here. 
    * `ufc_moneylines.csv` is from [sportsbookreview ufc money lines](https://www.sportsbookreview.com/betting-odds/ufc/?date=20211204), joined with info from `clean_matches.csv` to get each IDs for each fighter.
    * **Each fighter profile has a unique `FighterID`.** There might be some duplicate IDs out there (are there really five different russian MMA fighters all named Magomed Magomedov?) but honestly it seems like this isn't a huge problem.
* `scrape` contains python for scraping historical sportsbook odds and ESPN stuff
* `intro EDA.ipynb` **is the first file you should open.** It's short and light on words.

Eventual goal is to get good picks for UFC and Bellator cards, and just mess around. Who would win, Khabib or Jon Jones in their respective primes? Who should I bet on tonight?

Some tasks we'll have to do along the way:
* Exploratory analysis on fighter stats. EG total strike breakdown, takedown acc, etc, at the match level and overall. 
    * correlation heatmap of features, PCA, clustering, etc
    * Fight2Vec?
* When a fight goes to a decision, who's declared the winner at the end?
* What's a given fighter's weakness? 
* How good is Elo alone? How good is ignoring fighter identity and just comparing reach/weight?


129328021390180-3980-3843908204-

Rocus rocus pocus


rocus2