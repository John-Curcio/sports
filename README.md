# READ ME

work in progress for sure. including this README

* `data` contains *cleaned* data (you're welcome). It's small enough to fit in version control, luckily.
    * `clean_bios.csv` is from [ESPN fighter bios](https://www.espn.com/mma/fighter/bio/_/id/4426000/ciryl-gane)
    * `clean_matches.csv` is from [ESPN fighter fight histories](https://www.espn.com/mma/fighter/history/_/id/2560713/derrick-lewis). There are many duplicate rows in here, because the opponent has a fight history page, too. But I think this is easy to filter and possibly useful.
    * `clean_stats.csv` is from [ESPN fighter stats](https://www.espn.com/mma/fighter/stats/_/id/4350762/zhang-weili). The column names are pretty cryptic, but **every fighter stats page has a glossary at the bottom.** 
    * `bfo_fighter_odds.csv` is from [bestfightodds fighter pages](https://www.bestfightodds.com/fighters/Khamzat-Chimaev-10189). This is kind of hard to join with data from espn.
    * `clean_stats_plus_ml.csv` is from joining `clean_stats.csv` and `bfo_fighter_odds.csv`. 
* `scrape` contains python for scraping historical sportsbook odds and ESPN stuff.
* `model` for model code.
* `model_selection` for cross-validation, metrics
* `pystan2_env.yml` enumerates the packages you might need for this repo. You should run `conda env create -f pystan2_env.yml` and then `conda activate sports_pystan2`. This relies on the pystan2 library, which is a huge pain to install on mac - ask me if you have any trouble. 
<!-- 
* `intro EDA.ipynb` **is the first file you should open.** It's short and light on words.
* `environment.yml` enumerates the packages you might need for this repository. They're pretty basic, but if you want to set up a conda environment for this, run `conda env create -f environment.yml` and then `conda activate sports`. -->

# Getting started

* Install anaconda on your machine if you haven't already done so
* Clone this repo
* Run `conda env create -f pystan2_env.yml`
* Open `example run.ipynb` and try to run it
    * On your first run, the model may take a few minutes to compile. Afterwards, the compiled model gets saved to `sports/stan_builds` so this should only happen once
    * If successful, scroll to the bottom
