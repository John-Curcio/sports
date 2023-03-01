# READ ME

work in progress for sure. including this README

* `data` contains *cleaned* data (you're welcome). It's small enough to fit in version control, luckily.
    * `full_bfo_ufc_espn_data_clean.csv` contains fight stats, some features, and money line information. It's scraped from [ESPN fighter pages](https://www.espn.com/mma/fighter/history/_/id/2560713/derrick-lewis), [UFC Stats pages](http://ufcstats.com/fight-details/beaa6ae419b8c8c6), and [bestfightodds fighter pages](https://www.bestfightodds.com/fighters/Khamzat-Chimaev-10189). This contains data up to May 14. 
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
* Open `example run 6-25.ipynb` and try to run it
    * On your first run, the model may take a few minutes to compile. Afterwards, the compiled model gets saved to `sports/stan_builds` so this should only happen once. 

# Making predictions

* `python scrape_and_write_all.py`
* `python clean_all_data.py`