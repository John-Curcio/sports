# READ ME

This is Curcio's MMA picks project

## Jupyter Notebooks

Notebooks are organized into nested folders. To run them, add the following code snippet before importing any modules:
```
import sys
sys.path.append('/Users/john/play/sports/') # add parent directory to path
```
* `EDA`
* `EXPERIMENTS`
* `HACK`

## Code
* `model`
    * `mma_features.py` - contains useful classes like `RealEloWrapper`, `BinaryEloWrapper`, `PcaEloWrapper`, `AccEloWrapper`
    * `mma_log_reg_stan.py` 
* `model_selection`
    * `cross_val_pipeline.py` - contains `TimeSeriesCrossVal`
    * `metrics.py` - contains `MultiKellyPM`
* `scrape`
* `wrangle`
    * `simple_features.py`
* `db.py` - contains `base_db_interface`, a handy connection to the `mma.db` sqlite3 database
* `pystan2_env.yml` - Enumerates the packages you might need for this repo. You should run `conda env create -f pystan2_env.yml` and then `conda activate sports_pystan2`. This relies on the pystan2 library, which is a huge pain to install on mac - ask me if you have any trouble. 
* `scrape_and_write_all_historical.py`

# Diving back in
* GDI I have got to get this shit organized into pipelines. This sucks so hard.
* Working on incorporating proposition odds prices scraped from bestfightodds.com. As of my writing this, I've only been joining the opening money lines with fights scraped from ufcstats.com. Now, I want to join it all with espn.

# Getting started

* Install anaconda on your machine if you haven't already done so
* Clone this repo
* Run `conda env create -f pystan2_env.yml`
* Open `example run 6-25.ipynb` and try to run it
    * On your first run, the model may take a few minutes to compile. Afterwards, the compiled model gets saved to `sports/stan_builds` so this should only happen once. 

# Making predictions

* `python scrape_and_write_all.py`
* `python clean_all_data.py`