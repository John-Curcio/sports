# READ ME

This is Curcio's MMA picks project

## Jupyter Notebooks

Notebooks are organized into nested folders. To run them, add the following code snippet before importing any modules:
```
import sys
sys.path.append('/path/to/parent/directory')
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

# Getting started

* Install anaconda on your machine if you haven't already done so
* Clone this repo
* Run `conda env create -f pystan2_env.yml`
* `python scrape_and_write_all_historical.py`. This will take a very long time to run, leave it overnight. Yes it's profoundly cheap.
* `python clean_all_data.py`
* If you run into a bug lmk (I'm sure you will)

# Making predictions

* `python scrape_and_write_all.py`
* `python clean_all_data.py`