# DataLiteracy <!-- TODO replace with paper title-->

## About

This is the **private** repository of our project for the
Data Literacy course 2023/24 at the University of Tübingen.

This repo accompanies our project report,
which you can find under <!-- TODO link paper or say where it is in the repo-->.

It contains all code we used in our project,
as well as the **private** data we used,
for which we have permission to share it with
the course instructors only.

The public repo, which does not contain this data,
can be found here: <!-- TODO link other repo-->

## Repo structure

The structure of this repo should be somewhat self-explanatory:
- `related_work.md` contains a brief overview over related work
- `requirements.txt` is a file specifying python dependencies
- `exp/` contains code related to our experiments (see below for
    information on how to run them)
- `fig/` contains code related to plotting and the plots
- `dat/` is supposed to hold the datasets and other related files
    - `train_data/`
        - `frankfurt_hbf` holds our dataset. This directory,
            as well as the contained datasets, are exclusive to this
            **private** repo.
- `src/` contains source code files that are imported in other
    parts of the code, as well as the preprocessing script
    `preprocessing.py`.

## Data set

We used data provided by `bahn-analysen.de` (by B&P Data Solutions UG).
You can look at
`dat/train_data/frankfurt_hbf/{scraped_incoming_Frankfurt_Hbf.csv,scraped_outgoing_Frankfurt_Hbf.csv}`
to see how it is structured.
It consists of two csv-files, one for incoming train connections to
Frankfurt(Main)Hbf, and one for outgoing train connections from
Frankfurt(Main)Hbf.
Every row contains an origin station, a destination station, the date, planned
departure, planned arrival, the train name, the delay at the destination and
whether the train was cancelled or not.

Contained is data from 20.01.2021 to 03.12.2023
of all long-distance trains that travel between any
German train station and the station Frankfurt(Main)Hbf,
which we picked because of its central position within the German train network.

## How to reproduce our results

### How to run our code

Assuming you have the data available as explained above
and you have python 3.10 or later installed,
you can run our code as follows:

1. Create and activate a python virtual environment:
    - `python -m venv path_to_your_venv`
    - `source path_to_your_venv/bin/activate`
2. Install the required dependencies:
    `pip install -r requirements.txt`
3. Run the data preprocessing script:
    `python src/preprocessing.py`

This will process the dataset and run some basic initial analysis.
The results of this, as well as all of the following experiments
will be stored in `dat/`, and enable you to also run the following:

4. Run the desired experiment(s) and plotting code
    - Experiment 6: Comparison of gain estimates:
        - Experiment:
            `python exp/006_comparison_of_gain_estimates/experiment_gains.py`
        - Plots:
            `python fig/fig_gains.py`: Creates <!-- TODO no clue if this is
            correct-->
                - `fig/plot_meanDelay_gain.pdf` (Figure 3 in the project report)
                <!-- TODO replace with appropriate file, ensure this is correct-->
    - Experiment 7: Mean delay of all origins to all destinations:
        - Experiment:
            `python exp/007_delays_all_origins_all_destinations/analysis_all_parallel.py`
        - Plots:
            `python fig/mean_delays.py`: Creates <!-- TODO no clue if this is
            correct-->
                - `fig/plot_meanDelay_reachability_switchTime.pdf`
                    (Figure 1 in the project report)
                - `fig/plot_meanDelay_heatmap.pdf` (Figure 2 in the project report)
                <!-- TODO replace with appropriate files, ensure this is correct-->
    - Experiment 8: Best and worst case convergence:
        - Experiment:
            `python exp/008_experiment_convergence/experiment_convergence.py`
        - Plots:
            `python fig/fig_delay_decomposition.py`: Creates <!-- TODO no clue if this is
            correct-->
                - `fig/plot_delay_decomposition.pdf` (Figure 4 in the project report)
                <!-- TODO replace with appropriate files, ensure this is correct-->

Note that in the above, it is assumed that your working
directory is set to the root directory of this repo.
This is not necessary, but if you have a different working
directory, ensure that you use the correct file paths.

### Parallelization

Experiment 7 and experiment 8 utilize (hard-coded) parallelization to decrease
processing time. This also means that they may take up a large part of your
system's resources. In order to change this, you can edit the corresponding
python scripts, and change the line
`ParallelPandas.initialize(n_cpu=N, ...)`, where `N` corresponds to the
number of threads that will be used (1 turns off parallelization).

### Caveats

The code is very unoptimized. As we are working on a somewhat large dataset,
running the experiments will take a *long* time (eg. ~5h for experiment 7).
Experiments 7 and 8 are also RAM-intensive, and on our systems,
we have experienced ParallelPandas silently crashing/freezing if not enough RAM
was available for it, eg. if we tried to run multiple experiments
simultaneously.
