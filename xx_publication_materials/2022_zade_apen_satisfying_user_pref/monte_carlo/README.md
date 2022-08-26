Satisfying user preferences in community-based local energy markets — Auction-based clearing approaches
===
###### Scripts to reproduce the Monte Carlo simulation results presented in the Applied Energy publication https://doi.org/10.1016/j.apenergy.2021.118004


## Description
This readme describes how the Monte Carlo simulations can be reproduced, describes the steps leading to the results, 
and the most important configurations.

### How to reproduce the results?
Run the script *monte_carlo_lem_analysis_mit.py*

### What happens in the background?
The Monte Carlo simulations are performed with the *monte_carlo_lem_analysis_mit.py* file. At the bottom of the file the
main function contains the most important steps.
1. Load configuration file (.yaml file in the same folder)
2. Create a simulation folder (careful: simulations folder must exist beforehand). 
3. Save configuration to simulation folder
4. Perform Monte Carlo simulations in multiprocessing mode
5. Optional (uncommented): load pre-existing results and configurations
6. Evaluate and save placed and cleared market positions
7. Plot results


### How to configure the Monte Carlo simulation?
All important parameters for the Monte Carlo simulations are in the .yaml file. Within
this brief description, I highlight only the most important parameters.

*types_clearing_ex_ante*:   defines the clearing algorithms that will be executed (see market clearings in 
lemlab/lemlab/lem/clearing_ex_ante.py). 

*monte_carlo/n_iterations*: number of iterations of a trial. A trial is an execution of all clearing algorithms.

*monte_carlo/n_trials*:     defines how many trials shall be done. A trial is the execution of all clearing algorithms 
with the same number of inserted market positions.

*monte_carlo/n_positions_per_iteration*:    number of positions inserted in each trial.

*prosumer/general_number_of*:   number of prosumers registered on the market (should be larger than 
*n_positions_per_iteration*).

### You have questions that you cannot find answers to?

Write me a mail to michel.zade@tum.de and I will get back to you. 