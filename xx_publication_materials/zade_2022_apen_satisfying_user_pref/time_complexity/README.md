Time complexity analysis
===
###### Scripts to reproduce the time complexity analysis presented in the Applied Energy publication https://doi.org/10.1016/j.apenergy.2021.118004


## Description
This readme describes how the time complexity analysis can be reproduced, describes the steps leading to the results, 
and the most important configurations.

### How to reproduce the results?
Run the script *time_complexity_analysis.py*.

### What happens in the background?
The time complexity analysis is performed with the *time_complexity_analysis.py* file. At the bottom of the file the
main function contains the most important steps.
1. Create a simulations folder for the results if it does not exist already. 
2. Run time complexity analysis 
3. Plot results 
4. Optionally: results can be plotted from file (see uncommented part)

### How to configure the time complexity analysis?
All important parameters for the time complexity analysis are in the .yaml file. Within
this brief description, I highlight only the most important parameters.

*time_complexity/min_positions*:    smallest number of inserted market positions. 

*time_complexity/max_positions*:    highest number of inserted market positions.

*time_complexity/step_size*:        increment of market positions between min and max. 

*time_complexity/n_samples*:        number of samples for each step.

### You have questions that you cannot find answers to?

Write me a mail to michel.zade@tum.de and I will get back to you. 