# SchedSim
This repository contains the Python3 program SchedSim that can be used to simulate various energy efficiency scheduling strategies.
It can digest traces from LoadLeveler and (better) SLURM.

# Directory structure

 * analysis - Contains plotting tools
 * data/eex - This directory contains the hourly data generated for a paper
 * data/test-trace - This directory contains a SLURM trace
 * dev - Contains additional utilities
   - e.g., to dump SLURM jobs as trace. New traces must be prepared using --prepareCSV.
 * schedSim - The Core Python Code of SchedSim
 * test - The binaries to execute Schedsim

# Running the example simulation

The following command runs the example trace using the system configuration in data/test-trace
$ ./run-test-trace-sim.sh

To see the available options, run:
$ source dev/bash.src # This changes the PythonPath
$ ./test/simulationSlurm.py -h

Note that when running a CSV, loading takes considerable amount of time.
Additionally, the array of jobs can be stored using the Pickle format.

Conversion to Pickle can be done, for example, using:
$ ./test/simulationSlurm.py --input data/test-trace/data.csv --convert data.p

New traces must be converted using:
$ ./test/simulationSlurm.py --input slurm.csv --prepareCSV data.csv

# Analyzing the results

Result of the simulation is stored in the file: output-stats.csv

This script generates statistics:
$ ./analysis/statistics.R output-stats.csv

This script generates images of the utilization:
$ ./analysis/utilization.R output-stats.csv

# Publications
Interference of Billing and Scheduling Strategies for Energy and Cost Savings in Modern Data Centers
https://doi.org/10.1016/j.suscom.2019.04.003
