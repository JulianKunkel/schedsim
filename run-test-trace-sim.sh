#!/bin/bash

# This script runs the sample experiment

source dev/bash.src

TYP=test-trace

./test/simulationSlurm.py --configuration data/$TYP/configuration.py --input data/$TYP/data.csv  --energy-model HourlyStockPrice --scheduler FIFOBackfillShutdownDelay --energy-model-argument data/eex/T1.csv --scheduler-argument 24,72
