#!/bin/bash

source  source

function run(){
  if [[ ! -e $out.txt ]] ; then
    echo "Running $out.txt"
    LIMIT="--limit-count 10000"
    LIMIT=""
    (
    time ./test/simulationSlurm.py $@ $LIMIT --configuration data/$TYP/configuration.py --report-outname $out
    ./analysis/statistics.R "$out-stats.csv"
    ) >$out.txt 2>&1
  fi
}

for TYP in dkrz  dkrz-2017  dkrz-2017-832  lrz-2017 lrz lrz-744; do
  mkdir -p output/$TYP

  for data in timeline-fake-data2 timeline2 timeline-sine; do
    for scheduler in FIFOPriceAwareShutdown PriceAwareShutdown EnforcePriceAwareShutdown; do
      for hours in 12 24 36 48 72 96 ; do
        out=output/$TYP/HourlyStockPrice/$data/$scheduler-$hours
        mkdir -p $(dirname $out)
        run --input data/$TYP/data.p  --energy-model HourlyStockPrice --energy-model-argument eex/$data.csv --scheduler $scheduler --scheduler-argument $hours
      done
    done

    for scheduler in FIFO FIFOBackfill FIFOBackfillDelay FIFOBackfillShutdown FIFOBackfillShutdownDelay  BiggestFirstBackfill LongestFirstBackfill; do
      out=output/$TYP/HourlyStockPrice/$data/$scheduler
      run --input data/$TYP/data.p  --energy-model HourlyStockPrice --energy-model-argument eex/$data.csv --scheduler $scheduler
    done
  done

  for scheduler in FIFOPriceAwareShutdown PriceAwareShutdown EnforcePriceAwareShutdown; do
    for hours in 12 24 36 ; do
      out=output/$TYP/DayNightPrice/$scheduler-$hours
      mkdir -p $(dirname $out)
      run --input data/$TYP/data.p  --energy-model DayNightPrice  --scheduler $scheduler --scheduler-argument $hours
    done
  done


  for price in FixedPrice DayNightPrice ; do
    for scheduler in FIFO FIFOBackfill FIFOBackfillDelay FIFOBackfillShutdown FIFOBackfillShutdownDelay  BiggestFirstBackfill LongestFirstBackfill; do
      out=output/$TYP/$price/$scheduler
      mkdir -p $(dirname $out)
      run --input data/$TYP/data.p  --energy-model $price  --scheduler $scheduler
    done
  done

done
