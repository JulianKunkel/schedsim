#!/usr/bin/env python3

import numpy
import time
import sys
import argparse
import importlib


from schedSim.energyCosts import energyCostModelFactory
from schedSim.jobReader import JobReader
from schedSim.jobs import Job
from schedSim.simulator import Simulator
from schedSim.reporterUtilization import ReporterUtilization
from schedSim.scheduler import SchedulerFactory

if __name__ == "__main__":
  parser = argparse.ArgumentParser( description='Simulator for BatchJobs' )

  parser.add_argument("--input", type=str, help="the input file")
  parser.add_argument('--convert', type=str, help='Convert the data using pickle, define the output file (for convert)')
  parser.add_argument('--prepareCSV', type=str, help='Convert the CSV data dumped by SLURM, define the output file (for convert)')
  parser.add_argument('--limit-count', type=int, help='Limit the number of operations to process')
  parser.add_argument('--scheduler', type=str, help='The scheduling algorithm to use (use list to see available)', default="FIFO")
  parser.add_argument('--scheduler-argument', type=str, help='Any argument for the scheduler', default="")
  parser.add_argument('--error-model', type=bool, help='Use the error model True')
  parser.add_argument('--energy-model', type=str, help='The energy model to use (use list to see available)', default="FixedPrice")
  parser.add_argument('--energy-model-argument', type=str, help='The model argument to use', default="data/eex/T1.csv")
  parser.add_argument('--report-outname', type=str, help='The name of the output file', default="output")
  parser.add_argument('--configuration', type=str, help='The configuration file', default="")
  parser.add_argument('--print_jobs', action="store_true", help='Print the jobs', default=False)
  parser.add_argument('--set_submission_time_zero', action="store_true", help='Set all submission times to zero', default=False)

  args = parser.parse_args()

  jobspawner = JobReader()
  o_scheduler = "all"
  errorModel = False
  limitCount = 100000000

  if args.convert:
    print("Converting!")

  inputFile = args.input

  if args.prepareCSV:
    print("Converting!")
    jobspawner.prepareSlurm([inputFile], args.prepareCSV)
    sys.exit(0)

  if args.limit_count:
      limitCount = args.limit_count
  if args.scheduler:
      o_scheduler = args.scheduler
  if args.error_model:
    errorModel = args.error_model

  # jobspawner.prepareSlurm(["jobs-2015.txt", "jobs-2016.txt"], "jobs-dkrz.txt")
  energyModel = energyCostModelFactory.createModel(args.energy_model, args.energy_model_argument)

  schedFactory = SchedulerFactory()
  scheduler = schedFactory.createScheduler(args.scheduler, args.scheduler_argument)

  print("Parsing")
  t0 = time.clock()
  #jobs = jobspawner.jobs("data/dkrz/jobs-dkrz.txt", limitCount=limitCount)
  jobs = jobspawner.jobs(inputFile, limitCount=limitCount)
  #jobs = jobspawner.jobs("data/dkrz/jobs-dkrz.p", limitCount=limitCount, partition=["compute"])
  dt = time.clock() - t0
  print("Parsing time: %.1fs" % dt)

  if args.convert:
    JobReader.convertJobsToBinary(jobs, args.convert)
    sys.exit(1)

  if args.set_submission_time_zero:
    for j in jobs:
      j.submissionTime = 1433023200

  if args.print_jobs:
    for j in jobs:
      print(j)

  clusterModule = exec(open(args.configuration, "r").read())
  cluster = Cluster()

  sim = Simulator()

  sim.simulate(cluster, jobs, scheduler, energyModel, ReporterUtilization(args.report_outname), errorModel)

  sys.exit(0)

  # Old stuff for testing

  schedulerList = [FIFOScheduler(), FIFOBackfillScheduler(), BiggestFirstBackfillScheduler(),  LongestFirstBackfillScheduler()]

  print("Normal execution")
  for scheduler in schedulerList:
      schedName = type(scheduler).__name__
      print(schedName)
      sim.simulate(cluster, jobs, scheduler, energyModel, ReporterUtilization("normal-" + schedName), errorModel)
      #sys.exit(0)

  print("\n\nExecution jobs are submitted at t=1433023200")
  for j in jobs:
    j.submissionTime = 1433023200

  schedulerList = [schedFactory.createScheduler(args.scheduler, args.scheduler_argument)]
  for scheduler in schedulerList:
      schedName = type(scheduler).__name__
      print(schedName)
      sim.simulate(cluster, jobs, scheduler, energyModel, ReporterUtilization("zero-" + schedName), errorModel)
