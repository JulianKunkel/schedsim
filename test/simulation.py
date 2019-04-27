#!/usr/bin/env python3

import sys

from schedSim.jobspawner import JobSpawnerNormalDistributed
from schedSim.configuration import Cluster
from schedSim.jobs import Job
from schedSim.simulator import Simulator
from schedSim.reporterHTML import ReporterHTML
from schedSim.reporterUtilization import ReporterUtilization
from schedSim.scheduler import FIFOScheduler, FIFOBackfillScheduler
from schedSim.schedulerAdvanced import BiggestFirstBackfillScheduler, LongestFirstBackfillScheduler

import numpy

if __name__ == "__main__":
  cluster = Cluster()
  jobspawner = JobSpawnerNormalDistributed(cluster)
  errorModel = False
  o_scheduler = "all"
  limitCount = 1000
  if len(sys.argv) > 1:
      limitCount = int(sys.argv[1])
  if len(sys.argv) > 2:
      o_scheduler = sys.argv[2]
  if len(sys.argv) > 3:
    errorModel = bool(sys.argv[3])

  numpy.random.seed(seed=3)
  jobs = jobspawner.jobs(limitCount)

  sim = Simulator()
  schedulerList = [FIFOScheduler(), FIFOBackfillScheduler(), BiggestFirstBackfillScheduler(),  LongestFirstBackfillScheduler()]

  print("Normal execution")
  executed = False
  for scheduler in schedulerList:
      schedName = type(scheduler).__name__
      if o_scheduler != "all" and o_scheduler != schedName:
          continue
      executed = True
      print(schedName)
      sim.simulate(cluster, jobs, scheduler, ReporterUtilization("normal-" + schedName), errorModel)
      #sys.exit(0)
  if not executed:
    print("Error: unknown scheduler: " + o_scheduler)
    sys.exit(1)

  print("\n\nExecution jobs are submitted at t=0")
  for j in jobs:
    j.submissionTime = 0

  for scheduler in schedulerList:
      schedName = type(scheduler).__name__
      if o_scheduler != "all" and o_scheduler != schedName:
          continue
      print(schedName)
      sim.simulate(cluster, jobs, scheduler, ReporterUtilization("zero-" + schedName), errorModel)
