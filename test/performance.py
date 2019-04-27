#!/usr/bin/env python3

import time

from schedSim.jobspawner import JobSpawnerNormalDistributed
from schedSim.configuration import Cluster
from schedSim.jobs import Job
from schedSim.simulator import Simulator
from schedSim.reporter import SilentReporter
from schedSim.scheduler import BiggestFirstScheduler, FIFOScheduler

import numpy

if __name__ == "__main__":
  cluster = Cluster()
  jobspawner = JobSpawnerNormalDistributed(cluster)
  numpy.random.seed(seed=3)

  sim = Simulator()
  scheduler = BiggestFirstScheduler()
  scheduler = FIFOScheduler()

  jobCount = 1000
  for size in range(1, 12):
    jobs = jobspawner.jobs(jobCount, 0)

    t0 = time.clock()
    sim.simulate(cluster, jobs, scheduler, SilentReporter())
    dt = time.clock() - t0

    print("%d %.2fs = %.0f jobs/s" %(jobCount, dt, jobCount / dt))

    jobCount = jobCount * 2
