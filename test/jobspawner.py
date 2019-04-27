#!/usr/bin/env python3

from setuptools import setup, find_packages


from schedSim.jobspawner import JobSpawnerNormalDistributed
from schedSim.configuration import Cluster
from schedSim.jobs import Job


if __name__ == "__main__":
  cluster = Cluster()
  jobspawner = JobSpawnerNormalDistributed(cluster)

  for j in jobspawner.jobs(100) :
    print(j)
