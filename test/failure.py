#!/usr/bin/env python3
import numpy
import sys

from schedSim.configuration import Cluster
from schedSim.failureModel import FailureModelMTTBF


if __name__ == "__main__":
  c = Cluster()

  m = FailureModelMTTBF()
  m.setCluster(c)

  jobTime = 7*24*3600
  nodeCount = 1000

  if len(sys.argv) > 1:
    jobTime = float(sys.argv[1])
    nodeCount = float(sys.argv[2])

  fail = numpy.random.exponential(scale=c.nodeMTBF, size=c.nodes)
  fail = [x for x in fail if x < c.systemLifeDuration ]
  fail.sort()

  print("During the system runtime we would loose: %d nodes" % len(fail))

  print("Run 1/1000 in time")
  factor = 10
  t = c.nodeMTBF / factor
  count = 0

  for x in range(0, factor):
      fail = numpy.random.exponential(scale=c.nodeMTBF, size=c.nodes)
      count = count + ( min(fail) < t )
  print("Node failures when repeating the experiment 1000 times = %d" % count)

  print("Try to run a %.1f hour job on %d nodes" % (jobTime / 3600.0, nodeCount))
  count = 1000
  failureTimes = []
  for x in range(0, count):
    fail = m.checkWhenJobFails(nodeCount, jobTime)
    if fail == None:
      continue
    failureTimes.append(fail)

  print("The job fails %d out of %d times = %.2f %%" % (len(failureTimes), count, len(failureTimes) * 100.0 /count))

  print("Time when nodes are back:")
  for i in range(1, 10):
    x = m.timeUntilNodeIsBack()
    print("%.1f hours" % (x / 3600))

  #print("Failure times:")
  #for x in failureTimes:
    #print("%.1f hours" % (x / 3600.0))
