import numpy

class FailureModelMTTBF:
  def setCluster(self, cluster):
    self.c = cluster

  def checkWhenJobFails(self, nodeCount, jobRuntime):
    fail = numpy.random.exponential(self.c.nodeMTBF, nodeCount)

    mn = min(fail)
    if mn < jobRuntime:
      return mn
    return None

  def timeUntilNodeIsBack(self):
     vals = numpy.random.normal(loc=self.c.nodeMTTR, scale=self.c.nodeMTTRdeviation, size=1)
     return max(vals[0], self.c.nodeMinRepairTime)
