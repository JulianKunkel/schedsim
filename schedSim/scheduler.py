class Scheduler:
  'This class represents the job scheduler'

  cluster = None
  energyModel = None

  def setCluster(self, cluster, energyModel):
    self.cluster = cluster
    self.energyModel = energyModel

  def schedulingDelay(self):
    'This is a time delay for the simulator during which newly submitted jobs are batched until they are submitted to the scheduler via the newPendingJobs() method'
    return 1

  def newPendingJobs(self, jobs, time):
    'Indicate that several new job was submitted, this should trigger re-computation of the schedule'

  def tryToSchedule(self, time, jobCompleted):
    'Return a list of triples: Job, Duration, Partition (where to run) and update the runningJobList'
    return [] # (None, None, None)

  def jobCompleted(self, job, time):
    'The given job completed its execution'

  def jobAbortedWithErrors(self, job, time):
    'The jobs is stopped with a failure '

  def pickJobOptionsForSchedule(self, job, schedList):
    schedList.append((job, job.durationMin, {"cpu_pstate": 4}));

  def submitAllJobsWithStartTime(self, jobs, time):
    pass

class FIFOScheduler(Scheduler):

  pendingList = []

  def newPendingJobs(self, jobs, time):
    self.pendingList.extend(jobs)

  def jobAbortedWithErrors(self, job, time):
    # Add the job again on front of the list, i.e., it will re-run
    self.pendingList.insert(0, job)

  def tryToSchedule(self, time, jobCompleted):
    if not self.pendingList:
      return []
    # Now try to retrieve as many jobs from the list as fit.
    schedList = []
    freeNodes = self.cluster.nodes
    while(self.pendingList):
      job = self.pendingList[0]

      if freeNodes < job.nodes:
        return schedList
      freeNodes = freeNodes - job.nodes
      self.pendingList.pop(0)
      self.pickJobOptionsForSchedule(job, schedList)
    return schedList


class FIFOBackfillDelayScheduler(FIFOScheduler):
  'Try to backfill from the first N jobs but ignore if an already pending job is delayed'
  backfillLength = 1000

  def tryToSchedule(self, time, jobCompleted):
    if not self.pendingList:
      return []

    # Now try to retrieve as many jobs from the list as fit.
    schedList = []
    freeNodes = self.cluster.nodes
    i = 0
    while( i < len(self.pendingList) ):
      job = self.pendingList[i]
      if freeNodes == 0:
        return schedList

      #print("%d %d %d" % (i, time, job.nodes))

      if freeNodes < job.nodes:
        # now we backfill
        i = i + 1
        if i > self.backfillLength:
          return schedList
        continue

      freeNodes = freeNodes - job.nodes
      self.pendingList.pop(i)
      self.pickJobOptionsForSchedule(job, schedList)
    return schedList

from heapq import heappop, heappush

def findJobBuddy(pendingBuddy, n):
  old = None
  for k in pendingBuddy:
    if k < n:
      return old
    old = k

class FIFOBackfillTest(Scheduler):
    'This scheduler creates a FIFO schedule but can backfill if this won\'t delay any later job'
    pendingFIFO = []

    def submitAllJobsWithStartTime(self, jobs, time):
      "Create a single schedule"
      freeNodes = self.cluster.nodes

      freeFuture = {} # contains buddy, (timestamp, freedNodes)
      x = freeNodes
      while x >= 1:
        freeFuture[x] = []
        x = int(x / 2)

      # List with the FIFO order but exponentially ordered, for backfilling
      for j in jobs:
        if j.submissionTime > time:
          time = j.submissionTime #earliest time job can run
          # add all completed jobs
          for k in freeFuture:
            for (j, t) in freeFuture[k]:
              if t <= time:
                freeFuture[k].pop(0)
                freeNodes += j.nodes
              else:
                break
        # check if the job fits
        if j.nodes <= freeNodes:
          # schedule it !
          buddy = findJobBuddy(freeFuture, j.nodes)
          freeFuture[buddy].append((j, time + j.durationMin))
          freeNodes = freeNodes - j.nodes
          self.pendingFIFO.append((j, time))
          continue
        # complex case, when will the job possibly run?
        buddy = findJobBuddy(freeFuture, j.nodes - freeNodes)
        print("Buddy: %d FreeNodes: %d JobNodes: %d" % (buddy, freeNodes, j.nodes))


    def newPendingJobs(self, jobs, time):
      pass

    def jobAbortedWithErrors(self, job, time):
      pass

    def tryToSchedule(self, time, jobCompleted):
      # Try to retrieve as many jobs from the list as fit from the biggest size
      schedList = []
      freeNodes = self.cluster.nodes
      for (j, startMin) in self.pendingFIFO:
        if startMin <= time:
          self.pendingFIFO.pop(0)
          self.pickJobOptionsForSchedule(j, schedList)
        else:
          return schedList
      return schedList

class FIFOBackfillScheduler(FIFOScheduler):
  'Try to backfill from the first N jobs'
  backfillLength = 1000

  # currently dispatched jobs
  dispatchedJobs = []

  def purgeExpiredJobs(self, time):
    while(self.dispatchedJobs):
      if self.dispatchedJobs[0][0] > time:
        self.dispatchedJobs.pop(0)
      else:
        return

  def delaysExecutionOfPriorJob(self, time, job, freeNodes):
    # check if the job would delay the highest prior job
    # freeNodes: the number of currently available nodes
    if not self.pendingList:
      return False
    nodesHighest = self.pendingList[0].nodes

    nodeJobs = job.nodes
    if freeNodes - nodeJobs >= nodesHighest: # the job fits together with the highest prior job anyway
      return False

    # now check when the highest priority job would start
    dispatchTime = 0
    for j in self.dispatchedJobs:
      freeNodes += j[1]
      if freeNodes >= nodesHighest:
        dispatchTime = j[0]
        break

    if freeNodes - nodeJobs >= nodesHighest: # the job fits together with the highest prior job anyway
      return False

    # check if the job ends after dispatchTime
    #print([time + job.durationMin, dispatchTime, freeNodes, nodesHighest])
    return dispatchTime > time + job.durationMin

  def dispatchJobs(self, time, schedList):
    for job, runtime, _ in schedList:
      heappush(self.dispatchedJobs, [runtime + time, job.nodes])

  def tryToSchedule(self, time, jobCompleted):
    if not self.pendingList:
      return []

    # Now try to retrieve as many jobs from the list as fit.
    schedList = []
    freeNodes = self.cluster.nodes
    i = 0

    self.dispatchedJobs.sort() # sort the list
    self.purgeExpiredJobs(time)

    while( i < len( self.pendingList) ):
      job = self.pendingList[i]
      if freeNodes == 0:
        return schedList

      #print("%d %d %d" % (i, time, job.nodes))

      if job.nodes > freeNodes or (i != 0 and self.delaysExecutionOfPriorJob(time, job, freeNodes)):
        i = i + 1
        if i > self.backfillLength:
          # backfill length too big
          self.dispatchJobs(time, schedList)
          return schedList
        continue

      freeNodes -= job.nodes
      self.pendingList.pop(i)
      self.pickJobOptionsForSchedule(job, schedList)
    self.dispatchJobs(time, schedList)
    return schedList

from schedSim.schedulerAdvanced import BiggestFirstBackfillScheduler, LongestFirstBackfillScheduler
from schedSim.schedulerEE import FIFOBackfillShutdownScheduler, FIFOPriceAwareShutdownScheduler, PriceAwareShutdownScheduler, EnforcePriceAwareShutdownScheduler, FIFOBackfillShutdownDelayScheduler

class SchedulerFactory():
  def createScheduler(self, name, argument):
    if name == "test":
      return FIFOBackfillTest()
    if name == "FIFO":
      return FIFOScheduler()
    if name == "FIFOBackfillDelay":
      return FIFOBackfillDelayScheduler()
    if name == "FIFOBackfill":
      return FIFOBackfillScheduler()
    if name == "FIFOPriceAwareShutdown":
      return FIFOPriceAwareShutdownScheduler(argument)
    if name == "BiggestFirstBackfill":
      return BiggestFirstBackfillScheduler()
    if name == "LongestFirstBackfill":
      return LongestFirstBackfillScheduler()
    if name == "FIFOBackfillShutdown":
      return FIFOBackfillShutdownScheduler()
    if name == "FIFOBackfillShutdownDelay":
      return FIFOBackfillShutdownDelayScheduler()
    if name == "PriceAwareShutdown":
      return PriceAwareShutdownScheduler(argument)
    if name == "EnforcePriceAwareShutdown":
      return EnforcePriceAwareShutdownScheduler(argument)
    if name == "list":
      print(["FIFO" , "FIFOBackfill", "FIFOBackfillDeadline", "FIFOBackfillShutdown" , "BiggestFirstBackfill" , "LongestFirstBackfill", "FIFOPriceAwareShutdown", "PriceAwareShutdown", "EnforcePriceAwareShutdown"])
    raise Exception("No valid model defined!")
