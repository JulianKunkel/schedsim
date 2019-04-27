from schedSim.scheduler import FIFOScheduler, FIFOBackfillScheduler, FIFOBackfillDelayScheduler
from schedSim.jobs import Job

import datetime
import sys

class FIFOBackfillShutdownScheduler(FIFOBackfillScheduler):
  'Try to backfill from the first N jobs but shutdown nodes'
  backfillLength = 1000
  sleepingNodes = 0

  def tryToSchedule(self, time, jobCompleted):
    if not self.pendingList:
      self.cluster.nodes += self.sleepingNodes # wakeup nodes virtually
      self.sleepingNodes = self.cluster.nodes
      self.cluster.nodes = 0
      return []

    self.cluster.nodes += self.sleepingNodes # wakeup nodes virtually
    self.sleepingNodes = 0

    # Now try to retrieve as many jobs from the list as fit.
    schedList = []
    freeNodes = self.cluster.nodes
    i = 0

    self.dispatchedJobs.sort() # sort the list
    self.purgeExpiredJobs(time)

    while( i < len(self.pendingList) ):
      job = self.pendingList[i]
      if freeNodes == 0:
        self.dispatchJobs(time, schedList)
        return schedList
      # print(job.submissionTime)

      #print("%d %d %d" % (i, time, job.nodes))

      if freeNodes < job.nodes or (i != 0 and self.delaysExecutionOfPriorJob(time, job, freeNodes)):
        # now we backfill
        i = i + 1
        if i > self.backfillLength:
          self.sleepingNodes = freeNodes
          self.cluster.nodes -= freeNodes
          return schedList
        continue

      freeNodes = freeNodes - job.nodes
      self.pendingList.pop(i)
      self.pickJobOptionsForSchedule(job, schedList)

    self.sleepingNodes = freeNodes
    self.cluster.nodes -= freeNodes
    self.dispatchJobs(time, schedList)
    return schedList


class FIFOBackfillShutdownDelayScheduler(FIFOBackfillDelayScheduler):
  'Try to backfill from the first N jobs but shutdown nodes'
  backfillLength = 1000
  sleepingNodes = 0

  def tryToSchedule(self, time, jobCompleted):
    if not self.pendingList:
      self.sleepingNodes += self.cluster.nodes
      self.cluster.nodes = 0
      return []

    self.cluster.nodes += self.sleepingNodes # wakeup nodes virtually
    self.sleepingNodes = 0

    # Now try to retrieve as many jobs from the list as fit.
    schedList = []
    freeNodes = self.cluster.nodes
    if freeNodes == 0:
      return schedList

    i = 0
    while( i < len(self.pendingList) ):
      job = self.pendingList[i]
      #print("%d %d %d" % (i, time, job.nodes))
      if freeNodes < job.nodes:
        # now we backfill
        i = i + 1
        if i > self.backfillLength:
          self.sleepingNodes = freeNodes
          self.cluster.nodes -= freeNodes
          return schedList
        continue

      freeNodes = freeNodes - job.nodes
      self.pendingList.pop(i)
      self.pickJobOptionsForSchedule(job, schedList)
    self.sleepingNodes = freeNodes
    self.cluster.nodes -= freeNodes

    return schedList


class FIFOPriceAwareShutdownScheduler(FIFOScheduler):
  'FIFO but take the costs for the energy of the next hoursAhead into consideration, uses shutdown of nodes; does enforce to run a job if it waited for hoursAhead time already'
  sleepingNodes = 0

  backfillLength = 100
  hoursAhead = 0
  #hoursMaxWaitingTime = 0
  costsIdleNodeInWatt = None
  sleepEndTime = 0

  def setCluster(self, cluster, energyModel):
    super().setCluster(cluster, energyModel)
    self.costsIdleNodeInWatt = cluster.infrastructurePowerConsumption / cluster.nodes


  def __init__(self, arg):
    #data = arg.split(",")
    print("FIFOPriceAwareShutdownScheduler arguments: " + str(arg))
    self.hoursAhead = int(arg)
    #self.hoursMaxWaitingTime = int(data[1])
    #print("hours ahead: %s max-waiting-time-hours: %s" % tuple(data))

  def tryToSchedule(self, time, jobCompleted):
    if not self.pendingList:
      self.cluster.nodes += self.sleepingNodes # wakeup nodes virtually
      self.sleepingNodes = self.cluster.nodes
      self.cluster.nodes = 0
      return []

    # Now try to retrieve as many jobs from the list as fit.
    schedList = []
    self.cluster.nodes += self.sleepingNodes # wakeup nodes virtually
    self.sleepingNodes = 0
    freeNodes = self.cluster.nodes

    if freeNodes < self.pendingList[0].nodes:# or time < self.sleepEndTime:
      self.sleepingNodes = freeNodes
      self.cluster.nodes = 0

      return []

    # find price for next X hours
    price = []
    for i in range(0, self.hoursAhead):
      price.append(self.energyModel.timestampPrice(time + i*3600))

    timestamp = datetime.datetime.fromtimestamp(time)

    i = 0
    while( i < len(self.pendingList) ):
      job = self.pendingList[i]
      #print(job)

      #print(job)
      if freeNodes == 0:
        return schedList
      #print("%d %d %d" % (i, time, job.nodes))
      if freeNodes < job.nodes:
        break
      # energy costs when we keep the nodes of the job idle
      nodesForJobIdleConsumption = self.costsIdleNodeInWatt * job.nodes
      #print(nodesForJobIdleConsumption)
      jobPowerConsumption = job.powerConsumed(4, self.cluster) + nodesForJobIdleConsumption

      hoursNeeded = int(job.durationMin / 3600)

      # check job submissionTime
      if (time - job.submissionTime)/3600 <= self.hoursAhead and hoursNeeded + 2 < self.hoursAhead:
        # we may delay the job
        remainingSeconds = job.durationMin % 3600
        # compute energy cost for all starting times, for all windows
        cheapestPrice = 0
        expensivePrice = 0
        cheapestConfig = []

        # price for running the job now!
        tSecRemainThisHour = (59 - timestamp.minute) * 60 + 60 - timestamp.second
        if tSecRemainThisHour > remainingSeconds:
          cPrice = price[0] * remainingSeconds
        else:
          cPrice = price[0] * tSecRemainThisHour
          for h in range(1, hoursNeeded):
            cPrice += price[h] * 3600
          tRemaining = job.durationMin - tSecRemainThisHour - 3600 * (hoursNeeded - 1)
          if tRemaining > 0:
            cPrice += price[hoursNeeded] * tRemaining
        cPrice = cPrice * jobPowerConsumption / 1000 / 3600 * 0.999 # favorise immediate start

        cheapestConfig = [0, False]
        cheapestPrice = cPrice
        expensivePrice = cPrice
        #print(cPrice)

        # price for delaying the job
        # price for delaying until the end of this hour
        idlePrice = price[0] * tSecRemainThisHour * nodesForJobIdleConsumption / 1000 / 3600
        secondsFullHourEmpty = (3600 - remainingSeconds)

        if job.durationMin < 3600:
          for w in range(1, self.hoursAhead):
            #print(idlePrice)
            cPrice = idlePrice + price[w] * jobPowerConsumption * remainingSeconds / 1000 / 3600
            if cPrice < cheapestPrice:
              cheapestPrice = cPrice
              cheapestConfig = [w, True]
            idlePrice += price[w] * nodesForJobIdleConsumption / 1000 * 3600 / 3600
        else:
          for w in range(1, self.hoursAhead - hoursNeeded - 1):
            # pay a price for leaving the node empty:
            cPrice = idlePrice

            #print(idlePrice)

            # shortcut
            if idlePrice > cheapestPrice:
              break

            # price for running the job
            for h in range(w, hoursNeeded + w):
              cPrice += price[h] * jobPowerConsumption / 1000

            # check bounding box, first and last interval
            # Compute price when running it now, or when running it in the last hour
            pLast = price[hoursNeeded + w + 1] * (jobPowerConsumption * remainingSeconds + secondsFullHourEmpty * nodesForJobIdleConsumption ) / 1000 / 3600

            pFirst = price[w - 1] * (remainingSeconds * jobPowerConsumption + (secondsFullHourEmpty - remainingSeconds) * nodesForJobIdleConsumption) / 1000 / 3600

            ePrice = cPrice
            if pFirst > pLast or w == 1:
              last = True
              cPrice += pLast
              ePrice += pFirst
            else:
              last = False
              cPrice += pFirst
              ePrice += pLast
            #print(cPrice)

            if cPrice < cheapestPrice:
              cheapestPrice = cPrice
              cheapestConfig = [w, last]
            if cPrice > expensivePrice:
              expensivePrice = cPrice

            # pay a price for leaving the node empty for this hour
            idlePrice += price[w] * nodesForJobIdleConsumption / 1000

        (hour, last) = cheapestConfig
        #print("Cheapest %d %d" % (hour, last))
        if hour != 0: # otherwise schedule now!
          delay = (hour-1)*3600
          if last:
            delay += tSecRemainThisHour
          else: #first
            delay += tSecRemainThisHour - remainingSeconds
          self.sleepingNodes = freeNodes
          self.cluster.nodes -= freeNodes

          self.sleepEndTime = time + delay
          #print([time, cheapestConfig, job, self.sleepEndTime])
          return schedList + [[Job(jobid="SleepScheduling",dummy=True), delay, "default" ]]
        #print([time, cheapestConfig, job])

      #else:
        #print("Enforce to dispatch!")

      # now we dispatch the job
      freeNodes = freeNodes - job.nodes
      self.pendingList.pop(i)
      self.pickJobOptionsForSchedule(job, schedList)

    self.sleepingNodes = freeNodes
    self.cluster.nodes -= freeNodes
    return schedList




class PriceAwareShutdownScheduler(FIFOScheduler):
  'FIFO but take the costs for the energy of the next time intervals into consideration, uses shutdown of nodes'
  sleepingNodes = 0

  backfillLength = 100
  hoursAhead = 0
  #hoursMaxWaitingTime = 0
  costsIdleNodeInWatt = None
  sleepEndTime = 0

  def setCluster(self, cluster, energyModel):
    super().setCluster(cluster, energyModel)
    self.costsIdleNodeInWatt = cluster.infrastructurePowerConsumption / cluster.nodes


  def __init__(self, arg):
    #data = arg.split(",")
    print("PriceAwareShutdownScheduler arguments: " + str(arg))
    self.hoursAhead = int(arg)
    #self.hoursMaxWaitingTime = int(data[1])
    #print("hours ahead: %s max-waiting-time-hours: %s" % tuple(data))

  def tryToSchedule(self, time, jobCompleted):
    if not self.pendingList:
      self.cluster.nodes += self.sleepingNodes # wakeup nodes virtually
      self.sleepingNodes = self.cluster.nodes
      self.cluster.nodes = 0
      return []

    # Now try to retrieve as many jobs from the list as fit.
    schedList = []
    self.cluster.nodes += self.sleepingNodes # wakeup nodes virtually
    self.sleepingNodes = 0
    freeNodes = self.cluster.nodes

    if freeNodes < self.pendingList[0].nodes or time < self.sleepEndTime:
      self.sleepingNodes = freeNodes
      self.cluster.nodes = 0

      return []

    # find price for next X hours
    price = []
    for i in range(0, self.hoursAhead):
      price.append(self.energyModel.timestampPrice(time + i*3600))

    timestamp = datetime.datetime.fromtimestamp(time)

    i = 0
    while( i < len(self.pendingList) ):
      job = self.pendingList[i]
      #print(job)
      if freeNodes == 0:
        return schedList
      #print("%d %d %d" % (i, time, job.nodes))
      if freeNodes < job.nodes:
        break
      # energy costs when we keep the nodes of the job idle
      nodesForJobIdleConsumption = self.costsIdleNodeInWatt * job.nodes
      jobPowerConsumption = job.powerConsumed(4, self.cluster) + nodesForJobIdleConsumption

      hoursNeeded = int(job.durationMin / 3600)

      # check job submissionTime
      if hoursNeeded + 2 < self.hoursAhead:
        # we may delay the job
        remainingSeconds = job.durationMin % 3600
        # compute energy cost for all starting times, for all windows
        cheapestPrice = 0
        expensivePrice = 0
        cheapestConfig = []

        # price for running the job now!
        tSecRemainThisHour = (59 - timestamp.minute) * 60 + 60 - timestamp.second
        if tSecRemainThisHour > remainingSeconds:
          cPrice = price[0] * remainingSeconds
        else:
          cPrice = price[0] * tSecRemainThisHour
          for h in range(1, hoursNeeded):
            cPrice += price[h] * 3600
          tRemaining = job.durationMin - tSecRemainThisHour - 3600 * (hoursNeeded - 1)
          if tRemaining > 0:
            cPrice += price[hoursNeeded] * tRemaining
        cPrice = cPrice * jobPowerConsumption / 1000 / 3600 * 0.999 # favorise immediate start

        cheapestConfig = [0, False]
        cheapestPrice = cPrice
        expensivePrice = cPrice
        #print(cPrice)

        # price for delaying the job
        # price for delaying until the end of this hour
        idlePrice = price[0] * tSecRemainThisHour * nodesForJobIdleConsumption / 1000 / 3600
        secondsFullHourEmpty = (3600 - remainingSeconds)

        if job.durationMin < 3600:
          for w in range(1, self.hoursAhead):
            cPrice = idlePrice + price[w] * jobPowerConsumption * remainingSeconds / 1000 / 3600
            if cPrice < cheapestPrice:
              cheapestPrice = cPrice
              cheapestConfig = [w, True]
            idlePrice += price[w] * nodesForJobIdleConsumption / 1000
        else:
          for w in range(1, self.hoursAhead - hoursNeeded - 1):
            # pay a price for leaving the node empty:
            cPrice = idlePrice

            # shortcut
            if idlePrice > cheapestPrice:
              break

            # price for running the job
            for h in range(w, hoursNeeded + w):
              cPrice += price[h] * jobPowerConsumption / 1000

            # check bounding box, first and last interval
            # Compute price when running it now, or when running it in the last hour
            pLast = price[hoursNeeded + w + 1] * (jobPowerConsumption * remainingSeconds + secondsFullHourEmpty * nodesForJobIdleConsumption ) / 1000 / 3600

            pFirst = price[w - 1] * (remainingSeconds * jobPowerConsumption + (secondsFullHourEmpty - remainingSeconds) * nodesForJobIdleConsumption) / 1000 / 3600

            ePrice = cPrice
            if pFirst > pLast or w == 1:
              last = True
              cPrice += pLast
              ePrice += pFirst
            else:
              last = False
              cPrice += pFirst
              ePrice += pLast
            #print(cPrice)

            if cPrice < cheapestPrice:
              cheapestPrice = cPrice
              cheapestConfig = [w, last]
            if cPrice > expensivePrice:
              expensivePrice = cPrice

            # pay a price for leaving the node empty for this hour
            idlePrice += price[w] * nodesForJobIdleConsumption / 1000

        (hour, last) = cheapestConfig
        if hour != 0: # otherwise schedule now!
          delay = (hour-1)*3600
          if last:
            delay += tSecRemainThisHour
          else: #first
            delay += tSecRemainThisHour - remainingSeconds
          self.sleepingNodes = freeNodes
          self.cluster.nodes -= freeNodes

          self.sleepEndTime = time + delay
          #print([time, cheapestConfig, job, self.sleepEndTime])
          return schedList + [[Job(jobid="SleepScheduling",dummy=True), delay, "default" ]]
        #print([time, cheapestConfig, job])

      # now we dispatch the job
      freeNodes = freeNodes - job.nodes
      self.pendingList.pop(i)
      self.pickJobOptionsForSchedule(job, schedList)

    self.sleepingNodes = freeNodes
    self.cluster.nodes -= freeNodes
    return schedList


class EnforcePriceAwareShutdownScheduler(FIFOScheduler):
  'FIFO but take the costs for the energy of the next time intervals into consideration, uses shutdown of nodes'
  sleepingNodes = 0

  backfillLength = 100
  hoursAhead = 0
  sleepEndTime = 0


  def __init__(self, arg):
    #data = arg.split(",")
    print("PriceAwareShutdownScheduler arguments: " + str(arg))
    self.hoursAhead = int(arg)
    #self.hoursMaxWaitingTime = int(data[1])
    #print("hours ahead: %s max-waiting-time-hours: %s" % tuple(data))

  def tryToSchedule(self, time, jobCompleted):
    if not self.pendingList:
      return []

    if not jobCompleted and time < self.sleepEndTime:
      return []

    # find price for next X hours
    price = []
    for i in range(0, self.hoursAhead):
      price.append(self.energyModel.timestampPrice(time + i*3600))

    timestamp = datetime.datetime.fromtimestamp(time)

    self.cluster.nodes += self.sleepingNodes # wakeup nodes virtually
    self.sleepingNodes = 0

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
        break
      jobPowerConsumption = job.powerConsumed(4, self.cluster)

      hoursNeeded = int(job.durationMin / 3600)

      # check job submissionTime
      if hoursNeeded + 2 < self.hoursAhead:
        # we may delay the job
        remainingSeconds = job.durationMin % 3600
        # compute energy cost for all starting times, for all windows
        cheapestPrice = 0
        expensivePrice = 0
        cheapestConfig = []

        # price for running the job now!
        tSecRemainThisHour = (59 - timestamp.minute) * 60 + 60 - timestamp.second
        if tSecRemainThisHour > remainingSeconds:
          cPrice = price[0] * remainingSeconds
        else:
          cPrice = price[0] * tSecRemainThisHour
          for h in range(1, hoursNeeded):
            cPrice += price[h] * 3600
          tRemaining = job.durationMin - tSecRemainThisHour - 3600 * (hoursNeeded - 1)
          if tRemaining > 0:
            cPrice += price[hoursNeeded] * tRemaining
        cPrice = cPrice * jobPowerConsumption / 1000 / 3600 * 0.999 # favorise immediate start

        #print(["now", price[0], cPrice, tSecRemainThisHour, remainingSeconds])

        cheapestConfig = [0, False]
        cheapestPrice = cPrice
        expensivePrice = cPrice
        #print(cPrice)

        # price for delaying the job
        # price for delaying until the end of this hour
        secondsFullHourEmpty = (3600 - remainingSeconds)
        if job.durationMin < 3600:
          minT = 1e309
          step = 0
          for w in range(1, self.hoursAhead):
            if price[w] < minT:
              minT = price[w]
              step = w
          minT = minT * jobPowerConsumption * remainingSeconds / 1000 / 3600
          if minT < cPrice:
            cheapestPrice = minT
            cheapestConfig = [step, True]
        else:
          for w in range(1, self.hoursAhead - hoursNeeded - 1):
            # pay a price for leaving the node empty:
            cPrice = 0

            # price for running the job
            for h in range(w, hoursNeeded + w):
              cPrice += price[h] * jobPowerConsumption / 1000

            # check bounding box, first and last interval
            # Compute price when running it now, or when running it in the last hour
            pLast = price[hoursNeeded + w + 1] * jobPowerConsumption * remainingSeconds / 1000 / 3600

            pFirst = price[w - 1] * (remainingSeconds * jobPowerConsumption) / 1000 / 3600

            #print([w, price[w - 1], pLast, pFirst, cPrice])
            ePrice = cPrice
            if pFirst > pLast or w == 1:
              last = True
              cPrice += pLast
              ePrice += pFirst
            else:
              last = False
              cPrice += pFirst
              ePrice += pLast
            if cPrice < cheapestPrice * 0.999:
              cheapestPrice = cPrice
              cheapestConfig = [w, last]
            if cPrice > expensivePrice:
              expensivePrice = cPrice

        (hour, last) = cheapestConfig
        #print([cheapestConfig, cheapestPrice, job])
        if hour != 0: # otherwise schedule now!
          delay = (hour-1)*3600
          if last:
            delay += tSecRemainThisHour
          else:
            delay += tSecRemainThisHour - remainingSeconds
          self.sleepingNodes = freeNodes
          self.cluster.nodes -= freeNodes
          self.sleepEndTime = time + delay
          return schedList + [[Job(jobid="SleepScheduling",dummy=True), delay, "default" ]]

      # now we dispatch the job
      freeNodes = freeNodes - job.nodes
      self.pendingList.pop(i)
      self.pickJobOptionsForSchedule(job, schedList)

    self.sleepingNodes = freeNodes
    self.cluster.nodes -= freeNodes
    return schedList
