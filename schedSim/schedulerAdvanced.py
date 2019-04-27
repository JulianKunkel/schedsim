from schedSim.scheduler import Scheduler

from heapq import heappop, heappush, nsmallest


class BiggestFirstBackfillScheduler(Scheduler):
  '''This scheduler always schedules the biggest job (with the longest runtime first).
     If this job does not fit the current available nodes, it will wait until the allocation finish.
     Includes back-filling (but the list is sorted by size to optimize usage.)'''

  backfillLength = 100
  pendingList = []

  def newPendingJobs(self, jobs, time):
    self.pendingList.extend(jobs)
    self.pendingList.sort(key=lambda x: (x.nodes, x.durationMin), reverse=True )

  def jobAbortedWithErrors(self, job, time):
    # Add the job again on front of the list, i.e., it will re-run
    self.pendingList.append(job)
    self.pendingList.sort(key=lambda x: (x.nodes, x.durationMin), reverse=True )

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




class LongestFirstBackfillScheduler(BiggestFirstBackfillScheduler):
  ''''''

  def newPendingJobs(self, jobs, time):
    self.pendingList.extend(jobs)
    self.pendingList.sort(key=lambda x: (x.durationMin, x.nodes), reverse=True )

  def jobAbortedWithErrors(self, job, time):
    # Add the job again on front of the list, i.e., it will re-run
    self.pendingList.append(job)
    self.pendingList.sort(key=lambda x: (x.durationMin, x.nodes), reverse=True )
