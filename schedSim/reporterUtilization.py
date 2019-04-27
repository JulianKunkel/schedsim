import os
import sys
import re
import time as Time
from dateutil import parser as timeparser

from schedSim.reporter import SilentReporter

class ReporterUtilization(SilentReporter):
  'Write the system statistics into a CSV file'

  statFile = None

  # array of (time, pending jobs, running jobs, used nodes, broken nodes)
  status = (0,0,0,0,0)

  def emptyNodeFailed(self, time):
    SilentReporter.emptyNodeFailed(self, time)
    p = self.status
    self.status = (time, p[1], p[2], p[3], p[4] + 1)
    self._write_entry("B", None)

  def jobAbortedWithErrors(self, time, job):
    SilentReporter.jobAbortedWithErrors(self, time, job)
    p = self.status
    self.status = (time, p[1] + 1, p[2] - 1, p[3] - job.nodes, p[4] + 1)
    self._write_entry("b", job)

  def nodeRepaired(self, time):
    SilentReporter.nodeRepaired(self, time)
    p = self.status
    self.status = (time, p[1], p[2], p[3], p[4] - 1)
    self._write_entry("r", None)

  def __init__(self, filename):
    self.statFile = open(filename + "-stats.csv", "w")
    self.statFile.write("Time,PendingJobs,RunningJobs,UsedNodes,BrokenNodes,Operation,WaitingTime,account,jobid,name,nodes,PPN,durationMin,clusterEnergyConsumption,clusterEnergyCosts\n")

  def _write_entry(self, typ, job):
     if job  == None:
       self.statFile.write("%d,%d,%d,%d,%s,%s,,,,,,,,%s\n" % (self.status[0],self.status[1],self.status[2], self.status[3],self.status[4], typ, self.powerConsumption))
       return

     waitingTime = ""
     account = ""
     if typ == "+":
        waitingTime = job.startTime - job.submissionTime
        account = job.account
        if job.startTime < job.submissionTime:
          print("Error: negative waiting time" + str(waitingTime))
     self.statFile.write("%d,%d,%d,%d,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s\n" % (self.status[0],self.status[1],self.status[2], self.status[3],self.status[4], typ, waitingTime, account, job.jobid, job.name, job.nodes, job.PPN, job.durationMin, self.powerConsumption))


  def jobSubmitted(self, time, job):
    SilentReporter.jobSubmitted(self, time, job)
    p = self.status
    self.status = (time, p[1] + 1, p[2], p[3], p[4])
    self._write_entry("C", job)

  def jobFinished(self, time, job):
    SilentReporter.jobFinished(self, time, job)
    p = self.status
    self.status = (time, p[1], p[2] - 1, p[3] - job.nodes, p[4])
    self._write_entry("-", job)

  def jobStarted(self, time, job, runtime, partition):
    SilentReporter.jobStarted(self, time, job, runtime, partition)
    p = self.status
    self.status = (time, p[1] - 1, p[2] + 1, p[3] + job.nodes, p[4])
    self._write_entry("+", job)

  def printSummary(self, starttime, endtime):
    SilentReporter.printSummary(self, starttime, endtime)
    self.statFile.close()
