import os
import sys
import re
import time as Time
from dateutil import parser as timeparser

from schedSim.reporter import SilentReporter

class ReporterHTML(SilentReporter):

  outfile = None
  templateSuffix = None

  def __init__(self, filename, experimentName = ""):
    dire = os.path.dirname(__file__)
    outdir = os.path.dirname(filename)
    if outdir == "":
      outdir = os.getcwd()

    template = open(dire + "/vis/template.html", "r")
    data = template.read()
    template.close()

    data = re.sub("\$\{EXPERIMENT_NAME\}", experimentName, data)
    m = re.search("(.*)\$\{ITEMS\}(.*)", data, flags=re.DOTALL)
    if not m:
      print("Could not find ITEMS marker in template!")
      sys.exit(1)

    self.outfile = open(filename + ".html", "w")
    self.outfile.write(m.group(1))
    self.templateSuffix = m.group(2)

    try:
      os.symlink(dire + "/vis/vis.css", outdir + "/vis.css")
      os.symlink(dire + "/vis/vis.js", outdir + "/vis.js")
    except:
      pass

  def convertTime(self, time):
    return Time.strftime("%Y-%m-%d %H:%M:%S", Time.gmtime(time))

  def jobSubmitted(self, time, job):
    SilentReporter.jobSubmitted(self, time, job)
    #self.outfile.write("{content:'(%d)',start:'%s',type:'point'}," % (job.jobid, self.convertTime(time)))

  def jobFinished(self, time, job):
    SilentReporter.jobFinished(self, time, job)
    self.outfile.write("{content:'%d on %d',start:'%s',end:'%s'}," % (job.jobid, job.nodes, self.convertTime(job.startTime), self.convertTime(time)))

  def jobStarted(self, time, job, runtime, partition):
    SilentReporter.jobStarted(self, time, job, runtime, partition)

  def printSummary(self, starttime, endtime):
    SilentReporter.printSummary(self, starttime, endtime)
    self.outfile.write(self.templateSuffix)
    self.outfile.close()
