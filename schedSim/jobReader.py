import re
import sys
import time
import dateutil.parser
import pickle

from schedSim.jobs import Job
from schedSim.jobspawner import JobSpawner

class JobReader(JobSpawner):
  '''This class reads resource manager Jobs from a file and contains options to parses the input into a common format for all resource managers.
  Supported: Slurm, ...'''

  def _parseIntVal(self, val):
      if (val == "" or val == None):
          return -1
      if val.endswith("K"):
          val = float(val[:-1]) * 1000
      elif val.endswith("M"):
          val = float(val[:-1]) * 1000 * 1000
      elif val.endswith("G"):
          val = float(val[:-1]) * 1000 * 1000 * 1000
      elif val.endswith("T"):
          val = float(val[:-1]) * 1000 * 1000 * 1000 * 1000
      return int(val)

  def _parseNodeMemory(self, mem, PPN):
      val = -1
      if mem.endswith("Mc"):
          val = float(mem[:-2]) * 1024*1024 * PPN
      elif mem.endswith("Gc"):
          val = float(mem[:-2]) * 1024*1024*1024 * PPN
      elif mem.endswith("Mn"):
          val = float(mem[:-2]) * 1024*1024
      elif mem.endswith("Gn"):
          val = float(mem[:-2]) * 1024*1024*1024
      return int(val)


  def _parseSlurmDuration(self, time):
      if time == "UNLIMITED":
          return "UNLIMITED"
      if time == "Partition_Limit":
          return time
      re_elapsed_time = re.compile("^(?P<day>[0-9]+)?-?(?P<hour>[0-9]+):(?P<min>[0-9]+):(?P<sec>[0-9]+)$")
      m = re_elapsed_time.match(time)
      if not m:
          print("[Reader] Could not parse time: %s" % time)
          return -1
      return ((int(m.group("day")) * 24 + int(m.group("hour")))*60 + int(m.group("min")))*60 + int(m.group("sec"))

  def prepareSlurm(self, fileList, outFile, delim="|"):
      'This function reads the input for Slurm and creates the required input for jobs'
      fmt = "jobid,partition,NNodes,NTasks,ReqMem,Submit,Start,Elapsed,Account,User,Timelimit,ConsumedEnergy,Priority,State,MaxRSS,MaxVMSize,AveRSS,AveVMSize,jobname".split(",")
      re_int = re.compile("^[0-9]+$")

      out = open(outFile, "w")
      jobs = []
      stats = {"INVALID": 0}
      for f in fileList:
          elapsedBatch = -1
          fd = open(f, "r")
          for l in fd:
              l = l.strip()
              try:
                  (jobid,partition,NNodes,NTasks,ReqMem,Submit,Start,Elapsed,Account,User,Timelimit,ConsumedEnergy,Priority,State,MaxRSS,MaxVMSize,AveRSS,AveVMSize,jobname,_) = l.split(delim)
              except Exception as e:
                  print("[Reader] Error parsing line: %s" % l)
                  print(e)
                  stats["INVALID"] = stats["INVALID"] + 1
                  continue
              Elapsed = self._parseSlurmDuration(Elapsed)

              m = re_int.match(jobid)
              if not m:
                  if jobid.endswith(".batch"):
                      elapsedBatch = Elapsed
                  else:
                      elapsedBatch = elapsedBatch - Elapsed
                  continue

              if elapsedBatch != -1 and len(jobs) > 0:
                  jobs[-1][0] = elapsedBatch
              elapsedBatch = 0
              if State.startswith("CANCELLED"):
                  State = "CANCEL"
              elif State.startswith("FAILED"):
                  State = "FAIL"
              elif State.startswith("COMPLETED"):
                  State = "OK"
              elif State.startswith("TIMEOUT"):
                  pass
              elif State.startswith("NODE_FAIL"):
                  pass
              elif State == "RUNNING" or State == "PENDING":
                  # ignore
                  continue
              else:
                  print("Unknown State: %s" % State)

              if State not in stats:
                  stats[State] = 0
              stats[State] = stats[State] + 1

              nnodes = self._parseIntVal(NNodes)
              if(nnodes <= 0):
                  #print("Invalid nnodes: %s" % nnodes )
                  stats["INVALID"] = stats["INVALID"] + 1
                  continue
              ppn = self._parseIntVal(NTasks)
              if ppn == -1:
                  ppn = 1

              jobs.append([0,
                jobid,jobname,partition,
                nnodes,
                ppn,
                self._parseNodeMemory(ReqMem, ppn),
                dateutil.parser.parse(Submit),
                dateutil.parser.parse(Start),
                Elapsed,
                Account,
                User,
                self._parseSlurmDuration(Timelimit),
                self._parseIntVal(ConsumedEnergy),
                Priority,
                State])
          fd.close()

      # Sort jobs based on submission date
      jobs.sort(key=lambda x: int(time.mktime(x[7].timetuple()))) # WARNING crucial position

      out.write("%s\n" % "|".join(fmt))
      total_sum = 0
      total_sum_batch_only = 0
      for j in jobs:
          (BatchOnly,jobid,jobname,partition,NNodes,NTasks,ReqMem, Submit, Start, Elapsed,Account,User,Timelimit,ConsumedEnergy,Priority,State) = j
          out.write("%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s\n" % (jobid,jobname,partition,NNodes,NTasks,ReqMem, Submit, Start, Elapsed,Account,User,Timelimit,ConsumedEnergy,Priority,State,BatchOnly))
          total_sum = total_sum + Elapsed*NNodes
          total_sum_batch_only = total_sum_batch_only + BatchOnly
      out.close()

      stats["job_runtime_node_years"] = total_sum / 3600.0 / 365 / 24
      stats["job_runtime_batch_only_node_years"] = total_sum_batch_only / 3600.0 / 365 / 24

      print(stats)

  def _parseLRZ(self, inFile, limitCount = 10000000000, partition = None):
      jobs = []
      fd = open(inFile, "r")
      header = fd.readline()
      if (header != "Scheduler assigned job id|Submission Time|Start Time|End Time|Status|Energy Tag|Number of Utilized Nodes|EtS (kWh)|APC (W)\n"):
          print("[Reader] Warning could not find proper header!")
      lineNo = 2
      for line in fd:
          (jobid,Submit,Start,End,State,EnergyTag,NNodes, ETS, apc) = line.split("|")

          Submit = int(time.mktime(dateutil.parser.parse(Submit).timetuple()))
          # fix too early start times:
          if Submit < 1388530800:
              Submit = 1388530800
          Elapsed = int(time.mktime(dateutil.parser.parse(End).timetuple())) - int(time.mktime(dateutil.parser.parse(Start).timetuple()))

          if State == "Removed" and Elapsed < 2: # Skip
            continue

          try:
            apc = float(apc)
          except:
            apc = 0
            print("Error parsing: %s (will set APC to 0)" % line.strip())

          lineNo = lineNo + 1
          if Elapsed <= 0:
            print("Job is empty! %s" % jobid)
          else:
            jobs.append(Job(jobid, EnergyTag, int(NNodes), int(NNodes), Submit, [Elapsed], [], ETS=ETS, APC=apc))

          if len(jobs) % 10000 == 0:
              print(len(jobs))
          if len(jobs) >= limitCount:
            print("[Reader] Limit reached %d" % limitCount)
            break
      fd.close()
      jobs = sorted(jobs, key=lambda x: x.submissionTime)
      #for j in jobs:
      #      print(j)
      return jobs



  def _parseFile(self, inFile, limitCount = 10000000000, partition = None):
      jobs = []
      fd = open(inFile, "r")
      header = fd.readline()
      if (header == "Scheduler assigned job id|Submission Time|Start Time|End Time|Status|Energy Tag|Number of Utilized Nodes|EtS (kWh)|APC (W)\n"):
        fd.close()
        return self._parseLRZ(inFile, limitCount, partition)

      if(header != "jobid|jobname|partition|NNodes|NTasks|ReqMem|Submit|Start|Elapsed|Account|User|Timelimit|ConsumedEnergy|Priority|State|BatchOnly\n"):
          print("[Reader] Warning could not find proper header!")

      oldsubmissionTime = 0
      lineNo = 2
      for line in fd:
          (jobid,jobname,job_partition,NNodes,NTasks,ReqMem, Submit, Start, Elapsed, Account,User,Timelimit,ConsumedEnergy,Priority,State,BatchOnly) = line.split("|")
          if partition != None and not job_partition in partition:
              continue

          Submit = int(time.mktime(dateutil.parser.parse(Submit).timetuple()))
          if Submit < oldsubmissionTime:
              print("[Reader] WARNING: submissions are not sorted incrementally, line: %d" % lineNo)

          oldsubmissionTime = Submit
          lineNo = lineNo + 1
          jobs.append(Job(jobid, jobname, int(NNodes), int(NTasks), Submit, [int(Elapsed)], [], Account, User, partition=job_partition))

          if len(jobs) % 10000 == 0:
              print(len(jobs))
          if len(jobs) >= limitCount:
            print("[Reader] Limit reached %d" % limitCount)
            break
      fd.close()
      return jobs


  def convertToBinary(self, inFile, outFile, limitCount = 10000000000, partition = None):
    'Converting a TXT file to binary is more efficient but not mandatory'
    jobs = self._parseFile(inFile, limitCount, partition)
    pickle.dump( jobs, open( outFile, "wb" ) )
    return jobs

  def convertJobsToBinary(jobs, outFile):
    'Converting the jobs to binary is more efficient but not mandatory'
    pickle.dump( jobs, open( outFile, "wb" ) )

  def jobs(self, inFile, limitCount = 10000000000, partition = None):
    if inFile.endswith(".p"):
        jobs = pickle.load( open( inFile, "rb" ) )
        #if limitCount >= len(jobs) and partition == None:
        #    return jobs
        ret = []
        for j in jobs:
            if partition != None and not j.partition in partition:
                continue
            if len(ret) == limitCount:
                return ret
            if j.durationMin < 0 or (j.APC != None and j.APC < 0):
              print("Warning runtime or APC < 0 for %s", j)
              continue
            elif j.durationMin == 0:
              continue
            ret.append(j)
        return ret

    # Text file:
    return self._parseFile(inFile, limitCount, partition)
