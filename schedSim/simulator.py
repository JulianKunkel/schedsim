# import copy
import numpy
import sys

from schedSim.failureModel import FailureModelMTTBF

from heapq import heappop, heappush

JOB_SUBMITTED = 1
JOB_COMPLETED = 2
JOB_START_SCHEDULER = 3

# Failure cases:
JOB_STOPPED_WITH_FAILURES = 4
EMPTY_NODE_FAILURE = 5
NODE_REPAIRED = 6


class Simulator:
  '''
  This class contains the discrete event simulator

  Scheduling of jobs is performed as follows:
  1) Whenever a job submission is done, a START_SCHEDULER event is created a number second later (determined by the scheduler's schedulingDelay() method) before trying to schedule. Multiple submissions are batched and only one such event is created. This allows collecting of multiple jobs and prevents that the scheduler makes suboptimal decision when multiple jobs are submitted at one given time.
  2) When a job is terminated or the START_SCHEDULER event occurs, the scheduler's tryToSchedule() method is called and the returned list of jobs is stared. The scheduler has to ensure that the jobs fit on the currently available nodes.
  3) If a node fails, the job on all its nodes is terminated and must be restarted.

  Running jobs are managed by the scheduler but starting a job/stopping is adjusted by the Simulator.
  Pending jobs are completely managed by the scheduler and not known by the simulator.
  Managing the list by the scheduler gives it the chance to optimize the data structures based on the scheduling algorithm.
  '''


  def simulate(self, cluster, jobs, scheduler, energyModel, reporter, errorModel = True):
    nodesTotal = cluster.nodes

    el = []
    failureModel = FailureModelMTTBF()

    longestJobRuntime = 0
    minNodeRuntime = 0 # TODO check for shared jobs
    for j in jobs:
      if j.nodes > cluster.nodes: # or j.PPN > (cluster.cpusPerProcessor * cluster.processorsPerNode):  NO PPN check because of hyperthreading
          print("[SIM] WARNING skipped job because it needs too many nodes: " + str(j))
      else:
          heappush(el, (j.submissionTime, JOB_SUBMITTED, j) )
          minNodeRuntime = minNodeRuntime + j.durationMin * j.nodes
          if j.durationMin > longestJobRuntime:
              longestJobRuntime = j.durationMin


    print("[SIM] %d jobs, optimal runtime with 100%% utilization on %d nodes == %.2f days (longest job: %.2f days)" % (len(jobs), cluster.nodes, minNodeRuntime / float(cluster.nodes) / 3600 / 24, longestJobRuntime / 3600.0 / 24) )

    pendingJobsToSubmit = []

    scheduler.setCluster(cluster, energyModel)
    reporter.setCluster(cluster, energyModel)
    failureModel.setCluster(cluster)

    startScheduler = False

    if (len(el) == 0):
        print("[SIM] Nothing to do, no jobs available!")
        sys.exit(1)

    firstJob = el[0]
    starttime = firstJob[0]
    time = starttime
    completedJobs = 0
    jobCount = len(jobs)
    lasttime = time

    energyModel.initTimestamp(time) # initialize time

    fail = None
    scheduler.submitAllJobsWithStartTime(jobs, time)

    oldtime = -1
    while(len(el) > 0):
      reschedule = True

      (time, op, job) = heappop(el)
      #print("%d %s %s" % (time, op, job))
      assert time >= oldtime
      oldtime = time

      if jobCount == completedJobs:
        # may happen if we only see other events such as node repair events
        break

      if op == JOB_SUBMITTED:
        reporter.jobSubmitted(time, job)
        pendingJobsToSubmit.append(job)
        if not startScheduler:
          heappush(el, (time + scheduler.schedulingDelay(), JOB_START_SCHEDULER, None))
          startScheduler = True
        continue

      elif op == JOB_COMPLETED:
        reporter.clusterStatusChanged(time)
        reporter.jobFinished(time, job)
        cluster.nodes = cluster.nodes + job.nodes
        scheduler.jobCompleted(job, time)
        completedJobs = completedJobs + 1

      elif op == EMPTY_NODE_FAILURE:
        reporter.clusterStatusChanged(time)
        cluster.nodes = cluster.nodes - 1
        repairDuration = failureModel.timeUntilNodeIsBack()
        heappush(el, (time + repairDuration, NODE_REPAIRED, None))
        reschedule = False
        reporter.emptyNodeFailed(time)

      elif op == JOB_STOPPED_WITH_FAILURES:
        reporter.clusterStatusChanged(time)
        reporter.jobAbortedWithErrors(time, job)
        # Take one node offline
        cluster.nodes = cluster.nodes + job.nodes - 1
        scheduler.jobAbortedWithErrors(job, time)
        repairDuration = failureModel.timeUntilNodeIsBack()
        heappush(el, (time + repairDuration, NODE_REPAIRED, None))

      elif op == NODE_REPAIRED:
        reporter.clusterStatusChanged(time)
        reporter.nodeRepaired(time)
        cluster.nodes = cluster.nodes + 1

      elif op == JOB_START_SCHEDULER:
        scheduler.newPendingJobs(pendingJobsToSubmit, time)
        pendingJobsToSubmit = []
        startScheduler = False

      if reschedule:
        reporter.clusterStatusChanged(time)
        newJobs = scheduler.tryToSchedule(time, op == JOB_COMPLETED)

        for newJob in newJobs:
          (job, runtime, partition) = newJob
          job.startTime = time

          if job.dummy:
            if job.jobid == "SleepScheduling":
              assert runtime > 0
              heappush(el, (time + runtime, JOB_START_SCHEDULER, None))
              continue

          reporter.jobStarted(time, job, runtime, partition)
          cluster.nodes -= job.nodes
          assert cluster.nodes >= 0

          # check if the job fails during runtime....
          if errorModel:
            fail = failureModel.checkWhenJobFails(job.nodes, runtime)
          if fail == None:
            job.endTime = time + runtime
            heappush(el, (job.endTime, JOB_COMPLETED, job))
          else:
            job.endTime = time + fail
            heappush(el, (job.endTime, JOB_STOPPED_WITH_FAILURES, job))

      if errorModel and cluster.nodes > 0 and el:
        # check now if some of the remaining nodes failed from now till the next event
        fail = failureModel.checkWhenJobFails(cluster.nodes, el[0][0] - time)
        if fail != None:
          heappush(el, (time + fail, EMPTY_NODE_FAILURE, None))

    reporter.clusterStatusChanged(time)

    if completedJobs != jobCount:
      print("WARNING: did not process all jobs, some missing (%d of %d completed)" % (completedJobs, len(jobs)))
    cluster.nodes = nodesTotal
    reporter.printSummary(starttime, time)
