import numpy

from schedSim.jobs import Job

class JobSpawner:
  'This abstract class returns jobs when calling next()'

  def jobs(self, jobCount):
    'Returns the list of jobs if no further job exists'
    return []


class JobSpawnerNormalDistributed(JobSpawner):
  'This class spawns jobs with a bimodal distribution, one set of large jobs with a certain normal distribution and one for small jobs'

  cluster = None

  def __init__(self, cluster):
    self.cluster = cluster

  def jobs(self, jobCount, startScale = 3*3600):
    c = self.cluster

    if startScale == 0:
      jobStarts = [0 for x in range(0,jobCount)]
    else:
      jobStarts = numpy.random.exponential(startScale, jobCount)

      tmp = 0
      for j in range(0, jobCount):
        t = jobStarts[j]
        tmp += jobStarts[j]
        jobStarts[j] = tmp

    account = numpy.random.randint(1, 11, jobCount)

    jobRuntime = numpy.random.normal(c.maxJobTimeMin/3*2, c.maxJobTimeMin/2, jobCount)
    jobRuntime = [ round(60*max(min(x, c.maxJobTimeMin), c.maxJobTimeMin / 100)) for x in jobRuntime]

    nodes = numpy.random.randint(1, c.nodes+1, jobCount)

    jobs = []
    for i in range(0, jobCount):
      jobs.append(Job(i, str(i), nodes[i], c.cpusPerProcessor, jobStarts[i], [int(jobRuntime[i])], [], account=account[i]))
    return jobs
