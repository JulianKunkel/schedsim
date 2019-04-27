class Job:
  'This class represents a job'

  dummy = 0

  jobid = 0

  name = ""

  nodes = 0

  PPN = 0

  submissionTime = 0

  # durations for certain conditions such as changed frequency
  durations = []

  # the minimum runtime
  durationMin = 0
  durationMax = 0

  dependencies = []

  startTime = 0
  powerConsumption = 0 # managed externally

  account = None
  user = None
  APC = None

  partition = None

  def __init__(self, jobid = 0, name = 0, nodes = 1, PPN = 0, submissionTime = 0, durations = [0], dependencies = [], account = None, user = None, partition = None, ETS = None, dummy = False, APC = None):
    self.jobid = jobid
    self.name = name
    self.nodes = nodes
    self.PPN = PPN
    self.ETS = ETS
    self.APC = APC
    self.submissionTime = submissionTime
    self.endTime = 0
    self.durations = durations

    self.durationMin = min(durations)
    self.durationMax = max(durations)
    assert self.durationMin >= 0

    self.dependencies = dependencies
    self.account = account
    self.user = user
    self.partition = partition
    self.dummy = dummy

    assert name != ""
    assert nodes > 0
    assert PPN >= 0
    assert submissionTime >= 0
    assert len(durations) > 0

  def powerConsumed(self, pstate, cluster):
    if self.APC != None:
      #print("%f %f" % (self.APC,(self.nodes * cluster.nodePowerConsumption)))
      return self.APC
    energyConsumed = cluster.cpuFrequencyPower[4]
    return self.nodes * (energyConsumed * cluster.processorsPerNode + cluster.nodePowerConsumption)


  def __repr__( self ):
    return "Job(%s, %s, Nodes:%d, PPN:%d, submission:%d, duration:%s, %s, ETC:%s, APC:%s)" % (self.jobid, self.name, self.nodes, self.PPN, self.submissionTime, self.durations, self.dependencies, self.ETS, self.APC)

  def __cmp__(self, other):
    return cmp(self.jobid, other.jobid)

  def __lt__(self, other):
    return self.jobid < other.jobid
