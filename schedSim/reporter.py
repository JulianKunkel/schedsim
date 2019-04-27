class Reporter:
  'This class analyses the results of the simulation run'

  cluster = None
  energyCostModel = None

  def setCluster(self, cluster, eModel):
    self.energyCostModel = eModel
    self.cluster = cluster

  def printSummary(self, starttime, time):
    pass

  def jobSubmitted(self, time, job):
    pass

  def jobFinished(self, time, job):
    pass

  def jobStarted(self, time, job, partition):
    pass

  def clusterStatusChanged(self, time):
    pass

  def jobAbortedWithErrors(self, time, job):
    pass

  def emptyNodeFailed(self, time):
    pass

  def nodeRepaired(self, time):
    pass

class SilentReporter(Reporter):
  nodeTime = 0
  energyConsumed = 0.0
  powerConsumption = 0.0
  powerConsumptionStats = [1e309, 0] # min / max
  costsEnergy = 0.0
  stats = {}
  frequencyStats = []
  jobsStarted = 0
  nodeErrors = 0
  nodesRepaired = 0
  jobsAborted = 0
  lastTime = 0


  def jobAbortedWithErrors(self, time, job):
    self.nodeErrors = self.nodeErrors + 1
    self.jobsAborted = self.jobsAborted + 1

  def nodeRepaired(self, time):
    self.nodesRepaired = self.nodesRepaired + 1

  def emptyNodeFailed(self, time):
    self.nodeErrors = self.nodeErrors + 1

  def setCluster(self, cluster, eModel):
    self.cluster = cluster
    self.frequencyStats = [0 for x in cluster.cpuFrequencies ]
    self.powerConsumption = 0 #cluster.infrastructurePowerConsumption
    self.energyCostModel = eModel

  def jobSubmitted(self, time, job):
    if self.lastTime == 0:
      self.lastTime = time

  def jobFinished(self, time, job):
    self.powerConsumption -= job.powerConsumption
    if self.powerConsumption < 0:
      #print(self.powerConsumption)
      #print(job)
      self.powerConsumption = 0

    if self.powerConsumptionStats[0] > self.powerConsumption:
      self.powerConsumptionStats[0] = self.powerConsumption

  def clusterStatusChanged(self, time):
    if time != self.lastTime:
      print(self.cluster.nodes)
      curpower = self.powerConsumption #+ self.cluster.nodePowerConsumption * self.cluster.nodes # idle nodes
      assert curpower >= 0
      assert time >= self.lastTime
      ecosts = self.energyCostModel.energyCosts(self.lastTime, time, curpower)
      #assert ecosts >= 0
      #print([time, self.lastTime, self.cluster.nodes, ecosts])
      self.costsEnergy += ecosts
      self.energyConsumed += (time - self.lastTime) * curpower

      self.lastTime = time

  def jobStarted(self, time, job, runtime, partition):
    self.nodeTime = self.nodeTime + runtime * job.nodes
    self.jobsStarted = self.jobsStarted + 1

    c = self.cluster

    frequencyStats = self.frequencyStats
    PState = partition["cpu_pstate"]
    energyConsumed = c.cpuFrequencyPower[PState]
    frequencyStats[PState] = frequencyStats[PState] + runtime

    job.powerConsumption = job.powerConsumed(PState, c)
    self.powerConsumption += job.powerConsumption
    if self.powerConsumptionStats[1] < self.powerConsumption:
      self.powerConsumptionStats[1] = self.powerConsumption

  def printSummary(self, starttime, endtime):
    s = self.stats
    c = self.cluster
    runtime = (endtime - starttime)
    utilization = float(self.nodeTime) / (runtime * c.nodes)

    self.costsEnergy += self.energyCostModel.fixedPenalties(self.powerConsumptionStats[0], self.powerConsumptionStats[1])

    s["runtime_days"] = runtime / 3600 / 24
    s["nodetime"] = self.nodeTime
    s["jobsStarted"] = self.jobsStarted
    s["utilization_percent"] = utilization * 100.0
    s["energyConsumed"] = self.energyConsumed / 3600.0 / 1000
    s["costs_energy"] = self.costsEnergy

    s["costsCenter"] =  (runtime / c.systemLifeDuration) * c.costsSystem + runtime / 3600.0 / 24 / 365 * c.costsInfrastructureAnually
    s["costs"] = s["costs_energy"] + s["costsCenter"]
    s["nodeErrors"] = self.nodeErrors
    s["nodesRepaired"] = self.nodesRepaired
    s["jobsAborted"] = self.jobsAborted

    print("[REP] Total runtime: %(runtime_days).2f days, utilization: %(utilization_percent).1f %%, energy consumed: %(energyConsumed).0f kWh, costs energy: %(costs_energy).1f €, costsCenter: %(costsCenter).1f €, costs: %(costs).1f €, jobsAborted: %(jobsAborted)s nodeErrors: %(nodeErrors)d (repaired: %(nodesRepaired)d)" % s)
    #print("   " + str(s))

class NormalReporter(SilentReporter):
  def jobSubmitted(self, time, job):
    print("%d (submitted) %s" % (time, job))
    SilentReporter.jobSubmitted(self, time, job)

  def jobFinished(self, time, job):
    print("%d stop %s" % (time, job))
    SilentReporter.jobFinished(self, time, job)

  def jobStarted(self, time, job, runtime, partition):
    print("%d start %s" % (time, job))
    SilentReporter.jobStarted(self, time, job, runtime, partition)
