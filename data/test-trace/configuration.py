class Cluster:
  'This class contains information about the cluster'

  nodes = 1529

  cpusPerProcessor = 12
  processorsPerNode = 2

  # Failure states
  nodeMTBF = 20 * 365*24*3600
  nodeMTTR = 2 * 24*3600
  nodeMTTRdeviation = 0.5 * nodeMTTR
  nodeMinRepairTime = 120

  systemFailureMTBF = 1 * 365*24*3600
  systemFailureMTTR = 8*3600
  systemFailureMTTRdeviation = 0.5 * systemFailureMTTR
  systemFailureMinRepairTime = 120

  cpuFrequencies = [800, 1200, 1600, 2000, 2400, 2800, 3000]
  # cpuHalt is expected to cost 0

  'List with frequencies and power consumption, we refer to it by the P-State'
  cpuFrequencyPower = [95,95,95,95,95]
  nodePowerConsumption = 70 # Watts

  maxJobTimeMin = 8*60
  infrastructurePowerConsumption = 130000 # Watts

  costsSystem = 33*1000*1000 / 2
  costsInfrastructureAnually = 4*1000*1000
  systemLifeDuration = 5 * 365*24*3600
