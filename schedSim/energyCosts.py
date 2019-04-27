import datetime
import csv

class baseEnergyCostModel:
  def initTimestamp(self, startTime):
    self.firstTime = startTime

  def energyCosts(self, startTime, endTime, consumedEnergy):
    return 0

  def fixedPenalties(self, minPower, maxPower):
    return 0

  def timestampPrice(self, timestamp):
    '''Hourly price for the given timestamp'''
    return 0

class FixedPriceModel(baseEnergyCostModel):
  costsEnergyPerKWH = 0.145
  #  costsEnergyPerKWH = 0.14 DKRZ

  def timestampPrice(self, timestamp):
    return self.costsEnergyPerKWH

  def energyCosts(self, startTime, endTime, consumedEnergy):
    #print(str(endTime) +  ": " + str(consumedEnergy) + " " + str((endTime - startTime)))
    v = (endTime - startTime) * consumedEnergy * self.costsEnergyPerKWH / 1000.0 / 3600.0
    return v

class HourlyPriceModel(baseEnergyCostModel):
    price = {}

    def timestampPrice(self, timestamp):
      hour = datetime.datetime.fromtimestamp(timestamp).hour
      return self.price[hour]

    def energyCosts(self, startTime, endTime, consumedEnergy):
      # Inefficient way of computing for large intervalls, but mostly a high frequency of jobs is assumed.
      duration = (endTime - startTime)
      startDate = datetime.datetime.fromtimestamp(startTime)

      # compute up to the full hour
      tSec = (59 - startDate.minute) * 60 + 60 - startDate.second

      if tSec < duration:
        duration = duration - tSec
        curHour = startDate.hour
        costs = self.price[curHour] * consumedEnergy * tSec
        #print("   " + str(tSec) + " h:" + str(curHour))

        durationHours = int(duration / 3600)
        for i in range(0, durationHours):
          curHour = (curHour + 1) % 24
          #print(" h " + str(curHour))
          costs += self.price[curHour] * consumedEnergy * 3600
        curHour = (curHour + 1) % 24
        duration = duration % 3600
        #print(" R " + str(duration) + " h: " + str(curHour))
        costs += self.price[curHour] * consumedEnergy * duration
      else:
        costs = self.price[startDate.hour] * consumedEnergy * duration

      assert costs >= 0

      return costs / 1000.0 / 3600.0

class DayNightPriceModel(HourlyPriceModel):
  costsEnergyPerKWHDay   = 0.16675
  costsEnergyPerKWHNight = 0.1

  dayStarts = 6
  dayEnds = 22

  # Expensive:
  # https://www.swm.de/geschaeftskunden/m-strom/gewerbekunden/m-strom-business/m-strom-business-komfort.html
  def __init__(self):
    for i in range(0, 24):
      self.price[i] = self.costsEnergyPerKWHNight
    for i in range(self.dayStarts, self.dayEnds):
      self.price[i] = self.costsEnergyPerKWHDay


class HourlyStockPriceModel(baseEnergyCostModel):
    firstTime = None
    price = {}

    def __init__(self, filename):
      with open(filename, 'r') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter=',', quotechar='"')
        for l in csvreader:
          self.price[int(l[""])] = float(l["price"]) # price was in euro

    def timestampPrice(self, time):
      timestamp = int((time - self.firstTime) / 3600) + 1
      return self.price[timestamp]

    def energyCosts(self, startTime, endTime, consumedEnergy):
      #print("EnergyCosts: %d %d %.0f" % ( startTime, endTime, consumedEnergy))
      assert consumedEnergy >= 0
      timestamp =  int ((startTime - self.firstTime) / 3600) + 1

      duration = (endTime - startTime)
      assert duration > 0
      startDate = datetime.datetime.fromtimestamp(startTime)
      #assert self.price[timestamp] > 0 This may be allowed!

      # compute up to the full hour
      tSec = (59 - startDate.minute) * 60 + 60 - startDate.second
      if tSec < duration:
        duration = duration - tSec
        costs = self.price[timestamp] * consumedEnergy * tSec

        durationHours = int(duration / 3600)
        for i in range(0, durationHours):
          timestamp += 1
          #print(" h " + str(curHour))
          costs = costs + self.price[timestamp] * consumedEnergy * 3600
        timestamp += 1
        duration = duration % 3600
        #print(" R " + str(duration) + " h: " + str(timestamp))
        costs = costs + self.price[timestamp] * consumedEnergy * duration
      else:
        costs = self.price[timestamp] * consumedEnergy * duration

      #assert costs >= 0
      return costs / 1000.0 / 3600.0

class energyCostModelFactory:
  def createModel(name, arg):
    if name == "FixedPrice":
      return FixedPriceModel()
    if name == "DayNightPrice":
      return DayNightPriceModel()
    if name == "HourlyStockPrice":
      return HourlyStockPriceModel(arg)
    if name == "list":
      print(["FixedPrice", "DayNightPrice", "HourlyStockPrice"])
    raise Exception("No valid model defined!")
