#!/usr/bin/env Rscript

print("This script reads CSVs created by the reporterUtilization")

args = commandArgs(trailingOnly=TRUE)

csv = args[1]
d = read.csv(csv, header=T)

# print(str(d))
create = d[d$Operation == "+", ]
create$WaitingTimesMin = create$WaitingTime / 60
create$durationMin = create$durationMin / 60

print(summary(create$WaitingTimesMin))

what = c(0, 0.25, 0.5, 0.75, 0.90, 0.99)
cat("q: ")
for (w in what){
  cat(sprintf("%.0f & ", quantile(create$WaitingTimesMin,w) ))
}
cat(sprintf("%.0f\\\\ \\hline \n", max(create$WaitingTimesMin)))
