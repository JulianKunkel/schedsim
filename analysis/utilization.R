#!/usr/bin/env Rscript

library(ggplot2)
#library(grid)
#library(gridExtra)


print("This script reads CSVs created by the reporterUtilization")

args = commandArgs(trailingOnly=TRUE)

csv = args[1]
# csv="normal-FIFOScheduler-stats.csv"
out = paste(args[1], sep="", ".png")

d = read.csv(csv, header=T)

# print(str(d))
d$Time = (d$Time - min(d$Time)) / 3600.0 / 24

create = d[d$Operation == "+", ]
create$WaitingTimesMin = create$WaitingTime / 60
create$durationMin = create$durationMin / 60

print(summary(create$WaitingTimesMin))
print(quantile(create$WaitingTimesMin,0.9))

g_waiting_times = ggplot(create, aes(WaitingTimesMin)) + geom_density(adjust = 1/5) + xlab("Waiting time in minutes")
ggsave(file=sprintf("%s-waiting_times.png", csv))

create = create[order(create$WaitingTimesMin),]
create$jobNum = 1
create$jobNum = create$jobNum / nrow(create) * 100

g_waiting_times = ggplot(create, aes(x=WaitingTimesMin, y=cumsum(jobNum))) + geom_point() + xlab("Waiting time in minutes") + ylab("Percentage of jobs")
ggsave(file=sprintf("%s-waiting_times_cummulative.png", csv))
 ggplot(create, aes(y=WaitingTimesMin, x=cumsum(jobNum))) + geom_point() + ylab("Waiting time in minutes") + xlab("Percentage of jobs")
ggsave(file=sprintf("%s-waiting_times_cummulative2.png", csv))

#if ( length(levels(factor(create$account))) < 100){
#  g_waiting_time_box = ggplot(create, aes(factor(account), WaitingTimesMin)) + geom_boxplot() + xlab("Account")
#  ggsave(file=sprintf("%s-waiting_times_per_user.png", csv))
#}

r = d[! d$Operation %in% c("b","r", "B"),]

g_pending = ggplot(r, aes(colour=Operation))	+ geom_point(aes(x=Time,y=clusterEnergyConsumption), alpha = 0.5) + xlab("Time in days") #,colour=cut
ggsave(file=sprintf("%s-clusterEnergyConsumption.png", csv))


g_pending = ggplot(r, aes(colour=Operation))	+ geom_point(aes(x=Time,y=PendingJobs), alpha = 0.5) + xlab("Time in days") #,colour=cut
ggsave(file=sprintf("%s-pending.png", csv))

g_running = ggplot(r, aes(colour=Operation))	+ geom_point(aes(x=Time,y=RunningJobs), alpha = 0.5) + xlab("Time in days") #,colour=cut
ggsave(file=sprintf("%s-running.png", csv))

g_used = ggplot(r, aes(colour=Operation))	+ geom_point(aes(x=Time,y=UsedNodes), alpha = 0.5) + xlab("Time in days") #,colour=cut
ggsave(file=sprintf("%s-used.png", csv))

g_nodes_broken = ggplot(d[d$Operation %in% c("b","r","B"),], aes(colour=Operation))	+ geom_point(aes(x=Time,y=BrokenNodes), alpha = 0.5) + xlab("Time in days")
ggsave(file=sprintf("%s-broken.png", csv))

g_job_times = ggplot(create, aes(durationMin)) + geom_density(adjust = 1/5) + xlab("Job runtime in minutes")
ggsave(file=sprintf("%s-job-times.png", csv))


# grid.arrange(g_pending, g_running, g_used, g_nodes_broken, g_waiting_times, g_waiting_time_box, g_job_times, ncol=1)

# c = d[d$Operation == "C", ]
