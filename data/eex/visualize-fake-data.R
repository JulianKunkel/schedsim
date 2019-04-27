#!/usr/bin/env Rscript

library(ggplot2)

d = read.csv("T1.csv")
ggplot(data=d, aes(time, price, group=1)) + geom_line(size=1.5) + ylab("price in cents") +xlab("time (day)")
ggsave("T1.png", width=6, height=4)

ggplot(data=d[1:96,], aes(time, price, group=1)) + geom_line(size=1.5) + ylab("price in cents") +xlab("time (day)")
ggsave("T1.png", width=6, height=4)
