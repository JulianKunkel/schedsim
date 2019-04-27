#!/usr/bin/env Rscript

library(ggplot2)

count = 500*24 # create more fake data
Z = rnorm( count )
Y = rep(NA, count)
a = 0.5 # runif(p)
c = 0.25
s = 0.5 * (sin((1:count)/pi)) / 2

Y = runif(count)
for(i in 25:count) {
     Y[i] = s[i] + 0.8*Y[i-1] + 0.2* Y[i-24] + Z[i]
}

Y = Y + (14 - mean(Y))
time = (1:count)/24.0
d = data.frame(time, price=Y)
ggplot(data=d, aes(x=time, y=price, group=1)) +
    geom_line(size=1.5) + ylab("price in cents") +xlab("time (day)")
ggsave("simulated.png", width=6, height=4)

Y = Y / 100.0 # price in cents
d = data.frame(time, price=Y)
write.csv(d, file = "simulated.csv")
