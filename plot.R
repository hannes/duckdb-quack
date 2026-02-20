runs <- readr::read_tsv("macbook.tsv")

print(runs)

library(ggplot2)
pdf("rpc.pdf", height=5, width=15)
ggplot(runs, aes(x=rows, y=median_time_seconds, shape=method, colour=method)) + geom_line() + geom_point() + scale_x_log10() + scale_y_log10()
dev.off()

