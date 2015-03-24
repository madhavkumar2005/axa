


# ===============================
# AXA 
# Driver telematics
# ===============================


# =========================
# Data import
# =========================

dir.create("data/d_level/")
base <- "data/drivers"
f <- list.files(base)

# single df for each driver
library(parallel)
lapply(f, function(x){
  i <- list.files(paste(base, x, sep= "/"))
  tmp <- mclapply(i, function(y){
    j <- read.csv(paste(base, x, y, sep= "/"))
    j$d <- as.numeric(x)
    j$t <- as.numeric(gsub(".csv", "", y))
    j
  }, mc.cores= getOption("mc.cores", 2L))
  tmp <- do.call("rbind", tmp)
  cat(which(f == x), "of", length(f), "\n")
  write.csv(tmp, file= paste("data/d_level//", x, ".csv", sep= ""), row.names= FALSE)
})

# save driver list 
drivers <- as.numeric(f)
save(drivers, file= "data/prepapred_data/drivers.RData")
