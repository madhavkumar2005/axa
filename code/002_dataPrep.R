



# ===============================
# AXA 
# Driver telematics
# ===============================


# =========================
# Data Prep
# =========================

# ==================
# features 001
# ==================

base <- "data/d_level/"
f <- list.files(base)
drivers <- paste0(base, f)
source("code/utils.R")
prepareData(drivers = drivers, filename= "data/prepapred_data/features001.csv", FUN= features001, 
            cores= 2, prog= "explore/track_progress.txt")

# check file
d1 <- fread("data/prepapred_data/features001.csv")



# ==================
# features 002
# ==================

base <- "data/d_level/"
f <- list.files(base)
drivers <- paste0(base, f)
source("code/utils.R")
d2 <- prepareData(drivers = drivers, filename= NULL, FUN= features002,
                  cores= 2, prog= "explore/track_progress.txt")

# merge
d1 <- fread("data/prepapred_data/features001.csv")
d1 <- merge(d1, d2, by= c("d", "t"))

# save
write.csv(d1, file= "data/prepapred_data/features002.csv", row.names= FALSE)


# ==================
# features 003
# ==================

base <- "data/d_level/"
f <- list.files(base)
drivers <- paste0(base, f)
source("code/utils.R")
d2 <- prepareData(drivers = drivers, filename= NULL, FUN= features003,
                  cores= 2, prog= "explore/track_progress.txt")

# merge
d1 <- fread("data/prepapred_data/features001.csv")
d1 <- merge(d1, d2, by= c("d", "t"))

# save
write.csv(d1, file= "data/prepapred_data/features003.csv", row.names= FALSE)


# ==================
# features 004
# ==================

base <- "data/d_level/"
f <- list.files(base)
drivers <- paste0(base, f)
source("code/utils.R")
d1 <- prepareData(drivers = drivers, filename= NULL, FUN= features004,
                  cores= 2, prog= "explore/track_progress.txt")
d1 <- sanitize(d1)
summary(d1)

# save
write.csv(d1, file= "data/prepapred_data/features004.csv", row.names= FALSE)


# ==================
# features 005
# ==================

base <- "data/d_level/"
f <- list.files(base)
drivers <- paste0(base, f)
source("code/utils.R")
d2 <- prepareData(drivers = drivers, filename= NULL, FUN= features005,
                  cores= 2, prog= "explore/track_progress.txt")

# merge
d1 <- fread("data/prepapred_data/features004.csv")
d1 <- merge(d1, d2, by= c("d", "t"))

# save
write.csv(d1, file= "data/prepapred_data/features005.csv", row.names= FALSE)



# ==================
# features 006
# ==================

base <- "data/d_level/"
f <- list.files(base)
drivers <- paste0(base, f)
source("code/utils.R")
d2 <- prepareData(drivers= drivers, filename= NULL, FUN= features006,
                  cores= 2, prog= "explore/track_progress.txt")

# merge
d1 <- fread("data/prepared_data//features005.csv")
d1 <- merge(d1, d2, by= c("d", "t"))

# save
write.csv(d1, file= "data/prepared_data//features006.csv", row.names= FALSE)


# ==================
# features 007
# ==================

base <- "data/d_level/"
f <- list.files(base)
drivers <- paste0(base, f)
source("code/utils.R")
d2 <- prepareData(drivers= drivers, filename= NULL, FUN= features007,
                  cores= 2, prog= "explore/track_progress.txt")

# merge
d1 <- fread("data/prepared_data//features006.csv")
d1 <- merge(d1, d2, by= c("d", "t"))

# clean some vars
d1$speed_min[is.na(d1$speed_min)|is.infinite(d1$speed_min)] <- 0
d1$speed_avg_nz_5[is.na(d1$speed_avg_nz_5)|is.infinite(d1$speed_avg_nz_5)] <- 0
d1$speed_sd_nz_5[is.na(d1$speed_sd_nz_5)|is.infinite(d1$speed_sd_nz_5)] <- 0
d1$acc_avg_nz_5[is.na(d1$acc_avg_nz_5)|is.infinite(d1$acc_avg_nz_5)] <- 0
d1$acc_min_5[is.na(d1$acc_min_5)|is.infinite(d1$acc_min_5)] <- 0
d1$acc_sd_nz_5[is.na(d1$acc_sd_nz_5)|is.infinite(d1$acc_sd_nz_5)] <- 0
d1$speed_min_5[is.na(d1$speed_min_5)|is.infinite(d1$speed_min_5)] <- 0

# save
write.csv(d1, file= "data/prepared_data//features007.csv", row.names= FALSE)


# ==================
# features 008
# ==================

base <- "data/d_level/"
f <- list.files(base)
drivers <- paste0(base, f)
source("code/utils.R")
d2 <- prepareData(drivers= drivers, filename= NULL, FUN= features008,
                  cores= 2, prog= "explore/track_progress.txt")

# clean
d2$sp_acc_avg_nz[is.na(d2$sp_acc_avg_nz)|is.infinite(d2$sp_acc_avg_nz)] <- 0
d2$sp_acc_min[is.na(d2$sp_acc_min)|is.infinite(d2$sp_acc_min)] <- 0
d2$sp_acc_sd_nz[is.na(d2$sp_acc_sd_nz)|is.infinite(d2$sp_acc_sd_nz)] <- 0
d2$turn_speed_avg[is.na(d2$turn_speed_avg)] <- mean(d2$turn_speed_avg, na.rm= TRUE)
d2$turn_speed_max[is.infinite(d2$turn_speed_max)] <- mean(d2$turn_speed_avg, na.rm= TRUE)
d2$turn_speed_min[is.infinite(d2$turn_speed_min)] <- mean(d2$turn_speed_avg, na.rm= TRUE)
d2$turn_acc_avg[is.na(d2$turn_acc_avg)] <- mean(d2$turn_acc_avg, na.rm= TRUE)
d2$turn_acc_max[is.infinite(d2$turn_acc_max)] <- 0
d2$turn_acc_min[is.infinite(d2$turn_acc_min)] <- 0

# merge
d1 <- fread("data/prepared_data//features007.csv")
d1 <- merge(d1, d2, by= c("d", "t"))

# save
write.csv(d1, file= "data/prepared_data//features008.csv", row.names= FALSE)



# ==================
# features 009
# ==================

base <- "data/d_level/"
f <- list.files(base)
drivers <- paste0(base, f)
source("code/utils.R")
d2 <- prepareData(drivers= drivers, filename= NULL, FUN= features009,
                  cores= 2, prog= "explore/track_progress.txt")

# clean
summary(d2)
d2$angle_ch_max2[is.infinite(d2$angle_ch_max2)] <- 0
d2$angle_ch_max_nz2[is.infinite(d2$angle_ch_max_nz2)] <- 0
d2$sp_turn_max[is.infinite(d2$sp_turn_max)] <- 0
d2$acc_turn_max[is.infinite(d2$acc_turn_max)] <- 0
d2$sp_big_turn_max[is.infinite(d2$sp_big_turn_max)] <- 0
d2$acc_big_turn_max[is.infinite(d2$acc_big_turn_max)] <- 0
d2$sp_u_turn_max[is.infinite(d2$sp_u_turn_max)] <- 0
d2$acc_u_turn_max[is.infinite(d2$acc_u_turn_max)] <- 0

# merge
d1 <- fread("data/prepared_data//features008.csv")
d1 <- merge(d1, d2, by= c("d", "t"))

# save
write.csv(d1, file= "data/prepared_data//features009.csv", row.names= FALSE)


# ==================
# Merge Gaurav vars
# ==================

library(data.table)
d1 <- fread("data/prepared_data/features009.csv")
gd <- fread("gaurav/madhav_centri_data.csv")
gd$V1 <- NULL
setnames(gd, c("driver", "trip.number"), c("d", "t"))
names(gd)
gd$d <- as.integer(gd$d)
d1 <- merge(d1, gd, by= c("d", "t"))

# save
write.csv(d1, file= "data/prepared_data//features009.csv", row.names= FALSE)



# ==================
# Lagged features
# ==================

base <- "data/d_level/"
f <- list.files(base)
drivers <- paste0(base, f)
source("code/utils.R")
l <- 2
d2 <- prepareData(drivers= drivers, filename= NULL, FUN= laggedFeatures,
                  cores= 2, prog= "explore/track_progress.txt")
write.csv(d2, file= "data/prepared_data//lag2.csv", row.names= FALSE)
rm(d2)
gc()

l <- 3
d2 <- prepareData(drivers= drivers, filename= NULL, FUN= laggedFeatures,
                  cores= 2, prog= "explore/track_progress.txt")
write.csv(d2, file= "data/prepared_data//lag3.csv", row.names= FALSE)
rm(d2)
gc()


l <- 5
d2 <- prepareData(drivers= drivers, filename= NULL, FUN= laggedFeatures,
                  cores= 2, prog= "explore/track_progress.txt")
write.csv(d2, file= "data/prepared_data//lag5.csv", row.names= FALSE)
rm(d2)
gc()


l <- 8
d2 <- prepareData(drivers= drivers, filename= NULL, FUN= laggedFeatures,
                  cores= 2, prog= "explore/track_progress.txt")
write.csv(d2, file= "data/prepared_data//lag8.csv", row.names= FALSE)
rm(d2)
gc()


l <- 10
d2 <- prepareData(drivers= drivers, filename= NULL, FUN= laggedFeatures,
                  cores= 2, prog= "explore/track_progress.txt")
write.csv(d2, file= "data/prepared_data//lag10.csv", row.names= FALSE)
rm(d2)
gc()



# ==================
# interactions
# ==================

library(data.table)
d1 <- fread("data/prepared_data/features007.csv")

d1$speed_acc <- d1$disp*d1$time_act



# ==================
# clustering
# ==================

library(data.table)
d1 <- fread("data/prepapred_data/features001.csv")

# tmp
tmp <- data.frame(subset(d1, d == 1))
cl <- kmeans(tmp[, -c(1, 2)], centers= 2, iter.max= 10)
tmp$cl <- cl$cluster
table(tmp$cl)
by(data = tmp[, -c(1, 2, 8)], INDICES = tmp$cl, summary)
tmp$target <- 1
model <- glm(target ~ time + disp + dist + speed + acc + cl, data= tmp, family= "binomial")
summary(model)

