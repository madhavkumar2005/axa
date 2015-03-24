



# ===============================
# AXA 
# Driver telematics
# ===============================


# =========================
# utility functions
# =========================

library(data.table)
library(dplyr)
library(parallel)
options(scipen= 10)

imputeMean <- function(x) replace(x, is.na(x)|is.infinite(x)|x==-Inf, mean(x, na.rm= TRUE))
imputeInfN <- function(x) replace(x, x < -10e10, -1000)
imputeInfP <- function(x) replace(x, x > 10e10, 1000)


# ==================
# feature set 1
# ==================

features001 <- function(d1){
  d1 <- data.table(d1)
  d2 <- d1[, j= list(time= length(x),
                     disp= sqrt((x[length(x)] - x[1])^2 + (y[length(x)] - y[1])^2),
                     dist= sum(sqrt((x - lag(x))^2 + (y - lag(y))^2), na.rm= TRUE)),
           by= c("d", "t")]
  d2 <- d2[, ":=" (speed= dist/time), by= c("d", "t")]
  d2 <- d2[, ":=" (acc= speed/time), by= c("d", "t")]
  d2
}


# did not work -- replaced
features002 <- function(d1){
  d1 <- data.table(d1)
  d1 <- d1[, ":="(v1= sqrt((x - lag(x))^2 + (y - lag(y))^2)), by= c("d", "t")]
  d1$v1 <- ifelse(is.na(d1$v1), 0, d1$v1)
  d1 <- d1[, ":="(v2= abs(v1 - c(NA, v1[1:(.N-1)]))), by= c("d", "t")]
  d1$jump <- ifelse(d1$v2 > 100, 1, 0)
  d2 <- d1[, j= list(jump= sum(jump, na.rm= TRUE)), by= c("d", "t")]
  d2 <- d2[order(d, t),]
  d2
}

# features 
features003 <- function(d1){
  d1 <- data.table(d1)
  d1 <- d1[, ":=" (angle= (y - lag(y))/(x - lag(x))), by= c("d", "t")]
  d1 <- d1[, ":=" (angle_change = angle/lag(angle)), by= c("d", "t")]
  d1 <- d1[,  j= list(num_turns = length(which(angle_change[!is.infinite(angle_change) | !is.na(angle_change)] > 3)),
                   num_stops = length(which(is.na(angle_change)))), 
                   by= c("d", "t")]
  d1
}


# new features
features004 <- function(d1){
  d1 <- data.table(d1)
  d1 <- d1[, ":=" (dist= sqrt((x - lag(x))^2 + (y - lag(y))^2),
                   angle= (y - lag(y))/(x - lag(x))), by= c("d", "t")]
  d1 <- d1[, ":=" (angle_change = angle/lag(angle),
                   acc= dist - lag(dist)), by= c("d", "t")]
  d1 <- d1[, j= list(time= .N,
                     disp= sqrt((x[length(x)] - x[1])^2 + (y[length(x)] - y[1])^2),
                     trip_len= sum(dist, na.rm= TRUE),
                     time_act= length(x[dist > 3]),
                     speed_avg= mean(dist, na.rm= TRUE),
                     speed_avg_nz= mean(dist[dist > 3], na.rm= TRUE),
                     speed_max= max(dist, na.rm= TRUE),
                     speed_min= min(dist[dist > 3], na.rm= TRUE),
                     speed_sd= sd(dist, na.rm= TRUE),
                     speed_sd_nz= sd(dist[dist > 3], na.rm= TRUE),
                     acc_avg= mean(acc, na.rm= TRUE),
                     acc_avg_nz= mean(acc[dist > 3], na.rm= TRUE),
                     acc_max= max(acc, na.rm= TRUE),
                     acc_min= min(acc[dist > 3], na.rm= TRUE),
                     acc_sd= sd(acc, na.rm= TRUE),
                     acc_sd_nz= sd(acc[dist > 3], na.rm= TRUE),
                     acc_avg_p= mean(acc[acc > 0], na.rm= TRUE),
                     acc_avg_n= mean(acc[acc < 0], na.rm= TRUE),
                     num_turns = length(which(angle_change[!is.infinite(angle_change) | !is.na(angle_change)] > 3)),
                     num_stops = length(which(is.na(angle_change))),
                     ),
           by= c("d", "t")]
  d1
}


# new features
features005 <- function(d1){
  d1 <- data.table(d1)
  d1 <- d1[, ":=" (dist= sqrt((x - lag(x))^2 + (y - lag(y))^2),
                   angle= (y - lag(y))/(x - lag(x))), by= c("d", "t")]
  d1 <- d1[, ":=" (angle_change = angle/lag(angle),
                   acc= dist - lag(dist)), by= c("d", "t")]
  d1$angle_change[d1$angle_change == Inf] <- 1000
  d1$angle_change[d1$angle_change == -Inf] <- -1000
  d1$angle_change[is.na(d1$angle_change)] <- 0
  d1 <- d1[, j= list(angle_ch_avg= mean(angle_change, na.rm= TRUE),
                     angle_ch_avg_nz= mean(angle_change[angle_change > -100 & angle_change < 100], na.rm= TRUE),
                     angle_ch_max= max(angle_change[angle_change > -100 & angle_change < 100], na.rm= TRUE),
                     angle_ch_sd= sd(angle_change, na.rm= TRUE),
                     angle_ch_sd_nz= sd(angle_change[angle_change > -100 & angle_change < 100], na.rm= TRUE)),
           by= c("d", "t")]
  d1
}


# features galore!
features006 <- function(d1){
  d1 <- data.table(d1)
  d1 <- d1[, ":=" (dist= sqrt((x - lag(x, n= 5))^2 + (y - lag(y, n= 5))^2),
                   angle= (y - lag(y, n= 5))/(x - lag(x, n= 5))), by= c("d", "t")]
  d1 <- d1[, ":=" (angle_change = angle/lag(angle, 5),
                   speed= dist/5), by= c("d", "t")]
  d1 <- d1[, ":=" (acc= speed - lag(speed, 5)), by= c("d", "t")]
  d1 <- d1[, j= list(speed_avg= mean(dist, na.rm= TRUE),
                     speed_avg_nz= mean(dist[dist > 15], na.rm= TRUE),
                     speed_max= max(dist, na.rm= TRUE),
                     speed_min= min(dist[dist > 15], na.rm= TRUE),
                     speed_sd= sd(dist, na.rm= TRUE),
                     speed_sd_nz= sd(dist[dist > 15], na.rm= TRUE),
                     acc_avg= mean(acc, na.rm= TRUE),
                     acc_avg_nz= mean(acc[dist > 15], na.rm= TRUE),
                     acc_max= max(acc, na.rm= TRUE),
                     acc_min= min(acc[dist > 15], na.rm= TRUE),
                     acc_sd= sd(acc, na.rm= TRUE),
                     acc_sd_nz= sd(acc[dist > 15], na.rm= TRUE),
                     acc_avg_p= mean(acc[acc > 0], na.rm= TRUE),
                     acc_avg_n= mean(acc[acc < 0], na.rm= TRUE),
                     num_turns = length(which(angle_change[!is.infinite(angle_change) | !is.na(angle_change)] > 15)),
                     num_stops = length(which(is.na(angle_change)))),
           by= c("d", "t")]
  d1 <- d1[order(d, t)]
  setnames(d1, setdiff(names(d1), c("d", "t")), paste(setdiff(names(d1), c("d", "t")), "5", sep= "_"))
  d1
}


# more features
features007 <- function(d1){
  d1 <- data.table(d1)
  d1 <- d1[, ":=" (dist= sqrt((x - lag(x, n= 5))^2 + (y - lag(y, n= 5))^2),
                   angle= (y - lag(y, n= 5))/(x - lag(x, n= 5))), by= c("d", "t")]
  d1 <- d1[, ":=" (angle_change = angle/lag(angle, 5),
                   speed= dist/5), by= c("d", "t")]
  d1 <- d1[, ":=" (acc= speed - lag(speed, 5)), by= c("d", "t")]
  d1$angle_change[d1$angle_change == Inf] <- 1000
  d1$angle_change[d1$angle_change == -Inf] <- -1000
  d1$angle_change[is.na(d1$angle_change)] <- 0
  d1 <- d1[, j= list(angle_ch_avg= mean(angle_change, na.rm= TRUE),
                     angle_ch_avg_nz= mean(angle_change[angle_change > -100 & angle_change < 100], na.rm= TRUE),
                     angle_ch_max= max(angle_change[angle_change > -100 & angle_change < 100], na.rm= TRUE),
                     angle_ch_sd= sd(angle_change, na.rm= TRUE),
                     angle_ch_sd_nz= sd(angle_change[angle_change > -100 & angle_change < 100], na.rm= TRUE)),
           by= c("d", "t")]
  setnames(d1, setdiff(names(d1), c("d", "t")), paste(setdiff(names(d1), c("d", "t")), "5", sep= "_"))
  d1
}


# more features
features008 <- function(d1){
  d1 <- data.table(d1)
  d1 <- d1[, ":=" (dist= sqrt((x - lag(x))^2 + (y - lag(y))^2),
                   angle= (y - lag(y))/(x - lag(x))), by= c("d", "t")]
  d1 <- d1[, ":=" (angle_change = angle/lag(angle),
                   acc= dist - lag(dist)), by= c("d", "t")]
  d1$dist <- ifelse(is.na(d1$dist), 0, d1$dist)
  d1$acc <- ifelse(is.na(d1$acc), 0, d1$acc)
  d1$sp_acc <- ifelse(d1$dist > 1 & d1$dist < 150, d1$dist*d1$acc, 0)
  d1$angle_change[d1$angle_change == Inf] <- 100
  d1$angle_change[d1$angle_change == -Inf] <- -100
  d1$angle_change[is.na(d1$angle_change)] <- 0
  d1$turn <- ifelse(d1$angle_change[!is.infinite(d1$angle_change) | !is.na(d1$angle_change)] > 10 & d1$dist > 3, 1, 0)
  d1 <- d1[, j= list(time_speed_nz_frac= length(x[dist > 3])/.N,
                     time_speed_0= length(x[dist < 1]),
                     time_speed_1_2= length(x[dist >= 1 & dist <= 3]),
                     time_speed_3_0= length(x[dist > 3 & dist <= 9]),
                     time_speed_10_19= length(x[dist > 9 & dist <= 19]),
                     time_speed_hi= length(x[dist > 25]),
                     time_speed_vhi= length(x[dist > 35]),
                     time_acc_p= length(x[acc > 0]),
                     time_acc_n= length(x[acc < 0]),
                     time_acc_hi= length(x[acc > 3]),
                     time_acc_lo= length(x[acc >= 1 & acc <= 2]),
                     time_dcc_hi= length(x[acc < -3]),
                     time_dcc_lo= length(x[acc <= -1 & acc >= -2]),
                     sp_acc_avg= mean(sp_acc, na.rm= TRUE),
                     sp_acc_avg_nz= mean(sp_acc[dist > 3], na.rm= TRUE),
                     sp_acc_max= max(sp_acc, na.rm= TRUE),
                     sp_acc_min= min(sp_acc[dist > 3], na.rm= TRUE),
                     sp_acc_sd= sd(sp_acc, na.rm= TRUE),
                     sp_acc_sd_nz= sd(sp_acc[dist > 13], na.rm= TRUE),
                     num_turns2= sum(turn, na.rm= TRUE),
                     turn_speed_avg= mean(dist[turn == 1], na.rm= TRUE),
                     turn_speed_max= max(dist[turn == 1], na.rm= TRUE),
                     turn_speed_min= min(dist[turn == 1], na.rm= TRUE),
                     turn_acc_avg= mean(acc[turn == 1], na.rm= TRUE),
                     turn_acc_max= max(acc[turn == 1], na.rm= TRUE),
                     turn_acc_min= min(acc[turn == 1], na.rm= TRUE)),
           by= c("d", "t")]
  d1
}


# more features
features009 <- function(d1){
  d1 <- data.table(d1)
  d1 <- d1[, ":=" (dist= sqrt((x - lag(x))^2 + (y - lag(y))^2),
                   slope= (y - lag(y))/(x - lag(x))), by= c("d", "t")]
  d1$angle <- atan(d1$slope)*(180/pi)
  d1 <- d1[, ":=" (angle_change = round(angle - lag(angle), 8),
                   acc= dist - lag(dist)), by= c("d", "t")]
  d1$turn <- ifelse(abs(d1$angle_change) > 10 & d1$dist > 1 , 1, 0)
  d1$big_turn <- ifelse(abs(d1$angle_change) > 45 & d1$dist > 1, 1, 0)
  d1$u_turn <- ifelse(abs(d1$angle_change) > 90 & d1$dist > 1, 1, 0)
  d1 <- d1[, j= list(angle_ch_avg2= mean(angle_change, na.rm= TRUE),
                     angle_ch_max2= max(angle_change, na.rm= TRUE),
                     angle_ch_sd2= sd(angle_change, na.rm= TRUE),
                     angle_ch_avg_nz2= mean(angle_change[dist > 3], na.rm= TRUE),
                     angle_ch_max_nz2= max(angle_change[dist > 3], na.rm= TRUE),
                     angle_ch_sd_nz2= sd(angle_change[dist > 3], na.rm= TRUE),
                     num_turns3= sum(turn, na.rm= TRUE),
                     num_big_turns= sum(big_turn, na.rm= TRUE),
                     num_u_turns= sum(u_turn, na.rm= TRUE),
                     sp_turn_avg= mean(dist[turn == 1], na.rm= TRUE),
                     sp_turn_max= max(dist[turn == 1], na.rm= TRUE),
                     sp_turn_sd= sd(dist[turn == 1], na.rm= TRUE),
                     acc_turn_avg= mean(acc[turn == 1], na.rm= TRUE),
                     acc_turn_max= max(acc[turn == 1], na.rm= TRUE),
                     acc_turn_sd= sd(acc[turn == 1], na.rm= TRUE),
                     sp_big_turn_avg= mean(dist[big_turn == 1], na.rm= TRUE),
                     sp_big_turn_max= max(dist[big_turn == 1], na.rm= TRUE),
                     sp_big_turn_sd= sd(dist[big_turn == 1], na.rm= TRUE),
                     acc_big_turn_avg= mean(acc[big_turn == 1], na.rm= TRUE),
                     acc_big_turn_max= max(acc[big_turn == 1], na.rm= TRUE),
                     acc_big_turn_sd= sd(acc[big_turn == 1], na.rm= TRUE),
                     sp_u_turn_avg= mean(dist[u_turn == 1], na.rm= TRUE),
                     sp_u_turn_max= max(dist[u_turn == 1], na.rm= TRUE),
                     sp_u_turn_sd= sd(dist[u_turn == 1], na.rm= TRUE),
                     acc_u_turn_avg= mean(acc[u_turn == 1], na.rm= TRUE),
                     acc_u_turn_max= max(acc[u_turn == 1], na.rm= TRUE),
                     acc_u_turn_sd= sd(acc[u_turn == 1], na.rm= TRUE)),
           by= c("d", "t")]
  d1 <- d1[, lapply(.SD, imputeMean), by= c("d")]
  d1
}



prepareData <- function(drivers, filename= NULL, FUN= features001, cores= 2L,
                        prog= "explore/track_progress.txt"){
  a <- proc.time()
  d1 <- mclapply(drivers, function(x){
    writeLines(paste("driver", which(drivers == x), "of", length(drivers), sep= " "), con= prog)
    d1 <- fread(x)
    d1 <- FUN(d1)
    d1
  }, mc.cores= getOption("mc.cores", cores))
  d1 <- do.call("rbind", d1)
  d1 <- d1[order(d, t),]
  print(proc.time() - a)
  if (!is.null(filename)){
    write.csv(d1, file= filename, row.names= FALSE) 
  }
  d1
}

sanitize <- function(d1){
  d1$speed_avg_nz[is.na(d1$speed_avg_nz)|is.infinite(d1$speed_avg_nz)] <- 0
  d1$speed_sd_nz[is.na(d1$speed_sd_nz)|is.infinite(d1$speed_sd_nz)] <- 0
  d1$acc_avg_nz[is.na(d1$acc_avg_nz)|is.infinite(d1$acc_avg_nz)] <- 0
  d1$acc_min[is.na(d1$acc_min)|is.infinite(d1$acc_min)] <- 0
  d1$acc_sd_nz[is.na(d1$acc_sd_nz)|is.infinite(d1$acc_sd_nz)] <- 0
  d1
}


# =================
# create submission
# =================

createSubmission <- function(preds, filename){
  preds$driver_trip <- paste(preds$d, preds$t, sep= "_")
  write.csv(preds[, c("driver_trip", "prob")], filename, row.names= FALSE)
  cat("saved to disk \n")
}


# =================
# glm model
# =================

glmModel <- function(d1, drivers, feat, samp.size= 5, nruns= 2, cores= 2L, prog= "explore/track_progress.txt"){
  d1 <- data.frame(d1)
  out <- mclapply(drivers, function(x){
    false.cases <- lapply(1:nruns, function(i) sample(drivers[-which(drivers == x)], samp.size))
    writeLines(paste("driver", which(drivers == x), "of", length(drivers), sep= " "), con= prog)
    preds <- lapply(1:nruns, function(y){
      train <- subset(d1, d %in% c(x, false.cases[[y]]))
      train$target <- ifelse(train$d == x, 1, 0)
      train <- train[sample(nrow(train), nrow(train)),]
      form <- paste("target ~", paste(feat, collapse= " + "))
      model <- glm(form, data= train, family= "binomial")
      train$preds.train <- predict(model, train, type = "response")
      train <- train[order(train$d, train$t),]
      if (y == 1){
        subset(train, target== 1, select= c("d", "t", "preds.train"))
      } else {
        subset(train, target== 1, select= c("preds.train"))
      }
    })
    preds <- do.call("cbind", preds)
    preds <- data.frame(preds[, 1:2], prob= rowMeans(preds[, -c(1, 2)]))
    preds
  }, mc.cores= getOption("mc.cores", cores))
  out <- do.call("rbind", out)
  out
}



# =================
# gbm model
# =================

gbmModel <- function(d1, drivers, feat, samp.size= 5, nruns= 2, cores= 2L, prog= "explore/track_progress.txt",
                     ntree= 100, shrinkage= 0.05, depth= 2, minobs= 10, dist= "bernoulli"){
  a <- proc.time()
  require(gbm)
  d1 <- data.frame(subset(d1, select= c("d", "t", feat)))
  out <- mclapply(drivers, function(x){
    false.cases <- lapply(1:nruns, function(i) sample(drivers[-which(drivers == x)], samp.size))
    writeLines(paste("driver", which(drivers == x), "of", length(drivers), sep= " "), con= prog)
    preds <- lapply(1:nruns, function(y){
      train <- subset(d1, d %in% c(x, false.cases[[y]]))
      train$target <- ifelse(train$d == x, 1, 0)
      train <- train[sample(nrow(train), nrow(train)),]
      form <- as.formula(paste("target ~", paste(feat, collapse= " + ")))
      model <- gbm(form, data= train, distribution= dist, interaction.depth= depth, 
                   shrinkage= shrinkage, n.trees= ntree, n.minobsinnode= minobs, 
                   bag.fraction= 0.5, train.fraction= 0.9, keep.data= FALSE)
      train$preds.train <- predict(model, train, n.trees= model$n.trees, type= "response")
      train <- train[order(train$d, train$t),]
      if (y == 1){
        subset(train, target== 1, select= c("d", "t", "preds.train"))
      } else {
        subset(train, target== 1, select= c("preds.train"))
      }
    })
    preds <- do.call("cbind", preds)
    preds <- data.frame(preds[, 1:2], prob= rowMeans(preds[, -c(1, 2)]))
    preds
  }, mc.cores= getOption("mc.cores", cores))
  out <- do.call("rbind", out)
  print(proc.time() - a)
  out
}


# =================
# glmnet model
# =================


glmnetModel <- function(d1, drivers, feat, samp.size= 5, nruns= 2, cores= 2L, prog= "explore/track_progress.txt",
                        dist= "binomial", alpha= 0.5){
  a <- proc.time()
  require(glmnet)
  d1 <- data.frame(subset(d1, select= c("d", "t", feat)))
  out <- mclapply(drivers, function(x){
    false.cases <- lapply(1:nruns, function(i) sample(drivers[-which(drivers == x)], samp.size))
    writeLines(paste("driver", which(drivers == x), "of", length(drivers), sep= " "), con= prog)
    preds <- lapply(1:nruns, function(y){
      train <- subset(d1, d %in% c(x, false.cases[[y]]))
      train$target <- ifelse(train$d == x, 1, 0)
      train <- train[sample(nrow(train), nrow(train)),]
      model <- glmnet(data.matrix(train[, feat]), train$target, family= "binomial", alpha= 0)
      preds.train <- predict(model, data.matrix(train[, feat]), type= "response")
      train$preds.train <- preds.train[, ncol(preds.train)]
      #names(which(model$glmnet.fit$beta[ ,which(model$lambda == model$lambda.min)] != 0))
      train <- train[order(train$d, train$t),]
      if (y == 1){
        subset(train, target== 1, select= c("d", "t", "preds.train"))
      } else {
        subset(train, target== 1, select= c("preds.train"))
      }
    })
    preds <- do.call("cbind", preds)
    preds <- data.frame(preds[, 1:2], prob= rowMeans(preds[, -c(1, 2)]))
    preds
  }, mc.cores= getOption("mc.cores", cores))
  out <- do.call("rbind", out)
  print(proc.time() - a)
  out
}



# =================
# gbm model2
# =================

gbmModel2 <- function(d1, drivers, feat, nfeat= 10, samp.size= 5, nruns= 2, cores= 2L,
                     prog= "explore/track_progress.txt",
                     ntree= 100, shrinkage= 0.05, depth= 2, minobs= 10, dist= "bernoulli"){
  a <- proc.time()
  d1 <- data.frame(d1)
  require(gbm)
  out <- mclapply(drivers, function(x){
    false.cases <- lapply(1:nruns, function(i) sample(drivers[-which(drivers == x)], samp.size))
    writeLines(paste("driver", which(drivers == x), "of", length(drivers), sep= " "), con= prog)
    preds <- lapply(1:nruns, function(y){
      set.seed(round(runif(1, 100, 10000)))
      use.feat <- sample(feat, nfeat)
      train <- subset(d1, d %in% c(x, false.cases[[y]]))
      train$target <- ifelse(train$d == x, 1, 0)
      train <- train[sample(nrow(train), nrow(train)),]
      form <- as.formula(paste("target ~", paste(use.feat, collapse= " + ")))
      model <- gbm(form, data= train, distribution= dist, interaction.depth= depth, 
                   shrinkage= shrinkage, n.trees= ntree, n.minobsinnode= minobs, 
                   bag.fraction= 0.5, train.fraction= 0.9, keep.data= FALSE)
      train$preds.train <- predict(model, train, n.trees= model$n.trees, type= "response")
      train <- train[order(train$d, train$t),]
      if (y == 1){
        subset(train, target== 1, select= c("d", "t", "preds.train"))
      } else {
        subset(train, target== 1, select= c("preds.train"))
      }
    })
    preds <- do.call("cbind", preds)
    preds <- data.frame(preds[, 1:2], prob= rowMeans(preds[, -c(1, 2)]))
    preds
  }, mc.cores= getOption("mc.cores", cores))
  out <- do.call("rbind", out)
  print(proc.time() - a)
  out
}



# =================
# gbm model3
# =================

gbmModel3 <- function(d1, drivers, feat, nfeat= 10, samp.size= 200, nruns= 2, cores= 2L,
                      prog= "explore/track_progress.txt",
                      ntree= 100, shrinkage= 0.05, depth= 2, minobs= 10, dist= "bernoulli"){
  a <- proc.time()
  d1 <- data.frame(d1)
  d1$id <- 1:nrow(d1)
  require(gbm)
  out <- mclapply(drivers, function(x){
    false.cases <- lapply(1:nruns, function(i) sample(d1$id[-which(d1$d == x)], samp.size))
    writeLines(paste("driver", which(drivers == x), "of", length(drivers), sep= " "), con= prog)
    preds <- lapply(1:nruns, function(y){
      set.seed(round(runif(1, 100, 10000)))
      use.feat <- sample(feat, nfeat)
      train <- subset(d1, d == x | d1$id %in% false.cases[[y]])
      train$target <- ifelse(train$d == x, 1, 0)
      train <- train[sample(nrow(train), nrow(train)),]
      form <- as.formula(paste("target ~", paste(use.feat, collapse= " + ")))
      model <- gbm(form, data= train, distribution= dist, interaction.depth= depth, 
                   shrinkage= shrinkage, n.trees= ntree, n.minobsinnode= minobs, 
                   bag.fraction= 0.5, train.fraction= 1.0, keep.data= FALSE)
      train$preds.train <- predict(model, train, n.trees= model$n.trees, type= "response")
      train <- train[order(train$d, train$t),]
      if (y == 1){
        subset(train, target== 1, select= c("d", "t", "preds.train"))
      } else {
        subset(train, target== 1, select= c("preds.train"))
      }
    })
    preds <- do.call("cbind", preds)
    preds <- data.frame(preds[, 1:2], prob= rowMeans(preds[, -c(1, 2)]))
    preds
  }, mc.cores= getOption("mc.cores", cores))
  out <- do.call("rbind", out)
  print(proc.time() - a)
  out
}


# =================
# var lists
# =================

v.list.1 <- c("acc_avg_n", "time_speed_3_0", "acc_sd_nz_5", "speed_sd_nz", 
              "time_acc_lo", "num_stops_5", "angle_ch_sd", "acc_avg_p", "speed_sd_nz_5", 
              "disp", "speed_max_5", "time_dcc_lo", "sp_acc_max", "trip_len", 
              "acc_avg_p_5", "angle_ch_avg_nz_5", "speed_min_5", "time_acc_hi", 
              "num_turns", "time_speed_0", "acc_max", "time_acc_p", "num_stops", 
              "speed_min", "acc_sd_nz", "time_speed_10_19", "turn_acc_max", 
              "angle_ch_avg", "acc_min", "acc_max_5", "acc_sd", "sp_acc_avg_nz", 
              "time_act", "acc_min_5", "speed_sd", "sp_acc_sd_nz", "angle_ch_max", 
              "acc_avg", "angle_ch_sd_nz", "turn_acc_min", "angle_ch_avg_nz", 
              "angle_ch_sd_nz_5", "speed_avg_nz", "angle_ch_avg_5", "speed_avg_nz_5", 
              "sp_acc_sd", "sp_acc_avg", "angle_ch_sd_5", "time", "speed_max", 
              "acc_avg_5", "acc_avg_n_5", "speed_sd_5", "acc_avg_nz_5")


# randomly select 30 variables from set and add a few basic ones
v.list.2 <- c("acc_avg_5", "acc_avg_n", "acc_avg_nz_5", "acc_avg_p_5", "acc_sd_nz", 
              "acc_turn_max", "acc_u_turn_sd", "angle_ch_avg", "angle_ch_avg_nz2", 
              "angle_ch_sd_nz", "angle_ch_sd_nz_5", "angle_ch_sd2", "num_stops", 
              "sp_acc_avg_nz", "sp_acc_max", "sp_acc_min", "sp_big_turn_max", 
              "sp_big_turn_sd", "sp_u_turn_sd", "speed_avg", "speed_avg_5", 
              "speed_min", "speed_min_5", "speed_sd", "speed_sd_nz_5", "time_acc_n", 
              "time_speed_10_19", "time_speed_vhi", "turn_acc_avg", "turn_speed_min",
              "time", "time_act", "disp", "trip_len", "num_turns3")




# =================
# Lagged features
# =================


laggedFeatures <- function(d1){
  d1 <- data.table(d1)
  d1 <- d1[, ":=" (dist= sqrt((x - lag(x, n= l))^2 + (y - lag(y, n= l))^2),
                   angle= (y - lag(y, n= l))/(x - lag(x, n= l))), by= c("d", "t")]
  d1 <- d1[, ":=" (angle_change = angle/lag(angle, l),
                   speed= dist/l), by= c("d", "t")]
  d1 <- d1[, ":=" (acc= speed - lag(speed, l)), by= c("d", "t")]
  d2 <- d1[, j= list(speed_avg= mean(dist, na.rm= TRUE),
                     speed_avg_nz= mean(dist[dist > 3*l], na.rm= TRUE),
                     speed_max= max(dist, na.rm= TRUE),
                     speed_min= min(dist[dist > 3*l], na.rm= TRUE),
                     speed_sd= sd(dist, na.rm= TRUE),
                     speed_sd_nz= sd(dist[dist > 3*l], na.rm= TRUE),
                     acc_avg= mean(acc, na.rm= TRUE),
                     acc_avg_nz= mean(acc[dist > 3*l], na.rm= TRUE),
                     acc_max= max(acc, na.rm= TRUE),
                     acc_min= min(acc[dist > 3*l], na.rm= TRUE),
                     acc_sd= sd(acc, na.rm= TRUE),
                     acc_sd_nz= sd(acc[dist > 3*l], na.rm= TRUE),
                     acc_avg_p= mean(acc[acc > 0], na.rm= TRUE),
                     acc_avg_n= mean(acc[acc < 0], na.rm= TRUE),
                     num_turns = length(which(angle_change[!is.infinite(angle_change) | !is.na(angle_change)] > 3*l)),
                     num_stops = length(which(is.na(angle_change)))),
           by= c("d", "t")]
  d1$angle_change[d1$angle_change == Inf] <- 1000
  d1$angle_change[d1$angle_change == -Inf] <- -1000
  d1$angle_change[is.na(d1$angle_change)] <- 0
  d3 <- d1[, j= list(angle_ch_avg= mean(angle_change, na.rm= TRUE),
                     angle_ch_avg_nz= mean(angle_change[angle_change > -100 & angle_change < 100], na.rm= TRUE),
                     angle_ch_max= max(angle_change[angle_change > -100 & angle_change < 100], na.rm= TRUE),
                     angle_ch_sd= sd(angle_change, na.rm= TRUE),
                     angle_ch_sd_nz= sd(angle_change[angle_change > -100 & angle_change < 100], na.rm= TRUE)),
           by= c("d", "t")]
  d1 <- d1[, ":=" (dist= sqrt((x - lag(x, n= l))^2 + (y - lag(y, n= l))^2),
                   angle= (y - lag(y, n= l))/(x - lag(x, n= l))), by= c("d", "t")]
  d1 <- d1[, ":=" (angle_change = angle/lag(angle, l),
                   speed= dist/l), by= c("d", "t")]
  d1 <- d1[, ":=" (acc= speed - lag(speed, l)), by= c("d", "t")]
  d1$dist <- ifelse(is.na(d1$dist), 0, d1$dist)
  d1$acc <- ifelse(is.na(d1$acc), 0, d1$acc)
  d1$sp_acc <- ifelse(d1$dist > 1 & d1$dist < 150, d1$dist*d1$acc, 0)
  d1$angle_change[d1$angle_change == Inf] <- 100
  d1$angle_change[d1$angle_change == -Inf] <- -100
  d1$angle_change[is.na(d1$angle_change)] <- 0
  d1$turn <- ifelse(d1$angle_change[!is.infinite(d1$angle_change) | !is.na(d1$angle_change)] > 10*(l/2) & d1$dist > 3*(l/2), 1, 0)
  d4 <- d1[, j= list(time_speed_nz_frac= length(x[dist > 3*l])/.N,
                     time_speed_0= length(x[dist < 1*l]),
                     time_speed_1_2= length(x[dist >= 1*l & dist <= 3*l]),
                     time_speed_3_0= length(x[dist > 3*l & dist <= 9*l]),
                     time_speed_10_19= length(x[dist > 9*l & dist <= 19*l]),
                     time_speed_hi= length(x[dist > 25*l]),
                     time_speed_vhi= length(x[dist > 35*l]),
                     time_acc_p= length(x[acc > 0]),
                     time_acc_n= length(x[acc < 0]),
                     time_acc_hi= length(x[acc > 3]),
                     time_acc_lo= length(x[acc >= 1 & acc <= 2]),
                     time_dcc_hi= length(x[acc < -3]),
                     time_dcc_lo= length(x[acc <= -1 & acc >= -2]),
                     sp_acc_avg= mean(sp_acc, na.rm= TRUE),
                     sp_acc_avg_nz= mean(sp_acc[dist > 3*l], na.rm= TRUE),
                     sp_acc_max= max(sp_acc, na.rm= TRUE),
                     sp_acc_min= min(sp_acc[dist > 3*l], na.rm= TRUE),
                     sp_acc_sd= sd(sp_acc, na.rm= TRUE),
                     sp_acc_sd_nz= sd(sp_acc[dist > 13*l], na.rm= TRUE),
                     num_turns2= sum(turn, na.rm= TRUE),
                     turn_speed_avg= mean(dist[turn == 1], na.rm= TRUE),
                     turn_speed_max= max(dist[turn == 1], na.rm= TRUE),
                     turn_speed_min= min(dist[turn == 1], na.rm= TRUE),
                     turn_acc_avg= mean(acc[turn == 1], na.rm= TRUE),
                     turn_acc_max= max(acc[turn == 1], na.rm= TRUE),
                     turn_acc_min= min(acc[turn == 1], na.rm= TRUE)),
           by= c("d", "t")]
  d1 <- d1[, ":=" (dist= sqrt((x - lag(x, n= l))^2 + (y - lag(y, n= l))^2),
                   slope= (y - lag(y, n= l))/(x - lag(x, n= l))), by= c("d", "t")]
  d1$angle <- atan(d1$slope)*(180/pi)
  d1 <- d1[, ":=" (angle_change = round(angle - lag(angle, l), 8),
                   speed= dist/l), by= c("d", "t")]
  d1$turn <- ifelse(abs(d1$angle_change) > 10 & d1$dist > 1*l , 1, 0)
  d1$big_turn <- ifelse(abs(d1$angle_change) > 45 & d1$dist > 1*l, 1, 0)
  d1$u_turn <- ifelse(abs(d1$angle_change) > 90 & d1$dist > 1*l, 1, 0)
  d5 <- d1[, j= list(angle_ch_avg2= mean(angle_change, na.rm= TRUE),
                     angle_ch_max2= max(angle_change, na.rm= TRUE),
                     angle_ch_sd2= sd(angle_change, na.rm= TRUE),
                     angle_ch_avg_nz2= mean(angle_change[dist > 3*l], na.rm= TRUE),
                     angle_ch_max_nz2= max(angle_change[dist > 3*l], na.rm= TRUE),
                     angle_ch_sd_nz2= sd(angle_change[dist > 3*l], na.rm= TRUE),
                     num_turns3= sum(turn, na.rm= TRUE),
                     num_big_turns= sum(big_turn, na.rm= TRUE),
                     num_u_turns= sum(u_turn, na.rm= TRUE),
                     sp_turn_avg= mean(dist[turn == 1], na.rm= TRUE),
                     sp_turn_max= max(dist[turn == 1], na.rm= TRUE),
                     sp_turn_sd= sd(dist[turn == 1], na.rm= TRUE),
                     acc_turn_avg= mean(acc[turn == 1], na.rm= TRUE),
                     acc_turn_max= max(acc[turn == 1], na.rm= TRUE),
                     acc_turn_sd= sd(acc[turn == 1], na.rm= TRUE),
                     sp_big_turn_avg= mean(dist[big_turn == 1], na.rm= TRUE),
                     sp_big_turn_max= max(dist[big_turn == 1], na.rm= TRUE),
                     sp_big_turn_sd= sd(dist[big_turn == 1], na.rm= TRUE),
                     acc_big_turn_avg= mean(acc[big_turn == 1], na.rm= TRUE),
                     acc_big_turn_max= max(acc[big_turn == 1], na.rm= TRUE),
                     acc_big_turn_sd= sd(acc[big_turn == 1], na.rm= TRUE),
                     sp_u_turn_avg= mean(dist[u_turn == 1], na.rm= TRUE),
                     sp_u_turn_max= max(dist[u_turn == 1], na.rm= TRUE),
                     sp_u_turn_sd= sd(dist[u_turn == 1], na.rm= TRUE),
                     acc_u_turn_avg= mean(acc[u_turn == 1], na.rm= TRUE),
                     acc_u_turn_max= max(acc[u_turn == 1], na.rm= TRUE),
                     acc_u_turn_sd= sd(acc[u_turn == 1], na.rm= TRUE)),
           by= c("d", "t")]
  d1 <- merge(d2, d3, by= c("d", "t"))
  d1 <- merge(d1, d4, by= c("d", "t"))
  d1 <- merge(d1, d5, by= c("d", "t"))
  d1 <- d1[, lapply(.SD, imputeMean), by= c("d")]
  d1 <- d1[, lapply(.SD, imputeInfN), by= c("d")]
  d1 <- d1[, lapply(.SD, imputeInfP), by= c("d")]
  setnames(d1, setdiff(names(d1), c("d", "t")), paste(setdiff(names(d1), c("d", "t")), l, sep= "_"))
  d1
}
