#--------------------------------- Load BTYD (Buy TIll You Die) Package --------------------------------------------
library(BTYD)
library(grt)
library(amap)
#--------------------------------- Import the data for model training and forecasting ------------------------------
BR_LTV_CALIB_DATA <- read.table(file="//10.8.8.51/lv0/Tanumoy/Datasets/From Hive/BR_20140101_LTV_CALIB_TRANSACTION.TXT",
                          sep="|",head=FALSE)

colnames(BR_LTV_CALIB_DATA)[1]  <- "customer_key"
colnames(BR_LTV_CALIB_DATA)[2]  <- "acquisition_date"
colnames(BR_LTV_CALIB_DATA)[3]  <- "calib_start_date"
colnames(BR_LTV_CALIB_DATA)[4]  <- "calib_end_date"
colnames(BR_LTV_CALIB_DATA)[5]  <- "min_purchase_date"
colnames(BR_LTV_CALIB_DATA)[6]  <- "max_purchase_date"
colnames(BR_LTV_CALIB_DATA)[7]  <- "transaction_days"
colnames(BR_LTV_CALIB_DATA)[8]  <- "spend"
colnames(BR_LTV_CALIB_DATA)[9]  <- "repeat_transaction_days"
colnames(BR_LTV_CALIB_DATA)[10] <- "repeat_spend"
colnames(BR_LTV_CALIB_DATA)[11] <- "customer_duration"
colnames(BR_LTV_CALIB_DATA)[12] <- "study_duration"
colnames(BR_LTV_CALIB_DATA)[13] <- "trunc_transaction_days"
colnames(BR_LTV_CALIB_DATA)[14] <- "trunc_avg_spend"
colnames(BR_LTV_CALIB_DATA)[15] <- "trunc_repeat_transaction_days"
colnames(BR_LTV_CALIB_DATA)[16] <- "trunc_repeat_avg_spend"

BR_LTV_CALIB_DATA <- na.omit(BR_LTV_CALIB_DATA)

BR_LTV_CALIB_DATA$trunc_repeat_spend <- 
  BR_LTV_CALIB_DATA$trunc_repeat_transaction_days *
  BR_LTV_CALIB_DATA$trunc_repeat_avg_spend

BR_LTV_CALIB_DATA.TRAIN <- 
  BR_LTV_CALIB_DATA[as.Date(BR_LTV_CALIB_DATA$acquisition_date) >= 
                    as.Date(BR_LTV_CALIB_DATA$calib_start_date),]

attach(BR_LTV_CALIB_DATA.TRAIN)

scaled <- scale(cbind(repeat_spend,
                      study_duration, 
                      customer_duration,
                      trunc_repeat_avg_spend, 
                      trunc_repeat_transaction_days))

s.param <- t(attr(scaled,"scaled:scale"))
c.param <- t(attr(scaled,"scaled:center"))

BR_LTV_CALIB_DATA.TRAIN.CLUSTER <- cbind(customer_key, 
                                         scale(cbind(repeat_spend,
                                                     study_duration, 
                                                     customer_duration,
                                                     trunc_repeat_avg_spend, 
                                                     trunc_repeat_transaction_days)))

wssplot <- function(data, nc, seed, type)
{
  wss = rep(0,nc); adj.rsq = rep(0,20);
  for (i in 1:nc)
  {
   set.seed(seed)
   wss[i] <- sum(kmeans(data, centers=i, nstart=30,
                        algorithm = type, iter.max=100)$withinss)
   adj.rsq[i] = 1-(wss[i]*(nrow(data)-1))/(wss[1]*(nrow(data)-seq(1,nc)))
   
   print(paste(i, adj.rsq[i]))
  }
  plot(1:nc, wss, type="b", xlab="Number of Clusters",
       ylab="Within groups sum of squares")
  plot(1:nc, adj.rsq, type="b", xlab="Number of Clusters",
       ylab="adjusted r-square")
}

wssplot(BR_LTV_CALIB_DATA.TRAIN.CLUSTER[,2:ncol(BR_LTV_CALIB_DATA.TRAIN.CLUSTER)],
        type="Lloyd", nc=20, seed=1234)


set.seed(1234)
# K-Means Cluster Analysis
fit <- kmeans(BR_LTV_CALIB_DATA.TRAIN.CLUSTER[,2:ncol(BR_LTV_CALIB_DATA.TRAIN.CLUSTER)], 
              nstart=30, 3, algorithm="Lloyd", iter.max=100) 
table(cluster<- fit$cluster)
prop.table(table(cluster))

# get cluster means 
means <-
(aggregate(BR_LTV_CALIB_DATA.TRAIN.CLUSTER[,2:ncol(BR_LTV_CALIB_DATA.TRAIN.CLUSTER)],
          by=list(fit$cluster),FUN=mean))
means <- means[,2:ncol(means)]
plot(y=unlist(means[1,]), x=1:ncol(means), type="b", col="red", ylim=c(min(means), max(means)))
lines(y=unlist(means[2,]), x=1:ncol(means), type="b", col="blue", ylim=c(min(means), max(means)))
lines(y=unlist(means[3,]), x=1:ncol(means), type="b", col="green", ylim=c(min(means), max(means)))
lines(y=unlist(means[4,]), x=1:ncol(means), type="b", col="brown", ylim=c(min(means), max(means)))
lines(y=unlist(means[5,]), x=1:ncol(means), type="b", col="magenta", ylim=c(min(means), max(means)))



BR_LTV_CALIB_DATA.TRAIN <- cbind(BR_LTV_CALIB_DATA.TRAIN, cluster)

for (i in 1:1)
{
  
  options(warn=-1)
  
  print(i)
  
  mydata <- BR_LTV_CALIB_DATA.TRAIN[BR_LTV_CALIB_DATA.TRAIN$cluster==i, ]
  
  customer_key <- mydata$customer_key
  
  x <- mydata$trunc_repeat_transaction_days
  
  t.x <- mydata$customer_duration
  
  T.cal <- mydata$study_duration
  
  m.x <- mydata$trunc_repeat_avg_spend
  
  summary(mydata)
  
  #--------------------------------- Estimate the avg. spend parameters for the customers -----------------------------
  psc<-spend.EstimateParameters(m.x, x)
  summary(psc)
  
  gc()
  #--------------------------------- Estimate the Pareto NBD parameters for the customers -----------------------------
  
  cal.cbs.compressed <- pnbd.compress.cbs(cbind(x,t.x,T.cal))
  ptc <- pnbd.EstimateParameters(cal.cbs.compressed)
  summary(ptc)
  
  gc()
  
}


#-----------------------------Prepare the data for scoring the customers -------------------------------

customer_key <- BR_LTV_CALIB_DATA$customer_key

x <- BR_LTV_CALIB_DATA$trunc_repeat_transaction_days

t.x <- BR_LTV_CALIB_DATA$customer_duration

T.cal <- BR_LTV_CALIB_DATA$study_duration

m.x <- BR_LTV_CALIB_DATA$trunc_repeat_avg_spend

acquisition_date <- as.Date(BR_LTV_CALIB_DATA$acquisition_date)

calib_start_date <- as.Date(BR_LTV_CALIB_DATA$calib_start_date)

calib_end_date <- as.Date(BR_LTV_CALIB_DATA$calib_end_date)

gc()

#-----------------Forecast the no. of txns and avg spend in next 12 months/52 weeks for the customers ----------------

T.star <- 52

forecasted_txn_12 <- pnbd.ConditionalExpectedTransactions(ptc, T.star, x, t.x, T.cal)

forecasted_avg_spend_12 <- spend.expected.value(psc, m.x, x)

gc()

exp_ltv_12 <- forecasted_txn_12 * forecasted_avg_spend_12


#-----------------Forecast the no. of txns and avg spend in next 15 months/65 weeks for the customers ----------------

T.star <- 65

forecasted_txn_15 <- pnbd.ConditionalExpectedTransactions(ptc, T.star, x, t.x, T.cal)

forecasted_avg_spend_15 <- spend.expected.value(psc, m.x, x)

gc()

exp_ltv_15 <- forecasted_txn_15 * forecasted_avg_spend_15


scored_file <- data.frame(cbind(customer_key, 
                                acquisition_date,
                                calib_start_date,
                                calib_end_date,
                                forecasted_txn_12,
                                forecasted_avg_spend_12,
                                exp_ltv_12,
                                forecasted_txn_15, 
                                forecasted_avg_spend_15,
                                exp_ltv_15))


scored_file <- 
  scored_file[scored_file$acquisition_date <
                      scored_file$calib_start_date,]


BR_LTV_VALID_DATA <- read.table(file="//10.8.8.51/lv0/Tanumoy/Datasets/From Hive/BR_20140101_LTV_VALID_TRANSACTION.TXT",
                                    sep="|",head=FALSE)

colnames(BR_LTV_VALID_DATA)[1]  <- "customer_key"
colnames(BR_LTV_VALID_DATA)[2]  <- "valid_start_date"
colnames(BR_LTV_VALID_DATA)[3]  <- "valid_end_date_12"
colnames(BR_LTV_VALID_DATA)[4]  <- "valid_end_date_15"
colnames(BR_LTV_VALID_DATA)[5]  <- "min_purchase_date"
colnames(BR_LTV_VALID_DATA)[6]  <- "max_purchase_date_12"
colnames(BR_LTV_VALID_DATA)[7]  <- "max_purchase_date_15"
colnames(BR_LTV_VALID_DATA)[8]  <- "transaction_days_12"
colnames(BR_LTV_VALID_DATA)[9]  <- "transactions_12"
colnames(BR_LTV_VALID_DATA)[10] <- "spend_12"
colnames(BR_LTV_VALID_DATA)[11] <- "transaction_days_15"
colnames(BR_LTV_VALID_DATA)[12] <- "transactions_15"
colnames(BR_LTV_VALID_DATA)[13] <- "spend_15"

customer_key <- BR_LTV_VALID_DATA$customer_key
spend_12 <- BR_LTV_VALID_DATA$spend_12
spend_15 <- BR_LTV_VALID_DATA$spend_15

valid_data <- cbind(customer_key, spend_12, spend_15)

scored_file_valid <- merge(scored_file, valid_data, by="customer_key")
scored_file_valid <- na.omit (scored_file_valid)

write.table(scored_file_valid, 
            file = "//10.8.8.51/lv0/Tanumoy/Datasets/Model Replication/BR_20140101_LTV_SCORED_VALID.TXT", sep= "|", row.names= FALSE, col.names= TRUE)
rm(T.star)

#save.image("//10.8.8.51/lv0/Tanumoy/Datasets/Model Replication/BR LTV Model.RData")

gc();


