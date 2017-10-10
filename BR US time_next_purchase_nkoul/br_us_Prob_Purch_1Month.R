#initialise libraries required
library(dplyr);library(gbm);library(readr);library(pROC);library(ff);library(ffbase)

#import the dataset 
data<-read.table('C:/Users/nkoul/Projects/NextPurchsae/Alt_Np/gp_prob_purch_6month_export',sep='|', header=FALSE)
#C:\Users\nkoul\Projects\NextPurchsae\Alt_Np

#insert headers
names(data) <- c("masterkey",
                 "total_orders_placed",
                 "active_period",
                 "recency_purch",
                 "freq_purch",
                 "ratio_purchases",
                 "items_browsed",
                 "items_added",
                 "lastyear_samemonth_purchase",
                 "purchase_in_target_window",
                 "lastyear_same3months_purchase",
                 "purchase_in_3month_target_window",
                 "purchase_in_6month_target_window")

# Sort the data in descending order of orders placed
data <- data %>% arrange(desc(total_orders_placed))

data <- data %>% filter(total_orders_placed <= 50)

# Cap a few variables to 99.5%ile
data$total_orders_placed <- ifelse(data$total_orders_placed > 33, 33, data$total_orders_placed)                 
data$items_browsed <- ifelse(data$items_browsed > 125,125,data$items_browsed)
data$items_added <- ifelse(data$items_added > 55,55,data$items_added )
data$ratio_purchases <- ifelse(data$ratio_purchases > 10,10,data$ratio_purchases)


# Recalculate the frequency
data$freq_purch <- data$total_orders_placed / data$active_period
data$freq_purch <- ifelse(data$freq_purch > 5,5,data$freq_purch)

train.data <- sample_n(data,size = 20000)
validation.data <- sample_n(data,size = 150000)
validation.data <- sample_n(data,size = 4657521)
noreseller.glmmod <- glm(data = train.data, purchase_in_target_window ~ recency_purch + freq_purch + lastyear_samemonth_purchase + ratio_purchases + items_added + items_browsed, family = "binomial")
summary(noreseller.glmmod)
noreseller.glmmod <- glm(data = train.data, purchase_in_3month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases + items_added + items_browsed, family = "binomial")
summary(noreseller.glmmod)
noreseller.glmmod <- glm(data = train.data, purchase_in_6month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases + items_added + items_browsed, family = "binomial")
summary(noreseller.glmmod)
validation.data$sixmonthprob <- predict(noreseller.glmmod,newdata = validation.data,type="response")
save(validation.data,file='C:/Users/nkoul/Projects/NextPurchsae/Alt_Np/validation_6m.Rdata')

roc1 <- roc(response = validation.data$purchase_in_target_window, predictor = validation.data$onemonthprob,plot=TRUE)
roc1 <- roc(response = validation.data$purchase_in_3month_target_window, predictor = validation.data$threemonthprob,plot=TRUE)
roc1 <- roc(response = validation.data$purchase_in_6month_target_window, predictor = validation.data$sixmonthprob,plot=TRUE)

save(noreseller.glmmod,file='C:/Users/nkoul/Projects/NextPurchsae/Alt_Np/model6m.RData')

library(xgboost)
xgbmod1 <- xgboost(data = data.matrix(train.data[,c(2,3,4,5,6,7,8,9,11)]),label = train.data$purchase_in_6month_target_window,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 2, eval_metric="error", booster="gbtree",silent=1))
validation.data$xgbpred1 <- predict(xgbmod1,data.matrix(validation.data[,c(2,3,4,5,6,7,8,9,11)]))
roc(response = validation.data$purchase_in_6month_target_window, predictor = validation.data$xgbpred1,plot=TRUE) #0.7606
save(xgbmod1,file='C:/Users/nkoul/Projects/NextPurchsae/Alt_Np/model6m_xgb.RData')

data.allones <- data %>% filter(purchase_in_target_window == 1) %>% sample_n(7500)
data.fewzeroes <- data %>% filter(purchase_in_target_window == 0) %>% sample_n(7500)
balanced.train.data <- rbind(data.allones,data.fewzeroes)

balanced.glmmod <- glm(data = balanced.train.data, purchase_in_target_window ~ recency_purch + freq_purch + lastyear_samemonth_purchase + ratio_purchases + items_added + items_browsed, family = "binomial")
summary(balanced.glmmod)
validation.data$balancedonemonthprob <- predict(balanced.glmmod,newdata = validation.data,type="response")

roc1 <- roc(response = validation.data$purchase_in_target_window, predictor = validation.data$balancedonemonthprob,plot=TRUE) #0.7734


# Logistic Regression model for 1 month purchase
glmmod <- glm(data = train.data, purchase_in_target_window ~ recency_purch + freq_purch + lastyear_samemonth_purchase + ratio_purchases + items_added + items_browsed, family = "binomial")
summary(glmmod)

validation.data$onemonthprob <- predict(glmmod,newdata = validation.data,type="response")

roc2 <- roc(response = validation.data$purchase_in_target_window, predictor = validation.data$onemonthprob,plot=TRUE) #0.7734
validation.data <- validation.data %>% arrange(desc(onemonthprob))
#mean(as.numeric(validation.data$purchase_in_target_window[1:18840]))


# Try the 3 months logistic model
glm3monthmod <- glm(data = train.data, purchase_in_3month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases + items_added + items_browsed, family = "binomial")
summary(glm3monthmod)

validation.data$threemonthprob <- predict(glm3monthmod,newdata = validation.data,type="response")
roc(response = validation.data$purchase_in_3month_target_window, predictor = validation.data$threemonthprob) #0.7588
validation.data <- validation.data %>% arrange(desc(threemonthprob))

#mean(as.numeric(validation.data$purchase_in_3month_target_window[1:18840])) # 0.802
#mean(as.numeric(validation.data$purchase_in_3month_target_window))   # 0.355


library(xgboost)
xgbmod1 <- xgboost(data = data.matrix(train.data[,c(2:9)]),label = train.data$purchase_in_target_window,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 2, eval_metric="error", booster="gbtree",silent=1))
validation.data$xgbpred1 <- predict(xgbmod1,data.matrix(validation.data[,c(2:9)]))
roc(response = validation.data$purchase_in_target_window, predictor = validation.data$xgbpred1,plot=TRUE) #0.7606
save(xgbmod1,file='C:/Users/nkoul/Projects/NextPurchsae/Alt_Np/model1m_xgb.RData')


library(e1071)
xvars <- cbind(train.data$recency_purch_norm,train.data$freq_purch_norm,train.data$lastyear_samemonth_purchase)
validationxvars <- cbind(validation.data$recency_purch_norm,validation.data$freq_purch_norm,validation.data$lastyear_samemonth_purchase)

svm.tune <- tune(svm,train.x = xvars,train.y = train.data$purchase_in_target_window,
                 validation.x = validationxvars,validation.y = validation.data$purchase_in_target_window,
                 ranges = list(cost=c(0.001,0.005,0.01,0.1,1,10)),type = "C",kernel = "sigmoid")
summary(svm.tune)      


svmmod <- svm(data = train.data, purchase_in_target_window ~  recency_purch_norm + freq_purch_norm + lastyear_samemonth_purchase,
              type = "C", kernel = "sigmoid",probability = TRUE,cost = 0.001)
           


svmpred <- predict(svmmod,newdata = validation.data,probability =  T)
svmprobs <- attr(svmpred,"probabilities")
validation.data$svmpred <- svmprobs[,2]
roc(response = validation.data$purchase_in_target_window, predictor = validation.data$svmpred) #0.531


conf.matrix <- table(truth = validation.data$purchase_in_target_window,pred = validation.data$svmpred)
accuracy <- (conf.matrix[1] + conf.matrix[4]) / (conf.matrix[1] + conf.matrix[2] + conf.matrix[3] + conf.matrix[4])




conf.matrix <- table(truth = validation.data$purchase_in_target_window,pred = validation.data$svmpred)
accuracy <- (conf.matrix[1] + conf.matrix[4]) / (conf.matrix[1] + conf.matrix[2] + conf.matrix[3] + conf.matrix[4])
conf.matrix

plot(svmmod,xvars)

plot(train.data$freq_purch,train.data$recency_purch)
library(plotly)
plot_ly(data = train.data,x=recency_purch, y = freq_purch,mode="markers",color=purchase_in_target_window)

# Trying Random Forests
library(randomForest)
rfmod <- randomForest(data = train.data, purchase_in_target_window ~ recency_purch + freq_purch + lastyear_samemonth_purchase,
                      mtry = 2,importance = TRUE,ntree=500,replace=TRUE )
summary(rfmod)
attributes(rfmod)
rfmod$importance
plot(rfmod)

rfpred <- predict(rfmod,newdata = validation.data,type="prob")
validation.data$rfpred <- rfpred[,2]
roc(response = validation.data$purchase_in_target_window, predictor = validation.data$rfpred) #0.672


library(nnet)
train.data1 <- sample_n(data,size=5000)
train.data2 <- sample_n(data,size=10000)
train.data3 <- sample_n(data,size=20000)
train.data4 <- sample_n(data,size=25000)
train.data5 <- sample_n(data,size=30000)
train.data6 <- sample_n(data,size=35000)
train.data7 <- sample_n(data,size=40000)
train.data8 <- sample_n(data,size=45000)
train.data9 <- sample_n(data,size=50000)
train.data10 <- sample_n(data,size=55000)

#test <- inner_join(train.data10,validation.data)

validation.data <- sample_frac(data,size = 0.02)

nnmod1 <- nnet(data = train.data1, purchase_in_target_window ~ recency_purch + freq_purch + lastyear_samemonth_purchase + ratio_purchases + items_added + items_browsed,
              decay = 0.05,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nnmod2 <- nnet(data = train.data2, purchase_in_target_window ~ recency_purch + freq_purch + lastyear_samemonth_purchase + ratio_purchases + items_added + items_browsed,
              decay = 0.05,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nnmod3 <- nnet(data = train.data3, purchase_in_target_window ~ recency_purch + freq_purch + lastyear_samemonth_purchase + ratio_purchases + items_added + items_browsed,
              decay = 0.05,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nnmod4 <- nnet(data = train.data4, purchase_in_target_window ~ recency_purch + freq_purch + lastyear_samemonth_purchase + ratio_purchases + items_added + items_browsed,
              decay = 0.05,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nnmod5 <- nnet(data = train.data5, purchase_in_target_window ~ recency_purch + freq_purch + lastyear_samemonth_purchase + ratio_purchases + items_added + items_browsed,
              decay = 0.05,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nnmod6 <- nnet(data = train.data6, purchase_in_target_window ~ recency_purch + freq_purch + lastyear_samemonth_purchase + ratio_purchases + items_added + items_browsed,
              decay = 0.05,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nnmod7 <- nnet(data = train.data7, purchase_in_target_window ~ recency_purch + freq_purch + lastyear_samemonth_purchase + ratio_purchases + items_added + items_browsed,
              decay = 0.05,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nnmod8 <- nnet(data = train.data8, purchase_in_target_window ~ recency_purch + freq_purch + lastyear_samemonth_purchase + ratio_purchases + items_added + items_browsed,
              decay = 0.05,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nnmod9 <- nnet(data = train.data9, purchase_in_target_window ~ recency_purch + freq_purch + lastyear_samemonth_purchase + ratio_purchases + items_added + items_browsed,
              decay = 0.05,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nnmod10 <- nnet(data = train.data10, purchase_in_target_window ~ recency_purch + freq_purch + lastyear_samemonth_purchase + ratio_purchases + items_added + items_browsed,
              decay = 0.05,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)



validation.data$nnpred1 <- predict(nnmod1,newdata = validation.data,type = "raw")
validation.data$nnpred2 <- predict(nnmod2,newdata = validation.data,type = "raw")
validation.data$nnpred3 <- predict(nnmod3,newdata = validation.data,type = "raw")
validation.data$nnpred4 <- predict(nnmod4,newdata = validation.data,type = "raw")
validation.data$nnpred5 <- predict(nnmod5,newdata = validation.data,type = "raw")
validation.data$nnpred6 <- predict(nnmod6,newdata = validation.data,type = "raw")
validation.data$nnpred7 <- predict(nnmod7,newdata = validation.data,type = "raw")
validation.data$nnpred8 <- predict(nnmod8,newdata = validation.data,type = "raw")
validation.data$nnpred9 <- predict(nnmod9,newdata = validation.data,type = "raw")
validation.data$nnpred10 <- predict(nnmod10,newdata = validation.data,type = "raw")

validation.data$mediannnpred <- apply(validation.data[,13:22],1,median)
roc(response = validation.data$purchase_in_target_window, predictor = validation.data$mediannnpred) #0.7968


library(nnet)
nn3monthsmod <- nnet(data = train.data, purchase_in_3month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases + items_added + items_browsed,
              decay = 0.05,MaxNWts = 10000,maxit = 1000,size = 10,trace = T)

nn3mmod1 <- nnet(data = train.data1, purchase_in_3month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases + items_added + items_browsed,
               decay = 0.05,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nn3mmod2 <- nnet(data = train.data2, purchase_in_3month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases + items_added + items_browsed,
               decay = 0.05,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nn3mmod3 <- nnet(data = train.data3, purchase_in_3month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases + items_added + items_browsed,
               decay = 0.05,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nn3mmod4 <- nnet(data = train.data4, purchase_in_3month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases + items_added + items_browsed,
               decay = 0.05,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nn3mmod5 <- nnet(data = train.data5, purchase_in_3month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases + items_added + items_browsed,
               decay = 0.05,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nn3mmod6 <- nnet(data = train.data6, purchase_in_3month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases + items_added + items_browsed,
               decay = 0.05,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nn3mmod7 <- nnet(data = train.data7, purchase_in_3month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases + items_added + items_browsed,
               decay = 0.05,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nn3mmod8 <- nnet(data = train.data8, purchase_in_3month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases + items_added + items_browsed,
               decay = 0.05,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nn3mmod9 <- nnet(data = train.data9, purchase_in_3month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases + items_added + items_browsed,
               decay = 0.05,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nn3mmod10 <- nnet(data = train.data10, purchase_in_3month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases + items_added + items_browsed,
                decay = 0.05,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)


validation.data$nn3mpred1 <- predict(nn3mmod1,newdata = validation.data,type = "raw")
validation.data$nn3mpred2 <- predict(nn3mmod2,newdata = validation.data,type = "raw")
validation.data$nn3mpred3 <- predict(nn3mmod3,newdata = validation.data,type = "raw")
validation.data$nn3mpred4 <- predict(nn3mmod4,newdata = validation.data,type = "raw")
validation.data$nn3mpred5 <- predict(nn3mmod5,newdata = validation.data,type = "raw")
validation.data$nn3mpred6 <- predict(nn3mmod6,newdata = validation.data,type = "raw")
validation.data$nn3mpred7 <- predict(nn3mmod7,newdata = validation.data,type = "raw")
validation.data$nn3mpred8 <- predict(nn3mmod8,newdata = validation.data,type = "raw")
validation.data$nn3mpred9 <- predict(nn3mmod9,newdata = validation.data,type = "raw")
validation.data$nn3mpred10 <- predict(nn3mmod10,newdata = validation.data,type = "raw")


validation.data$mediannn3mpred <- apply(validation.data[,24:33],1,median)
roc(response = validation.data$purchase_in_3month_target_window, predictor = validation.data$mediannn3mpred) #0.783


