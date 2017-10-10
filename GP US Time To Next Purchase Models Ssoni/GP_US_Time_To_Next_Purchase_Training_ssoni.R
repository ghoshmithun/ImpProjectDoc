#initialise libraries required
library(dplyr);library(gbm);library(readr);library(pROC);library(readr)

#import the dataset 
data<-read.table('C:/Users/ssoni/Documents/Gap/Prob Next Purchase 1 Month/gp_prob_purch_1month_export_training1',sep='|', header=FALSE)

#insert headers
names(data) <- c("masterkey",
                 "total_orders_placed",
                 "active_period",
                 "recency_purch",
                 "freq_purch",
                 "ratio_purchases",
                 "items_browsed",
                 "items_added",
                 "count_distinct_divisions",
                 "discount_percentage",
                 "total_net_sales",
                 "lastyear_samemonth_purchase",
                 "purchase_in_target_window",
                 "lastyear_same3months_purchase",
                 "purchase_in_3month_target_window",
                 "purchase_in_6month_target_window",
                 "purchase_in_12month_target_window")






# Cap a few variables to 99.5%ile
data$total_orders_placed <- ifelse(data$total_orders_placed > 33, 33, data$total_orders_placed)                 
data$items_browsed <- ifelse(data$items_browsed > 125,125,data$items_browsed)
data$items_added <- ifelse(data$items_added > 55,55,data$items_added )
data$ratio_purchases <- ifelse(data$ratio_purchases > 10,10,data$ratio_purchases)
data$total_net_sales <- ifelse(data$total_net_sales > 100000,100000,data$total_net_sales)

# Recalculate the frequency
data$freq_purch <- data$total_orders_placed / data$active_period
data$freq_purch <- ifelse(data$freq_purch > 5,5,data$freq_purch)


# Create train and validations dataset

train.data1 <- sample_n(data,size=90000)
train.data2 <- sample_n(data,size=90000)
train.data3 <- sample_n(data,size=90000)
train.data4 <- sample_n(data,size=90000)
train.data5 <- sample_n(data,size=90000)
train.data6 <- sample_n(data,size=90000)
train.data7 <- sample_n(data,size=90000)
train.data8 <- sample_n(data,size=90000)
train.data9 <- sample_n(data,size=90000)
train.data10 <- sample_n(data,size=90000)


#test <- inner_join(train.data14,validation.data)

validation.data <- sample_n(data,size = 150000)

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

# Try the 6 months logistic model
glm6monthmod <- glm(data = train.data10, purchase_in_6month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage , family = "binomial")
summary(glm6monthmod)

validation.data$sixmonthprob <- predict(glm6monthmod,newdata = validation.data,type="response")
roc(response = validation.data$purchase_in_6month_target_window, predictor = validation.data$sixmonthprob) #0.7588


# Try the 12 months logistic model
glm12monthmod <- glm(data = train.data10, purchase_in_12month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage , family = "binomial")
summary(glm12monthmod)

validation.data$twelvemonthprob <- predict(glm12monthmod,newdata = validation.data,type="response")
roc(response = validation.data$purchase_in_12month_target_window, predictor = validation.data$twelvemonthprob) #0.7588


validation.data.subset <- validation.data %>% select(purchase_in_target_window,purchase_in_3month_target_window,purchase_in_6month_target_window,purchase_in_12month_target_window,medianxgb1mpred,medianxgb3mpred,medianxgb6mpred,medianxgb12mpred)
saveRDS(validation.data.subset,file="GP_Prob_Purchase_Validation_Data_Subset.rds")

library(xgboost)

# 1month
xgb1mmod1a <- xgboost(data = data.matrix(train.data1[,c(4,5,6,7,8,9,10,11,12)]),label = train.data1$purchase_in_target_window,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb1mmod1b <- xgboost(data = data.matrix(train.data1[,c(4,5,6,7,8,9,10,11,12)]),label = train.data1$purchase_in_target_window,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb1mmod1c <- xgboost(data = data.matrix(train.data1[,c(4,5,6,7,8,9,10,11,12)]),label = train.data1$purchase_in_target_window,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))

xgb1mmod2a <- xgboost(data = data.matrix(train.data2[,c(4,5,6,7,8,9,10,11,12)]),label = train.data2$purchase_in_target_window,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb1mmod2b <- xgboost(data = data.matrix(train.data2[,c(4,5,6,7,8,9,10,11,12)]),label = train.data2$purchase_in_target_window,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb1mmod2c <- xgboost(data = data.matrix(train.data2[,c(4,5,6,7,8,9,10,11,12)]),label = train.data2$purchase_in_target_window,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))

xgb1mmod3a <- xgboost(data = data.matrix(train.data3[,c(4,5,6,7,8,9,10,11,12)]),label = train.data3$purchase_in_target_window,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb1mmod3b <- xgboost(data = data.matrix(train.data3[,c(4,5,6,7,8,9,10,11,12)]),label = train.data3$purchase_in_target_window,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb1mmod3c <- xgboost(data = data.matrix(train.data3[,c(4,5,6,7,8,9,10,11,12)]),label = train.data3$purchase_in_target_window,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))

xgb1mmod4a <- xgboost(data = data.matrix(train.data4[,c(4,5,6,7,8,9,10,11,12)]),label = train.data4$purchase_in_target_window,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb1mmod4b <- xgboost(data = data.matrix(train.data4[,c(4,5,6,7,8,9,10,11,12)]),label = train.data4$purchase_in_target_window,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb1mmod4c <- xgboost(data = data.matrix(train.data4[,c(4,5,6,7,8,9,10,11,12)]),label = train.data4$purchase_in_target_window,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))

xgb1mmod5a <- xgboost(data = data.matrix(train.data5[,c(4,5,6,7,8,9,10,11,12)]),label = train.data5$purchase_in_target_window,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb1mmod5b <- xgboost(data = data.matrix(train.data5[,c(4,5,6,7,8,9,10,11,12)]),label = train.data5$purchase_in_target_window,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb1mmod5c <- xgboost(data = data.matrix(train.data5[,c(4,5,6,7,8,9,10,11,12)]),label = train.data5$purchase_in_target_window,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))

xgb1mmod6a <- xgboost(data = data.matrix(train.data6[,c(4,5,6,7,8,9,10,11,12)]),label = train.data6$purchase_in_target_window,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb1mmod6b <- xgboost(data = data.matrix(train.data6[,c(4,5,6,7,8,9,10,11,12)]),label = train.data6$purchase_in_target_window,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb1mmod6c <- xgboost(data = data.matrix(train.data6[,c(4,5,6,7,8,9,10,11,12)]),label = train.data6$purchase_in_target_window,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))

xgb1mmod7a <- xgboost(data = data.matrix(train.data7[,c(4,5,6,7,8,9,10,11,12)]),label = train.data7$purchase_in_target_window,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb1mmod7b <- xgboost(data = data.matrix(train.data7[,c(4,5,6,7,8,9,10,11,12)]),label = train.data7$purchase_in_target_window,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb1mmod7c <- xgboost(data = data.matrix(train.data7[,c(4,5,6,7,8,9,10,11,12)]),label = train.data7$purchase_in_target_window,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))

xgb1mmod8a <- xgboost(data = data.matrix(train.data8[,c(4,5,6,7,8,9,10,11,12)]),label = train.data8$purchase_in_target_window,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb1mmod8b <- xgboost(data = data.matrix(train.data8[,c(4,5,6,7,8,9,10,11,12)]),label = train.data8$purchase_in_target_window,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb1mmod8c <- xgboost(data = data.matrix(train.data8[,c(4,5,6,7,8,9,10,11,12)]),label = train.data8$purchase_in_target_window,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))

xgb1mmod9a <- xgboost(data = data.matrix(train.data9[,c(4,5,6,7,8,9,10,11,12)]),label = train.data9$purchase_in_target_window,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb1mmod9b <- xgboost(data = data.matrix(train.data9[,c(4,5,6,7,8,9,10,11,12)]),label = train.data9$purchase_in_target_window,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb1mmod9c <- xgboost(data = data.matrix(train.data9[,c(4,5,6,7,8,9,10,11,12)]),label = train.data9$purchase_in_target_window,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))

xgb1mmod10a <- xgboost(data = data.matrix(train.data10[,c(4,5,6,7,8,9,10,11,12)]),label = train.data10$purchase_in_target_window,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb1mmod10b <- xgboost(data = data.matrix(train.data10[,c(4,5,6,7,8,9,10,11,12)]),label = train.data10$purchase_in_target_window,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb1mmod10c <- xgboost(data = data.matrix(train.data10[,c(4,5,6,7,8,9,10,11,12)]),label = train.data10$purchase_in_target_window,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))


validation.data$xgb1mpred1a <- predict(xgb1mmod1a,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,12)]))
validation.data$xgb1mpred1b <- predict(xgb1mmod1b,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,12)]))
validation.data$xgb1mpred1c <- predict(xgb1mmod1c,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,12)]))

validation.data$xgb1mpred2a <- predict(xgb1mmod2a,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,12)]))
validation.data$xgb1mpred2b <- predict(xgb1mmod2b,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,12)]))
validation.data$xgb1mpred2c <- predict(xgb1mmod2c,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,12)]))

validation.data$xgb1mpred3a <- predict(xgb1mmod3a,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,12)]))
validation.data$xgb1mpred3b <- predict(xgb1mmod3b,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,12)]))
validation.data$xgb1mpred3c <- predict(xgb1mmod3c,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,12)]))

validation.data$xgb1mpred4a <- predict(xgb1mmod4a,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,12)]))
validation.data$xgb1mpred4b <- predict(xgb1mmod4b,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,12)]))
validation.data$xgb1mpred4c <- predict(xgb1mmod4c,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,12)]))

validation.data$xgb1mpred5a <- predict(xgb1mmod5a,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,12)]))
validation.data$xgb1mpred5b <- predict(xgb1mmod5b,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,12)]))
validation.data$xgb1mpred5c <- predict(xgb1mmod5c,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,12)]))

validation.data$xgb1mpred6a <- predict(xgb1mmod6a,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,12)]))
validation.data$xgb1mpred6b <- predict(xgb1mmod6b,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,12)]))
validation.data$xgb1mpred6c <- predict(xgb1mmod6c,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,12)]))

validation.data$xgb1mpred7a <- predict(xgb1mmod7a,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,12)]))
validation.data$xgb1mpred7b <- predict(xgb1mmod7b,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,12)]))
validation.data$xgb1mpred7c <- predict(xgb1mmod7c,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,12)]))

validation.data$xgb1mpred8a <- predict(xgb1mmod8a,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,12)]))
validation.data$xgb1mpred8b <- predict(xgb1mmod8b,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,12)]))
validation.data$xgb1mpred8c <- predict(xgb1mmod8c,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,12)]))

validation.data$xgb1mpred9a <- predict(xgb1mmod9a,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,12)]))
validation.data$xgb1mpred9b <- predict(xgb1mmod9b,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,12)]))
validation.data$xgb1mpred9c <- predict(xgb1mmod9c,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,12)]))

validation.data$xgb1mpred10a <- predict(xgb1mmod10a,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,12)]))
validation.data$xgb1mpred10b <- predict(xgb1mmod10b,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,12)]))
validation.data$xgb1mpred10c <- predict(xgb1mmod10c,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,12)]))

validation.data$medianxgb1mpred <- apply(validation.data[,18:47],1,median)
roc(response = validation.data$purchase_in_target_window, predictor = validation.data$medianxgb1mpred,plot=TRUE) #0.7606

# 3 month
xgb3mmod1a <- xgboost(data = data.matrix(train.data1[,c(4,5,6,7,8,9,10,11,14)]),label = train.data1$purchase_in_3month_target_window,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb3mmod1b <- xgboost(data = data.matrix(train.data1[,c(4,5,6,7,8,9,10,11,14)]),label = train.data1$purchase_in_3month_target_window,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb3mmod1c <- xgboost(data = data.matrix(train.data1[,c(4,5,6,7,8,9,10,11,14)]),label = train.data1$purchase_in_3month_target_window,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))

xgb3mmod2a <- xgboost(data = data.matrix(train.data2[,c(4,5,6,7,8,9,10,11,14)]),label = train.data2$purchase_in_3month_target_window,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb3mmod2b <- xgboost(data = data.matrix(train.data2[,c(4,5,6,7,8,9,10,11,14)]),label = train.data2$purchase_in_3month_target_window,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb3mmod2c <- xgboost(data = data.matrix(train.data2[,c(4,5,6,7,8,9,10,11,14)]),label = train.data2$purchase_in_3month_target_window,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))

xgb3mmod3a <- xgboost(data = data.matrix(train.data3[,c(4,5,6,7,8,9,10,11,14)]),label = train.data3$purchase_in_3month_target_window,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb3mmod3b <- xgboost(data = data.matrix(train.data3[,c(4,5,6,7,8,9,10,11,14)]),label = train.data3$purchase_in_3month_target_window,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb3mmod3c <- xgboost(data = data.matrix(train.data3[,c(4,5,6,7,8,9,10,11,14)]),label = train.data3$purchase_in_3month_target_window,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))

xgb3mmod4a <- xgboost(data = data.matrix(train.data4[,c(4,5,6,7,8,9,10,11,14)]),label = train.data4$purchase_in_3month_target_window,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb3mmod4b <- xgboost(data = data.matrix(train.data4[,c(4,5,6,7,8,9,10,11,14)]),label = train.data4$purchase_in_3month_target_window,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb3mmod4c <- xgboost(data = data.matrix(train.data4[,c(4,5,6,7,8,9,10,11,14)]),label = train.data4$purchase_in_3month_target_window,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))

xgb3mmod5a <- xgboost(data = data.matrix(train.data5[,c(4,5,6,7,8,9,10,11,14)]),label = train.data5$purchase_in_3month_target_window,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb3mmod5b <- xgboost(data = data.matrix(train.data5[,c(4,5,6,7,8,9,10,11,14)]),label = train.data5$purchase_in_3month_target_window,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb3mmod5c <- xgboost(data = data.matrix(train.data5[,c(4,5,6,7,8,9,10,11,14)]),label = train.data5$purchase_in_3month_target_window,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))

xgb3mmod6a <- xgboost(data = data.matrix(train.data6[,c(4,5,6,7,8,9,10,11,14)]),label = train.data6$purchase_in_3month_target_window,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb3mmod6b <- xgboost(data = data.matrix(train.data6[,c(4,5,6,7,8,9,10,11,14)]),label = train.data6$purchase_in_3month_target_window,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb3mmod6c <- xgboost(data = data.matrix(train.data6[,c(4,5,6,7,8,9,10,11,14)]),label = train.data6$purchase_in_3month_target_window,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))

xgb3mmod7a <- xgboost(data = data.matrix(train.data7[,c(4,5,6,7,8,9,10,11,14)]),label = train.data7$purchase_in_3month_target_window,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb3mmod7b <- xgboost(data = data.matrix(train.data7[,c(4,5,6,7,8,9,10,11,14)]),label = train.data7$purchase_in_3month_target_window,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb3mmod7c <- xgboost(data = data.matrix(train.data7[,c(4,5,6,7,8,9,10,11,14)]),label = train.data7$purchase_in_3month_target_window,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))

xgb3mmod8a <- xgboost(data = data.matrix(train.data8[,c(4,5,6,7,8,9,10,11,14)]),label = train.data8$purchase_in_3month_target_window,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb3mmod8b <- xgboost(data = data.matrix(train.data8[,c(4,5,6,7,8,9,10,11,14)]),label = train.data8$purchase_in_3month_target_window,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb3mmod8c <- xgboost(data = data.matrix(train.data8[,c(4,5,6,7,8,9,10,11,14)]),label = train.data8$purchase_in_3month_target_window,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))

xgb3mmod9a <- xgboost(data = data.matrix(train.data9[,c(4,5,6,7,8,9,10,11,14)]),label = train.data9$purchase_in_3month_target_window,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb3mmod9b <- xgboost(data = data.matrix(train.data9[,c(4,5,6,7,8,9,10,11,14)]),label = train.data9$purchase_in_3month_target_window,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb3mmod9c <- xgboost(data = data.matrix(train.data9[,c(4,5,6,7,8,9,10,11,14)]),label = train.data9$purchase_in_3month_target_window,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))

xgb3mmod10a <- xgboost(data = data.matrix(train.data10[,c(4,5,6,7,8,9,10,11,14)]),label = train.data10$purchase_in_3month_target_window,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb3mmod10b <- xgboost(data = data.matrix(train.data10[,c(4,5,6,7,8,9,10,11,14)]),label = train.data10$purchase_in_3month_target_window,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb3mmod10c <- xgboost(data = data.matrix(train.data10[,c(4,5,6,7,8,9,10,11,14)]),label = train.data10$purchase_in_3month_target_window,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))


validation.data$xgb3mpred1a <- predict(xgb1mmod1a,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb3mpred1b <- predict(xgb1mmod1b,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb3mpred1c <- predict(xgb1mmod1c,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))

validation.data$xgb3mpred2a <- predict(xgb1mmod2a,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb3mpred2b <- predict(xgb1mmod2b,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb3mpred2c <- predict(xgb1mmod2c,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))

validation.data$xgb3mpred3a <- predict(xgb1mmod3a,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb3mpred3b <- predict(xgb1mmod3b,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb3mpred3c <- predict(xgb1mmod3c,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))

validation.data$xgb3mpred4a <- predict(xgb1mmod4a,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb3mpred4b <- predict(xgb1mmod4b,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb3mpred4c <- predict(xgb1mmod4c,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))

validation.data$xgb3mpred5a <- predict(xgb1mmod5a,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb3mpred5b <- predict(xgb1mmod5b,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb3mpred5c <- predict(xgb1mmod5c,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))

validation.data$xgb3mpred6a <- predict(xgb1mmod6a,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb3mpred6b <- predict(xgb1mmod6b,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb3mpred6c <- predict(xgb1mmod6c,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))

validation.data$xgb3mpred7a <- predict(xgb1mmod7a,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb3mpred7b <- predict(xgb1mmod7b,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb3mpred7c <- predict(xgb1mmod7c,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))

validation.data$xgb3mpred8a <- predict(xgb1mmod8a,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb3mpred8b <- predict(xgb1mmod8b,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb3mpred8c <- predict(xgb1mmod8c,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))

validation.data$xgb3mpred9a <- predict(xgb1mmod9a,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb3mpred9b <- predict(xgb1mmod9b,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb3mpred9c <- predict(xgb1mmod9c,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))

validation.data$xgb3mpred10a <- predict(xgb1mmod10a,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb3mpred10b <- predict(xgb1mmod10b,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb3mpred10c <- predict(xgb1mmod10c,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))

validation.data$medianxgb3mpred <- apply(validation.data[,49:78],1,median)
roc(response = validation.data$purchase_in_3month_target_window, predictor = validation.data$medianxgb3mpred,plot=TRUE) #0.7606



# 6months
xgb6mmod1a <- xgboost(data = data.matrix(train.data1[,c(4,5,6,7,8,9,10,11,14)]),label = train.data1$purchase_in_6month_target_window,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb6mmod1b <- xgboost(data = data.matrix(train.data1[,c(4,5,6,7,8,9,10,11,14)]),label = train.data1$purchase_in_6month_target_window,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb6mmod1c <- xgboost(data = data.matrix(train.data1[,c(4,5,6,7,8,9,10,11,14)]),label = train.data1$purchase_in_6month_target_window,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))

xgb6mmod2a <- xgboost(data = data.matrix(train.data2[,c(4,5,6,7,8,9,10,11,14)]),label = train.data2$purchase_in_6month_target_window,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb6mmod2b <- xgboost(data = data.matrix(train.data2[,c(4,5,6,7,8,9,10,11,14)]),label = train.data2$purchase_in_6month_target_window,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb6mmod2c <- xgboost(data = data.matrix(train.data2[,c(4,5,6,7,8,9,10,11,14)]),label = train.data2$purchase_in_6month_target_window,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))

xgb6mmod3a <- xgboost(data = data.matrix(train.data3[,c(4,5,6,7,8,9,10,11,14)]),label = train.data3$purchase_in_6month_target_window,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb6mmod3b <- xgboost(data = data.matrix(train.data3[,c(4,5,6,7,8,9,10,11,14)]),label = train.data3$purchase_in_6month_target_window,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb6mmod3c <- xgboost(data = data.matrix(train.data3[,c(4,5,6,7,8,9,10,11,14)]),label = train.data3$purchase_in_6month_target_window,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))

xgb6mmod4a <- xgboost(data = data.matrix(train.data4[,c(4,5,6,7,8,9,10,11,14)]),label = train.data4$purchase_in_6month_target_window,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb6mmod4b <- xgboost(data = data.matrix(train.data4[,c(4,5,6,7,8,9,10,11,14)]),label = train.data4$purchase_in_6month_target_window,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb6mmod4c <- xgboost(data = data.matrix(train.data4[,c(4,5,6,7,8,9,10,11,14)]),label = train.data4$purchase_in_6month_target_window,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))

xgb6mmod5a <- xgboost(data = data.matrix(train.data5[,c(4,5,6,7,8,9,10,11,14)]),label = train.data5$purchase_in_6month_target_window,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb6mmod5b <- xgboost(data = data.matrix(train.data5[,c(4,5,6,7,8,9,10,11,14)]),label = train.data5$purchase_in_6month_target_window,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb6mmod5c <- xgboost(data = data.matrix(train.data5[,c(4,5,6,7,8,9,10,11,14)]),label = train.data5$purchase_in_6month_target_window,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))

xgb6mmod6a <- xgboost(data = data.matrix(train.data6[,c(4,5,6,7,8,9,10,11,14)]),label = train.data6$purchase_in_6month_target_window,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb6mmod6b <- xgboost(data = data.matrix(train.data6[,c(4,5,6,7,8,9,10,11,14)]),label = train.data6$purchase_in_6month_target_window,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb6mmod6c <- xgboost(data = data.matrix(train.data6[,c(4,5,6,7,8,9,10,11,14)]),label = train.data6$purchase_in_6month_target_window,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))

xgb6mmod7a <- xgboost(data = data.matrix(train.data7[,c(4,5,6,7,8,9,10,11,14)]),label = train.data7$purchase_in_6month_target_window,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb6mmod7b <- xgboost(data = data.matrix(train.data7[,c(4,5,6,7,8,9,10,11,14)]),label = train.data7$purchase_in_6month_target_window,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb6mmod7c <- xgboost(data = data.matrix(train.data7[,c(4,5,6,7,8,9,10,11,14)]),label = train.data7$purchase_in_6month_target_window,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))

xgb6mmod8a <- xgboost(data = data.matrix(train.data8[,c(4,5,6,7,8,9,10,11,14)]),label = train.data8$purchase_in_6month_target_window,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb6mmod8b <- xgboost(data = data.matrix(train.data8[,c(4,5,6,7,8,9,10,11,14)]),label = train.data8$purchase_in_6month_target_window,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb6mmod8c <- xgboost(data = data.matrix(train.data8[,c(4,5,6,7,8,9,10,11,14)]),label = train.data8$purchase_in_6month_target_window,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))

xgb6mmod9a <- xgboost(data = data.matrix(train.data9[,c(4,5,6,7,8,9,10,11,14)]),label = train.data9$purchase_in_6month_target_window,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb6mmod9b <- xgboost(data = data.matrix(train.data9[,c(4,5,6,7,8,9,10,11,14)]),label = train.data9$purchase_in_6month_target_window,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb6mmod9c <- xgboost(data = data.matrix(train.data9[,c(4,5,6,7,8,9,10,11,14)]),label = train.data9$purchase_in_6month_target_window,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))

xgb6mmod10a <- xgboost(data = data.matrix(train.data10[,c(4,5,6,7,8,9,10,11,14)]),label = train.data10$purchase_in_6month_target_window,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb6mmod10b <- xgboost(data = data.matrix(train.data10[,c(4,5,6,7,8,9,10,11,14)]),label = train.data10$purchase_in_6month_target_window,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb6mmod10c <- xgboost(data = data.matrix(train.data10[,c(4,5,6,7,8,9,10,11,14)]),label = train.data10$purchase_in_6month_target_window,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))


validation.data$xgb6mpred1a <- predict(xgb6mmod1a,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb6mpred1b <- predict(xgb6mmod1b,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb6mpred1c <- predict(xgb6mmod1c,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))

validation.data$xgb6mpred2a <- predict(xgb6mmod2a,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb6mpred2b <- predict(xgb6mmod2b,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb6mpred2c <- predict(xgb6mmod2c,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))

validation.data$xgb6mpred3a <- predict(xgb6mmod3a,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb6mpred3b <- predict(xgb6mmod3b,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb6mpred3c <- predict(xgb6mmod3c,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))

validation.data$xgb6mpred4a <- predict(xgb6mmod4a,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb6mpred4b <- predict(xgb6mmod4b,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb6mpred4c <- predict(xgb6mmod4c,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))

validation.data$xgb6mpred5a <- predict(xgb6mmod5a,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb6mpred5b <- predict(xgb6mmod5b,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb6mpred5c <- predict(xgb6mmod5c,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))

validation.data$xgb6mpred6a <- predict(xgb6mmod6a,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb6mpred6b <- predict(xgb6mmod6b,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb6mpred6c <- predict(xgb6mmod6c,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))

validation.data$xgb6mpred7a <- predict(xgb6mmod7a,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb6mpred7b <- predict(xgb6mmod7b,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb6mpred7c <- predict(xgb6mmod7c,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))

validation.data$xgb6mpred8a <- predict(xgb6mmod8a,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb6mpred8b <- predict(xgb6mmod8b,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb6mpred8c <- predict(xgb6mmod8c,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))

validation.data$xgb6mpred9a <- predict(xgb6mmod9a,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb6mpred9b <- predict(xgb6mmod9b,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb6mpred9c <- predict(xgb6mmod9c,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))

validation.data$xgb6mpred10a <- predict(xgb6mmod10a,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb6mpred10b <- predict(xgb6mmod10b,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb6mpred10c <- predict(xgb6mmod10c,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))

validation.data$medianxgb6mpred <- apply(validation.data[,80:109],1,median)
roc(response = validation.data$purchase_in_6month_target_window, predictor = validation.data$medianxgb6mpred,plot=TRUE) #0.7606

# 12 months
xgb12mmod1a <- xgboost(data = data.matrix(train.data1[,c(4,5,6,7,8,9,10,11,14)]),label = train.data1$purchase_in_12month_target_window,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb12mmod1b <- xgboost(data = data.matrix(train.data1[,c(4,5,6,7,8,9,10,11,14)]),label = train.data1$purchase_in_12month_target_window,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb12mmod1c <- xgboost(data = data.matrix(train.data1[,c(4,5,6,7,8,9,10,11,14)]),label = train.data1$purchase_in_12month_target_window,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))

xgb12mmod2a <- xgboost(data = data.matrix(train.data2[,c(4,5,6,7,8,9,10,11,14)]),label = train.data2$purchase_in_12month_target_window,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb12mmod2b <- xgboost(data = data.matrix(train.data2[,c(4,5,6,7,8,9,10,11,14)]),label = train.data2$purchase_in_12month_target_window,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb12mmod2c <- xgboost(data = data.matrix(train.data2[,c(4,5,6,7,8,9,10,11,14)]),label = train.data2$purchase_in_12month_target_window,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))

xgb12mmod3a <- xgboost(data = data.matrix(train.data3[,c(4,5,6,7,8,9,10,11,14)]),label = train.data3$purchase_in_12month_target_window,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb12mmod3b <- xgboost(data = data.matrix(train.data3[,c(4,5,6,7,8,9,10,11,14)]),label = train.data3$purchase_in_12month_target_window,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb12mmod3c <- xgboost(data = data.matrix(train.data3[,c(4,5,6,7,8,9,10,11,14)]),label = train.data3$purchase_in_12month_target_window,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))

xgb12mmod4a <- xgboost(data = data.matrix(train.data4[,c(4,5,6,7,8,9,10,11,14)]),label = train.data4$purchase_in_12month_target_window,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb12mmod4b <- xgboost(data = data.matrix(train.data4[,c(4,5,6,7,8,9,10,11,14)]),label = train.data4$purchase_in_12month_target_window,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb12mmod4c <- xgboost(data = data.matrix(train.data4[,c(4,5,6,7,8,9,10,11,14)]),label = train.data4$purchase_in_12month_target_window,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))

xgb12mmod5a <- xgboost(data = data.matrix(train.data5[,c(4,5,6,7,8,9,10,11,14)]),label = train.data5$purchase_in_12month_target_window,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb12mmod5b <- xgboost(data = data.matrix(train.data5[,c(4,5,6,7,8,9,10,11,14)]),label = train.data5$purchase_in_12month_target_window,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb12mmod5c <- xgboost(data = data.matrix(train.data5[,c(4,5,6,7,8,9,10,11,14)]),label = train.data5$purchase_in_12month_target_window,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))

xgb12mmod6a <- xgboost(data = data.matrix(train.data6[,c(4,5,6,7,8,9,10,11,14)]),label = train.data6$purchase_in_12month_target_window,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb12mmod6b <- xgboost(data = data.matrix(train.data6[,c(4,5,6,7,8,9,10,11,14)]),label = train.data6$purchase_in_12month_target_window,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb12mmod6c <- xgboost(data = data.matrix(train.data6[,c(4,5,6,7,8,9,10,11,14)]),label = train.data6$purchase_in_12month_target_window,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))

xgb12mmod7a <- xgboost(data = data.matrix(train.data7[,c(4,5,6,7,8,9,10,11,14)]),label = train.data7$purchase_in_12month_target_window,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb12mmod7b <- xgboost(data = data.matrix(train.data7[,c(4,5,6,7,8,9,10,11,14)]),label = train.data7$purchase_in_12month_target_window,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb12mmod7c <- xgboost(data = data.matrix(train.data7[,c(4,5,6,7,8,9,10,11,14)]),label = train.data7$purchase_in_12month_target_window,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))

xgb12mmod8a <- xgboost(data = data.matrix(train.data8[,c(4,5,6,7,8,9,10,11,14)]),label = train.data8$purchase_in_12month_target_window,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb12mmod8b <- xgboost(data = data.matrix(train.data8[,c(4,5,6,7,8,9,10,11,14)]),label = train.data8$purchase_in_12month_target_window,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb12mmod8c <- xgboost(data = data.matrix(train.data8[,c(4,5,6,7,8,9,10,11,14)]),label = train.data8$purchase_in_12month_target_window,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))

xgb12mmod9a <- xgboost(data = data.matrix(train.data9[,c(4,5,6,7,8,9,10,11,14)]),label = train.data9$purchase_in_12month_target_window,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb12mmod9b <- xgboost(data = data.matrix(train.data9[,c(4,5,6,7,8,9,10,11,14)]),label = train.data9$purchase_in_12month_target_window,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb12mmod9c <- xgboost(data = data.matrix(train.data9[,c(4,5,6,7,8,9,10,11,14)]),label = train.data9$purchase_in_12month_target_window,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))

xgb12mmod10a <- xgboost(data = data.matrix(train.data10[,c(4,5,6,7,8,9,10,11,14)]),label = train.data10$purchase_in_12month_target_window,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb12mmod10b <- xgboost(data = data.matrix(train.data10[,c(4,5,6,7,8,9,10,11,14)]),label = train.data10$purchase_in_12month_target_window,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgb12mmod10c <- xgboost(data = data.matrix(train.data10[,c(4,5,6,7,8,9,10,11,14)]),label = train.data10$purchase_in_12month_target_window,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))


validation.data$xgb12mpred1a <- predict(xgb12mmod1a,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb12mpred1b <- predict(xgb12mmod1b,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb12mpred1c <- predict(xgb12mmod1c,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))

validation.data$xgb12mpred2a <- predict(xgb12mmod2a,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb12mpred2b <- predict(xgb12mmod2b,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb12mpred2c <- predict(xgb12mmod2c,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))

validation.data$xgb12mpred3a <- predict(xgb12mmod3a,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb12mpred3b <- predict(xgb12mmod3b,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb12mpred3c <- predict(xgb12mmod3c,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))

validation.data$xgb12mpred4a <- predict(xgb12mmod4a,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb12mpred4b <- predict(xgb12mmod4b,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb12mpred4c <- predict(xgb12mmod4c,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))

validation.data$xgb12mpred5a <- predict(xgb12mmod5a,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb12mpred5b <- predict(xgb12mmod5b,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb12mpred5c <- predict(xgb12mmod5c,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))

validation.data$xgb12mpred6a <- predict(xgb12mmod6a,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb12mpred6b <- predict(xgb12mmod6b,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb12mpred6c <- predict(xgb12mmod6c,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))

validation.data$xgb12mpred7a <- predict(xgb12mmod7a,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb12mpred7b <- predict(xgb12mmod7b,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb12mpred7c <- predict(xgb12mmod7c,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))

validation.data$xgb12mpred8a <- predict(xgb12mmod8a,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb12mpred8b <- predict(xgb12mmod8b,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb12mpred8c <- predict(xgb12mmod8c,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))

validation.data$xgb12mpred9a <- predict(xgb12mmod9a,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb12mpred9b <- predict(xgb12mmod9b,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb12mpred9c <- predict(xgb12mmod9c,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))

validation.data$xgb12mpred10a <- predict(xgb12mmod10a,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb12mpred10b <- predict(xgb12mmod10b,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))
validation.data$xgb12mpred10c <- predict(xgb12mmod10c,data.matrix(validation.data[,c(4,5,6,7,8,9,10,11,14)]))

validation.data$medianxgb12mpred <- apply(validation.data[,111:140],1,median)
roc(response = validation.data$purchase_in_12month_target_window, predictor = validation.data$medianxgb12mpred,plot=TRUE) #0.7606


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


nnmod1 <- nnet(data = train.data1, purchase_in_target_window ~ recency_purch + freq_purch + lastyear_samemonth_purchase + ratio_purchases + items_added + items_browsed,
              decay = 0.01,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nnmod2 <- nnet(data = train.data2, purchase_in_target_window ~ recency_purch + freq_purch + lastyear_samemonth_purchase + ratio_purchases + items_added + items_browsed,
              decay = 0.01,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nnmod3 <- nnet(data = train.data3, purchase_in_target_window ~ recency_purch + freq_purch + lastyear_samemonth_purchase + ratio_purchases + items_added + items_browsed,
              decay = 0.01,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nnmod4 <- nnet(data = train.data4, purchase_in_target_window ~ recency_purch + freq_purch + lastyear_samemonth_purchase + ratio_purchases + items_added + items_browsed,
              decay = 0.01,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nnmod5 <- nnet(data = train.data5, purchase_in_target_window ~ recency_purch + freq_purch + lastyear_samemonth_purchase + ratio_purchases + items_added + items_browsed,
              decay = 0.01,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nnmod6 <- nnet(data = train.data6, purchase_in_target_window ~ recency_purch + freq_purch + lastyear_samemonth_purchase + ratio_purchases + items_added + items_browsed,
              decay = 0.01,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nnmod7 <- nnet(data = train.data7, purchase_in_target_window ~ recency_purch + freq_purch + lastyear_samemonth_purchase + ratio_purchases + items_added + items_browsed,
              decay = 0.01,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nnmod8 <- nnet(data = train.data8, purchase_in_target_window ~ recency_purch + freq_purch + lastyear_samemonth_purchase + ratio_purchases + items_added + items_browsed,
              decay = 0.01,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nnmod9 <- nnet(data = train.data9, purchase_in_target_window ~ recency_purch + freq_purch + lastyear_samemonth_purchase + ratio_purchases + items_added + items_browsed,
              decay = 0.01,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nnmod10 <- nnet(data = train.data10, purchase_in_target_window ~ recency_purch + freq_purch + lastyear_samemonth_purchase + ratio_purchases + items_added + items_browsed,
              decay = 0.01,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)



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

##### Try the 6 months model

nn6mmod1 <- nnet(data = train.data1, purchase_in_6month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                 decay = 0.01,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nn6mmod2 <- nnet(data = train.data2, purchase_in_6month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                 decay = 0.01,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nn6mmod3 <- nnet(data = train.data3, purchase_in_6month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                 decay = 0.01,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nn6mmod4 <- nnet(data = train.data4, purchase_in_6month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                 decay = 0.01,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nn6mmod5 <- nnet(data = train.data5, purchase_in_6month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                 decay = 0.01,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nn6mmod6 <- nnet(data = train.data6, purchase_in_6month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                 decay = 0.01,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nn6mmod7 <- nnet(data = train.data7, purchase_in_6month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                 decay = 0.01,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nn6mmod8 <- nnet(data = train.data8, purchase_in_6month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                 decay = 0.01,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nn6mmod9 <- nnet(data = train.data9, purchase_in_6month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                 decay = 0.01,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nn6mmod10 <- nnet(data = train.data10, purchase_in_6month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                  decay = 0.01,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)

nn6mmod111 <- nnet(data = train.data1, purchase_in_6month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage + items_added + total_net_sales,
                  decay = 0.01,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nn6mmod112 <- nnet(data = train.data1, purchase_in_6month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                  decay = 0.01,MaxNWts = 10000,maxit = 1000,size = 10,trace = T)
nn6mmod113 <- nnet(data = train.data1, purchase_in_6month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                  decay = 0.01,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nn6mmod114 <- nnet(data = train.data1, purchase_in_6month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                  decay = 0.01,MaxNWts = 10000,maxit = 1000,size = 20,trace = T)

nn6mmod121 <- nnet(data = train.data1, purchase_in_6month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                 decay = 0.05,MaxNWts = 10000,maxit = 1000,size = 5,trace = T)
nn6mmod122 <- nnet(data = train.data1, purchase_in_6month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                  decay = 0.05,MaxNWts = 10000,maxit = 1000,size = 10,trace = T)
nn6mmod123 <- nnet(data = train.data1, purchase_in_6month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                  decay = 0.05,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nn6mmod124 <- nnet(data = train.data1, purchase_in_6month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                  decay = 0.05,MaxNWts = 10000,maxit = 1000,size = 20,trace = T)
nn6mmod131 <- nnet(data = train.data1, purchase_in_6month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                  decay = 0.1,MaxNWts = 10000,maxit = 1000,size = 5,trace = T)
nn6mmod132 <- nnet(data = train.data1, purchase_in_6month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                  decay = 0.1,MaxNWts = 10000,maxit = 1000,size = 10,trace = T)
nn6mmod133 <- nnet(data = train.data1, purchase_in_6month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                  decay = 0.1,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nn6mmod134 <- nnet(data = train.data1, purchase_in_6month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                  decay = 0.1,MaxNWts = 10000,maxit = 1000,size = 20,trace = T)
nn6mmod141 <- nnet(data = train.data1, purchase_in_6month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                  decay = 0.5,MaxNWts = 10000,maxit = 1000,size = 5,trace = T)
nn6mmod142 <- nnet(data = train.data1, purchase_in_6month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                  decay = 0.5,MaxNWts = 10000,maxit = 1000,size = 10,trace = T)
nn6mmod143 <- nnet(data = train.data1, purchase_in_6month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                  decay = 0.5,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nn6mmod144 <- nnet(data = train.data1, purchase_in_6month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                  decay = 0.5,MaxNWts = 10000,maxit = 1000,size = 20,trace = T)

nn6mmod151 <- nnet(data = train.data11, purchase_in_6month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                   decay = 0.01,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nn6mmod152 <- nnet(data = train.data11, purchase_in_6month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                   decay = 0.05,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nn6mmod153 <- nnet(data = train.data11, purchase_in_6month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                   decay = 0.1,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nn6mmod154 <- nnet(data = train.data11, purchase_in_6month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                   decay = 0.5,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)


nn6mmod161 <- nnet(data = train.data12, purchase_in_6month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                   decay = 0.01,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nn6mmod162 <- nnet(data = train.data12, purchase_in_6month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                   decay = 0.05,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nn6mmod163 <- nnet(data = train.data12, purchase_in_6month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                   decay = 0.1,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nn6mmod164 <- nnet(data = train.data12, purchase_in_6month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                   decay = 0.5,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)

nn6mmod171 <- nnet(data = train.data13, purchase_in_6month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                   decay = 0.001,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nn6mmod172 <- nnet(data = train.data13, purchase_in_6month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                   decay = 0.005,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nn6mmod173 <- nnet(data = train.data13, purchase_in_6month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                   decay = 0.01,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nn6mmod174 <- nnet(data = train.data13, purchase_in_6month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                   decay = 0.05,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)


nn6mmod181 <- nnet(data = train.data14, purchase_in_6month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                   decay = 0.0001,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nn6mmod182 <- nnet(data = train.data14, purchase_in_6month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                   decay = 0.0005,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nn6mmod183 <- nnet(data = train.data14, purchase_in_6month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                   decay = 0.001,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nn6mmod184 <- nnet(data = train.data14, purchase_in_6month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                   decay = 0.005,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nn6mmod185 <- nnet(data = train.data14, purchase_in_6month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                   decay = 0.01,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)


nn6mmod191 <- nnet(data = train.data15, purchase_in_6month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                   decay = 0.01,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)



validation.data$nn6mpred111 <- predict(nn6mmod111,newdata = validation.data,type = "raw")
validation.data$nn6mpred112 <- predict(nn6mmod112,newdata = validation.data,type = "raw")
validation.data$nn6mpred113 <- predict(nn6mmod113,newdata = validation.data,type = "raw")
validation.data$nn6mpred114 <- predict(nn6mmod114,newdata = validation.data,type = "raw")

validation.data$nn6mpred121 <- predict(nn6mmod121,newdata = validation.data,type = "raw")
validation.data$nn6mpred122 <- predict(nn6mmod122,newdata = validation.data,type = "raw")
validation.data$nn6mpred123 <- predict(nn6mmod123,newdata = validation.data,type = "raw")
validation.data$nn6mpred124 <- predict(nn6mmod124,newdata = validation.data,type = "raw")

validation.data$nn6mpred131 <- predict(nn6mmod131,newdata = validation.data,type = "raw")
validation.data$nn6mpred132 <- predict(nn6mmod132,newdata = validation.data,type = "raw")
validation.data$nn6mpred133 <- predict(nn6mmod133,newdata = validation.data,type = "raw")
validation.data$nn6mpred134 <- predict(nn6mmod134,newdata = validation.data,type = "raw")

validation.data$nn6mpred141 <- predict(nn6mmod141,newdata = validation.data,type = "raw")
validation.data$nn6mpred142 <- predict(nn6mmod142,newdata = validation.data,type = "raw")
validation.data$nn6mpred143 <- predict(nn6mmod143,newdata = validation.data,type = "raw")
validation.data$nn6mpred144 <- predict(nn6mmod144,newdata = validation.data,type = "raw")

validation.data$nn6mpred151 <- predict(nn6mmod151,newdata = validation.data,type = "raw")
validation.data$nn6mpred152 <- predict(nn6mmod152,newdata = validation.data,type = "raw")
validation.data$nn6mpred153 <- predict(nn6mmod153,newdata = validation.data,type = "raw")
validation.data$nn6mpred154 <- predict(nn6mmod154,newdata = validation.data,type = "raw")

validation.data$nn6mpred161 <- predict(nn6mmod161,newdata = validation.data,type = "raw")
validation.data$nn6mpred162 <- predict(nn6mmod162,newdata = validation.data,type = "raw")
validation.data$nn6mpred163 <- predict(nn6mmod163,newdata = validation.data,type = "raw")
validation.data$nn6mpred164 <- predict(nn6mmod164,newdata = validation.data,type = "raw")

validation.data$nn6mpred171 <- predict(nn6mmod171,newdata = validation.data,type = "raw")
validation.data$nn6mpred172 <- predict(nn6mmod172,newdata = validation.data,type = "raw")
validation.data$nn6mpred173 <- predict(nn6mmod173,newdata = validation.data,type = "raw")
validation.data$nn6mpred174 <- predict(nn6mmod174,newdata = validation.data,type = "raw")

validation.data$nn6mpred181 <- predict(nn6mmod181,newdata = validation.data,type = "raw")
validation.data$nn6mpred182 <- predict(nn6mmod182,newdata = validation.data,type = "raw")
validation.data$nn6mpred183 <- predict(nn6mmod183,newdata = validation.data,type = "raw")
validation.data$nn6mpred184 <- predict(nn6mmod184,newdata = validation.data,type = "raw")
validation.data$nn6mpred185 <- predict(nn6mmod185,newdata = validation.data,type = "raw")


validation.data$nn6mpred191 <- predict(nn6mmod191,newdata = validation.data,type = "raw")



roc(response = validation.data$purchase_in_6month_target_window, predictor = validation.data$nn6mpred111) #0.783
roc(response = validation.data$purchase_in_6month_target_window, predictor = validation.data$nn6mpred112) #0.783
roc(response = validation.data$purchase_in_6month_target_window, predictor = validation.data$nn6mpred113) #0.783
roc(response = validation.data$purchase_in_6month_target_window, predictor = validation.data$nn6mpred114) #0.783

roc(response = validation.data$purchase_in_6month_target_window, predictor = validation.data$nn6mpred121) #0.783
roc(response = validation.data$purchase_in_6month_target_window, predictor = validation.data$nn6mpred122) #0.783
roc(response = validation.data$purchase_in_6month_target_window, predictor = validation.data$nn6mpred123) #0.783
roc(response = validation.data$purchase_in_6month_target_window, predictor = validation.data$nn6mpred124) #0.783

roc(response = validation.data$purchase_in_6month_target_window, predictor = validation.data$nn6mpred131) #0.783
roc(response = validation.data$purchase_in_6month_target_window, predictor = validation.data$nn6mpred132) #0.783
roc(response = validation.data$purchase_in_6month_target_window, predictor = validation.data$nn6mpred133) #0.783
roc(response = validation.data$purchase_in_6month_target_window, predictor = validation.data$nn6mpred134) #0.783

roc(response = validation.data$purchase_in_6month_target_window, predictor = validation.data$nn6mpred141) #0.783
roc(response = validation.data$purchase_in_6month_target_window, predictor = validation.data$nn6mpred142) #0.783
roc(response = validation.data$purchase_in_6month_target_window, predictor = validation.data$nn6mpred143) #0.783
roc(response = validation.data$purchase_in_6month_target_window, predictor = validation.data$nn6mpred144) #0.783

roc(response = validation.data$purchase_in_6month_target_window, predictor = validation.data$nn6mpred151) #0.783
roc(response = validation.data$purchase_in_6month_target_window, predictor = validation.data$nn6mpred152) #0.783
roc(response = validation.data$purchase_in_6month_target_window, predictor = validation.data$nn6mpred153) #0.783
roc(response = validation.data$purchase_in_6month_target_window, predictor = validation.data$nn6mpred154) #0.783

roc(response = validation.data$purchase_in_6month_target_window, predictor = validation.data$nn6mpred161) #0.783
roc(response = validation.data$purchase_in_6month_target_window, predictor = validation.data$nn6mpred162) #0.783
roc(response = validation.data$purchase_in_6month_target_window, predictor = validation.data$nn6mpred163) #0.783
roc(response = validation.data$purchase_in_6month_target_window, predictor = validation.data$nn6mpred164) #0.783

roc(response = validation.data$purchase_in_6month_target_window, predictor = validation.data$nn6mpred171) #0.783
roc(response = validation.data$purchase_in_6month_target_window, predictor = validation.data$nn6mpred172) #0.783
roc(response = validation.data$purchase_in_6month_target_window, predictor = validation.data$nn6mpred173) #0.783
roc(response = validation.data$purchase_in_6month_target_window, predictor = validation.data$nn6mpred174) #0.783

roc(response = validation.data$purchase_in_6month_target_window, predictor = validation.data$nn6mpred181) #0.783
roc(response = validation.data$purchase_in_6month_target_window, predictor = validation.data$nn6mpred182) #0.783
roc(response = validation.data$purchase_in_6month_target_window, predictor = validation.data$nn6mpred183) #0.783
roc(response = validation.data$purchase_in_6month_target_window, predictor = validation.data$nn6mpred184) #0.783
roc(response = validation.data$purchase_in_6month_target_window, predictor = validation.data$nn6mpred185) #0.783

roc(response = validation.data$purchase_in_6month_target_window, predictor = validation.data$nn6mpred191) #0.783






validation.data$nn6mpred1 <- predict(nn6mmod1,newdata = validation.data,type = "raw")
validation.data$nn6mpred2 <- predict(nn6mmod2,newdata = validation.data,type = "raw")
validation.data$nn6mpred3 <- predict(nn6mmod3,newdata = validation.data,type = "raw")
validation.data$nn6mpred4 <- predict(nn6mmod4,newdata = validation.data,type = "raw")
validation.data$nn6mpred5 <- predict(nn6mmod5,newdata = validation.data,type = "raw")
validation.data$nn6mpred6 <- predict(nn6mmod6,newdata = validation.data,type = "raw")
validation.data$nn6mpred7 <- predict(nn6mmod7,newdata = validation.data,type = "raw")
validation.data$nn6mpred8 <- predict(nn6mmod8,newdata = validation.data,type = "raw")
validation.data$nn6mpred9 <- predict(nn6mmod9,newdata = validation.data,type = "raw")
validation.data$nn6mpred10 <- predict(nn6mmod10,newdata = validation.data,type = "raw")


validation.data$mediannn6mpred <- apply(validation.data[,20:29],1,median)
roc(response = validation.data$purchase_in_6month_target_window, predictor = validation.data$mediannn6mpred) #0.783

##### Try the 12 months model

nn12mmod1 <- nnet(data = train.data1, purchase_in_12month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                 decay = 0.05,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nn12mmod2 <- nnet(data = train.data2, purchase_in_12month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                 decay = 0.05,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nn12mmod3 <- nnet(data = train.data3, purchase_in_12month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                 decay = 0.05,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nn12mmod4 <- nnet(data = train.data4, purchase_in_12month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                 decay = 0.05,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nn12mmod5 <- nnet(data = train.data5, purchase_in_12month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                 decay = 0.05,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nn12mmod6 <- nnet(data = train.data6, purchase_in_12month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                 decay = 0.05,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nn12mmod7 <- nnet(data = train.data7, purchase_in_12month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                 decay = 0.05,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nn12mmod8 <- nnet(data = train.data8, purchase_in_12month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                 decay = 0.05,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nn12mmod9 <- nnet(data = train.data9, purchase_in_12month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                 decay = 0.05,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
nn12mmod10 <- nnet(data = train.data10, purchase_in_12month_target_window ~ recency_purch + freq_purch + lastyear_same3months_purchase + ratio_purchases  + items_browsed + count_distinct_divisions + discount_percentage,
                  decay = 0.05,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)


validation.data$nn12mpred1 <- predict(nn12mmod1,newdata = validation.data,type = "raw")
validation.data$nn12mpred2 <- predict(nn12mmod2,newdata = validation.data,type = "raw")
validation.data$nn12mpred3 <- predict(nn12mmod3,newdata = validation.data,type = "raw")
validation.data$nn12mpred4 <- predict(nn12mmod4,newdata = validation.data,type = "raw")
validation.data$nn12mpred5 <- predict(nn12mmod5,newdata = validation.data,type = "raw")
validation.data$nn12mpred6 <- predict(nn12mmod6,newdata = validation.data,type = "raw")
validation.data$nn12mpred7 <- predict(nn12mmod7,newdata = validation.data,type = "raw")
validation.data$nn12mpred8 <- predict(nn12mmod8,newdata = validation.data,type = "raw")
validation.data$nn12mpred9 <- predict(nn12mmod9,newdata = validation.data,type = "raw")
validation.data$nn12mpred10 <- predict(nn12mmod10,newdata = validation.data,type = "raw")


validation.data$mediannn12mpred <- apply(validation.data[,31:40],1,median)
roc(response = validation.data$purchase_in_12month_target_window, predictor = validation.data$mediannn12mpred) #0.783
