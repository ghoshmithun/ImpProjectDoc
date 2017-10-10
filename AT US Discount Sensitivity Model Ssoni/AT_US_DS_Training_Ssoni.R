#initialise libraries required
library(dplyr);library(gbm);library(readr);library(pROC)

#import the dfset 
df<-read.table('C:/Users/ssoni/Documents/Athleta/Discount Sensitivity/AT_DS_all_variables_training_export',sep='|', header=FALSE)

#insert headers
names(df) <- c("customer_key",
                 "flag_employee",
                 "active_period",
                 "recency_purch",
                 "freq_purch",
                 "total_net_sales",
                 "total_purchases",
                 "discount_percentage",
                 "count_distinct_divisions",
                 "ratio_purchases",
                 "at_items_browsed",
                 "br_items_browsed",
                 "gp_items_browsed",
                 "on_items_browsed",
                 "sister_brands_recency_purch",
                 "sister_brands_total_purchases",
                 "sister_brands_total_net_sales",
                 "sister_brands_discount_percentage",
                 "promotion_period_sales_ratio",
                 "holiday_season_sales_ratio",
                 "card_highest_tier",
                 "next_year_discount_percentage",
                  "next_year_total_purchases")

#Set factor variables
df$flag_employee <- as.factor(df$flag_employee)
df$card_highest_tier <- as.factor(df$card_highest_tier)

# Cap a few variables to 99.5%ile
df$total_purchases <- ifelse(df$total_purchases > 23, 23, df$total_purchases)                 
df$at_items_browsed <- ifelse(df$at_items_browsed > 190,190,df$at_items_browsed)
df$br_items_browsed <- ifelse(df$br_items_browsed > 100,100,df$br_items_browsed)
df$gp_items_browsed <- ifelse(df$gp_items_browsed > 130,130,df$gp_items_browsed)
df$on_items_browsed <- ifelse(df$on_items_browsed > 120,120,df$on_items_browsed)
df$ratio_purchases <- ifelse(df$ratio_purchases > 10,10,df$ratio_purchases)
df$total_net_sales <- ifelse(df$total_net_sales > 4000,4000,df$total_net_sales)
df$sister_brands_total_net_sales <- ifelse(df$sister_brands_total_net_sales > 6300,6300,df$sister_brands_total_net_sales)
df$sister_brands_total_purchases <- ifelse(df$sister_brands_total_purchases > 65,65,df$sister_brands_total_purchases)
df$active_period <- ifelse(df$active_period == 0, 0.01,df$active_period)
df$flag_employee <- ifelse(df$flag_employee == 'Y', 1,0)

# Recalculate the frequency
df$freq_purch <- df$total_purchases / df$active_period
df$freq_purch <- ifelse(df$freq_purch > 10,10,df$freq_purch)

df$card_highest_tier[df$card_highest_tier == 3] <- 0

# Set the target variable cutoff
df$high_discount <- ifelse(df$next_year_discount_percentage > 15,1,0)

library(dummies)
dummy.df <- dummy.data.frame(df[,c(2,5:21,24)])

# Create train and validations dfset

train.df1 <- sample_n(dummy.df,size=150000)
train.df2 <- sample_n(dummy.df,size=150000)
train.df3 <- sample_n(dummy.df,size=150000)
train.df4 <- sample_n(dummy.df,size=150000)
train.df5 <- sample_n(dummy.df,size=150000)
train.df6 <- sample_n(dummy.df,size=150000)
train.df7 <- sample_n(dummy.df,size=150000)
train.df8 <- sample_n(dummy.df,size=150000)
train.df9 <- sample_n(dummy.df,size=150000)
train.df10 <- sample_n(dummy.df,size=150000)


#test <- inner_join(train.df14,validation.df)

validation.df <- sample_n(dummy.df,size = 150000)
validation.df <- sample_n(dummy.df,size = 150000)



# Logistic Regression model for getting baseline idea
glmmod <- glm(data = train.df1[,c(1:23)], high_discount ~ ., family = "binomial")
summary(glmmod)

validation.df$glmpred <- predict(glmmod,newdata = validation.df,type="response")

roc <- roc(response = validation.df$high_discount, predictor = validation.df$glmpred,plot=TRUE) #0.7734



library(xgboost)

xgbmod1a <- xgboost(data = data.matrix(train.df1[,c(1:23)]),label = train.df1$high_discount,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1,lambda = 0.5, alpha = 0.5))
xgbmod1b <- xgboost(data = data.matrix(train.df1[,c(1:23)]),label = train.df1$high_discount,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgbmod1c <- xgboost(data = data.matrix(train.df1[,c(1:23)]),label = train.df1$high_discount,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))

xgbmod2a <- xgboost(data = data.matrix(train.df2[,c(1:23)]),label = train.df2$high_discount,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgbmod2b <- xgboost(data = data.matrix(train.df2[,c(1:23)]),label = train.df2$high_discount,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgbmod2c <- xgboost(data = data.matrix(train.df2[,c(1:23)]),label = train.df2$high_discount,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))

xgbmod3a <- xgboost(data = data.matrix(train.df3[,c(1:23)]),label = train.df3$high_discount,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgbmod3b <- xgboost(data = data.matrix(train.df3[,c(1:23)]),label = train.df3$high_discount,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgbmod3c <- xgboost(data = data.matrix(train.df3[,c(1:23)]),label = train.df3$high_discount,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))

xgbmod4a <- xgboost(data = data.matrix(train.df4[,c(1:23)]),label = train.df4$high_discount,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgbmod4b <- xgboost(data = data.matrix(train.df4[,c(1:23)]),label = train.df4$high_discount,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgbmod4c <- xgboost(data = data.matrix(train.df4[,c(1:23)]),label = train.df4$high_discount,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))

xgbmod5a <- xgboost(data = data.matrix(train.df5[,c(1:23)]),label = train.df5$high_discount,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgbmod5b <- xgboost(data = data.matrix(train.df5[,c(1:23)]),label = train.df5$high_discount,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgbmod5c <- xgboost(data = data.matrix(train.df5[,c(1:23)]),label = train.df5$high_discount,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))

xgbmod6a <- xgboost(data = data.matrix(train.df6[,c(1:23)]),label = train.df6$high_discount,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgbmod6b <- xgboost(data = data.matrix(train.df6[,c(1:23)]),label = train.df6$high_discount,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgbmod6c <- xgboost(data = data.matrix(train.df6[,c(1:23)]),label = train.df6$high_discount,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))

xgbmod7a <- xgboost(data = data.matrix(train.df7[,c(1:23)]),label = train.df7$high_discount,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgbmod7b <- xgboost(data = data.matrix(train.df7[,c(1:23)]),label = train.df7$high_discount,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgbmod7c <- xgboost(data = data.matrix(train.df7[,c(1:23)]),label = train.df7$high_discount,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))

xgbmod8a <- xgboost(data = data.matrix(train.df8[,c(1:23)]),label = train.df8$high_discount,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgbmod8b <- xgboost(data = data.matrix(train.df8[,c(1:23)]),label = train.df8$high_discount,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgbmod8c <- xgboost(data = data.matrix(train.df8[,c(1:23)]),label = train.df8$high_discount,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))

xgbmod9a <- xgboost(data = data.matrix(train.df9[,c(1:23)]),label = train.df9$high_discount,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgbmod9b <- xgboost(data = data.matrix(train.df9[,c(1:23)]),label = train.df9$high_discount,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgbmod9c <- xgboost(data = data.matrix(train.df9[,c(1:23)]),label = train.df9$high_discount,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))

xgbmod10a <- xgboost(data = data.matrix(train.df10[,c(1:23)]),label = train.df10$high_discount,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgbmod10b <- xgboost(data = data.matrix(train.df10[,c(1:23)]),label = train.df10$high_discount,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgbmod10c <- xgboost(data = data.matrix(train.df10[,c(1:23)]),label = train.df10$high_discount,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))


validation.df$xgbpred1a <- predict(xgbmod1a,data.matrix(validation.df[,c(1:23)]))
validation.df$xgbpred1b <- predict(xgbmod1b,data.matrix(validation.df[,c(1:23)]))
validation.df$xgbpred1c <- predict(xgbmod1c,data.matrix(validation.df[,c(1:23)]))

validation.df$xgbpred2a <- predict(xgbmod2a,data.matrix(validation.df[,c(1:23)]))
validation.df$xgbpred2b <- predict(xgbmod2b,data.matrix(validation.df[,c(1:23)]))
validation.df$xgbpred2c <- predict(xgbmod2c,data.matrix(validation.df[,c(1:23)]))

validation.df$xgbpred3a <- predict(xgbmod3a,data.matrix(validation.df[,c(1:23)]))
validation.df$xgbpred3b <- predict(xgbmod3b,data.matrix(validation.df[,c(1:23)]))
validation.df$xgbpred3c <- predict(xgbmod3c,data.matrix(validation.df[,c(1:23)]))

validation.df$xgbpred4a <- predict(xgbmod4a,data.matrix(validation.df[,c(1:23)]))
validation.df$xgbpred4b <- predict(xgbmod4b,data.matrix(validation.df[,c(1:23)]))
validation.df$xgbpred4c <- predict(xgbmod4c,data.matrix(validation.df[,c(1:23)]))

validation.df$xgbpred5a <- predict(xgbmod5a,data.matrix(validation.df[,c(1:23)]))
validation.df$xgbpred5b <- predict(xgbmod5b,data.matrix(validation.df[,c(1:23)]))
validation.df$xgbpred5c <- predict(xgbmod5c,data.matrix(validation.df[,c(1:23)]))

validation.df$xgbpred6a <- predict(xgbmod6a,data.matrix(validation.df[,c(1:23)]))
validation.df$xgbpred6b <- predict(xgbmod6b,data.matrix(validation.df[,c(1:23)]))
validation.df$xgbpred6c <- predict(xgbmod6c,data.matrix(validation.df[,c(1:23)]))

validation.df$xgbpred7a <- predict(xgbmod7a,data.matrix(validation.df[,c(1:23)]))
validation.df$xgbpred7b <- predict(xgbmod7b,data.matrix(validation.df[,c(1:23)]))
validation.df$xgbpred7c <- predict(xgbmod7c,data.matrix(validation.df[,c(1:23)]))

validation.df$xgbpred8a <- predict(xgbmod8a,data.matrix(validation.df[,c(1:23)]))
validation.df$xgbpred8b <- predict(xgbmod8b,data.matrix(validation.df[,c(1:23)]))
validation.df$xgbpred8c <- predict(xgbmod8c,data.matrix(validation.df[,c(1:23)]))

validation.df$xgbpred9a <- predict(xgbmod9a,data.matrix(validation.df[,c(1:23)]))
validation.df$xgbpred9b <- predict(xgbmod9b,data.matrix(validation.df[,c(1:23)]))
validation.df$xgbpred9c <- predict(xgbmod9c,data.matrix(validation.df[,c(1:23)]))

validation.df$xgbpred10a <- predict(xgbmod10a,data.matrix(validation.df[,c(1:23)]))
validation.df$xgbpred10b <- predict(xgbmod10b,data.matrix(validation.df[,c(1:23)]))
validation.df$xgbpred10c <- predict(xgbmod10c,data.matrix(validation.df[,c(1:23)]))


validation.df$medianxgbpred <- apply(validation.df[,25:54],1,median)
roc(response = validation.df$high_discount, predictor = validation.df$medianxgbpred,plot=TRUE) #0.7606


### Check the  importance of features
modelimportance <- xgb.importance(model = xgbmod1a, feature_names = names(dummy.df[c(1:23)]))
xgb.ggplot.importance(modelimportance)


### Try the balanced data model
train.df.allones <- dummy.df %>% filter(high_discount == 1)

train.df.zeroes <- dummy.df %>% filter(high_discount == 0) %>% sample_n(size = 73134)
train.df.balanced <- rbind(train.df.allones,train.df.zeroes)

xgbbalancedmod1a <- xgboost(data = data.matrix(train.df.balanced[,c(1:23)]),label = train.df.balanced$high_discount,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 4, eval_metric="error", booster="gbtree",silent=1))
validation.df$xgbbalancedpred1a <- predict(xgbbalancedmod1a,data.matrix(validation.df[,c(1:23)]))
roc(response = validation.df$high_discount, predictor = validation.df$xgbbalancedpred1a,plot=TRUE) #0.866


library(nnet)
nnmod1a <- nnet(data = train.df1, high_discount ~ . , decay = 0.3,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
validation.df$nnpred1a <- predict(nnmod1a,newdata = validation.df,type = "raw")
roc(response = validation.df$high_discount, predictor = validation.df$nnpred1a,plot=TRUE) #0.8287

nnbalancedmod1a <- nnet(data = train.df.balanced, high_discount ~ . , decay = 0.01,MaxNWts = 10000,maxit = 1000,size = 15,trace = T)
validation.df$nnbalancedpred1a <- predict(nnbalancedmod1a,newdata = validation.df,type = "raw")
roc(response = validation.df$high_discount, predictor = validation.df$nnbalancedpred1a,plot=TRUE) #0.7606


### Check the discount percentage transition matrix
df$last_year_disc_perc_group <- ifelse(df$discount_percentage > 20,5,ifelse(df$discount_percentage > 15,4,ifelse(df$discount_percentage > 10,3,ifelse(df$discount_percentage > 5,2,1))))
df$next_year_disc_perc_group <- ifelse(df$next_year_total_purchases == 0,0,ifelse(df$next_year_discount_percentage > 20,5,ifelse(df$next_year_discount_percentage > 15,4,ifelse(df$next_year_discount_percentage > 10,3,ifelse(df$next_year_discount_percentage > 5,2,1)))))

  
table(df$last_year_disc_perc_group,df$next_year_disc_perc_group)

dummy.df$xgbpred1a <- predict(xgbmod1a,data.matrix(dummy.df[,c(1:23)]))

Target <- dummy.df$high_discount
Prediction <- dummy.df$xgbpred1a
DF <- data.frame(cbind(Target,Prediction))
DF <- DF %>% arrange(desc(Prediction)) %>% mutate(RowNumber = row_number(),Decile = ntile(RowNumber,10))

Lift <- rep(NA,10)
Gain <- rep(NA,10)
SubsetPositiveResponders <- rep(NA,10)
SubsetTotalResponders <- rep(NA,10)
AvgResponse <- mean(DF$Target)
SumPositiveResponders <- sum(DF$Target)
CumPositiveResponders <- 0
CumTotalResponders <- 0
for(i in 1:10) {
  Subset <- DF  %>% filter(Decile == i)
  SubsetPositiveResponders[i] <- sum(Subset$Target)
  SubsetTotalResponders[i] <- nrow(Subset)
  CumTotalResponders <- CumTotalResponders + SubsetTotalResponders[i]
  CumPositiveResponders <- CumPositiveResponders + SubsetPositiveResponders[i]
  Lift[i] <- CumPositiveResponders / (AvgResponse*CumTotalResponders)
  Gain[i] <- CumPositiveResponders*100 / SumPositiveResponders
}  