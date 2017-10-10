library(readr );library(dplyr);library(xgboost);library(pROC)

df<-read.table('C:/Users/ssoni/Documents/Gap/Reactivation Model/GP_Reactivation_All_Variables_Export',sep=',', header=FALSE)

names(df) <- c("customer_key" ,
               "total_purchases" ,
               "total_net_sales" ,
               "discount_percentage" ,
               "count_distinct_divisions" ,
               "count_distinct_department",
               "top3dept_net_sales" ,
               "top3dept_net_sales_ratio" ,
               "sister_brands_total_purchases" ,
               "sister_brands_total_net_sales" ,
               "sister_brands_discount_percentage" ,
               "sister_brands_count_distinct_divisions" ,
               "sister_brands_count_distinct_department" ,
               "sister_brands_secondyear_total_purchases" ,
               "sister_brands_secondyear_total_net_sales" ,
               "sister_brands_secondyear_discount_percentage" ,
               "sister_brands_secondyear_count_distinct_divisions",
               "sister_brands_secondyear_count_distinct_department" ,
               "items_browsed" ,
               "thirdyear_purchase" )

####   Upper capping the variables at 99.9 percentile

df$total_purchases <- ifelse(df$total_purchases > 25,25,df$total_purchases)
df$total_net_sales <- ifelse(df$total_net_sales > 2000,2000,df$total_net_sales)
df$sister_brands_total_purchases <- ifelse(df$sister_brands_total_purchases > 50,50,df$sister_brands_total_purchases)
df$sister_brands_total_net_sales <- ifelse(df$sister_brands_total_net_sales > 5000,5000,df$sister_brands_total_net_sales)
df$sister_brands_secondyear_total_purchases <- ifelse(df$sister_brands_secondyear_total_purchases > 50,50,df$sister_brands_secondyear_total_purchases)
df$sister_brands_secondyear_total_net_sales <- ifelse(df$sister_brands_secondyear_total_net_sales > 5000,5000,df$sister_brands_secondyear_total_net_sales)
df$items_browsed <- ifelse(df$items_browsed > 120,120,df$items_browsed)

### Create a new variable for ratio of gp to sister brands sales
df$ratio_gp_sister_brands_sales <- df$total_net_sales / df$sister_brands_total_net_sales



# Create train and validations dataset

train.data1 <- sample_n(df,size=100000)
train.data2 <- sample_n(df,size=100000)
train.data3 <- sample_n(df,size=100000)
train.data4 <- sample_n(df,size=100000)
train.data5 <- sample_n(df,size=100000)
train.data6 <- sample_n(df,size=100000)
train.data7 <- sample_n(df,size=100000)
train.data8 <- sample_n(df,size=100000)
train.data9 <- sample_n(df,size=100000)
train.data10 <- sample_n(df,size=100000)



validation.data <- sample_n(df,size = 150000)

### Try the xgboost models

xgbmod1a <- xgboost(data = data.matrix(train.data1[,c(2:6,8:19,21)]),label = train.data1$thirdyear_purchase,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.15,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgbmod1b <- xgboost(data = data.matrix(train.data1[,c(2:6,8:19,21)]),label = train.data1$thirdyear_purchase,nrounds = 500, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))
xgbmod1c <- xgboost(data = data.matrix(train.data1[,c(2:6,8:19,21)]),label = train.data1$thirdyear_purchase,nrounds = 200, verbose = 1,params = list(objective = "binary:logistic",eta=0.25,max.depth = 3, eval_metric="error", booster="gbtree",silent=1))



validation.data$xgbpred1a <- predict(xgbmod1a,data.matrix(validation.data[,c(2:6,8:19,21)]))
validation.data$xgbpred1b <- predict(xgbmod1b,data.matrix(validation.data[,c(2:6,8:19,21)]))
validation.data$xgbpred1c <- predict(xgbmod1c,data.matrix(validation.data[,c(2:6,8:19,21)]))

roc(response = validation.data$thirdyear_purchase, predictor = validation.data$xgbpred1a,plot=TRUE) #0.6533
roc(response = validation.data$thirdyear_purchase, predictor = validation.data$xgbpred1b,plot=TRUE) #0.7606
roc(response = validation.data$thirdyear_purchase, predictor = validation.data$xgbpred1c,plot=TRUE) #0.7606

xgb.importance(feature_names = names(df)[c(2:6,8:19,21)],model = xgbmod1a)
