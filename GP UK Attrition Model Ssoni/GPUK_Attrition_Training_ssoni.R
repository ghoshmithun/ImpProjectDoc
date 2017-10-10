#initialise libraries required
library(dplyr);library(gbm);library(readr);library(pROC)
library(randomForest);library(caret);library(nnet);library(e1071);library(xgboost)

#import the dataset 
# change the path if reading file from the share drive
df<-read.table('C:/Users/ssoni/Documents/Gap/Uk Attrition Model/GPUK_Attrition_Export_v2',sep='|', header=FALSE)

#insert headers
names(df) <- c("customer_key",
               "weeks_since_last_purch",
               "total_purchases",
               "total_net_sales",
               "discount_percentage",
               "items_return_ratio",
               "count_distinct_divisions",
               "attrited")

# Upper cap a few variablesat 99.9 %ile
df$total_purchases <- ifelse(df$total_purchases>35,35,df$total_purchases)
df$discount_percentage <- ifelse(df$discount_percentage > 55,55,df$discount_percentage)
df$total_net_sales <- ifelse(df$total_net_sales>3250,3250,df$total_net_sales)
df$items_return_ratio <- ifelse(df$items_return_ratio > 1, 1, df$items_return_ratio)


#df$attrited <- as.factor(df$attrited)


# Create train test and validation datasets
#   index.train <- bigsample(1:nrow(df),size = 15000,replace = F,prob = NULL)
#   train.data <- data.frame(df[index.train, ])
#   test.data <- data.frame(df[-index.train, ])
#   validation.data <- sample_frac(test.data,size=0.1,replace = F)

#Standardize the variables
#df$weeks_since_last_purch_std <- scale(df$weeks_since_last_purch,center = T,scale = T)
#df$total_purchases_std <- scale(df$total_purchases,center = T,scale = T)
#df$total_net_sales_std <- scale(df$total_net_sales,center = T,scale = T)
#df$discount_percentage_std <- scale(df$discount_percentage,center = T,scale = T)
#df$items_return_ratio_std <- scale(df$items_return_ratio,center = T,scale = T)
#df$count_distinct_divisions_std <- scale(df$count_distinct_divisions,center = T,scale = T)



set.seed(1234)
folds <- createFolds(df$customer_key,k=20,list = T)    
df1 <- df[folds$Fold01,]
df2 <- df[folds$Fold02,]
df3 <- df[folds$Fold03,]
df4 <- df[folds$Fold04,]
df5 <- df[folds$Fold05,]
df6 <- df[folds$Fold06,]
df7 <- df[folds$Fold07,]
df8 <- df[folds$Fold08,]
df9 <- df[folds$Fold09,]
df10 <- df[folds$Fold10,]
df11 <- df[folds$Fold11,]
df12 <- df[folds$Fold12,]
df13 <- df[folds$Fold13,]


# Build neural network models

#df1
nnmod1a <- nnet(attrited ~ . , data = df1[,2:8],decay = 0.03, maxit = 10000, MaxNWts = 10000, size = 15, trace = T)
nnmod1b <- nnet(attrited ~ . , data = df1[,2:8],decay = 0.05, maxit = 10000, MaxNWts = 10000, size = 15, trace = T)
nnmod1c <- nnet(attrited ~ . , data = df1[,2:8],decay = 0.07, maxit = 10000, MaxNWts = 10000, size = 15, trace = T)

#df2
nnmod2a <- nnet(attrited ~ . , data = df2[,2:8],decay = 0.03, maxit = 10000, MaxNWts = 10000, size = 15, trace = T)
nnmod2b <- nnet(attrited ~ . , data = df2[,2:8],decay = 0.05, maxit = 10000, MaxNWts = 10000, size = 15, trace = T)
nnmod2c <- nnet(attrited ~ . , data = df2[,2:8],decay = 0.07, maxit = 10000, MaxNWts = 10000, size = 15, trace = T)

#df3
nnmod3a <- nnet(attrited ~ . , data = df3[,2:8],decay = 0.03, maxit = 10000, MaxNWts = 10000, size = 15, trace = T)
nnmod3b <- nnet(attrited ~ . , data = df3[,2:8],decay = 0.05, maxit = 10000, MaxNWts = 10000, size = 15, trace = T)
nnmod3c <- nnet(attrited ~ . , data = df3[,2:8],decay = 0.07, maxit = 10000, MaxNWts = 10000, size = 15, trace = T)

#df4
nnmod4a <- nnet(attrited ~ . , data = df4[,2:8],decay = 0.03, maxit = 10000, MaxNWts = 10000, size = 15, trace = T)
nnmod4b <- nnet(attrited ~ . , data = df4[,2:8],decay = 0.05, maxit = 10000, MaxNWts = 10000, size = 15, trace = T)
nnmod4c <- nnet(attrited ~ . , data = df4[,2:8],decay = 0.07, maxit = 10000, MaxNWts = 10000, size = 15, trace = T)

#df5
nnmod5a <- nnet(attrited ~ . , data = df5[,2:8],decay = 0.03, maxit = 10000, MaxNWts = 10000, size = 15, trace = T)
nnmod5b <- nnet(attrited ~ . , data = df5[,2:8],decay = 0.05, maxit = 10000, MaxNWts = 10000, size = 15, trace = T)
nnmod5c <- nnet(attrited ~ . , data = df5[,2:8],decay = 0.07, maxit = 10000, MaxNWts = 10000, size = 15, trace = T)

#df6
nnmod6a <- nnet(attrited ~ . , data = df6[,2:8],decay = 0.03, maxit = 10000, MaxNWts = 10000, size = 15, trace = T)
nnmod6b <- nnet(attrited ~ . , data = df6[,2:8],decay = 0.05, maxit = 10000, MaxNWts = 10000, size = 15, trace = T)
nnmod6c <- nnet(attrited ~ . , data = df6[,2:8],decay = 0.07, maxit = 10000, MaxNWts = 10000, size = 15, trace = T)

#df7
nnmod7a <- nnet(attrited ~ . , data = df7[,2:8],decay = 0.03, maxit = 10000, MaxNWts = 10000, size = 15, trace = T)
nnmod7b <- nnet(attrited ~ . , data = df7[,2:8],decay = 0.05, maxit = 10000, MaxNWts = 10000, size = 15, trace = T)
nnmod7c <- nnet(attrited ~ . , data = df7[,2:8],decay = 0.07, maxit = 10000, MaxNWts = 10000, size = 15, trace = T)

#df8
nnmod8a <- nnet(attrited ~ . , data = df8[,2:8],decay = 0.03, maxit = 10000, MaxNWts = 10000, size = 15, trace = T)
nnmod8b <- nnet(attrited ~ . , data = df8[,2:8],decay = 0.05, maxit = 10000, MaxNWts = 10000, size = 15, trace = T)
nnmod8c <- nnet(attrited ~ . , data = df8[,2:8],decay = 0.07, maxit = 10000, MaxNWts = 10000, size = 15, trace = T)

#df9
nnmod9a <- nnet(attrited ~ . , data = df9[,2:8],decay = 0.03, maxit = 10000, MaxNWts = 10000, size = 15, trace = T)
nnmod9b <- nnet(attrited ~ . , data = df9[,2:8],decay = 0.05, maxit = 10000, MaxNWts = 10000, size = 15, trace = T)
nnmod9c <- nnet(attrited ~ . , data = df9[,2:8],decay = 0.07, maxit = 10000, MaxNWts = 10000, size = 15, trace = T)

#df10
nnmod10a <- nnet(attrited ~ . , data = df10[,2:8],decay = 0.03, maxit = 10000, MaxNWts = 10000, size = 15, trace = T)
nnmod10b <- nnet(attrited ~ . , data = df10[,2:8],decay = 0.05, maxit = 10000, MaxNWts = 10000, size = 15, trace = T)
nnmod10c <- nnet(attrited ~ . , data = df10[,2:8],decay = 0.07, maxit = 10000, MaxNWts = 10000, size = 15, trace = T)


### Save the models
save(nnmod1a,file="nnmod1a.RDA")
save(nnmod1b,file="nnmod1b.RDA")
save(nnmod1c,file="nnmod1c.RDA")

save(nnmod2a,file="nnmod2a.RDA")
save(nnmod2b,file="nnmod2b.RDA")
save(nnmod2c,file="nnmod2c.RDA")

save(nnmod3a,file="nnmod3a.RDA")
save(nnmod3b,file="nnmod3b.RDA")
save(nnmod3c,file="nnmod3c.RDA")

save(nnmod4a,file="nnmod4a.RDA")
save(nnmod4b,file="nnmod4b.RDA")
save(nnmod4c,file="nnmod4c.RDA")

save(nnmod5a,file="nnmod5a.RDA")
save(nnmod5b,file="nnmod5b.RDA")
save(nnmod5c,file="nnmod5c.RDA")

save(nnmod6a,file="nnmod6a.RDA")
save(nnmod6b,file="nnmod6b.RDA")
save(nnmod6c,file="nnmod6c.RDA")

save(nnmod7a,file="nnmod7a.RDA")
save(nnmod7b,file="nnmod7b.RDA")
save(nnmod7c,file="nnmod7c.RDA")

save(nnmod8a,file="nnmod8a.RDA")
save(nnmod8b,file="nnmod8b.RDA")
save(nnmod8c,file="nnmod8c.RDA")

save(nnmod9a,file="nnmod9a.RDA")
save(nnmod9b,file="nnmod9b.RDA")
save(nnmod9c,file="nnmod9c.RDA")

save(nnmod10a,file="nnmod10a.RDA")
save(nnmod10b,file="nnmod10b.RDA")
save(nnmod10c,file="nnmod10c.RDA")



### Make the predictions from all models
#df11
df11$nnpred1a <- predict(nnmod1a, newdata = df11,type = "raw")
df11$nnpred1b <- predict(nnmod1b, newdata = df11,type = "raw")
df11$nnpred1c <- predict(nnmod1c, newdata = df11,type = "raw")

df11$nnpred2a <- predict(nnmod2a, newdata = df11,type = "raw")
df11$nnpred2b <- predict(nnmod2b, newdata = df11,type = "raw")
df11$nnpred2c <- predict(nnmod2c, newdata = df11,type = "raw")

df11$nnpred3a <- predict(nnmod3a, newdata = df11,type = "raw")
df11$nnpred3b <- predict(nnmod3b, newdata = df11,type = "raw")
df11$nnpred3c <- predict(nnmod3c, newdata = df11,type = "raw")

df11$nnpred4a <- predict(nnmod4a, newdata = df11,type = "raw")
df11$nnpred4b <- predict(nnmod4b, newdata = df11,type = "raw")
df11$nnpred4c <- predict(nnmod4c, newdata = df11,type = "raw")

df11$nnpred5a <- predict(nnmod5a, newdata = df11,type = "raw")
df11$nnpred5b <- predict(nnmod5b, newdata = df11,type = "raw")
df11$nnpred5c <- predict(nnmod5c, newdata = df11,type = "raw")

df11$nnpred6a <- predict(nnmod6a, newdata = df11,type = "raw")
df11$nnpred6b <- predict(nnmod6b, newdata = df11,type = "raw")
df11$nnpred6c <- predict(nnmod6c, newdata = df11,type = "raw")

df11$nnpred7a <- predict(nnmod7a, newdata = df11,type = "raw")
df11$nnpred7b <- predict(nnmod7b, newdata = df11,type = "raw")
df11$nnpred7c <- predict(nnmod7c, newdata = df11,type = "raw")

df11$nnpred8a <- predict(nnmod8a, newdata = df11,type = "raw")
df11$nnpred8b <- predict(nnmod8b, newdata = df11,type = "raw")
df11$nnpred8c <- predict(nnmod8c, newdata = df11,type = "raw")

df11$nnpred9a <- predict(nnmod9a, newdata = df11,type = "raw")
df11$nnpred9b <- predict(nnmod9b, newdata = df11,type = "raw")
df11$nnpred9c <- predict(nnmod9c, newdata = df11,type = "raw")

df11$nnpred10a <- predict(nnmod10a, newdata = df11,type = "raw")
df11$nnpred10b <- predict(nnmod10b, newdata = df11,type = "raw")
df11$nnpred10c <- predict(nnmod10c, newdata = df11,type = "raw")


#df12
df12$nnpred1a <- predict(nnmod1a, newdata = df12,type = "raw")
df12$nnpred1b <- predict(nnmod1b, newdata = df12,type = "raw")
df12$nnpred1c <- predict(nnmod1c, newdata = df12,type = "raw")

df12$nnpred2a <- predict(nnmod2a, newdata = df12,type = "raw")
df12$nnpred2b <- predict(nnmod2b, newdata = df12,type = "raw")
df12$nnpred2c <- predict(nnmod2c, newdata = df12,type = "raw")

df12$nnpred3a <- predict(nnmod3a, newdata = df12,type = "raw")
df12$nnpred3b <- predict(nnmod3b, newdata = df12,type = "raw")
df12$nnpred3c <- predict(nnmod3c, newdata = df12,type = "raw")

df12$nnpred4a <- predict(nnmod4a, newdata = df12,type = "raw")
df12$nnpred4b <- predict(nnmod4b, newdata = df12,type = "raw")
df12$nnpred4c <- predict(nnmod4c, newdata = df12,type = "raw")

df12$nnpred5a <- predict(nnmod5a, newdata = df12,type = "raw")
df12$nnpred5b <- predict(nnmod5b, newdata = df12,type = "raw")
df12$nnpred5c <- predict(nnmod5c, newdata = df12,type = "raw")

df12$nnpred6a <- predict(nnmod6a, newdata = df12,type = "raw")
df12$nnpred6b <- predict(nnmod6b, newdata = df12,type = "raw")
df12$nnpred6c <- predict(nnmod6c, newdata = df12,type = "raw")

df12$nnpred7a <- predict(nnmod7a, newdata = df12,type = "raw")
df12$nnpred7b <- predict(nnmod7b, newdata = df12,type = "raw")
df12$nnpred7c <- predict(nnmod7c, newdata = df12,type = "raw")

df12$nnpred8a <- predict(nnmod8a, newdata = df12,type = "raw")
df12$nnpred8b <- predict(nnmod8b, newdata = df12,type = "raw")
df12$nnpred8c <- predict(nnmod8c, newdata = df12,type = "raw")

df12$nnpred9a <- predict(nnmod9a, newdata = df12,type = "raw")
df12$nnpred9b <- predict(nnmod9b, newdata = df12,type = "raw")
df12$nnpred9c <- predict(nnmod9c, newdata = df12,type = "raw")

df12$nnpred10a <- predict(nnmod10a, newdata = df12,type = "raw")
df12$nnpred10b <- predict(nnmod10b, newdata = df12,type = "raw")
df12$nnpred10c <- predict(nnmod10c, newdata = df12,type = "raw")



#df13
df13$nnpred1a <- predict(nnmod1a, newdata = df13,type = "raw")
df13$nnpred1b <- predict(nnmod1b, newdata = df13,type = "raw")
df13$nnpred1c <- predict(nnmod1c, newdata = df13,type = "raw")

df13$nnpred2a <- predict(nnmod2a, newdata = df13,type = "raw")
df13$nnpred2b <- predict(nnmod2b, newdata = df13,type = "raw")
df13$nnpred2c <- predict(nnmod2c, newdata = df13,type = "raw")

df13$nnpred3a <- predict(nnmod3a, newdata = df13,type = "raw")
df13$nnpred3b <- predict(nnmod3b, newdata = df13,type = "raw")
df13$nnpred3c <- predict(nnmod3c, newdata = df13,type = "raw")

df13$nnpred4a <- predict(nnmod4a, newdata = df13,type = "raw")
df13$nnpred4b <- predict(nnmod4b, newdata = df13,type = "raw")
df13$nnpred4c <- predict(nnmod4c, newdata = df13,type = "raw")

df13$nnpred5a <- predict(nnmod5a, newdata = df13,type = "raw")
df13$nnpred5b <- predict(nnmod5b, newdata = df13,type = "raw")
df13$nnpred5c <- predict(nnmod5c, newdata = df13,type = "raw")

df13$nnpred6a <- predict(nnmod6a, newdata = df13,type = "raw")
df13$nnpred6b <- predict(nnmod6b, newdata = df13,type = "raw")
df13$nnpred6c <- predict(nnmod6c, newdata = df13,type = "raw")

df13$nnpred7a <- predict(nnmod7a, newdata = df13,type = "raw")
df13$nnpred7b <- predict(nnmod7b, newdata = df13,type = "raw")
df13$nnpred7c <- predict(nnmod7c, newdata = df13,type = "raw")

df13$nnpred8a <- predict(nnmod8a, newdata = df13,type = "raw")
df13$nnpred8b <- predict(nnmod8b, newdata = df13,type = "raw")
df13$nnpred8c <- predict(nnmod8c, newdata = df13,type = "raw")

df13$nnpred9a <- predict(nnmod9a, newdata = df13,type = "raw")
df13$nnpred9b <- predict(nnmod9b, newdata = df13,type = "raw")
df13$nnpred9c <- predict(nnmod9c, newdata = df13,type = "raw")

df13$nnpred10a <- predict(nnmod10a, newdata = df13,type = "raw")
df13$nnpred10b <- predict(nnmod10b, newdata = df13,type = "raw")
df13$nnpred10c <- predict(nnmod10c, newdata = df13,type = "raw")


# Take mean and median of ensembles
df11$meanpred <- rowSums(df11[,9:38])/30
df11$medianpred <- apply(df11[,9:38],1,median)

df12$meanpred <- rowSums(df12[,9:38])/30
df12$medianpred <- apply(df12[,9:38],1,median)

df13$meanpred <- rowSums(df13[,9:38])/30
df13$medianpred <- apply(df13[,9:38],1,median)

# Check the ROC values of ensembles
roc(response = df11$attrited, predictor = df11$meanpred,plot=TRUE) #0.7566
roc(response = df11$attrited, predictor = df11$medianpred,plot=TRUE) #0.7564

roc(response = df12$attrited, predictor = df12$meanpred,plot=TRUE) #0.7562
roc(response = df12$attrited, predictor = df12$medianpred,plot=TRUE) #0.7561

roc(response = df13$attrited, predictor = df13$meanpred,plot=TRUE) #0.7546
roc(response = df13$attrited, predictor = df13$medianpred,plot=TRUE) #0.7545

### Make predictions on the training datasets
#df1
df1$nnpred1a <- predict(nnmod1a, newdata = df1,type = "raw")
df1$nnpred1b <- predict(nnmod1b, newdata = df1,type = "raw")
df1$nnpred1c <- predict(nnmod1c, newdata = df1,type = "raw")

df1$nnpred2a <- predict(nnmod2a, newdata = df1,type = "raw")
df1$nnpred2b <- predict(nnmod2b, newdata = df1,type = "raw")
df1$nnpred2c <- predict(nnmod2c, newdata = df1,type = "raw")

df1$nnpred3a <- predict(nnmod3a, newdata = df1,type = "raw")
df1$nnpred3b <- predict(nnmod3b, newdata = df1,type = "raw")
df1$nnpred3c <- predict(nnmod3c, newdata = df1,type = "raw")

df1$nnpred4a <- predict(nnmod4a, newdata = df1,type = "raw")
df1$nnpred4b <- predict(nnmod4b, newdata = df1,type = "raw")
df1$nnpred4c <- predict(nnmod4c, newdata = df1,type = "raw")

df1$nnpred5a <- predict(nnmod5a, newdata = df1,type = "raw")
df1$nnpred5b <- predict(nnmod5b, newdata = df1,type = "raw")
df1$nnpred5c <- predict(nnmod5c, newdata = df1,type = "raw")

df1$nnpred6a <- predict(nnmod6a, newdata = df1,type = "raw")
df1$nnpred6b <- predict(nnmod6b, newdata = df1,type = "raw")
df1$nnpred6c <- predict(nnmod6c, newdata = df1,type = "raw")

df1$nnpred7a <- predict(nnmod7a, newdata = df1,type = "raw")
df1$nnpred7b <- predict(nnmod7b, newdata = df1,type = "raw")
df1$nnpred7c <- predict(nnmod7c, newdata = df1,type = "raw")

df1$nnpred8a <- predict(nnmod8a, newdata = df1,type = "raw")
df1$nnpred8b <- predict(nnmod8b, newdata = df1,type = "raw")
df1$nnpred8c <- predict(nnmod8c, newdata = df1,type = "raw")

df1$nnpred9a <- predict(nnmod9a, newdata = df1,type = "raw")
df1$nnpred9b <- predict(nnmod9b, newdata = df1,type = "raw")
df1$nnpred9c <- predict(nnmod9c, newdata = df1,type = "raw")

df1$nnpred10a <- predict(nnmod10a, newdata = df1,type = "raw")
df1$nnpred10b <- predict(nnmod10b, newdata = df1,type = "raw")
df1$nnpred10c <- predict(nnmod10c, newdata = df1,type = "raw")


#df2
df2$nnpred1a <- predict(nnmod1a, newdata = df2,type = "raw")
df2$nnpred1b <- predict(nnmod1b, newdata = df2,type = "raw")
df2$nnpred1c <- predict(nnmod1c, newdata = df2,type = "raw")

df2$nnpred2a <- predict(nnmod2a, newdata = df2,type = "raw")
df2$nnpred2b <- predict(nnmod2b, newdata = df2,type = "raw")
df2$nnpred2c <- predict(nnmod2c, newdata = df2,type = "raw")

df2$nnpred3a <- predict(nnmod3a, newdata = df2,type = "raw")
df2$nnpred3b <- predict(nnmod3b, newdata = df2,type = "raw")
df2$nnpred3c <- predict(nnmod3c, newdata = df2,type = "raw")

df2$nnpred4a <- predict(nnmod4a, newdata = df2,type = "raw")
df2$nnpred4b <- predict(nnmod4b, newdata = df2,type = "raw")
df2$nnpred4c <- predict(nnmod4c, newdata = df2,type = "raw")

df2$nnpred5a <- predict(nnmod5a, newdata = df2,type = "raw")
df2$nnpred5b <- predict(nnmod5b, newdata = df2,type = "raw")
df2$nnpred5c <- predict(nnmod5c, newdata = df2,type = "raw")

df2$nnpred6a <- predict(nnmod6a, newdata = df2,type = "raw")
df2$nnpred6b <- predict(nnmod6b, newdata = df2,type = "raw")
df2$nnpred6c <- predict(nnmod6c, newdata = df2,type = "raw")

df2$nnpred7a <- predict(nnmod7a, newdata = df2,type = "raw")
df2$nnpred7b <- predict(nnmod7b, newdata = df2,type = "raw")
df2$nnpred7c <- predict(nnmod7c, newdata = df2,type = "raw")

df2$nnpred8a <- predict(nnmod8a, newdata = df2,type = "raw")
df2$nnpred8b <- predict(nnmod8b, newdata = df2,type = "raw")
df2$nnpred8c <- predict(nnmod8c, newdata = df2,type = "raw")

df2$nnpred9a <- predict(nnmod9a, newdata = df2,type = "raw")
df2$nnpred9b <- predict(nnmod9b, newdata = df2,type = "raw")
df2$nnpred9c <- predict(nnmod9c, newdata = df2,type = "raw")

df2$nnpred10a <- predict(nnmod10a, newdata = df2,type = "raw")
df2$nnpred10b <- predict(nnmod10b, newdata = df2,type = "raw")
df2$nnpred10c <- predict(nnmod10c, newdata = df2,type = "raw")



#df3
df3$nnpred1a <- predict(nnmod1a, newdata = df3,type = "raw")
df3$nnpred1b <- predict(nnmod1b, newdata = df3,type = "raw")
df3$nnpred1c <- predict(nnmod1c, newdata = df3,type = "raw")

df3$nnpred2a <- predict(nnmod2a, newdata = df3,type = "raw")
df3$nnpred2b <- predict(nnmod2b, newdata = df3,type = "raw")
df3$nnpred2c <- predict(nnmod2c, newdata = df3,type = "raw")

df3$nnpred3a <- predict(nnmod3a, newdata = df3,type = "raw")
df3$nnpred3b <- predict(nnmod3b, newdata = df3,type = "raw")
df3$nnpred3c <- predict(nnmod3c, newdata = df3,type = "raw")

df3$nnpred4a <- predict(nnmod4a, newdata = df3,type = "raw")
df3$nnpred4b <- predict(nnmod4b, newdata = df3,type = "raw")
df3$nnpred4c <- predict(nnmod4c, newdata = df3,type = "raw")

df3$nnpred5a <- predict(nnmod5a, newdata = df3,type = "raw")
df3$nnpred5b <- predict(nnmod5b, newdata = df3,type = "raw")
df3$nnpred5c <- predict(nnmod5c, newdata = df3,type = "raw")

df3$nnpred6a <- predict(nnmod6a, newdata = df3,type = "raw")
df3$nnpred6b <- predict(nnmod6b, newdata = df3,type = "raw")
df3$nnpred6c <- predict(nnmod6c, newdata = df3,type = "raw")

df3$nnpred7a <- predict(nnmod7a, newdata = df3,type = "raw")
df3$nnpred7b <- predict(nnmod7b, newdata = df3,type = "raw")
df3$nnpred7c <- predict(nnmod7c, newdata = df3,type = "raw")

df3$nnpred8a <- predict(nnmod8a, newdata = df3,type = "raw")
df3$nnpred8b <- predict(nnmod8b, newdata = df3,type = "raw")
df3$nnpred8c <- predict(nnmod8c, newdata = df3,type = "raw")

df3$nnpred9a <- predict(nnmod9a, newdata = df3,type = "raw")
df3$nnpred9b <- predict(nnmod9b, newdata = df3,type = "raw")
df3$nnpred9c <- predict(nnmod9c, newdata = df3,type = "raw")

df3$nnpred10a <- predict(nnmod10a, newdata = df3,type = "raw")
df3$nnpred10b <- predict(nnmod10b, newdata = df3,type = "raw")
df3$nnpred10c <- predict(nnmod10c, newdata = df3,type = "raw")

# Take mean and median of ensembles
df1$meanpred <- rowSums(df1[,9:38])/30
df1$medianpred <- apply(df1[,9:38],1,median)

df2$meanpred <- rowSums(df2[,9:38])/30
df2$medianpred <- apply(df2[,9:38],1,median)

df3$meanpred <- rowSums(df3[,9:38])/30
df3$medianpred <- apply(df3[,9:38],1,median)


### Run validations and check for accuracy metrics



confmatrix <- confusionMatrix(data = df11$class,reference = df11$attrited,positive = "1")
confmatrix

sensitivity1 <- NA ; sensitivity2 <- NA ; sensitivity3 <- NA ; sensitivity11 <- NA ; sensitivity12 <- NA ; sensitivity13 <- NA ; 
specificity1 <- NA ; specificity2 <- NA ; specificity3 <- NA ; specificity11 <- NA ; specificity12 <- NA ; specificity13 <- NA ; 
precision1 <- NA ; precision2 <- NA ; precision3 <- NA ; precision11 <- NA ; precision12 <- NA ; precision13 <- NA ; 
accuracy1 <- NA ; accuracy2 <- NA ; accuracy3 <- NA ; accuracy11 <- NA ; accuracy12 <- NA ; accuracy13 <- NA ; 
senplusspec1 <- NA ; senplusspec2 <- NA ; senplusspec3 <- NA ; senplusspec11 <- NA ; senplusspec12 <- NA ; senplusspec13 <- NA ; 
senminusspec1 <- NA ; senminusspec2 <- NA ; senminusspec3 <- NA ; senminusspec11 <- NA ; senminusspec12 <- NA ; senminusspec13 <- NA ; 


k <- 1
for(i in seq(0.01,0.99,0.01))
{
    # Training sets
    df1$class <- ifelse(df1$medianpred > i,1,0)
    df1confmatrix <- confusionMatrix(data = df1$class,reference = df1$attrited,positive = "1")
    sensitivity1[k] <- df1confmatrix$byClass[1]
    specificity1[k] <- df1confmatrix$byClass[2]
    precision1[k] <- df1confmatrix$byClass[3]
    accuracy1[k] <- df1confmatrix$overall[1]
    senplusspec1[k] <- sensitivity1[k] + specificity1[k]
    senminusspec1[k] <- abs(sensitivity1[k] - specificity1[k])
    
    
    df2$class <- ifelse(df2$medianpred > i,1,0)
    df2confmatrix <- confusionMatrix(data = df2$class,reference = df2$attrited,positive = "1")
    sensitivity2[k] <- df2confmatrix$byClass[1]
    specificity2[k] <- df2confmatrix$byClass[2]
    precision2[k] <- df2confmatrix$byClass[3]
    accuracy2[k] <- df2confmatrix$overall[1]
    senplusspec2[k] <- sensitivity2[k] + specificity2[k]
    senminusspec2[k] <- abs(sensitivity2[k] - specificity2[k])
    
    df3$class <- ifelse(df3$medianpred > i,1,0)
    df3confmatrix <- confusionMatrix(data = df3$class,reference = df3$attrited,positive = "1")
    sensitivity3[k] <- df3confmatrix$byClass[1]
    specificity3[k] <- df3confmatrix$byClass[2]
    precision3[k] <- df3confmatrix$byClass[3]
    accuracy3[k] <- df3confmatrix$overall[1]
    senplusspec3[k] <- sensitivity3[k] + specificity3[k]
    senminusspec3[k] <- abs(sensitivity3[k] - specificity3[k])
    
    # Testing sets
    df11$class <- ifelse(df11$medianpred > i,1,0)
    df11confmatrix <- confusionMatrix(data = df11$class,reference = df11$attrited,positive = "1")
    sensitivity11[k] <- df11confmatrix$byClass[1]
    specificity11[k] <- df11confmatrix$byClass[2]
    precision11[k] <- df11confmatrix$byClass[3]
    accuracy11[k] <- df11confmatrix$overall[1]
    senplusspec11[k] <- sensitivity11[k] + specificity11[k]
    senminusspec11[k] <- abs(sensitivity11[k] - specificity11[k])
    
    df12$class <- ifelse(df12$medianpred > i,1,0)
    df12confmatrix <- confusionMatrix(data = df12$class,reference = df12$attrited,positive = "1")
    sensitivity12[k] <- df12confmatrix$byClass[1]
    specificity12[k] <- df12confmatrix$byClass[2]
    precision12[k] <- df12confmatrix$byClass[3]
    accuracy12[k] <- df12confmatrix$overall[1]
    senplusspec12[k] <- sensitivity12[k] + specificity12[k]
    senminusspec12[k] <- abs(sensitivity12[k] - specificity12[k])
    
    
    df13$class <- ifelse(df13$medianpred > i,1,0)
    df13confmatrix <- confusionMatrix(data = df13$class,reference = df13$attrited,positive = "1")
    sensitivity13[k] <- df13confmatrix$byClass[1]
    specificity13[k] <- df13confmatrix$byClass[2]
    precision13[k] <- df13confmatrix$byClass[3]
    accuracy13[k] <- df13confmatrix$overall[1]
    senplusspec13[k] <- sensitivity13[k] + specificity13[k]
    senminusspec13[k] <- abs(sensitivity13[k] - specificity13[k])
    
    k <- k + 1
    k
}

senplusspec1





#--------------------Old stuff below, used to test various models--------------






# Build the xgboost model
#xgbmod1 <- xgboost(data = data.matrix(df1[,c(2:7)]),label = df1$attrited,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 2, eval_metric="error", booster="gbtree",silent=1))
#xgbmod2 <- xgboost(data = data.matrix(df2[,c(2:7)]),label = df2$attrited,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 2, eval_metric="error", booster="gbtree",silent=1))
#xgbmod3 <- xgboost(data = data.matrix(df3[,c(2:7)]),label = df3$attrited,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 2, eval_metric="error", booster="gbtree",silent=1))
#xgbmod4 <- xgboost(data = data.matrix(df4[,c(2:7)]),label = df4$attrited,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 2, eval_metric="error", booster="gbtree",silent=1))
#xgbmod5 <- xgboost(data = data.matrix(df5[,c(2:7)]),label = df5$attrited,nrounds = 250, verbose = 1,params = list(objective = "binary:logistic",eta=0.05,max.depth = 2, eval_metric="error", booster="gbtree",silent=1))




# try gbm
#gbmmod1 <- gbm(attrited ~ . ,data = df1[,8:14], distribution = "bernoulli", shrinkage = 0.3, n.trees = 200, bag.fraction = 0.7, n.cores = 2 ,cv.folds=4)
#df6$gbmpred1 <- predict(gbmmod1,newdata = df6, n.trees = 200,type="response")
#roc(response = df6$attrited, predictor = df6$gbmpred1,plot=TRUE) #0.7533
#gbm.perf(gbmmod1)


# For XGBoost
#df6$xgbpred1 <- predict(xgbmod1,data.matrix(df6[,c(2:7)]))
#df6$xgbpred2 <- predict(xgbmod2,data.matrix(df6[,c(2:7)]))
#df6$xgbpred3 <- predict(xgbmod3,data.matrix(df6[,c(2:7)]))
#df6$xgbpred4 <- predict(xgbmod4,data.matrix(df6[,c(2:7)]))
#df6$xgbpred5 <- predict(xgbmod5,data.matrix(df6[,c(2:7)]))



df6$nnensemblepred <- rowSums(df6[,9:13])/5
roc(response = df6$attrited, predictor = df6$nnensemblepred,plot=TRUE) #0.7533


# Check ROC for individual NN models
roc(response = df6$attrited, predictor = df6$nnpred1) #0.7516
roc(response = df6$attrited, predictor = df6$nnpred2) #0.7523
roc(response = df6$attrited, predictor = df6$nnpred3) #0.752
roc(response = df6$attrited, predictor = df6$nnpred4) #0.7445
roc(response = df6$attrited, predictor = df6$nnpred5) #0.7443


# Check ROC for individual XGB models
roc(response = df6$attrited, predictor = df6$xgbpred1) #0.7521
roc(response = df6$attrited, predictor = df6$xgbpred2) #0.7518
roc(response = df6$attrited, predictor = df6$xgbpred3) #0.7526
roc(response = df6$attrited, predictor = df6$xgbpred4) #0.7517
roc(response = df6$attrited, predictor = df6$xgbpred5) #0.7522


### get predictions and ROC for df7

# For Neural Network
df7$nnpred1 <- predict(nnmod1, newdata = df7,type = "raw")
df7$nnpred2 <- predict(nnmod2, newdata = df7,type = "raw")
df7$nnpred3 <- predict(nnmod3, newdata = df7,type = "raw")
df7$nnpred4 <- predict(nnmod4, newdata = df7,type = "raw")
df7$nnpred5 <- predict(nnmod5, newdata = df7,type = "raw")


# For XGBoost
df7$xgbpred1 <- predict(xgbmod1,data.matrix(df7[,c(2:7)]))
df7$xgbpred2 <- predict(xgbmod2,data.matrix(df7[,c(2:7)]))
df7$xgbpred3 <- predict(xgbmod3,data.matrix(df7[,c(2:7)]))
df7$xgbpred4 <- predict(xgbmod4,data.matrix(df7[,c(2:7)]))
df7$xgbpred5 <- predict(xgbmod5,data.matrix(df7[,c(2:7)]))

# For Neural Network
df1$nnpred1 <- predict(nnmod1, newdata = df1,type = "raw")
df1$nnpred2 <- predict(nnmod2, newdata = df1,type = "raw")
df1$nnpred3 <- predict(nnmod3, newdata = df1,type = "raw")
df1$nnpred4 <- predict(nnmod4, newdata = df1,type = "raw")
df1$nnpred5 <- predict(nnmod5, newdata = df1,type = "raw")
df1$nnensemblepred <- rowSums(df1[,15:19])/5
roc(response = df1$attrited, predictor = df1$nnensemblepred) #0.755



df7$nnensemblepred <- rowSums(df7[,27:31])/5
roc(response = df7$attrited, predictor = df7$nnensemblepred) #0.755

df7$xgbensemblepred <- rowSums(df7[,14:18])/5
roc(response = df7$attrited, predictor = df7$xgbensemblepred) #0.7551

# Check ROC for individual NN models
roc(response = df7$attrited, predictor = df7$nnpred1) #0.7538
roc(response = df7$attrited, predictor = df7$nnpred2) #0.7537
roc(response = df7$attrited, predictor = df7$nnpred3) #0.754
roc(response = df7$attrited, predictor = df7$nnpred4) #
roc(response = df7$attrited, predictor = df7$nnpred5) #


# Check ROC for individual XGB models
roc(response = df7$attrited, predictor = df7$xgbpred1) #0.7537
roc(response = df7$attrited, predictor = df7$xgbpred2) #0.7538
roc(response = df7$attrited, predictor = df7$xgbpred3) #
roc(response = df7$attrited, predictor = df7$xgbpred4) #
roc(response = df7$attrited, predictor = df7$xgbpred5) #



# Try NN with scaled variables
# Neural network
nnmod11 <- nnet(attrited ~ . , data = df1[,8:14],decay = 0.05, maxit = 10000, MaxNWts = 10000, size = 6, trace = T)
nnmod12 <- nnet(attrited ~ . , data = df2[,8:14],decay = 0.05, maxit = 10000, MaxNWts = 10000, size = 6, trace = T)
nnmod13 <- nnet(attrited ~ . , data = df3[,8:14],decay = 0.05, maxit = 10000, MaxNWts = 10000, size = 6, trace = T)
nnmod14 <- nnet(attrited ~ . , data = df4[,8:14],decay = 0.05, maxit = 10000, MaxNWts = 10000, size = 6, trace = T)
nnmod15 <- nnet(attrited ~ . , data = df5[,8:14],decay = 0.05, maxit = 10000, MaxNWts = 10000, size = 6, trace = T)

# Neural network
nnmod111 <- nnet(attrited ~ . , data = df1[,8:14],decay = 0.01, maxit = 10000, MaxNWts = 10000, size = 6, trace = T)
nnmod112 <- nnet(attrited ~ . , data = df1[,8:14],decay = 0.02, maxit = 10000, MaxNWts = 10000, size = 6, trace = T)
nnmod113 <- nnet(attrited ~ . , data = df1[,8:14],decay = 0.03, maxit = 10000, MaxNWts = 10000, size = 6, trace = T)
nnmod114 <- nnet(attrited ~ . , data = df1[,8:14],decay = 0.04, maxit = 10000, MaxNWts = 10000, size = 6, trace = T)
nnmod115 <- nnet(attrited ~ . , data = df1[,8:14],decay = 0.05, maxit = 10000, MaxNWts = 10000, size = 6, trace = T)




# For Neural Network
df7$nnpred18 <- predict(nnmod18, newdata = df7,type = "raw")
df7$nnpred12 <- predict(nnmod12, newdata = df7,type = "raw")
df7$nnpred13 <- predict(nnmod13, newdata = df7,type = "raw")
df7$nnpred14 <- predict(nnmod14, newdata = df7,type = "raw")
df7$nnpred15 <- predict(nnmod15, newdata = df7,type = "raw")

# For Neural Network
df7$nnpred111 <- predict(nnmod111, newdata = df7,type = "raw")
df7$nnpred112 <- predict(nnmod112, newdata = df7,type = "raw")
df7$nnpred113 <- predict(nnmod113, newdata = df7,type = "raw")
df7$nnpred114 <- predict(nnmod114, newdata = df7,type = "raw")
df7$nnpred115 <- predict(nnmod115, newdata = df7,type = "raw")

df7$nnensemblepred11 <- rowSums(df7[,21:25])/5
roc(response = df7$attrited, predictor = df7$nnensemblepred11) #0.7538


# Check ROC for individual NN models
roc(response = df7$attrited, predictor = df7$nnpred112) #0.7534
roc(response = df7$attrited, predictor = df7$nnpred18) #0.7537
roc(response = df7$attrited, predictor = df7$nnpred13) #0.754
roc(response = df7$attrited, predictor = df7$nnpred14) #
roc(response = df7$attrited, predictor = df7$nnpred15) #

df7$nnensemblepred <- rowSums(df7[,15:19])/5
roc(response = df7$attrited, predictor = df7$nnensemblepred) #0.755


# Save the nn models
save(nnmod1,file="nnmod1.RDA")
save(nnmod2,file="nnmod2.RDA")
save(nnmod3,file="nnmod3.RDA")
save(nnmod4,file="nnmod4.RDA")
save(nnmod5,file="nnmod5.RDA")










# Test nn for multiple weight decays

nnmod11 <- nnet(attrited ~ . , data = df2[,2:8],decay = 0.00001, maxit = 10000, MaxNWts = 10000, size = 6, trace = T)
nnmod12 <- nnet(attrited ~ . , data = df2[,2:8],decay = 0.00005, maxit = 10000, MaxNWts = 10000, size = 6, trace = T)
nnmod13 <- nnet(attrited ~ . , data = df2[,2:8],decay = 0.0001, maxit = 10000, MaxNWts = 10000, size = 6, trace = T)
nnmod14 <- nnet(attrited ~ . , data = df2[,2:8],decay = 0.0005, maxit = 10000, MaxNWts = 10000, size = 6, trace = T)
nnmod15 <- nnet(attrited ~ . , data = df2[,2:8],decay = 0.001, maxit = 10000, MaxNWts = 10000, size = 6, trace = T)
nnmod16 <- nnet(attrited ~ . , data = df2[,2:8],decay = 0.005, maxit = 10000, MaxNWts = 10000, size = 20, trace = T)
nnmod17 <- nnet(attrited ~ . , data = df2[,2:8],decay = 0.01, maxit = 10000, MaxNWts = 10000, size = 6, trace = T)
nnmod18 <- nnet(attrited ~ . , data = df2[,2:8],decay = 0.05, maxit = 10000, MaxNWts = 10000, size = 15, trace = T)
nnmod19 <- nnet(attrited ~ . , data = df2[,2:8],decay = 0.1, maxit = 10000, MaxNWts = 10000, size = 6, trace = T)
nnmod110 <- nnet(attrited ~ . , data = df2[,2:8],decay = 0.5, maxit = 10000, MaxNWts = 10000, size = 6, trace = T)
nnmod111 <- nnet(attrited ~ . , data = df2[,2:8],decay = 1, maxit = 10000, MaxNWts = 10000, size = 6, trace = T)
nnmod112 <- nnet(attrited ~ . , data = df2[,2:8],decay = 0.06, maxit = 10000, MaxNWts = 10000, size = 15, trace = T)
nnmod113 <- nnet(attrited ~ . , data = df2[,2:8],decay = 0.07, maxit = 10000, MaxNWts = 10000, size = 6, trace = T)
nnmod114 <- nnet(attrited ~ . , data = df2[,2:8],decay = 0.08, maxit = 10000, MaxNWts = 10000, size = 6, trace = T)
nnmod115 <- nnet(attrited ~ . , data = df2[,2:8],decay = 0.09, maxit = 10000, MaxNWts = 10000, size = 6, trace = T)
nnmod116 <- nnet(attrited ~ . , data = df2[,2:8],decay = 0.04, maxit = 10000, MaxNWts = 10000, size = 6, trace = T)
nnmod117 <- nnet(attrited ~ . , data = df2[,2:8],decay = 0.03, maxit = 10000, MaxNWts = 10000, size = 6, trace = T)
nnmod118 <- nnet(attrited ~ . , data = df2[,2:8],decay = 0.02, maxit = 10000, MaxNWts = 10000, size = 6, trace = T)
nnmod119 <- nnet(attrited ~ . , data = df2[,2:8],decay = 0.009, maxit = 10000, MaxNWts = 10000, size = 6, trace = T)










# Try RF Model
library(randomForest)
tuneRF(y = factor(df1$attrited),x = df1[,c(9:14)],mtryStart = 3,ntreeTry = 200,stepFactor = 1,trace = T,plot = T)

rfmod11 <- randomForest(y = factor(df1$attrited),x = df1[,c(2:7)],mtry = 2,ntree = 500,importance = T)
rfmod11$importance
df6$rfpred11 <- predict(rfmod11, newdata = df6,type = "prob")[,2]
roc(response = df6$attrited, predictor = df6$rfpred11) #0.7449

rfmod2 <- randomForest(y = train.data$attrited,x = train.data[,c(2:7)],mtry = 3,ntree = 200,importance = T)
rfmod2$importance
rfpred2 <- predict(rfmod2, newdata = validation.data,type = "prob")
validation.data$rfpred2 <- rfpred2[,2]
roc(response = validation.data$attrited, predictor = validation.data$rfpred2) #0.732

# Ensemble
validation.data$meanpred <- rowSums(validation.data[,9:13])/5
roc(response = validation.data$attrited, predictor = validation.data$meanpred) #0.7525


# Try SVM
library(e1071)
# Radial kernel
TS <- Sys.time()
svm.tune1 = tune(svm,train.y = factor(df1$attrited), train.x = df1[,c(2:7)],validation.y = factor(df3$attrited),validation.x = df3[,c(2:7)],
                 type = "C", kernel = "radial",ranges = list(cost=c(0.1,1,10)))
TE <- Sys.time()
print(TE-TS)
summary(svm.tune1)

# Sigmoid kernel
TS <- Sys.time()
svm.tune2 = tune(svm,train.y = factor(df1$attrited), train.x = df1[,c(2:7)],validation.y = factor(df3$attrited) ,validation.x = df3[,c(2:7)],
                 type = "C", kernel = "sigmoid",ranges = list(cost=c(0.1,1,10)))
TE <- Sys.time()
print(TE-TS)
summary(svm.tune2)

# Poly kernel
TS <- Sys.time()
svm.tune3 = tune(svm,train.y = factor(df1$attrited) , train.x = df1[,c(2:7)],validation.y = factor(df3$attrited),validation.x = df3[,c(2:7)],
                 type = "C", kernel = "poly",ranges = list(cost=c(0.1,1,10)))
TE <- Sys.time()
print(TE-TS)
summary(svm.tune3)

# Linear kernel
TS <- Sys.time()
svm.tune4 = tune(svm,train.y = factor(df1$attrited), train.x = df1[,c(2:7)],validation.y = factor(df3$attrited), validation.x = df3[,c(2:7)],
                 type = "C", kernel = "linear",ranges = list(cost=c(0.1,1,10)))
TE <- Sys.time()
print(TE-TS)
summary(svm.tune4)



# Build the best model
TS <- Sys.time()
svmmod1 <- svm(y=factor(df1$attrited),x=df1[,c(2:7)],kernel = "radial",cost=1,probability = T)
TE <- Sys.time()
print(TE-TS)

svmpred <- predict(svmmod1,df3[,c(2:7)],probability = T)
svmpred1 <- attr(svmpred,"probabilities")
df3$svmpred <- svmpred1[,1]
roc(response = df3$attrited, predictor = df3$svmpred) #0.734

df3$svmpred1 <- attr(predict(svmmod1,df3[,c(2:7)],probability = T),"probabilities")[,1]
df1$attrited <- as.factor(df1$attrited)
df3$attrited <- as.factor(df3$attrited)






# Try the xgboost algorithm
df1xgbtrain <- xgb.DMatrix(df1[,c(2:8)],label = df1$attrited)

xgbmod1 <- xgboost(data = data.matrix(df1[,c(2:7)]),label = df1$attrited,nrounds = 250, verbose = 1,
                   params = list(objective = "binary:logistic",eta=0.05,gamma=0,
                                                       max.depth = 2, eval_metric="error", booster="gbtree",silent=1))


df3$xgbpred1 <- predict(xgbmod1,data.matrix(df3[,c(2:7)]))
roc(response = df3$attrited, predictor = df3$xgbpred1) #0.7412


# Logistic
lmmod <- glm(data = df1[,c(2:8)], factor(attrited) ~ .,family = "binomial" )
summary(lmmod)
