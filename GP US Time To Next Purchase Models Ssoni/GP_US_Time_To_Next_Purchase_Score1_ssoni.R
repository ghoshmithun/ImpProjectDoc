#initialise libraries required
install.packages(c("dplyr","gbm","readr"))
library(dplyr);library(gbm);library(readr)

# load the R environment which contains all the models
load('//10.8.8.51/lv0/ShrikantSoni/Gap/Time To Next Purchase Models/GP_Time_To_Next_Purchase_All_Models.RData')

#import the score.dataset 
####### change the path here to the new file that you will export from hive
score.data<-read.table('C:/Users/ssoni/Documents/Gap/Prob Next Purchase 1 Month/gp_prob_purch_1month_export',sep='|', header=FALSE)

#insert headers
names(score.data) <- c("customer_key",
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
                       "lastyear_same3months_purchase")



# Cap a few variables to 99.5%ile
score.data$total_orders_placed <- ifelse(score.data$total_orders_placed > 33, 33, score.data$total_orders_placed)                 
score.data$items_browsed <- ifelse(score.data$items_browsed > 125,125,score.data$items_browsed)
score.data$items_added <- ifelse(score.data$items_added > 55,55,score.data$items_added )
score.data$ratio_purchases <- ifelse(score.data$ratio_purchases > 10,10,score.data$ratio_purchases)


# Recalculate the frequency
score.data$freq_purch <- score.data$total_orders_placed / score.data$active_period
score.data$freq_purch <- ifelse(score.data$freq_purch > 5,5,score.data$freq_purch)



score.data.subset <- sample_n(score.data,size = 150000)


# 1 month using xgb
library(xgboost)

score.data.subset$xgb1mpred1a <- predict(xgb1mmod1a,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,12)]))
score.data.subset$xgb1mpred1b <- predict(xgb1mmod1b,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,12)]))
score.data.subset$xgb1mpred1c <- predict(xgb1mmod1c,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,12)]))

score.data.subset$xgb1mpred2a <- predict(xgb1mmod2a,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,12)]))
score.data.subset$xgb1mpred2b <- predict(xgb1mmod2b,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,12)]))
score.data.subset$xgb1mpred2c <- predict(xgb1mmod2c,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,12)]))

score.data.subset$xgb1mpred3a <- predict(xgb1mmod3a,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,12)]))
score.data.subset$xgb1mpred3b <- predict(xgb1mmod3b,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,12)]))
score.data.subset$xgb1mpred3c <- predict(xgb1mmod3c,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,12)]))

score.data.subset$xgb1mpred4a <- predict(xgb1mmod4a,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,12)]))
score.data.subset$xgb1mpred4b <- predict(xgb1mmod4b,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,12)]))
score.data.subset$xgb1mpred4c <- predict(xgb1mmod4c,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,12)]))

score.data.subset$xgb1mpred5a <- predict(xgb1mmod5a,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,12)]))
score.data.subset$xgb1mpred5b <- predict(xgb1mmod5b,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,12)]))
score.data.subset$xgb1mpred5c <- predict(xgb1mmod5c,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,12)]))

score.data.subset$xgb1mpred6a <- predict(xgb1mmod6a,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,12)]))
score.data.subset$xgb1mpred6b <- predict(xgb1mmod6b,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,12)]))
score.data.subset$xgb1mpred6c <- predict(xgb1mmod6c,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,12)]))

score.data.subset$xgb1mpred7a <- predict(xgb1mmod7a,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,12)]))
score.data.subset$xgb1mpred7b <- predict(xgb1mmod7b,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,12)]))
score.data.subset$xgb1mpred7c <- predict(xgb1mmod7c,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,12)]))

score.data.subset$xgb1mpred8a <- predict(xgb1mmod8a,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,12)]))
score.data.subset$xgb1mpred8b <- predict(xgb1mmod8b,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,12)]))
score.data.subset$xgb1mpred8c <- predict(xgb1mmod8c,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,12)]))

score.data.subset$xgb1mpred9a <- predict(xgb1mmod9a,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,12)]))
score.data.subset$xgb1mpred9b <- predict(xgb1mmod9b,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,12)]))
score.data.subset$xgb1mpred9c <- predict(xgb1mmod9c,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,12)]))

score.data.subset$xgb1mpred10a <- predict(xgb1mmod10a,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,12)]))
score.data.subset$xgb1mpred10b <- predict(xgb1mmod10b,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,12)]))
score.data.subset$xgb1mpred10c <- predict(xgb1mmod10c,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,12)]))

# change the column indices here accordingly
score.data.subset$medianxgb1mpred <- apply(score.data.subset[,14:43],1,median)


# 3 months using xgb


score.data.subset$xgb3mpred1a <- predict(xgb3mmod1a,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb3mpred1b <- predict(xgb3mmod1b,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb3mpred1c <- predict(xgb3mmod1c,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))

score.data.subset$xgb3mpred2a <- predict(xgb3mmod2a,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb3mpred2b <- predict(xgb3mmod2b,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb3mpred2c <- predict(xgb3mmod2c,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))

score.data.subset$xgb3mpred3a <- predict(xgb3mmod3a,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb3mpred3b <- predict(xgb3mmod3b,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb3mpred3c <- predict(xgb3mmod3c,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))

score.data.subset$xgb3mpred4a <- predict(xgb3mmod4a,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb3mpred4b <- predict(xgb3mmod4b,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb3mpred4c <- predict(xgb3mmod4c,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))

score.data.subset$xgb3mpred5a <- predict(xgb3mmod5a,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb3mpred5b <- predict(xgb3mmod5b,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb3mpred5c <- predict(xgb3mmod5c,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))

score.data.subset$xgb3mpred6a <- predict(xgb3mmod6a,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb3mpred6b <- predict(xgb3mmod6b,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb3mpred6c <- predict(xgb3mmod6c,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))

score.data.subset$xgb3mpred7a <- predict(xgb3mmod7a,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb3mpred7b <- predict(xgb3mmod7b,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb3mpred7c <- predict(xgb3mmod7c,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))

score.data.subset$xgb3mpred8a <- predict(xgb3mmod8a,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb3mpred8b <- predict(xgb3mmod8b,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb3mpred8c <- predict(xgb3mmod8c,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))

score.data.subset$xgb3mpred9a <- predict(xgb3mmod9a,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb3mpred9b <- predict(xgb3mmod9b,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb3mpred9c <- predict(xgb3mmod9c,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))

score.data.subset$xgb3mpred10a <- predict(xgb3mmod10a,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb3mpred10b <- predict(xgb3mmod10b,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb3mpred10c <- predict(xgb3mmod10c,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))

# change the column indices accordingly
score.data.subset$medianxgb3mpred <- apply(score.data.subset[,45:74],1,median)


# 6 months using xgb


score.data.subset$xgb6mpred1a <- predict(xgb6mmod1a,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb6mpred1b <- predict(xgb6mmod1b,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb6mpred1c <- predict(xgb6mmod1c,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))

score.data.subset$xgb6mpred2a <- predict(xgb6mmod2a,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb6mpred2b <- predict(xgb6mmod2b,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb6mpred2c <- predict(xgb6mmod2c,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))

score.data.subset$xgb6mpred3a <- predict(xgb6mmod3a,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb6mpred3b <- predict(xgb6mmod3b,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb6mpred3c <- predict(xgb6mmod3c,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))

score.data.subset$xgb6mpred4a <- predict(xgb6mmod4a,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb6mpred4b <- predict(xgb6mmod4b,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb6mpred4c <- predict(xgb6mmod4c,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))

score.data.subset$xgb6mpred5a <- predict(xgb6mmod5a,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb6mpred5b <- predict(xgb6mmod5b,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb6mpred5c <- predict(xgb6mmod5c,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))

score.data.subset$xgb6mpred6a <- predict(xgb6mmod6a,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb6mpred6b <- predict(xgb6mmod6b,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb6mpred6c <- predict(xgb6mmod6c,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))

score.data.subset$xgb6mpred7a <- predict(xgb6mmod7a,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb6mpred7b <- predict(xgb6mmod7b,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb6mpred7c <- predict(xgb6mmod7c,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))

score.data.subset$xgb6mpred8a <- predict(xgb6mmod8a,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb6mpred8b <- predict(xgb6mmod8b,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb6mpred8c <- predict(xgb6mmod8c,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))

score.data.subset$xgb6mpred9a <- predict(xgb6mmod9a,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb6mpred9b <- predict(xgb6mmod9b,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb6mpred9c <- predict(xgb6mmod9c,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))

score.data.subset$xgb6mpred10a <- predict(xgb6mmod10a,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb6mpred10b <- predict(xgb6mmod10b,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb6mpred10c <- predict(xgb6mmod10c,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))

# change the column indices accordingly
score.data.subset$medianxgb6mpred <- apply(score.data.subset[,76:105],1,median)


####################### 12 months using xgb


score.data.subset$xgb12mpred1a <- predict(xgb12mmod1a,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb12mpred1b <- predict(xgb12mmod1b,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb12mpred1c <- predict(xgb12mmod1c,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))

score.data.subset$xgb12mpred2a <- predict(xgb12mmod2a,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb12mpred2b <- predict(xgb12mmod2b,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb12mpred2c <- predict(xgb12mmod2c,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))

score.data.subset$xgb12mpred3a <- predict(xgb12mmod3a,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb12mpred3b <- predict(xgb12mmod3b,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb12mpred3c <- predict(xgb12mmod3c,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))

score.data.subset$xgb12mpred4a <- predict(xgb12mmod4a,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb12mpred4b <- predict(xgb12mmod4b,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb12mpred4c <- predict(xgb12mmod4c,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))

score.data.subset$xgb12mpred5a <- predict(xgb12mmod5a,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb12mpred5b <- predict(xgb12mmod5b,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb12mpred5c <- predict(xgb12mmod5c,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))

score.data.subset$xgb12mpred6a <- predict(xgb12mmod6a,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb12mpred6b <- predict(xgb12mmod6b,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb12mpred6c <- predict(xgb12mmod6c,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))

score.data.subset$xgb12mpred7a <- predict(xgb12mmod7a,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb12mpred7b <- predict(xgb12mmod7b,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb12mpred7c <- predict(xgb12mmod7c,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))

score.data.subset$xgb12mpred8a <- predict(xgb12mmod8a,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb12mpred8b <- predict(xgb12mmod8b,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb12mpred8c <- predict(xgb12mmod8c,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))

score.data.subset$xgb12mpred9a <- predict(xgb12mmod9a,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb12mpred9b <- predict(xgb12mmod9b,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb12mpred9c <- predict(xgb12mmod9c,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))

score.data.subset$xgb12mpred10a <- predict(xgb12mmod10a,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb12mpred10b <- predict(xgb12mmod10b,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))
score.data.subset$xgb12mpred10c <- predict(xgb12mmod10c,data.matrix(score.data.subset[,c(4,5,6,7,8,9,10,11,13)]))

# change the column indices accordingly
score.data.subset$medianxgb12mpred <- apply(score.data.subset[,106:135],1,median)

