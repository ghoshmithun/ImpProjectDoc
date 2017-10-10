#initialise libraries required
library(dplyr);library(readr);library(nnet);library(caret)

#import the dataset 
# change the path if reading file from the share drive

#score.df<-read.table('C:/Users/ssoni/Documents/Gap/Uk Attrition Model/GPUK_Attrition_Emailkey_Export_score3',sep='|', header=FALSE)
#score.df <- read.table('//10.8.8.51/lv0/ShrikantSoni/Gap/UK_Attrition_Model/GPUK_Attrition_Export_score3',sep='|', header=FALSE)
score.df<-read.table('C:/Users/ssoni/Downloads/GPUK_ATTRITION_ALL_VARIABLES_1.csv',sep=',', header=FALSE)


#insert headers
names(score.df) <- c("emailkey","customer_key",
               "weeks_since_last_purch",
               "total_purchases",
               "total_net_sales",
               "discount_percentage",
               "items_return_ratio",
               "count_distinct_divisions")

# Upper cap a few variables
score.df$total_purchases <- ifelse(score.df$total_purchases>35,35,score.df$total_purchases)
score.df$discount_percentage <- ifelse(score.df$discount_percentage > 55,55,score.df$discount_percentage)
score.df$total_net_sales <- ifelse(score.df$total_net_sales>3250,3250,score.df$total_net_sales)
score.df$items_return_ratio <- ifelse(score.df$items_return_ratio > 1, 1, score.df$items_return_ratio)


# Load the models
load("//10.8.8.51/lv0/ShrikantSoni/Gap/UK_Attrition_Model/Saved_NN_Models/nnmod1a.RDA")
load("//10.8.8.51/lv0/ShrikantSoni/Gap/UK_Attrition_Model/Saved_NN_Models/nnmod1b.RDA")
load("//10.8.8.51/lv0/ShrikantSoni/Gap/UK_Attrition_Model/Saved_NN_Models/nnmod1c.RDA")

load("//10.8.8.51/lv0/ShrikantSoni/Gap/UK_Attrition_Model/Saved_NN_Models/nnmod2a.RDA")
load("//10.8.8.51/lv0/ShrikantSoni/Gap/UK_Attrition_Model/Saved_NN_Models/nnmod2b.RDA")
load("//10.8.8.51/lv0/ShrikantSoni/Gap/UK_Attrition_Model/Saved_NN_Models/nnmod2c.RDA")

load("//10.8.8.51/lv0/ShrikantSoni/Gap/UK_Attrition_Model/Saved_NN_Models/nnmod3a.RDA")
load("//10.8.8.51/lv0/ShrikantSoni/Gap/UK_Attrition_Model/Saved_NN_Models/nnmod3b.RDA")
load("//10.8.8.51/lv0/ShrikantSoni/Gap/UK_Attrition_Model/Saved_NN_Models/nnmod3c.RDA")

load("//10.8.8.51/lv0/ShrikantSoni/Gap/UK_Attrition_Model/Saved_NN_Models/nnmod4a.RDA")
load("//10.8.8.51/lv0/ShrikantSoni/Gap/UK_Attrition_Model/Saved_NN_Models/nnmod4b.RDA")
load("//10.8.8.51/lv0/ShrikantSoni/Gap/UK_Attrition_Model/Saved_NN_Models/nnmod4c.RDA")

load("//10.8.8.51/lv0/ShrikantSoni/Gap/UK_Attrition_Model/Saved_NN_Models/nnmod5a.RDA")
load("//10.8.8.51/lv0/ShrikantSoni/Gap/UK_Attrition_Model/Saved_NN_Models/nnmod5b.RDA")
load("//10.8.8.51/lv0/ShrikantSoni/Gap/UK_Attrition_Model/Saved_NN_Models/nnmod5c.RDA")

load("//10.8.8.51/lv0/ShrikantSoni/Gap/UK_Attrition_Model/Saved_NN_Models/nnmod6a.RDA")
load("//10.8.8.51/lv0/ShrikantSoni/Gap/UK_Attrition_Model/Saved_NN_Models/nnmod6b.RDA")
load("//10.8.8.51/lv0/ShrikantSoni/Gap/UK_Attrition_Model/Saved_NN_Models/nnmod6c.RDA")

load("//10.8.8.51/lv0/ShrikantSoni/Gap/UK_Attrition_Model/Saved_NN_Models/nnmod7a.RDA")
load("//10.8.8.51/lv0/ShrikantSoni/Gap/UK_Attrition_Model/Saved_NN_Models/nnmod7b.RDA")
load("//10.8.8.51/lv0/ShrikantSoni/Gap/UK_Attrition_Model/Saved_NN_Models/nnmod7c.RDA")

load("//10.8.8.51/lv0/ShrikantSoni/Gap/UK_Attrition_Model/Saved_NN_Models/nnmod8a.RDA")
load("//10.8.8.51/lv0/ShrikantSoni/Gap/UK_Attrition_Model/Saved_NN_Models/nnmod8b.RDA")
load("//10.8.8.51/lv0/ShrikantSoni/Gap/UK_Attrition_Model/Saved_NN_Models/nnmod8c.RDA")

load("//10.8.8.51/lv0/ShrikantSoni/Gap/UK_Attrition_Model/Saved_NN_Models/nnmod9a.RDA")
load("//10.8.8.51/lv0/ShrikantSoni/Gap/UK_Attrition_Model/Saved_NN_Models/nnmod9b.RDA")
load("//10.8.8.51/lv0/ShrikantSoni/Gap/UK_Attrition_Model/Saved_NN_Models/nnmod9c.RDA")

load("//10.8.8.51/lv0/ShrikantSoni/Gap/UK_Attrition_Model/Saved_NN_Models/nnmod10a.RDA")
load("//10.8.8.51/lv0/ShrikantSoni/Gap/UK_Attrition_Model/Saved_NN_Models/nnmod10b.RDA")
load("//10.8.8.51/lv0/ShrikantSoni/Gap/UK_Attrition_Model/Saved_NN_Models/nnmod10c.RDA")






# Make the predictions
score.df$nnpred1a <- predict(nnmod1a, newdata = score.df,type = "raw")
score.df$nnpred1b <- predict(nnmod1b, newdata = score.df,type = "raw")
score.df$nnpred1c <- predict(nnmod1c, newdata = score.df,type = "raw")

score.df$nnpred2a <- predict(nnmod2a, newdata = score.df,type = "raw")
score.df$nnpred2b <- predict(nnmod2b, newdata = score.df,type = "raw")
score.df$nnpred2c <- predict(nnmod2c, newdata = score.df,type = "raw")

score.df$nnpred3a <- predict(nnmod3a, newdata = score.df,type = "raw")
score.df$nnpred3b <- predict(nnmod3b, newdata = score.df,type = "raw")
score.df$nnpred3c <- predict(nnmod3c, newdata = score.df,type = "raw")

score.df$nnpred4a <- predict(nnmod4a, newdata = score.df,type = "raw")
score.df$nnpred4b <- predict(nnmod4b, newdata = score.df,type = "raw")
score.df$nnpred4c <- predict(nnmod4c, newdata = score.df,type = "raw")

score.df$nnpred5a <- predict(nnmod5a, newdata = score.df,type = "raw")
score.df$nnpred5b <- predict(nnmod5b, newdata = score.df,type = "raw")
score.df$nnpred5c <- predict(nnmod5c, newdata = score.df,type = "raw")

score.df$nnpred6a <- predict(nnmod6a, newdata = score.df,type = "raw")
score.df$nnpred6b <- predict(nnmod6b, newdata = score.df,type = "raw")
score.df$nnpred6c <- predict(nnmod6c, newdata = score.df,type = "raw")

score.df$nnpred7a <- predict(nnmod7a, newdata = score.df,type = "raw")
score.df$nnpred7b <- predict(nnmod7b, newdata = score.df,type = "raw")
score.df$nnpred7c <- predict(nnmod7c, newdata = score.df,type = "raw")

score.df$nnpred8a <- predict(nnmod8a, newdata = score.df,type = "raw")
score.df$nnpred8b <- predict(nnmod8b, newdata = score.df,type = "raw")
score.df$nnpred8c <- predict(nnmod8c, newdata = score.df,type = "raw")

score.df$nnpred9a <- predict(nnmod9a, newdata = score.df,type = "raw")
score.df$nnpred9b <- predict(nnmod9b, newdata = score.df,type = "raw")
score.df$nnpred9c <- predict(nnmod9c, newdata = score.df,type = "raw")

score.df$nnpred10a <- predict(nnmod10a, newdata = score.df,type = "raw")
score.df$nnpred10b <- predict(nnmod10b, newdata = score.df,type = "raw")
score.df$nnpred10c <- predict(nnmod10c, newdata = score.df,type = "raw")


# Make the ensemble
score.df$meanpred <- rowSums(score.df[,9:38])/30
score.df$score <- apply(score.df[,9:38],1,median)

score.df.subset <- score.df %>% select(emailkey,score) %>% arrange(desc(score))
score.df.subset$segment <- ntile(score.df.subset$score,10)
score.df.subset$segment <- ifelse(score.df.subset$segment > 7, "high",ifelse(score.df.subset$segment > 4,"med","low"))

score.df.subset.control <- score.df.subset %>% group_by(segment) %>% sample_frac(0.1) %>% mutate(testcontrol = "control")
score.df.subset <- score.df.subset %>% left_join(score.df.subset.control)
score.df.subset$testcontrol[is.na(score.df.subset$testcontrol)] <- "test"
score.df.subset$market <- "GB"
score.df.subset <- score.df.subset %>% filter(emailkey != "")

score.df.subset.high <- score.df.subset %>% filter(segment == "high") 


saveRDS(score.df.subset,file="GPUK_Attrition_Score3_Feb2017.RDS")
write.csv(score.df.subset,file="gpuk_attrition_score_20170627.csv",row.names=F,col.names=T,quote = F)
write.csv(score.df.subset.high,file="gpuk_attrition_score_20170627_highsegment.csv",row.names=F,col.names=T,quote = F)
