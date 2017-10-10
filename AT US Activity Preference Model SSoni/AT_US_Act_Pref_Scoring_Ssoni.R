#initialise libraries required
library(dplyr);library(gbm);library(ff);library(ffbase);library(readr);

#import the dataset ready for scoring
#data<-read.table('//10.8.8.51/lv0/ShrikantSoni/Athleta/Activity Preference/at_actpref_score1',sep='|', header=FALSE)
data <- read.table('C:/Users/ssoni/Documents/Athleta/Activity_Preference/at_act_pref_custkey_export',sep='|', header=FALSE)

#insert headers
names(data) <- c("customerkey","masterkey",
               "activity",
               "nrml_items_abandoned",
               "nrml_items_browsed",
               "nrml_items_purch_first_qrtr",
               "nrml_items_purch_second_qrtr",
               "nrml_items_purch_third_qrtr",
               "nrml_items_purch_fourth_qrtr",
               "recency_abandon",
               "recency_click"
               )


# Sort the table by masterkey and activity
data <- data %>% arrange(masterkey,customerkey,activity)

# Get the list of distinct masterkeys
#all.masterkey <- data %>% select(masterkey) %>% distinct()

#create the additional variable -
data$weighted_purchase <- (data$nrml_items_purch_fourth_qrtr + (0.75*data$nrml_items_purch_third_qrtr)
                         + (0.5*data$nrml_items_purch_second_qrtr) + (0.25*data$nrml_items_purch_first_qrtr))/2.5


#load the model object
 load("//10.8.8.51/lv0/ShrikantSoni/Athleta/Activity Preference/gbmmod.rda")

# obtain the best performance number of iterations
gbm.ndcg.perf <- gbm.perf(gbm.ndcg, method='cv')


#score the dataset
score <- predict(gbm.ndcg, data, gbm.ndcg.perf)
data$score<-score



# Perform transformations to get the file in required format of customerkey and activities in decreasing preference
data.subset <- data %>% arrange(masterkey,customerkey,desc(score),activity) %>% select(customerkey,activity,score)

data.subset <- data.subset %>% group_by(customerkey) %>% mutate(rankorder = row_number())

data.rank1 <- data.subset %>% filter(rankorder == 1)
data.rank1 <- rename(data.rank1,Activity1 = activity)


data.rank2 <- data.subset %>% filter(rankorder == 2)
data.rank2 <- rename(data.rank2,Activity2 = activity)

data.rank3 <- data.subset %>% filter(rankorder == 3)
data.rank3 <- rename(data.rank3,Activity3 = activity)

data.rank4 <- data.subset %>% filter(rankorder == 4)
data.rank4 <- rename(data.rank4,Activity4 = activity)

data.activity <- (data.rank1 %>% select(customerkey,Activity1)) %>% left_join(data.rank2 %>% select(customerkey,Activity2)) %>%
  left_join(data.rank3 %>% select(customerkey,Activity3)) %>% left_join(data.rank4 %>% select(customerkey,Activity4))

# Save the formatted output file, change the path according to your local paths. Have already saved these on Box sync
saveRDS(data.activity,file = "AT_Act_Pref_Custkey_Activity_Ranking_Mar2017.RDS")
write.csv(data.activity,file="AT_Act_Pref_Custkey_Activity_Ranking_Mar2017.csv",row.names = F)


