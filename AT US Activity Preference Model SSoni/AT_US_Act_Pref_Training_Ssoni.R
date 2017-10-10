library(dplyr);library(gbm);library(ff);library(ffbase);library(readr);

# Read the table which is is txt format
df <- read.table("C:/Users/Ssoni/Documents/Athleta/Activity Preference/at_actpref_training_new_qrtrwise",header = F,sep="|")

# Set the column names for the data frame
names(df) <- c("masterkey",
               "activity",
               "nrml_items_abandoned",
               "nrml_items_browsed",
               "nrml_items_purch_first_qrtr",
               "nrml_items_purch_second_qrtr",
               "nrml_items_purch_third_qrtr",
               "nrml_items_purch_fourth_qrtr",
               "recency_abandon",
               "recency_click",
               "items_purch_pred_window")

# Sort the table by masterkey and activity
df <- df %>% arrange(masterkey,activity)

# Limit the target variable on the top by trimming it at 99.9 percentile
ul<-quantile(df$items_purch_pred_window,0.999)
df$items_purch_pred_window <- ifelse(df$items_purch_pred_window>ul, ul, df$items_purch_pred_window)


df$weighted_purchase <- (df$nrml_items_purch_fourth_qrtr + (0.75*df$nrml_items_purch_third_qrtr)
                        + (0.5*df$nrml_items_purch_second_qrtr) + (0.25*df$nrml_items_purch_first_qrtr))/2.5
# Get the list of distinct masterkeys
all.masterkey <- df %>% select(masterkey) %>% distinct()

# Create the train an test datasets based upon random sampling of masterkeys
index.train <- bigsample(1:nrow(all.masterkey),size = 200000,replace = F,prob = NULL)
train.masterkey <- data.frame(all.masterkey[ index.train, ])
test.masterkey  <- data.frame(all.masterkey[-index.train, ])

names(train.masterkey) <- "masterkey"
names(test.masterkey) <- "masterkey"

df.train <- merge(df, train.masterkey, by='masterkey')
df.test  <- merge(df, test.masterkey,  by='masterkey')

# Build the gbm ndcg model
TS <- Sys.time()
gbm.ndcg <- gbm(items_purch_pred_window ~
                  nrml_items_abandoned + nrml_items_browsed+
                  weighted_purchase +
                  recency_abandon + recency_click,         
                data=df.train,     
                distribution=list(name='pairwise', metric="ndcg", group='masterkey'),    
                n.trees=3000,        
                shrinkage=0.005,     
                interaction.depth=3, 
                bag.fraction = 0.5,  
                train.fraction = 1,  
                n.minobsinnode = 10, 
                keep.data=TRUE,      
                cv.folds=10
)    
TE <- Sys.time()
print(TE-TS)
save(gbm.ndcg,file="gbmmod.RDA")

# Check the best performance number of iterations
gbm.ndcg.perf <- gbm.perf(gbm.ndcg, method='cv')

# Make predictions on the training dataset
score <- predict(gbm.ndcg, df.train, gbm.ndcg.perf)
df.train.score <- cbind(df.train, score) 
df.train.score$training = 1

# Make predictions on the test dataset
score <- predict(gbm.ndcg, df.test, gbm.ndcg.perf)
df.test.score <- cbind(df.test, score)
df.test.score$training = 0

# Combine the two predictions to make the final scored data frame
df.score <- rbind(df.train.score, df.test.score)


write.csv(df.score,"at_activity_pref_scores.csv",row.names=F)
write.csv(df.test.score,"at_act_pref_test_scores.csv",row.names=F)
#saveRDS(df.test.score,file = "df.test.score.RDS")