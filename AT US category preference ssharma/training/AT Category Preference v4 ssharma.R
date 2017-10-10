library(gbm)
library(dplyr)
library(sqldf)
library(ff)
library(ffbase)
library(kernlab)
library(readr)

at.data <- read_delim(file="//10.8.8.51/lv0/Saumya/Datasets/at_lastmnth_rec.txt", 
                      col_names=F, delim="|", n_max=-1)


colnames(at.data)<-c("masterkey",
                     "category",
                     "nrml_items_abandoned",
                     "nrml_items_browsed",
                     "nrml_items_purchased",
                     "nrml_items_purch_first",
                     "items_purch_pred",
                     "nrml_items_purch_lastmnth",
                     "nrml_items_abandoned_lastmnth",
                     "nrml_items_browsed_lastmnth",
                     "recency_purch",
                     "recency_abandon",
                     "recency_click"
                     )


summary(at.data)

at.data$masterkey <- factor(at.data$masterkey)
at.data$category  <- factor(at.data$category)

ul<-quantile(at.data$items_purch_pred,0.995)
at.data$items_purch_pred <- ifelse(at.data$items_purch_pred>ul, ul, at.data$items_purch_pred)

summary(at.data)

all.masterkey <- unique(at.data[,1])
index.train <- bigsample(1:nrow(all.masterkey), size=1000000, replace=F)
train.masterkey <- all.masterkey[ index.train, ]
test.masterkey  <- all.masterkey[-index.train, ]

at.train <- merge(at.data, train.masterkey, by='masterkey')
at.test  <- merge(at.data, test.masterkey,  by='masterkey')

gbm.ndcg <- gbm(items_purch_pred ~
                                  nrml_items_abandoned+nrml_items_browsed+
                                  nrml_items_purchased+nrml_items_purch_first+
                                  nrml_items_purch_lastmnth+nrml_items_abandoned_lastmnth+
                                  nrml_items_browsed_lastmnth+recency_purch+
                                  recency_abandon+recency_click
                ,         
                data=at.train,     
                distribution=list(name='pairwise', metric="ndcg", group='masterkey'),    
                n.trees=2000,        
                shrinkage=0.005,     
                interaction.depth=3, 
                bag.fraction = 0.5,  
                train.fraction = 1,  
                n.minobsinnode = 10, 
                keep.data=TRUE,      
                cv.folds=10
                )    

save(gbm.ndcg, file='//10.8.8.51/lv0/Tanumoy/Datasets/Parameter Files/Category Preference/AT/at_normal_train_v2.RData')

load("//10.8.8.51/lv0/Tanumoy/Datasets/Parameter Files/Category Preference/AT/at_normal_train_v2.RData")
gbm.ndcg.perf <- gbm.perf(gbm.ndcg, method='cv')
score <- predict(gbm.ndcg, at.train, gbm.ndcg.perf)
at.train.score <- cbind(at.train, score) 
at.train.score$training = 1

score <- predict(gbm.ndcg, at.test, gbm.ndcg.perf)
at.test.score <- cbind(at.test, score)
at.test.score$training = 0

at.score <- rbind(at.train.score, at.test.score)
write.table(at.score, file="//10.8.8.51/lv0/Tanumoy/Datasets/Model Replication/at_normal_first_score_v2.txt", 
            col.names=T, row.names=F, sep=",")


masterkey <- factor(at.train$masterkey)
masterkeyid <- as.numeric(masterkey)

category <- factor(at.train$category)
categoryid <- as.numeric(category)

var1 <- paste0("1:", at.train[,3])
var2 <- paste0("2:", at.train[,4])
var3 <- paste0("3:", at.train[,5])
var4 <- paste0("4:", at.train[,6])

var0 <- at.train[,7]
  
var5 <- paste0("5:", at.train[,8])
var6 <- paste0("6:", at.train[,9])
var7 <- paste0("7:", at.train[,10])
var8 <- paste0("8:", at.train[,11])
var9 <- paste0("9:", at.train[,12])
var10 <- paste0("10:", at.train[,13])


data.ranklib.train <- cbind(var0,
                            paste0("qid:", masterkeyid), paste0("cid:", categoryid),
                            var1, var2, var3, var4, var5, var6, var7, var8, var9, var10
                            )

data.ranklib.train.java <- data.ranklib.train[,c(1,2,4:13)]

write.table(data.ranklib.train.java, file="//10.8.8.51/lv0/Tanumoy/Datasets/Model Replication/at_normal_first_train_v2.txt", 
            col.names=F, row.names=F, sep=" ")
write.table(train.masterkey, file="//10.8.8.51/lv0/Tanumoy/Datasets/Model Replication/at_masterkey_train_v2.txt", 
            col.names=F, row.names=F, sep=" ")

train.masterkey <- read.delim("//10.8.8.51/lv0/Tanumoy/Datasets/Model Replication/at_masterkey_train.txt", header=F)
colnames(train.masterkey) <- c('masterkey')


x <- readLines("//10.8.8.51/lv0/Tanumoy/Datasets/Model Replication/at_normal_first_train_v2.txt")
y <- gsub( '"', '', x )
cat(y, file="//10.8.8.51/lv0/Tanumoy/Datasets/Model Replication/at_normal_first_train_v2.txt", sep="\n")




masterkey <- factor(at.test$masterkey)
masterkeyid <- as.numeric(masterkey)

category <- factor(at.test$category)
categoryid <- as.numeric(category)

var1 <- paste0("1:", at.test[,3])
var2 <- paste0("2:", at.test[,4])
var3 <- paste0("3:", at.test[,5])
var4 <- paste0("4:", at.test[,6])

var0 <- at.test[,7]

var5 <- paste0("1:", at.test[,8])
var6 <- paste0("2:", at.test[,9])
var7 <- paste0("3:", at.test[,10])
var8 <- paste0("4:", at.test[,11])
var9 <- paste0("1:", at.test[,12])
var10 <- paste0("2:", at.test[,13])


data.ranklib.test <- cbind(var0,
                           paste0("qid:", masterkeyid), paste0("cid:", categoryid),
                           var1, var2, var3, var4, var5, var6, var7, var8, var9, var10
)

data.ranklib.test.java <- data.ranklib.test[,c(1,2,4:13)]

write.table(data.ranklib.test.java, file="//10.8.8.51/lv0/Tanumoy/Datasets/Model Replication/at_normal_first_test_v2.txt", 
            col.names=F, row.names=F, sep=" ")
write.table(test.masterkey, file="//10.8.8.51/lv0/Tanumoy/Datasets/Model Replication/at_masterkey_test_v2.txt", 
            col.names=F, row.names=F, sep=" ")

test.masterkey <- read.delim("//10.8.8.51/lv0/Tanumoy/Datasets/Model Replication/at_masterkey_test.txt", header=F)
colnames(test.masterkey) <- c('masterkey')


x <- readLines("//10.8.8.51/lv0/Tanumoy/Datasets/Model Replication/at_normal_first_test_v2.txt")
y <- gsub( '"', '', x )
cat(y, file="//10.8.8.51/lv0/Tanumoy/Datasets/Model Replication/at_normal_first_test_v2.txt", sep="\n")
