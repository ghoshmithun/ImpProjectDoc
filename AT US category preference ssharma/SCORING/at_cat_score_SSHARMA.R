library(gbm)
library(dplyr)
library(sqldf)
library(ff)
library(ffbase)
library(kernlab)
install.packages("readr")
library(readr)
library(plyr)

at.data <- read_delim(file="//10.8.8.51/lv0/Saumya/Datasets/at_lastmnth_rec_run.txt", 
                      col_names=F, delim="|")


colnames(at.data)<-c("masterkey",
                     "category",
                     "nrml_items_abandoned",
                     "nrml_items_browsed",
                     "nrml_items_purchased",
                     "nrml_items_purch_first",
                     "nrml_items_purch_lastmnth",
                     "nrml_items_abandoned_lastmnth",
                     "nrml_items_browsed_lastmnth",
                     "recency_purch",
                     "recency_abandon",
                     "recency_click")

load('//10.8.8.51/lv0/Tanumoy/Datasets/Parameter Files/Category Preference/AT/at_normal_train_v2.RData')

score <- predict(gbm.ndcg, at.data)

c_score<-cbind(at.data[,1:2],score)

arr_score<-c_score %>% group_by(masterkey) %>% arrange(desc(score))

wr_score<-arr_score[,1:2]

wr_list<-merge(wr_score,ddply(wr_score,.(masterkey),summarise,cat_List=paste0(category,collapse="|")),by="masterkey")

listing<-wr_list[,c(1,3)]

li<-unique(listing)



write.table(li, 
            file = "//10.8.8.51/lv0/Saumya/Datasets/at_cat_list.txt", sep= "\t", row.names= FALSE, col.names= FALSE,quote=FALSE)