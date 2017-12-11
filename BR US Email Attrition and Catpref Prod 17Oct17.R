

BRUS_attrition <- read.table(file = "//10.8.8.51./lv0/Move to Box/Mithun/Data/BRUS_attritionscore/BRUS_attritionscore.txt",sep="|",header=F,stringsAsFactors = F)
names(BRUS_attrition) <- c("customer_key","AttritionScore","Decile")

require(dplyr)
BRUS_attrition %>% group_by(Decile) %>% summarise(Customer_Number=n()) %>% as.data.frame()
write.table(x = BRUS_attrition,file = "//10.8.8.51/lv0/Tanumoy/Datasets/BRUS_Camp17Oct17/BRUS_attrition.csv",sep=",",row.names=F,col.names=T,append=F)




BRUS_emailresponse <- read.table(file = "//10.8.8.51./lv0/Move to Box/Mithun/Data/BRUS_emailresponse/BRUS_emailresponse.txt",sep="|",header=F,stringsAsFactors = F)
names(BRUS_emailresponse) <- c("Customer_Key","EmailScore","Decile")
write.table(x = BRUS_emailresponse,file = "//10.8.8.51/lv0/Tanumoy/Datasets/BRUS_Camp17Oct17/BRUS_emailresponse.csv",sep=",",row.names=F,col.names=T,append=F)



brus_catpref <- read.table(file = "//10.8.8.51./lv0/Move to Box/Mithun/Data/brus_catpref/brus_catpref.txt",sep="|",header=F,stringsAsFactors = F)
names(brus_catpref) <- c('customer_key','Attribute')
require(dplyr)
brus_catpref2 <- brus_catpref %>% group_by(customer_key) %>% summarise(Attr = paste(Attribute,collapse=" "))

brus_catpref3 <- cbind.data.frame(brus_catpref2$customer_key,stringr::str_split_fixed(brus_catpref2$Attr, pattern = "|",n=11))
brus_catpref3 <- brus_catpref3[,-12]
names(brus_catpref3) <- c("Customer_key","Pref1","Pref2","Pref3","Pref4","Pref5","Pref6","Pref7","Pref8","Pref9","Pref10")

write.table(x = brus_catpref3,file = "//10.8.8.51/lv0/Tanumoy/Datasets/BRUS_Camp17Oct17/brus_catpref.csv",sep=",",row.names=F,col.names=T,append=F,quote = F)


brus_catpref5 <- brus_catpref %>% group_by(customer_key) %>% summarise(Attr = paste(Attribute,collapse="|")) 
brus_catpref6 <- splitstackshape::cSplit(brus_catpref5, "Attr", "|")[,1:11]

names(brus_catpref6) <- c("Customer_key","Pref1","Pref2","Pref3","Pref4","Pref5","Pref6","Pref7","Pref8","Pref9","Pref10")

write.table(x = brus_catpref6,file = "//10.8.8.51/lv0/Tanumoy/Datasets/BRUS_Camp17Oct17/brus_catpref.csv",sep=",",row.names=F,col.names=T,append=F,quote = F,na = "")

saveRDS(object = brus_catpref6,file = "//10.8.8.51/lv0/Tanumoy/Datasets/BRUS_Camp17Oct17/brus_catpref.rds")
