
require(ff)
require(ffbase)
require(R.utils)
# Variable Selection and Importance 
num_rows=countLines("//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/1. data/on_full_data_base.txt")
#25000003
l#ines_to_skip=sample(27257261,27257261*0.8,replace=F)
#lines_to_skip<- sort(lines_to_skip)

#on_data<-read.table("//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/1. data/on_full_data_base2.txt",sep="|",header=T,skip=lines_to_skip)

on_data <- read.table.ffdf(file="//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/1. data/on_full_data_base2.txt",sep="|",VERBOSE=T,
                           header=TRUE,colClasses = c(rep("numeric",53)))

nrow(on_data)
27257262

rf <- randomForest::randomForest(as.factor(Response)~.,data=on_data[sample(nrow(on_data),100000),-1])

important <- randomForest::importance(rf)
important_var_on_data <-as.data.frame(important)
important_var_on_data <- cbind(rownames(important_var_on_data),important_var_on_data)

rownames(important_var_on_data) <-NULL
names(important_var_on_data) <- c("Var","MeanDecreaseGini")
important_var_on_data <- important_var_on_data[order(-important_var_on_data$MeanDecreaseGini),]

write.table(important_var_on_data ,"//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/important_var_on_data.csv",sep=",",col.names=T,row.names = F,append = F)

require(ggplot2)

ggplot(data = important_var_on_data, aes(x = reorder(Var,-MeanDecreaseGini),y = MeanDecreaseGini)) +
  geom_bar(stat="identity") + theme(axis.text.x = element_text(angle=90)) + xlab("Var")

#on_data <- read.table.ffdf(file="//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/1. data/on_full_data_base2.txt",sep="|",header=T)
for (i in 1:30){
  print(paste("Run start :", i, "loop"))
data_tst <- on_data[sample(nrow(on_data),60000),]
gc()

form.active<-paste('Response ~',paste(colnames(data_tst)[!colnames(data_tst) %in% c("customer_key","Response")],collapse='+'))
mod1_fulldata<-lm(form.active,data=data_tst)
summary(mod1_fulldata)
require(VIF)
keep.dat<-vif_func(in_frame=data_tst[!colnames(data_tst) %in% c("customer_key","Response")],thresh=3,trace=F)
form.active2<-paste('Response ~',paste(keep.dat,collapse='+'))
mod2_fulldata<-lm(form.active2,data=data_tst)
summary(mod2_fulldata)
write.table(x = t(keep.dat),file = "//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/1. data/ON_Variable_List_VIF.csv",sep=",",col.names = F,row.names=F,append=T)
rm(data_tst)
gc()
print(paste("Run Complete :", i, "loop"))
}
