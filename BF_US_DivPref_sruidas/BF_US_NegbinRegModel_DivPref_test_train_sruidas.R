library(nnet)
library(MASS)
library(mlogit)
library(foreign)
library(ggplot2)
library(plot3D)
library(reshape2)
library(readr)
library(tidyr)
library(gamlss)
library(COUNT)

#reading data
rawdata=read_delim(file="C:/Users/sruidas/Documents/negbin/nbtraindata.txt", 
                   col_names=FALSE, delim="|", n_max=-1)
#defining column names
colnames(rawdata)=c("masterkey","accessories_prop_12","men_prop_12","women_prop_12","petites_prop_12",
                    "jewelry_prop_12","shoes_prop_12","accessories_resp","men_resp","women_resp" ,
                    "petites_resp",
                    "jewelry_resp" ,"shoes_resp" ,
                    "accessories_count_12" ,
                    "men_count_12" ,
                    "women_count_12" ,
                    "petites_count_12" ,
                    "jewelry_count_12" ,
                    "shoes_count_12" ,"tot_prop")
summary(rawdata)
#merging accessories and shoes for count and response
rawdata$accessories_propnew_12=rawdata$accessories_prop_12+rawdata$shoes_prop_12
 rawdata=rawdata[,c(-2,-7)]
 rawdata=rawdata[,c(1,19,2:17)]
 rawdata$accessories_respnew=rawdata$accessories_resp+rawdata$shoes_resp
 rawdata=rawdata[,c(-7,-12)]
 rawdata=rawdata[,c(1:6,17,7:16)]
 rawdata$accessories_countnew_12=rawdata$accessories_count_12+rawdata$shoes_count_12
  rawdata=rawdata[,c(-17,-12)]
  rawdata=rawdata[,c(1:11,16,12:15)]
#not sunbjecting jewelry response to outlier treatment
jew= order(rawdata$jewelry_resp, decreasing = T)[1:5]
a=rawdata$jewelry_resp[jew]
#49 47 44 44 38
#replacing highest value of jewelry-count_12 by its 2nd highest values and not capping this variable
jew1=order(rawdata$jewelry_count_12,decreasing=T)[1:5]
b=rawdata$jewelry_count_12[jew1]
#1240  109   96   96   84

rawdata$jewelry_count_12[rawdata$jewelry_count_12 == b[1]]=b[2]
#capping other variables
capout <- function(x){
  x[ x > quantile(x,.995) ] <- quantile(x,.995)
  x
}
capdata1=apply(rawdata[,7:10],2,capout)
capdata2=apply(rawdata[,12:15],2,capout)
d=rawdata[,1:6]
d1=rawdata[,11]
d2=rawdata[,16]
newdata=cbind(d,capdata1,d1,capdata2,d2)

rm(d1,d2,rawdata,d,capdata1,capdata2)
gc()

#select sample 1M
sample2M <- newdata[sample(1:nrow(newdata),size=2000000),]
index = sample(1:nrow(sample2M),size=1000000,replace=FALSE)
train = sample2M[index,]
test = sample2M[-index,]
write.table(train,"C:/Users/sruidas/Desktop/negbin/train.txt",
            sep=",",row.names=FALSE)
write.table(test,"C:/Users/sruidas/Desktop/negbin/test.txt",
            sep=",",row.names=FALSE)
train=read.table("C:/Users/sruidas/Desktop/negbin/train.txt",sep=",",header=TRUE)
set.seed(2959)
smaller.sample.index = sample(1:1000000,size=100000,replace=F)
smaller.test = test[smaller.sample.index,]

rm(sample2M,test,newdata,index,smaller.sample.index)
gc()
### START MODELLING
# want smaller AIC

# try count to prop first
#modeling for accessories division
nb.acc.model <- glm.nb(accessories_respnew~accessories_propnew_12,data=train)               
summary(nb.acc.model)
modelfit(nb.acc.model)
#aic 446861.6
#bic  446885.2

nb.acc.modela <- glm.nb(accessories_respnew~accessories_propnew_12+men_prop_12,data=train)              
summary(nb.acc.modela)
modelfit(nb.acc.modela)
#AIC   445719.4
#bic  445754.9

nb.acc.modelb <- glm.nb(accessories_respnew~accessories_propnew_12+men_prop_12+women_prop_12,data=train)              
summary(nb.acc.modelb)
modelfit(nb.acc.modelb)
# AIC   445713.2
#bic  445760.4


nb.acc.modelc <- glm.nb(accessories_respnew~accessories_propnew_12+men_prop_12+women_prop_12+petites_prop_12,
                        data=train)              
summary(nb.acc.modelc)
modelfit(nb.acc.modelc)
#AIC 445671.8
#bic 445730.9

nb.acc.modeld <- glm.nb(accessories_respnew~accessories_propnew_12+men_prop_12+women_prop_12+petites_prop_12+
                          jewelry_prop_12,data=train)              
summary(nb.acc.modeld)
modelfit(nb.acc.modeld)
# AIC: 445673
#bic 445743.9

#final accessory  model based on smallest aic
nb.acc.modelc <- glm.nb(accessories_respnew~accessories_propnew_12+men_prop_12+women_prop_12+petites_prop_12,
                        data=train)              
summary(nb.acc.modelc)
modelfit(nb.acc.modelc)
#AIC 445671.8
#bic 445730.9


rm(nb.acc.model,nb.acc.modela,nb.acc.modelb,nb.acc.modeld)
gc()

#modeling for men division
nb.men.model=glm.nb(men_resp~men_prop_12,data=train)
summary(nb.men.model)
modelfit(nb.men.model)
#Aic 774270.8
#Bic  774294.5

nb.men.modela=glm.nb(men_resp~men_prop_12+accessories_propnew_12,data=train)
summary(nb.men.modela)
modelfit(nb.men.modela)
#AIC:   772736.3
#bic  772771.7

nb.men.modelb=glm.nb(men_resp~men_prop_12+accessories_propnew_12+women_prop_12,data=train)
summary(nb.men.modelb)
modelfit(nb.men.modelb)
#AIC:  772689.8
#bic  772737

nb.men.modelc=glm.nb(men_resp~men_prop_12+accessories_propnew_12+women_prop_12+petites_prop_12,data=train)
summary(nb.men.modelc)
modelfit(nb.men.modelc)
#AIC: 772676.2
#bic : 772735.3

nb.men.modeld=glm.nb(men_resp~men_prop_12+accessories_propnew_12+women_prop_12+petites_prop_12+
                       jewelry_prop_12,data=train)
summary(nb.men.modeld)
modelfit(nb.men.modeld)
#AIC:  772676.1
#bic   772747


#Final model for men division based on smallest Aic
nb.men.modelc=glm.nb(men_resp~men_prop_12+accessories_propnew_12+women_prop_12+petites_prop_12,data=train)
summary(nb.men.modelc)
modelfit(nb.men.modelc)
#AIC: 772676.2
#bic : 772735.3


rm(nb.men.model,nb.men.modela,nb.men.modelb,nb.men.modeld)
gc()

#modeling for women division
nb.women.model=glm.nb(women_resp~women_prop_12,data=train)
summary(nb.women.model)
modelfit(nb.women.model)
#aic 1348932
#bic   1348956

nb.women.modela=glm.nb(women_resp~women_prop_12+men_prop_12,data=train)
summary(nb.women.modela)
modelfit(nb.women.modela)
#AIC: 1346533
#bic 1346569


nb.women.modelb=glm.nb(women_resp~women_prop_12+men_prop_12+accessories_propnew_12,data=train)
summary(nb.women.modelb)
modelfit(nb.women.modelb)
#AIC: 1346194
#bic 1346241

nb.women.modelc=glm.nb(women_resp~women_prop_12+men_prop_12+accessories_propnew_12+
                         petites_prop_12,data=train)
summary(nb.women.modelc)
modelfit(nb.women.modelc)
#AIC:   1346151
#bic  1346211

nb.women.modeld=glm.nb(women_resp~women_prop_12+men_prop_12+accessories_propnew_12+petites_prop_12+
                         jewelry_prop_12,data=train)
summary(nb.women.modeld)
modelfit(nb.women.modeld)
#AIC: 1346148
#bic 1346219


#final model for women division based on smallest aic
nb.women.modeld=glm.nb(women_resp~women_prop_12+men_prop_12+accessories_propnew_12+petites_prop_12+
                         jewelry_prop_12,data=train)
summary(nb.women.modeld)
modelfit(nb.women.modeld)
#AIC: 1346148
#bic 1346219

rm(mfitwmen,nb.women.model,nb.women.modelb,nb.women.modelc,nb.women.modela)
gc()

#modeling for petites division
nb.petites.model=glm.nb(petites_resp~petites_prop_12,data=train)
summary(nb.petites.model)
modelfit(nb.petites.model)
#AIC: 414378.1
#bic  414401.8

nb.petites.modela=glm.nb(petites_resp~petites_prop_12+women_prop_12,data=train)
summary(nb.petites.modela)
modelfit(nb.petites.modela)
#AIC: 413605.2
#bic   413640.7

nb.petites.modelb=glm.nb(petites_resp~petites_prop_12+women_prop_12+men_prop_12,data=train)
summary(nb.petites.modelb)
modelfit(nb.petites.modelb)
#AIC: 413414.2
#bic 413461.5

nb.petites.modelc=glm.nb(petites_resp~petites_prop_12+women_prop_12+men_prop_12+accessories_propnew_12,
                         data=train)
summary(nb.petites.modelc)
modelfit(nb.petites.modelc)
#AIC:  413384.9
#bic  413444

nb.petites.modeld=glm.nb(petites_resp~petites_prop_12+women_prop_12+men_prop_12+accessories_propnew_12+
                         jewelry_prop_12,data=train)
summary(nb.petites.modeld)
modelfit(nb.petites.modeld)
#AIC:413385.9
#bic 413456.8

#final model for petites based on smallest aic
nb.petites.modelc=glm.nb(petites_resp~petites_prop_12+women_prop_12+men_prop_12+accessories_propnew_12,
                         data=train)
summary(nb.petites.modelc)
modelfit(nb.petites.modelc)
#AIC:  413384.9
#bic  413444

rm(nb.petites.model,nb.petites.modela,nb.petites.modelb,nb.petites.modeld)
gc()

#modeling for jewelry division
nb.jewelry.model=glm.nb(jewelry_resp~jewelry_prop_12,data=train)
summary(nb.jewelry.model)
modelfit(nb.jewelry.model)
#Aic 218992.8
#Bic 219016.4

nb.jewelry.modela=glm.nb(jewelry_resp~jewelry_prop_12+accessories_propnew_12,data=train)
summary(nb.jewelry.modela)
modelfit(nb.jewelry.modela)
#Aic  218994.5
#Bic 219029.9

nb.jewelry.modelb=glm.nb(jewelry_resp~jewelry_prop_12+accessories_propnew_12+men_prop_12,
                         data=train)
summary(nb.jewelry.modelb)
modelfit(nb.jewelry.modelb)
#Aic 217728.8
#Bic 217776.1

nb.jewelry.modelc=glm.nb(jewelry_resp~jewelry_prop_12+accessories_propnew_12+men_prop_12+women_prop_12,
                         data=train)
summary(nb.jewelry.modelc)
modelfit(nb.jewelry.modelc)
#Aic  217730.8
#Bic 217789.9

nb.jewelry.modeld=glm.nb(jewelry_resp~jewelry_prop_12+accessories_propnew_12+men_prop_12+women_prop_12+
                           petites_prop_12,data=train)
summary(nb.jewelry.modeld)
modelfit(nb.jewelry.modeld)
#Aic 217731.6
#Bic 217802.5


#Final Model for jewelry based on smallest aic
nb.jewelry.modelb=glm.nb(jewelry_resp~jewelry_prop_12+accessories_propnew_12+men_prop_12,
                         data=train)
summary(nb.jewelry.modelb)
modelfit(nb.jewelry.modelb)
#Aic 217728.8
#Bic 217776.1

rm(nb.jewelry.modeld,nb.jewelry.modelc,nb.jewelry.modela,nb.jewelry.model)
gc()

#saving the model objects
saveRDS(nb.acc.modelc,"C:/Users/sruidas/Desktop/accessories_nb.rds")
saveRDS(nb.men.modelc,"C:/Users/sruidas/Desktop/men_nb.rds")
saveRDS(nb.women.modeld,"C:/Users/sruidas/Desktop/women_nb.rds")
saveRDS(nb.petites.modelc,"C:/Users/sruidas/Desktop/petites_nb.rds")
saveRDS(nb.jewelry.modelb,"C:/Users/sruidas/Desktop/jewelry_nb.rds")


#predicting test data
test=read.table("C:/Users/sruidas/Desktop/negbin/test.txt",sep=",",header=TRUE)
sapply(test,class)
testdata=smaller.test[,1:6]
predacc=predict(nb.acc.modelc,testdata,type="response")
predmen=predict(nb.men.modelc,testdata,type="response")
predwomen=predict(nb.women.modeld,testdata,type="response")
predjew=predict(nb.jewelry.modelb,testdata,type="response")
predpet=predict(nb.petites.modelc,testdata,type="response")
new.test=cbind(smaller.test,predacc,predmen,predwomen,predpet,predjew)

rm(predacc,predmen,predwomen,predjew,predpet,testdata)
gc()

#tiebreak

#jewelry	761352 0.1
#petites	1968176 0.2
#accessories	2859600 0.3
#men	6616415 0.4
#women	11115415 0.5

#exact match percent
tieb=c (0.3,0.4,0.5,0.2,0.1)
divrankbase= NULL
divrankresp= NULL
divrankpred= NULL
for(i in 1:nrow(new.test)) {
  base=new.test[i,c("accessories_countnew_12","men_count_12","women_count_12","petites_count_12",
                  "jewelry_count_12")]
  colnames(base)=c("acc_rank","men_rank","women_rank","pet_rank","jew_rank")
  actualbase=base+tieb
  actualbase1 = names(actualbase)[actualbase>1]
  divrankbase= rbind(divrankbase, c(actualbase1[order(actualbase[actualbase>1],decreasing=TRUE)],
                                    rep("NULL",5-sum(actualbase>1))))
  resp=new.test[i,c("accessories_respnew","men_resp","women_resp","petites_resp",
                    "jewelry_resp")]
  colnames(resp)=c("acc_rank","men_rank","women_rank","pet_rank","jew_rank")
  actualresp=resp+tieb
  actualresp1=names(actualresp)[actualresp>1]
  divrankresp=rbind(divrankresp,c(actualresp1[order(actualresp[actualresp>1],decreasing=TRUE)],
                                  rep("NULL",5-sum(actualresp>1))))
  pred=new.test[i,c("predacc","predmen","predwomen","predpet", "predjew")]
  colnames(pred)= c("acc_rank","men_rank","women_rank","pet_rank","jew_rank")
  actualpred=pred
  actualpred1=names(pred)
  divrankpred=rbind(divrankpred,c(actualpred1[order(actualpred,decreasing = TRUE)]))
  cat("Iteration: ",i,"\n")
}

  
# count number of  divisions that are non-null
notnulls <- function(x) {
  return(sum(x!='NULL'))
}
# count number of division purchased for response
numranksa <- apply(divrankresp,1,notnulls)

#Model predicts the top rank vs Actual ranks
sum(divrankpred[,1]==divrankresp[,1])/sum(numranksa>0)
#0.7289436

#Model vs Actuals upto rank 2 
sum((divrankpred[,1]==divrankresp[,1]) & (divrankpred[,2]==divrankresp[,2]))/sum(numranksa>1)
#  0.2627129

#Model vs Actual upto rank 3
sum((divrankpred[,1]==divrankresp[,1]) & (divrankpred[,2]==divrankresp[,2])& 
      (divrankpred[,3]==divrankresp[,3]) )/sum(numranksa>2)
# 0.1694625

#model vs actual upto rank 4
sum((divrankpred[,1]==divrankresp[,1]) & (divrankpred[,2]==divrankresp[,2])& 
      (divrankpred[,3]==divrankresp[,3]) & (divrankpred[,4]==divrankresp[,4]))/sum(numranksa>3)
#0.1564537

#model vs actual upto rank 5
sum((divrankpred[,1]==divrankresp[,1]) & (divrankpred[,2]==divrankresp[,2])& 
      (divrankpred[,3]==divrankresp[,3]) & (divrankpred[,4]==divrankresp[,4])&
      (divrankpred[,5]==divrankresp[,5]))/sum(numranksa>4)
# 0.1538462


#Baseline predicts the top rank vs Actual ranks
sum(divrankbase[,1]==divrankresp[,1]&divrankbase[,1]!="NULL")/sum(numranksa>0)
#0.6900428

#Baseline predicts upto rank 2 vs actual ranks
sum((divrankbase[,1]==divrankresp[,1]) & (divrankbase[,2]==divrankresp[,2]) & divrankbase[,1]!="NULL" &
      divrankbase[,2]!="NULL")/sum(numranksa>1)
# 0.2485807

#Baseline predicts upto 3 vs actual ranks
sum((divrankbase[,1]==divrankresp[,1]) & (divrankbase[,2]==divrankresp[,2]) &
            (divrankbase[,3]==divrankresp[,3]) & divrankbase[,1]!="NULL" &
      divrankbase[,2]!="NULL"& divrankbase[,3]!="NULL")/sum(numranksa>2)
#0.1006505

#Baseline vs actual upto rank 4
sum((divrankbase[,1]==divrankresp[,1]) & (divrankbase[,2]==divrankresp[,2]) &
      (divrankbase[,3]==divrankresp[,3]) & (divrankbase[,4]==divrankresp[,4]) & 
      divrankbase[,1]!="NULL" & divrankbase[,2]!="NULL"& divrankbase[,3]!="NULL"
    & divrankbase[,4]!="NULL")/sum(numranksa>3)
# 0.0547588

#Baseline vs actual upto rank 5
sum((divrankbase[,1]==divrankresp[,1]) & (divrankbase[,2]==divrankresp[,2]) &
      (divrankbase[,3]==divrankresp[,3]) & (divrankbase[,4]==divrankresp[,4]) & 
      (divrankbase[,5]==divrankresp[,5]) & divrankbase[,1]!="NULL" & divrankbase[,2]!="NULL"& 
      divrankbase[,3]!="NULL" & divrankbase[,4]!="NULL"& divrankbase[,5]!="NULL")/sum(numranksa>4)
#0.02564103

#Partial match for top 3
partial3pred=NULL
partial3base=NULL
for (i in 1:nrow(new.test)){
rankresp=sort(divrankresp[i,1:3])
rankpred =sort(divrankpred[i,1:3])
partial3pred <- c(partial3pred,sum(rankresp %in% rankpred))

rankbase<- sort(divrankbase[i,1:3])
partial3base <- c(partial3base,sum(rankresp %in% rankbase))

cat("Iteration:",i,"\n")
}

length(partial3pred[numranksa>=3&partial3pred==3])/sum(numranksa>=3)
#0.4032865
length(partial3base[numranksa>=3&partial3base==3])/sum(numranksa>=3)
#0.247518


#Partial match for top 2 ranks
sum((divrankpred[,1]==divrankresp[,1]) & (divrankpred[,2]==divrankresp[,2])|
      (divrankpred[,1]==divrankresp[,2]) & (divrankpred[,2]==divrankresp[,1]))/sum(numranksa>1)

partial2pred=NULL
partial2base=NULL
for (i in 1:nrow(new.test)){
  rankresp=sort(divrankresp[i,1:2])
  rankpred =sort(divrankpred[i,1:2])
  partial2pred <- c(partial2pred,sum(rankresp %in% rankpred))
  
  rankbase<- sort(divrankbase[i,1:2])
  partial2base <- c(partial2base,sum(rankresp %in% rankbase))
  
  cat("Iteration:",i,"\n")
}
length(partial2pred[numranksa>=2&partial2pred==2])/sum(numranksa>=2)
# 0.3918348
length(partial2base[numranksa>=2&partial2base==2])/sum(numranksa>=2)
#  0.356444

#Partial match for top 4 ranks

partial4pred=NULL
partial4base=NULL
for (i in 1:nrow(new.test)){
  rankresp=sort(divrankresp[i,1:4])
  rankpred =sort(divrankpred[i,1:4])
  partial4pred <- c(partial4pred,sum(rankresp %in% rankpred))
  
  rankbase<- sort(divrankbase[i,1:4])
  partial4base <- c(partial4base,sum(rankresp %in% rankbase))
  
  cat("Iteration:",i,"\n")
}

length(partial4pred[numranksa>=4&partial4pred==4])/sum(numranksa>=4)
#0.5658409
length(partial4base[numranksa>=4&partial4base==4])/sum(numranksa>=4)
# 0.2659713

rm(partial2base,partial2pred,partial3base,partial3pred,partial4base,partial4pred,
   rankbase,rankresp,rankpred,divrankpred,divrankresp,divrankbase)
gc()

  #Model for count to count
#modeling for accessories division
  nb.acc.model0=glm.nb(accessories_respnew~accessories_countnew_12,data=train)
  summary(nb.acc.model0)
  modelfit(nb.acc.model0)
  #Aic  436705.2
  #Bic 436728.9
  
nb.acc.model0a=glm.nb(accessories_respnew~accessories_countnew_12+men_count_12,data=train)
summary(nb.acc.model0a)
modelfit(nb.acc.model0a)
#Aic  433025.5
#Bic  433061

nb.acc.model0b=glm.nb(accessories_respnew~accessories_countnew_12+men_count_12+women_count_12,data=train)
summary(nb.acc.model0b)
modelfit(nb.acc.model0b)
#Aic   431013.1
#Bic 431060.3

nb.acc.model0c=glm.nb(accessories_respnew~accessories_countnew_12+men_count_12+women_count_12+
                        petites_count_12,data=train)
summary(nb.acc.model0c)
modelfit(nb.acc.model0c)
#Aic 430880.9
#Bic 430940
nb.acc.model0d=glm.nb(accessories_respnew~accessories_countnew_12+men_count_12+women_count_12+
                        petites_count_12+jewelry_count_12,data=train)
summary(nb.acc.model0d)
modelfit(nb.acc.model0d)
#Aic  430805.7
#Bic 430876.6


#final model for accessories based on smallest aic
nb.acc.model0d=glm.nb(accessories_respnew~accessories_countnew_12+men_count_12+women_count_12+
                        petites_count_12+jewelry_count_12,data=train)
summary(nb.acc.model0d)
modelfit(nb.acc.model0d)
#Aic  430805.7
#Bic 430876.6

rm(nb.acc.model0,nb.acc.model0a,nb.acc.model0b,nb.acc.model0c)
gc()

#modeling for men divsion
nb.men.model0=glm.nb(men_resp~men_count_12+women_count_12+petites_count_12+
                       accessories_countnew_12+jewelry_count_12,data=train)
summary(nb.men.model0)
modelfit(nb.men.model0)

#final model for men based on smallest aic
nb.men.model0d=glm.nb(men_resp~men_count_12+women_count_12+petites_count_12+
                        accessories_countnew_12,data=train)
summary(nb.men.model0d)
modelfit(nb.men.model0d)
#Aic   754838.7
#Bic  754897.8

#modeling for women division
nb.wmen.model=glm.nb(women_resp~accessories_countnew_12+men_count_12+
                       women_count_12+petites_count_12 ,data=train)
modelfit(nb.wmen.model)

#final model for women division based on smallest aic
nb.wmen.model0d=glm.nb(women_resp~accessories_countnew_12+men_count_12+women_count_12+
                       petites_count_12+jewelry_count_12,data=train)
summary(nb.wmen.model0d)
modelfit(nb.wmen.model0d)
#Aic   1308324
#Bic   1308395

rm(nb.wmen.model)
gc()

#modeling for jewelry
nb.jew.model=glm.nb(jewelry_resp~jewelry_count_12+accessories_countnew_12+
                      men_count_12+women_count_12 ,data=train)
modelfit(nb.jew.model)

#final model for jewelry based on smallest aic
nb.jew.model0d=glm.nb(jewelry_resp~accessories_countnew_12+men_count_12+women_count_12+
                       petites_count_12+jewelry_count_12,data=train)
summary(nb.jew.model0d)
modelfit(nb.jew.model0d)
#Aic 207254.1
#Bic 207325

rm(nb.jew.model)
gc()

#modeling for petites division
nb.pet.model=glm.nb(petites_resp~petites_count_12+accessories_countnew_12+
                     men_count_12+women_count_12,data=train)
modelfit(nb.pet.model)
#final model for petites based on smallest aic
nb.pet.model0d=glm.nb(petites_resp~accessories_countnew_12+men_count_12+
                        women_count_12+petites_count_12+jewelry_count_12,
                      data=train)
summary(nb.pet.model0d)
modelfit(nb.pet.model0d)
#Aic 392698.5
#Bic 392769.4

rm(nb.pet.model)
gc()

#saving model objects
saveRDS(nb.acc.model0d,"C:/Users/sruidas/Desktop/nb.acc.count.rds")
saveRDS(nb.men.model0d,"C:/Users/sruidas/Desktop/nb.men.count.rds")
saveRDS(nb.wmen.model0d,"C:/Users/sruidas/Desktop/nb.wmen.count.rds")
saveRDS(nb.jew.model0d,"C:/Users/sruidas/Desktop/nb.jew.count.rds")
saveRDS(nb.pet.model0d,"C:/Users/sruidas/Desktop/nb.pet.count.rds")

#predictiong testdata
testdata=smaller.test[,c(1,12:16)]
predacc0=predict(nb.acc.model0d,testdata,type="response")
predmen0=predict(nb.men.model0d,testdata,type="response")
predwomen0=predict(nb.wmen.model0d,testdata,type="response")
predjew0=predict(nb.jew.model0d,testdata,type="response")
predpet0=predict(nb.pet.model0d,testdata,type="response")

new.test0=cbind(smaller.test,predacc0,predmen0,predwomen0,predpet0,predjew0)

rm(predacc0,predmen0,predwomen0,predjew0,predpet0)
gc()

#exact match percent
tieb=c(0.4,0.5,0.6,0.3,0.2,0.1)
divrankbase0=NULL
divrankresp0=NULL
divrankpred0=NULL
for(i in 1:nrow(new.test0)) {
  base=new.test0[i,c("accessories_countnew_12","men_count_12","women_count_12",
                     "petites_count_12","jewelry_count_12")]
  colnames(base)=c("acc_rank","men_rank","women_rank","pet_rank","jew_rank")
  actualbase=base+tieb
  actualbase1 = names(actualbase)[actualbase>1]
  divrankbase0= rbind(divrankbase0, c(actualbase1[order(actualbase[actualbase>1],decreasing=TRUE)],
                                    rep("NULL",5-sum(actualbase>1))))
  resp=new.test0[i,c("accessories_respnew","men_resp","women_resp",
                     "petites_resp","jewelry_resp")]
  colnames(resp)=c("acc_rank","men_rank","women_rank","pet_rank","jew_rank")
  actualresp=resp+tieb
  actualresp1=names(actualresp)[actualresp>1]
  divrankresp0=rbind(divrankresp0,c(actualresp1[order(actualresp[actualresp>1],decreasing=TRUE)],
                                  rep("NULL",5-sum(actualresp>1))))
  pred=new.test0[i,c("predacc0","predmen0","predwomen0","predpet0", "predjew0")]
  colnames(pred)= c("acc_rank","men_rank","women_rank","pet_rank","jew_rank")
  actualpred=pred
  actualpred1=names(pred)
  divrankpred0=rbind(divrankpred0,c(actualpred1[order(actualpred,decreasing = TRUE)]))
  cat("Iteration: ",i,"\n")
}

# count number of  divisions that are non-null
notnulls <- function(x) {
  return(sum(x!='NULL'))
}
# count number of division purchased for response
numranksa <- apply(divrankresp0,1,notnulls)

#Model predicts the top rank vs Actual ranks
sum(divrankpred0[,1]==divrankresp0[,1])/sum(numranksa>0)
# 0.6959315

#Model vs Actuals upto rank 2 
sum((divrankpred0[,1]==divrankresp0[,1]) & (divrankpred0[,2]==divrankresp0[,2]))/sum(numranksa>1)
# 0.27914

#Model vs Actual upto rank 3
sum((divrankpred0[,1]==divrankresp0[,1]) & (divrankpred0[,2]==divrankresp0[,2])& 
      (divrankpred0[,3]==divrankresp0[,3]) )/sum(numranksa>2)
#  0.1643273

#model vs actual upto rank 4
sum((divrankpred0[,1]==divrankresp0[,1]) & (divrankpred0[,2]==divrankresp0[,2])& 
      (divrankpred0[,3]==divrankresp0[,3]) & (divrankpred0[,4]==divrankresp0[,4]))/sum(numranksa>3)
# 0.136897

#model vs actual upto rank 5
sum((divrankpred0[,1]==divrankresp0[,1]) & (divrankpred0[,2]==divrankresp0[,2])& 
      (divrankpred0[,3]==divrankresp0[,3]) & (divrankpred0[,4]==divrankresp0[,4])&
      (divrankpred0[,5]==divrankresp0[,5]))/sum(numranksa>4)
# 0.09401709


#Baseline predicts the top rank vs Actual ranks
sum(divrankbase0[,1]==divrankresp0[,1]&divrankbase0[,1]!="NULL")/sum(numranksa>0)
#   0.6900428

#Baseline predicts upto rank 2 vs actual ranks
sum((divrankbase0[,1]==divrankresp0[,1]) & (divrankbase0[,2]==divrankresp0[,2]) & divrankbase0[,1]!="NULL" &
      divrankbase0[,2]!="NULL")/sum(numranksa>1)
# 0.2485807

#Baseline predicts upto 3 vs actual ranks
sum((divrankbase0[,1]==divrankresp0[,1]) & (divrankbase0[,2]==divrankresp0[,2]) &
      (divrankbase0[,3]==divrankresp0[,3]) & divrankbase0[,1]!="NULL" &
      divrankbase0[,2]!="NULL"& divrankbase0[,3]!="NULL")/sum(numranksa>2)
# 0.1006505

#Baseline vs actual upto rank 4
sum((divrankbase0[,1]==divrankresp0[,1]) & (divrankbase0[,2]==divrankresp0[,2]) &
      (divrankbase0[,3]==divrankresp0[,3]) & (divrankbase0[,4]==divrankresp0[,4]) & 
      divrankbase0[,1]!="NULL" & divrankbase0[,2]!="NULL"& divrankbase0[,3]!="NULL"
    & divrankbase0[,4]!="NULL")/sum(numranksa>3)
#0.0547588

#Baseline vs actual upto rank 5
sum((divrankbase0[,1]==divrankresp0[,1]) & (divrankbase0[,2]==divrankresp0[,2]) &
      (divrankbase0[,3]==divrankresp0[,3]) & (divrankbase0[,4]==divrankresp0[,4]) & 
      (divrankbase0[,5]==divrankresp0[,5]) & divrankbase0[,1]!="NULL" & divrankbase0[,2]!="NULL"& 
      divrankbase0[,3]!="NULL" & divrankbase0[,4]!="NULL"& divrankbase0[,5]!="NULL")/sum(numranksa>4)
#0.02564103

#Partial match for top 3
partial3pred0=NULL
partial3base0=NULL
for (i in 1:nrow(new.test0)){
  rankresp=sort(divrankresp0[i,1:3])
  rankpred =sort(divrankpred0[i,1:3])
  partial3pred0 <- c(partial3pred0,sum(rankresp %in% rankpred))
  
  rankbase<- sort(divrankbase0[i,1:3])
  partial3base0 <- c(partial3base0,sum(rankresp %in% rankbase))
  
  cat("Iteration:",i,"\n")
}

length(partial3pred0[numranksa>=3&partial3pred0==3])/sum(numranksa>=3)
# 0.4166381
length(partial3base0[numranksa>=3&partial3base0==3])/sum(numranksa>=3)
#0.247518

#Partial match for top 2 ranks
partial2pred0=NULL
partial2base0=NULL
for (i in 1:nrow(new.test0)){
  rankresp=sort(divrankresp0[i,1:2])
  rankpred =sort(divrankpred0[i,1:2])
  partial2pred0 <- c(partial2pred0,sum(rankresp %in% rankpred))
  
  rankbase<- sort(divrankbase0[i,1:2])
  partial2base0 <- c(partial2base0,sum(rankresp %in% rankbase))
  
  cat("Iteration:",i,"\n")
}
length(partial2pred0[numranksa>=2&partial2pred0==2])/sum(numranksa>=2)
#0.4056045
length(partial2base0[numranksa>=2&partial2base0==2])/sum(numranksa>=2)
#  0.356444

#Partial match for top 4 ranks

partial4pred0=NULL
partial4base0=NULL
for (i in 1:nrow(new.test0)){
  rankresp=sort(divrankresp0[i,1:4])
  rankpred =sort(divrankpred0[i,1:4])
  partial4pred0 <- c(partial4pred0,sum(rankresp %in% rankpred))
  
  rankbase<- sort(divrankbase0[i,1:4])
  partial4base0 <- c(partial4base0,sum(rankresp %in% rankbase))
  
  cat("Iteration:",i,"\n")
}
length(partial4pred0[numranksa>=4&partial4pred0==4])/sum(numranksa>=4)
# 0.5697523
length(partial4base0[numranksa>=4&partial4base0==4])/sum(numranksa>=4)
#  0.2659713

#ndcg cal
dfresp=big.test[,c(1,7:11)]
colnames(dfresp)=c("masterkey","acc","men","women","pet","jew")
df2=t(apply(dfresp[,2:6],1,function(x) x+tieb))
df3=data.frame(cbind(dfresp[,1],df2))
colnames(df3)[1]=c("masterkey")
longresp <- melt(df3, id.vars=c("masterkey"))
long1=(longresp[with(longresp, order(masterkey)), ]
long11=data.frame(long1)
colnames(long1)[2:3]=c("division","response")
rm(dfresp,df2,df3,longresp,long1)
gc()

dfbase=big.test[,c(1,12:16)]
colnames(dfbase)=c("masterkey","acc","men","women","pet","jew")
df2=t(apply(dfbase[,2:6],1,function(x) x+tieb))
df3=data.frame(cbind(dfbase[,1],df2))
colnames(df3)[1]=c("masterkey")
longbase <- melt(df3, id.vars=c("masterkey"))
long2=longbase[with(longbase, order(masterkey)), ]
colnames(long2)[2:3]=c("division","baseline")
long22=data.frame(long2)
rm(dfbase,df2,df3,longbase,long2)
gc()


dfscore=new.test[,c(1,17:21)]
colnames(dfscore)=c("masterkey","acc","men","women","pet","jew")
df2=dfscore
df3=data.frame(cbind(dfscore[,1],df2))
colnames(df3)[1]=c("masterkey")
longscore <- melt(df3, id.vars=c("masterkey"))
long3=longscore[with(longscore, order(masterkey)), ]
colnames(long3)[2:3]=c("division","score")
long33=data.frame(long3)
rm(dfscore,df2,df3,longscore,long3)
gc()

long=merge(long11,long22,by=c("masterkey","division"))
longfull=merge(long,long33,by=c("masterkey","division"))
longfull1=transform(longfull, response = as.numeric(response), 
                    baseline = as.numeric(baseline))
ndcg=longfull1 %>% group_by(masterkey) %>% filter(response>=1) %>% filter(baseline >=1)
write.table(ndcg,"C:/Users/sruidas/Desktop/negbin/ndcgfull.txt",
            sep=",",row.names=FALSE,col.names=FALSE,quote=FALSE)
write.table(longfull,"C:/Users/sruidas/Desktop/negbin/ndcgtest.txt",
            sep=",",row.names=FALSE,col.names=FALSE,quote=FALSE)

#ndcg for full test data
longfull1 = longfull1 %>% arrange(masterkey,desc(response),division)
longfull1=longfull1 %>% group_by(masterkey) %>% 
  mutate(idealrank = row_number())

longfull1=longfull1 %>% arrange(masterkey,desc(score),division)

longfull1=longfull1%>% group_by(masterkey) %>% 
  mutate(scorerank = row_number())

longfull1=longfull1 %>% mutate(disc.gain = ((2^response - 1)/log2(scorerank + 1)),
         ideal.disc.gain = ((2^response - 1)/log2(idealrank + 1)))
scorendcg=c()
for(i in seq(1,5,1)){
  cat("Iteration: ",i,"\n")
rank=longfull1 %>% filter(idealrank %in% seq(1,i,1))
tst.dcg = rank %>% group_by(masterkey) %>% 
  summarise(dcg = sum(disc.gain),idcg=sum(ideal.disc.gain))
tstscore.ndcg = tst.dcg %>% filter(idcg != 0) %>% mutate(ndcgscore = dcg/idcg)
scorendcg[i]=mean(tstscore.ndcg$ndcgscore)
rm(rank,tst.dcg,tstscore.ndcg)
gc()
}
longfull2 = longfull1 %>% arrange(masterkey,desc(response),division )
longfull2=longfull2 %>% group_by(masterkey) %>% 
  mutate(idealrank = row_number())

longfull2=longfull2 %>% arrange(masterkey,desc(baseline),division )

longfull2=longfull2%>% group_by(masterkey) %>% 
  mutate(baserank = row_number() )

longfull2=longfull2 %>% mutate(disc.gain = ((2^response - 1)/log2(baserank + 1)),
                               ideal.disc.gain = ((2^response - 1)/log2(idealrank + 1)) )
basendcg=c( )
for(i in seq(1,5,1)){
  cat("Iteration: ",i,"\n")
  rank1=longfull2 %>% filter(idealrank %in% seq(1,i,1))
  tst.dcg1 = rank1 %>% group_by(masterkey) %>% 
    summarise(dcg = sum(disc.gain),idcg=sum(ideal.disc.gain))
  tstbase.ndcg = tst.dcg1 %>% filter(idcg != 0) %>% mutate(ndcgbase = dcg/idcg)
  basendcg[i]=mean(tstbase.ndcg$ndcgbase)
  rm(rank1,tst.dcg1,tstbase.ndcg)
  gc()
}
# 0.8303462

#ndcg for full test data with non-zero response and baseline

ndcg1 = ndcg %>% arrange(masterkey,desc(response),division)
ndcg1=ndcg1 %>% group_by(masterkey) %>% 
  mutate(idealrank = row_number())

ndcg1=ndcg1 %>% arrange(masterkey,desc(score),division)

ndcg1=ndcg1%>% group_by(masterkey) %>% 
  mutate(scorerank = row_number())

ndcg1=ndcg1 %>% mutate(disc.gain = ((2^response - 1)/log2(scorerank + 1)),
                             ideal.disc.gain = ((2^response - 1)/log2(idealrank + 1)))
scorendcg1=c()
for(i in seq(1,5,1)){
  cat("Iteration: ",i,"\n")
  rank=ndcg1 %>% filter(idealrank %in% seq(1,i,1))
  tst.dcg = rank %>% group_by(masterkey) %>% 
    summarise(dcg = sum(disc.gain),idcg=sum(ideal.disc.gain))
  tstscore.ndcg = tst.dcg %>% filter(idcg != 0) %>% mutate(ndcgscore = dcg/idcg)
  scorendcg1[i]=mean(tstscore.ndcg$ndcgscore)
  rm(rank,tst.dcg,tstscore.ndcg)
  gc()
}

ndcg2 = ndcg %>% arrange(masterkey,desc(response),division)
ndcg2=ndcg2 %>% group_by(masterkey) %>% 
  mutate(idealrank = row_number())

ndcg2=ndcg2 %>% arrange(masterkey,desc(baseline),division)

ndcg2=ndcg2%>% group_by(masterkey) %>% 
  mutate(scorerank = row_number())

ndcg2=ndcg2 %>% mutate(disc.gain = ((2^response - 1)/log2(scorerank + 1)),
                       ideal.disc.gain = ((2^response - 1)/log2(idealrank + 1)))
basendcg1=c()
for(i in seq(1,5,1)){
  cat("Iteration: ",i,"\n")
  rank=ndcg2 %>% filter(idealrank %in% seq(1,i,1))
  tst.dcg = rank %>% group_by(masterkey) %>% 
    summarise(dcg = sum(disc.gain),idcg=sum(ideal.disc.gain))
  tstbase.ndcg = tst.dcg %>% filter(idcg != 0) %>% mutate(ndcgbase = dcg/idcg)
  basendcg1[i]=mean(tstbase.ndcg$ndcgbase)
  rm(rank,tst.dcg,tstbase.ndcg)
  gc()
}