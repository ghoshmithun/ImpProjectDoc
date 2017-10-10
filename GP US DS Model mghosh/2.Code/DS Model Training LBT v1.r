install.packages("ada")
install.packages("ffbase")
install.packages("ff")
install.packages("plyr")
install.packages("VIF")
install.packages("pracma")
install.packages("verification")
install.packages("ROCR")
install.packages("AUC")
install.packages("gbm")
install.packages("party")
install.packages("partykit")

rm(list=ls())
gc()

library(ff)
library(ada)
library(ffbase)
library(plyr)
library(pracma)
library(VIF)
library(verification)
library(ROCR)
library(AUC)
library(gbm)
library(party)
library(partykit)
library(caTools)

OptimisedConc=function(indvar,fittedvalues)
{
  x<-matrix(fittedvalues, length(fittedvalues), 1)
  y<-matrix(indvar, length(indvar), 1)
  z<-cbind(x,y)
  zeroes <- z[ which(y==0), ]
  ones <- z[ which(y==1), ]
  loopi<-nrow(ones)
  
  concordant<-0
  discordant<-0
  ties<-0
  totalpairs<- nrow(zeroes) * nrow(ones)
  
  
  for (k in 1:loopi)
  {
    
    diffx <- as.numeric(matrix(rep(ones[k,1], nrow(zeroes)),ncol=1)) - as.numeric(matrix(zeroes[,1],ncol=1))
    concordant<-concordant+length(subset(diffx,sign(diffx) == 1))
    discordant<-discordant+length(subset(diffx,sign(diffx) ==-1))
    ties<-ties+length(subset(diffx,sign(diffx) ==-0))
  }
  
  concordance<-concordant/totalpairs
  discordance<-discordant/totalpairs
  percentties<-1-concordance-discordance
  return(list("Percent Concordance"=concordance,
              "Percent Discordance"=discordance,
              "Percent Tied"=percentties,
              "Pairs"=totalpairs))
}


options(fftempdir = "C:/Users/mghosh/Documents/ffdf")

gp_data <-read.table.ffdf(file = "//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/1. data/GP_full_data_base.txt",
                          header = TRUE,VERBOSE = TRUE,
                          sep='|',colClasses = c(rep("numeric",50)))

gp_data <- subset(gp_data,select=c('customer_key','Response','percent_disc_last_12_mth','percent_disc_last_6_mth','per_elec_comm',
'gp_hit_ind_tot','num_units_12mth','num_em_campaign','disc_ats','avg_order_amt_last_6_mth','Time_Since_last_disc_purchase',
'non_disc_ats','gp_on_net_sales_ratio','on_sales_rev_ratio_12mth','mobile_ind_tot','ratio_order_6_12_mth',
'pct_off_hit_ind_tot','ratio_rev_wo_rewd_12mth','ratio_disc_non_disc_ats','card_status','br_hit_ind_tot',
'gp_br_sales_ratio','ratio_order_units_6_12_mth','num_disc_comm_responded','purchased','ratio_rev_rewd_12mth',
'num_dist_catg_purchased','num_order_num_last_6_mth','at_hit_ind_tot','gp_go_net_sales_ratio','clearance_hit_ind_tot',
'searchdex_ind_tot','total_plcc_cards','factory_hit_ind_tot','gp_bf_net_sales_ratio','markdown_hit_ind_tot' ))


min.sample.size <- 5000; max.sample.size <- 15000;

sample.sizes <- seq(min.sample.size, max.sample.size, by=1000)

ntree.list <- c(50, 100, 150, 200, 250, 300)

balance <- 1

headers1<-cbind("samplesize", "run", "ntree", "TPR", "TNR", "Accuracy", "AUC","Concordance")
write.table(headers1, paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/GP_DS_LBT_train_', min.sample.size,'_', max.sample.size, '_v1.csv'), 
            append=FALSE, sep=",",row.names=FALSE,col.names=FALSE)

headers2<-cbind("samplesize","run", "ntree" , "SampleNumber", "TestRun", "AUC","Concordance")
write.table(headers2, paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/GP_DS_LBT_test_', min.sample.size,'_', max.sample.size, '_v1.csv'), 
            append=FALSE, sep=",",row.names=FALSE,col.names=FALSE)
			
for(i in 1:length(sample.sizes))
{ 
  for (s in 1:5)
  {
    
    if (balance==1)
    {
      gp_data.ones <- gp_data[gp_data[,2]==1,]
      index.ones.tra <- bigsample(1:nrow(gp_data.ones), size=0.5*sample.sizes[i], replace=F)
      gp_tra.ones <- gp_data.ones[ index.ones.tra,]
      gp_tst.ones <- gp_data.ones[-index.ones.tra,]
      
      rm(gp_data.ones); gc();
      
      
      gp_data.zeroes <- gp_data[gp_data[,2]!=1,]
      index.zeroes.tra <- bigsample(1:nrow(gp_data.zeroes), size=0.5*sample.sizes[i], replace=F)
      gp_tra.zeroes <- gp_data.zeroes[ index.zeroes.tra,]
      gp_tst.zeroes <- gp_data.zeroes[-index.zeroes.tra,]
      
      rm(gp_data.zeroes); gc();
      
      gp_tra <- rbind(gp_tra.ones, gp_tra.zeroes)
      rm(gp_tra.ones, gp_tra.zeroes); gc();
      
    }
    
    if (balance==0)
    {
      index.tra <- bigsample(1:nrow(gp_data), size=sample.sizes[i], replace=F)
      gp_tra <- gp_data[ index.tra,]
      gp_tst.all <- gp_data[-index.tra,]
      
    }
    
    gp_tra <- gp_tra[,c(-1)]
    
    prop <- sum(gp_tra[,1])/nrow(gp_tra)
    
    gp_tra <- data.frame(gp_tra)
    
    gp_tra[,1] <- as.factor(gp_tra[,1])
    
    
    
      
    for (ls in 1:length(ntree.list))
    {
      
      lbt.object <- paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/Model_Objects/LogitBoost/LogitBoost_', sample.sizes[i],'_', s, '_', 
                           ntree.list[ls], '.RData')
      
      measurevar <- colnames(gp_tra)[ 1]
      groupvars  <- colnames(gp_tra)[-1]
      
      # This creates the appropriate string:
      paste(measurevar, paste(groupvars, collapse=" + "), sep=" ~ ")
      
      # This returns the formula:
      f <- as.formula(paste(measurevar, paste(groupvars, collapse=" + "), sep=" ~ "))
      
      
      lbt.model <- LogitBoost(xlearn=gp_tra[,c(-1)], ylearn=gp_tra[,1], nIter=20)
      
      
      
      prob_tra <- predict(object=lbt.model, xtest=data.frame(gp_tra[,c(-1)]), type='raw')
      
      confusion <- table(gp_tra[,1] == 1, prob_tra[,2] > 0.5)
      
      tpr <- confusion[1,1] / sum(confusion[1,])
      tnr <- confusion[2,2] / sum(confusion[2,])
      acc <- (confusion[1,1] + confusion[2,2])/nrow(gp_tra)
      
      print('------------------------------------------')
      print(lbt.model)
      print('------------------------------------------')
      
      save(lbt.model, file=lbt.object)
      
      roc.area <- auc(roc(prob_tra[,2],as.factor(gp_tra[,1])))
      
      concordance <- OptimisedConc(gp_tra$Response, prob_tra[,2])[1]
      
      print(paste('SampleSize', 'Run', 'ntree', 'Prior', 'TPR', 'TNR', 'Accuracy', 'AUC', 'Concordance'))
      print(paste(sample.sizes[i], s, ntree.list[ls], prop, tpr, tnr, acc, roc.area, concordance))
      
      
      write.table(cbind(sample.sizes[i], s, ntree.list[ls], tpr, tnr, acc, roc.area, concordance),
                  paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/GP_DS_LBT_train_', min.sample.size,'_', max.sample.size, '_v1.csv'), 
                  append=TRUE, sep=",",row.names=FALSE,col.names=FALSE)
      
      print('------------------------------------------')
      print('---------- Running Validations -----------')
      print('------------------------------------------')
      
      if (balance==1)
      {
        index.gp_tst <- sample(1:nrow(rbind(gp_tst.ones, gp_tst.zeroes)),
                               size=100000*10, replace=F)
      }
      
      if (balance==0)
      {
        index.gp_tst <- sample(1:nrow(gp_tst.all),
                               size=100000*10, replace=F)        
      }
      
      print(paste('SampleSize', 'ntree', 'Run', 'TestRun', 'Prior', 'AUC', 'Concordance'))
      
      for (l in 1:10)
      {
        if (balance==1)
        {
          gp_tst <- rbind(gp_tst.ones, gp_tst.zeroes)[index.gp_tst[((l-1)*10000 + 1):(l*10000)],]
          gp_tst <- gp_tst[,c(-1)]
          gp_tst <- data.frame(gp_tst)
        }
        
        if (balance==0)
        {
          gp_tst <- gp_tst.all[index.gp_tst[((l-1)*10000 + 1):(l*10000)],]
          gp_tst <- gp_tst[c(-1)]
          gp_tst <- data.frame(gp_tst)
        }
        
        prop.tst <- sum(gp_tst[,1])/nrow(gp_tst)
        
        gc()
        prob_tst <- predict(lbt.model, xtest=data.frame(gp_tst[,c(-1)]), type='raw')

        roc.area.tst <- auc(roc( prob_tst[,2],as.factor(gp_tst[,1])))
        
        concordance.tst <- OptimisedConc(gp_tst$Response, prob_tst[,2])[1]
        
        print(paste(sample.sizes[i], s, ntree.list[ls], l, prop.tst, roc.area.tst, concordance.tst))
        
        write.table(cbind(sample.sizes[i], s, ntree.list[ls], l, roc.area.tst, concordance.tst),
                    paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/GP_DS_LBT_test_', min.sample.size,'_', max.sample.size, '_v1.csv'), 
                    append=TRUE, sep=",",row.names=FALSE,col.names=FALSE)
        
        
        
      }
      
    }
    
  
  
    rm(gp_tst.ones, gp_tst.zeroes)
    gc()
    
  }
  
}