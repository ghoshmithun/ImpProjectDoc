# install.packages("ada")
# install.packages("ffbase")
# install.packages("ff")
# install.packages("plyr")
# install.packages("VIF")
# install.packages("pracma")
# install.packages("verification")
# install.packages("ROCR")
# install.packages("pROC")
# install.packages("gbm")
# install.packages("party")
# install.packages("partykit")
# install.packages("caret")
# install.packages("adabag")

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
library(pROC)
library(gbm)
library(party)
library(partykit)
library(caret)
library(adabag)
library(AUC)


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




bf_data_all <-read.table.ffdf(file="//10.8.8.51/lv0/Move to Box/Mithun/projects/12.BRFS_US_DS_Project/1.data/BF_data_full_transformed.txt",sep="|",header = TRUE,VERBOSE = TRUE,colClasses = c(rep("numeric",50)))


bf_data_all <- subset(bf_data_all,select=c('customer_key','Response','ratio_rev_rewd_12mth',
                                           'num_days_on_books',
                                           'percent_disc_last_6_mth',
                                           'time_since_last_retail_purchase',
                                           'num_units_12mth',
                                           'avg_order_amt_last_6_mth',
                                           'disc_ats',
                                           'ratio_rev_wo_rewd_12mth',
                                           'non_disc_ats',
                                           'bf_go_net_sales_ratio',
                                           'bf_on_sales_ratio',
                                           'on_sales_rev_ratio_12mth',
                                           'bf_br_net_sales_ratio',
                                           'bf_gp_net_sales_ratio',
                                           'br_hit_ind_tot',
                                           'num_disc_comm_responded',
                                           'mobile_ind_tot',
                                           'ratio_order_6_12_mth',
                                           'gp_hit_ind_tot',
                                           'factory_hit_ind_tot',
                                           'ratio_disc_non_disc_ats',
                                           'card_status',
                                           'on_hit_ind_tot',
                                           'num_dist_catg_purchased',
                                           'pct_off_hit_ind_tot',
                                           'ratio_order_units_6_12_mth',
                                           'clearance_hit_ind_tot',
                                           'num_order_num_last_6_mth',
                                           'at_hit_ind_tot',
                                           'purchased',
                                           'searchdex_ind_tot',
                                           'markdown_hit_ind_tot',
                                           'time_since_last_online_purchase'))

min.sample.size <- 5000; max.sample.size <- 20000;

sample.sizes <- seq(min.sample.size, max.sample.size, by=1000)

coeflearn.list <- c('Breiman', 'Freund', 'Zhu')

balance <- 1

headers1<-cbind("samplesize", "run", "coeflearn", "TPR", "TNR", "Accuracy", "CVAccuracy", "AUC", "Concordance")
write.table(headers1, paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/12.BRFS_US_DS_Project/3.Documents/model_report/on_US_bst_training_', min.sample.size,'_', max.sample.size, '.csv'), 
            append=FALSE, sep=",",row.names=FALSE,col.names=FALSE)

headers2<-cbind('SampleSize', 'coeflearn', 'Run', 'TestRun', 'Prior', 'AUC', 'TPR', 'TNR', 'Concordance')
write.table(headers2, paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/12.BRFS_US_DS_Project/3.Documents/model_report/on_US_bst_testing_', min.sample.size,'_', max.sample.size, '.csv'), 
            append=FALSE, sep=",",row.names=FALSE,col.names=FALSE)

total_obs =nrow(bf_data_all)
for(i in 1:length(sample.sizes))
{ 
 model_data_size <- round(0.15*total_obs)
  bf_data <- bf_data_all[bigsample(1:total_obs,size=model_data_size,replace = F),]
  names(bf_data) <- colnames(bf_data_all)
  rownames(bf_data) <- seq_len(model_data_size)
  
  for (s in 1:5)
  {
    
    if (balance==1)
    {
      bf_data.ones <- bf_data[bf_data[,2]==1,]
      index.ones.tra <- bigsample(1:nrow(bf_data.ones), size=0.5*sample.sizes[i], replace=F)
      tra.bf.ones <- bf_data.ones[ index.ones.tra,]
      tst.bf.ones <- bf_data.ones[-index.ones.tra,]
      
      rm(bf_data.ones); gc();
      
      
      bf_data.zeroes <- bf_data[bf_data[,2]!=1,]
      index.zeroes.tra <- bigsample(1:nrow(bf_data.zeroes), size=0.5*sample.sizes[i], replace=F)
      tra.bf.zeroes <- bf_data.zeroes[ index.zeroes.tra,]
      tst.bf.zeroes <- bf_data.zeroes[-index.zeroes.tra,]
      
      rm(bf_data.zeroes); gc();
      
      tra.bf <- rbind(tra.bf.ones, tra.bf.zeroes)
      rm(tra.bf.ones, tra.bf.zeroes); gc();
      
    }
    
    if (balance==0)
    {
      index.tra <- bigsample(1:nrow(bf_data), size=sample.sizes[i], replace=F)
      tra.bf <- bf_data[ index.tra,]
      tst.bf.all <- bf_data[-index.tra,]
      
    }
    
    tra.bf <- tra.bf[,c(-1)]
    
    prop <- sum(tra.bf[,1])/nrow(tra.bf)
    
    tra.bf <- data.frame(tra.bf)
    
    tra.bf[,1] <- as.factor(tra.bf[,1])
    
    
    
      
    for (ls in 1:length(coeflearn.list))
    {
      
      bst.object <- paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/12.BRFS_US_DS_Project/Model_objects/bst/Boost_', sample.sizes[i],'_', s, '_', 
                           coeflearn.list[ls], '.RData')
      
      measurevar <- colnames(tra.bf)[ 1]
      groupvars  <- colnames(tra.bf)[-1]
      
      # This creates the appropriate string:
      paste(measurevar, paste(groupvars, collapse=" + "), sep=" ~ ")
      
      # This returns the formula:
      f <- as.formula(paste(measurevar, paste(groupvars, collapse=" + "), sep=" ~ "))
      
      bst.cv <- boosting.cv(f, tra.bf, v = 10, boos = TRUE, mfinal = 100,
                            coeflearn = coeflearn.list[ls], control=rpart.control(maxdepth=5))
      
      cvacc <- 1 - bst.cv$error
      
      bst.model <- boosting(f, tra.bf, boos = TRUE, mfinal = 100,
                            coeflearn = coeflearn.list[ls], control=rpart.control(maxdepth=5))
        
      prob_tra <- predict(bst.model, tra.bf[,-1], newmfinal=100)$prob
      
      confusion <- table(tra.bf[,1] == 1, prob_tra[,2] > 0.5)
      
      tpr <- confusion[1,1] / sum(confusion[1,])
      tnr <- confusion[2,2] / sum(confusion[2,])
      acc <- (confusion[1,1] + confusion[2,2])/nrow(tra.bf)
      
      print('------------------------------------------')
      print(bst.model)
      print('------------------------------------------')
      
      save(bst.model, file=bst.object)
      load(file=bst.object)
      
      roc.area <- auc(roc( tra.bf[,1],prob_tra[,2]))
      
      concordance <- OptimisedConc(tra.bf[,1], prob_tra[,2])[1]
      
      print(paste('SampleSize', 'Run', 'coeflearn', 'Prior', 'TPR', 'TNR', 'Accuracy', 'CVAccuracy', 'AUC', 'Concordance'))
      print(paste(sample.sizes[i], s, coeflearn.list[ls], prop, tpr, tnr, acc, roc.area, concordance))
      
      
      write.table(cbind(sample.sizes[i], s, coeflearn.list[ls], tpr, tnr, acc, cvacc, roc.area, concordance),
                  paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/12.BRFS_US_DS_Project/3.Documents/model_report/on_US_bst_training_', min.sample.size,'_', max.sample.size, '.csv'), 
                  append=TRUE, sep=",",row.names=FALSE,col.names=FALSE)
      
      print('------------------------------------------')
      print('---------- Running Validations -----------')
      print('------------------------------------------')
      
      if (balance==1)
      {
        index.tst.bf <- sample(1:nrow(rbind(tst.bf.ones, tst.bf.zeroes)),
                               size=10000*10, replace=F)
      }
      
      if (balance==0)
      {
        index.tst.bf <- sample(1:nrow(tst.bf.all),
                               size=10000*10, replace=F)        
      }
      
      print(paste('SampleSize', 'coeflearn', 'Run', 'TestRun', 'Prior', 'AUC', 'TPR', 'TNR', 'Concordance'))
      
      for (l in 1:10)
      {
        if (balance==1)
        {
          tst.bf <- rbind(tst.bf.ones, tst.bf.zeroes)[index.tst.bf[((l-1)*10000 + 1):(l*10000)],]
          tst.bf <- tst.bf[,c(-1)]
          tst.bf <- data.frame(tst.bf)
        }
        
        if (balance==0)
        {
          tst.bf <- tst.bf.all[index.tst.bf[((l-1)*10000 + 1):(l*10000)],]
          tst.bf <- tst.bf[c(-1)]
          tst.bf <- data.frame(tst.bf)
        }
        
        prop.tst <- sum(tst.bf[,1])/nrow(tst.bf)
        
        gc()
        prob_tst <- predict(bst.model, data.frame(tst.bf[,c(-1)]), newmfinal=100)$prob

        roc.area.tst <- auc(roc( tst.bf[,1] , prob_tst[,2] ))
        
        concordance.tst <- OptimisedConc(tst.bf[,1], prob_tst[,2])[1]
        tpr.test <- prop.table(table(tst.bf[,1], prob_tst[,2] > 0.5),1)[2, 2]
        tnr.test <- prop.table(table(tst.bf[,1],prob_tst[,2] > 0.5),1)[1, 1]
        
        print(paste(sample.sizes[i], s, coeflearn.list[ls], l, prop.tst, roc.area.tst,tpr.test,tnr.test ,concordance.tst))
        
        write.table(cbind(sample.sizes[i], s, coeflearn.list[ls], l, roc.area.tst,tpr.test,tnr.test, concordance.tst),
                    paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/12.BRFS_US_DS_Project/3.Documents/model_report/on_US_bst_testing_', min.sample.size,'_', max.sample.size, '.csv'), 
                    append=TRUE, sep=",",row.names=FALSE,col.names=FALSE)
        
        
        
      }
      
    }
    
gc()
    
  }
    rm(bf_data) ; gc();
}