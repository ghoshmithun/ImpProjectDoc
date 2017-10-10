install.packages("ada")
install.packages("ffbase")
install.packages("ff")
install.packages("plyr")
install.packages("VIF")
install.packages("pracma")
install.packages("verification")
install.packages("ROCR")
install.packages("pROC")
install.packages("gbm")
install.packages("party")
install.packages("partykit")
install.packages("caret")
install.packages("adabag")

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




gp_data_all <-read.table.ffdf(file="//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/1. data/GP_full_data_base.txt",
                              header = TRUE,VERBOSE = TRUE,
                              sep='|',colClasses = c(rep("numeric",50)))


gp_data_all <- subset(gp_data_all,select=c('customer_key','Response','percent_disc_last_12_mth','percent_disc_last_6_mth','per_elec_comm',
                                           'gp_hit_ind_tot','num_units_12mth','num_em_campaign','disc_ats','avg_order_amt_last_6_mth','Time_Since_last_disc_purchase',
                                           'non_disc_ats','gp_on_net_sales_ratio','on_sales_rev_ratio_12mth','mobile_ind_tot','ratio_order_6_12_mth',
                                           'pct_off_hit_ind_tot','ratio_rev_wo_rewd_12mth','ratio_disc_non_disc_ats','card_status','br_hit_ind_tot',
                                           'gp_br_sales_ratio','ratio_order_units_6_12_mth','num_disc_comm_responded','purchased','ratio_rev_rewd_12mth',
                                           'num_dist_catg_purchased','num_order_num_last_6_mth','at_hit_ind_tot','gp_go_net_sales_ratio','clearance_hit_ind_tot',
                                           'searchdex_ind_tot','total_plcc_cards','factory_hit_ind_tot','gp_bf_net_sales_ratio','markdown_hit_ind_tot' ))



min.sample.size <- 5000; max.sample.size <- 20000;

sample.sizes <- seq(min.sample.size, max.sample.size, by=1000)

coeflearn.list <- c('Breiman', 'Freund', 'Zhu')

balance <- 1

headers1<-cbind("samplesize", "run", "coeflearn", "TPR", "TNR", "Accuracy", "CVAccuracy", "AUC", "Concordance")
write.table(headers1, paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/GP_US_bst_training_', min.sample.size,'_', max.sample.size, '.csv'), 
            append=FALSE, sep=",",row.names=FALSE,col.names=FALSE)

headers2<-cbind("samplesize","run", "coeflearn" , "SampleNumber", "TestRun", "AUC","Concordance")
write.table(headers2, paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/GP_US_bst_testing_', min.sample.size,'_', max.sample.size, '.csv'), 
            append=FALSE, sep=",",row.names=FALSE,col.names=FALSE)

total_obs =nrow(gp_data_all)
for(i in 1:length(sample.sizes))
{ 
 model_data_size <- round(0.33*total_obs)
  gp_data <- gp_data_all[bigsample(1:total_obs,size=model_data_size,replace = F),]
  names(gp_data) <- colnames(gp_data_all)
  rownames(gp_data) <- seq_len(model_data_size)
  
  for (s in 1:5)
  {
    
    if (balance==1)
    {
      gp_data.ones <- gp_data[gp_data[,2]==1,]
      index.ones.tra <- bigsample(1:nrow(gp_data.ones), size=0.5*sample.sizes[i], replace=F)
      tra.gp.ones <- gp_data.ones[ index.ones.tra,]
      tst.gp.ones <- gp_data.ones[-index.ones.tra,]
      
      rm(gp_data.ones); gc();
      
      
      gp_data.zeroes <- gp_data[gp_data[,2]!=1,]
      index.zeroes.tra <- bigsample(1:nrow(gp_data.zeroes), size=0.5*sample.sizes[i], replace=F)
      tra.gp.zeroes <- gp_data.zeroes[ index.zeroes.tra,]
      tst.gp.zeroes <- gp_data.zeroes[-index.zeroes.tra,]
      
      rm(gp_data.zeroes); gc();
      
      tra.gp <- rbind(tra.gp.ones, tra.gp.zeroes)
      rm(tra.gp.ones, tra.gp.zeroes); gc();
      
    }
    
    if (balance==0)
    {
      index.tra <- bigsample(1:nrow(gp_data), size=sample.sizes[i], replace=F)
      tra.gp <- gp_data[ index.tra,]
      tst.gp.all <- gp_data[-index.tra,]
      
    }
    
    tra.gp <- tra.gp[,c(-1)]
    
    prop <- sum(tra.gp[,1])/nrow(tra.gp)
    
    tra.gp <- data.frame(tra.gp)
    
    tra.gp[,1] <- as.factor(tra.gp[,1])
    
    
    
      
    for (ls in 1:length(coeflearn.list))
    {
      
      bst.object <- paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/Model_Objects/bst/Boost_', sample.sizes[i],'_', s, '_', 
                           coeflearn.list[ls], '.RData')
      
      measurevar <- colnames(tra.gp)[ 1]
      groupvars  <- colnames(tra.gp)[-1]
      
      # This creates the appropriate string:
      paste(measurevar, paste(groupvars, collapse=" + "), sep=" ~ ")
      
      # This returns the formula:
      f <- as.formula(paste(measurevar, paste(groupvars, collapse=" + "), sep=" ~ "))
      
      bst.cv <- boosting.cv(f, tra.gp, v = 10, boos = TRUE, mfinal = 100,
                            coeflearn = coeflearn.list[ls], control=rpart.control(maxdepth=5))
      
      cvacc <- 1 - bst.cv$error
      
      bst.model <- boosting(f, tra.gp, boos = TRUE, mfinal = 100,
                            coeflearn = coeflearn.list[ls], control=rpart.control(maxdepth=5))
        
      prob_tra <- predict(bst.model, tra.gp[,-1], newmfinal=100)$prob
      
      confusion <- table(tra.gp[,1] == 1, prob_tra[,2] > 0.5)
      
      tpr <- confusion[1,1] / sum(confusion[1,])
      tnr <- confusion[2,2] / sum(confusion[2,])
      acc <- (confusion[1,1] + confusion[2,2])/nrow(tra.gp)
      
      print('------------------------------------------')
      print(bst.model)
      print('------------------------------------------')
      
      save(bst.model, file=bst.object)
      load(file=bst.object)
      
      roc.area <- auc(roc(tra.gp[,1], prob_tra[,2]))
      
      concordance <- OptimisedConc(tra.gp[,1], prob_tra[,2])[1]
      
      print(paste('SampleSize', 'Run', 'coeflearn', 'Prior', 'TPR', 'TNR', 'Accuracy', 'CVAccuracy', 'AUC', 'Concordance'))
      print(paste(sample.sizes[i], s, coeflearn.list[ls], prop, tpr, tnr, acc, roc.area, concordance))
      
      
      write.table(cbind(sample.sizes[i], s, coeflearn.list[ls], tpr, tnr, acc, cvacc, roc.area, concordance),
                  paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/GP_US_bst_training_', min.sample.size,'_', max.sample.size, '.csv'), 
                  append=TRUE, sep=",",row.names=FALSE,col.names=FALSE)
      
      print('------------------------------------------')
      print('---------- Running Validations -----------')
      print('------------------------------------------')
      
      if (balance==1)
      {
        index.tst.gp <- sample(1:nrow(rbind(tst.gp.ones, tst.gp.zeroes)),
                               size=10000*10, replace=F)
      }
      
      if (balance==0)
      {
        index.tst.gp <- sample(1:nrow(tst.gp.all),
                               size=10000*10, replace=F)        
      }
      
      print(paste('SampleSize', 'coeflearn', 'Run', 'TestRun', 'Prior', 'AUC', 'Concordance'))
      
      for (l in 1:10)
      {
        if (balance==1)
        {
          tst.gp <- rbind(tst.gp.ones, tst.gp.zeroes)[index.tst.gp[((l-1)*10000 + 1):(l*10000)],]
          tst.gp <- tst.gp[,c(-1)]
          tst.gp <- data.frame(tst.gp)
        }
        
        if (balance==0)
        {
          tst.gp <- tst.gp.all[index.tst.gp[((l-1)*10000 + 1):(l*10000)],]
          tst.gp <- tst.gp[c(-1)]
          tst.gp <- data.frame(tst.gp)
        }
        
        prop.tst <- sum(tst.gp[,1])/nrow(tst.gp)
        
        gc()
        prob_tst <- predict(bst.model, data.frame(tst.gp[,c(-1)]), newmfinal=100)$prob

        roc.area.tst <- auc(roc(tst.gp[,1], prob_tst[,2]))
        
        concordance.tst <- OptimisedConc(tst.gp[,1], prob_tst[,2])[1]
        
        print(paste(sample.sizes[i], s, coeflearn.list[ls], l, prop.tst, roc.area.tst, concordance.tst))
        
        write.table(cbind(sample.sizes[i], s, coeflearn.list[ls], l, roc.area.tst, concordance.tst),
                    paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/GP_US_bst_testing_', min.sample.size,'_', max.sample.size, '.csv'), 
                    append=TRUE, sep=",",row.names=FALSE,col.names=FALSE)
        
        
        
      }
      
    }
    
  
  
    rm(tst.gp.ones, tst.gp.zeroes)
    gc()
    
  }
    rm(gp_data) ; gc();
}