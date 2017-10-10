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
    ties<-ties+length(subset(diffx,sign(diffx) == 0))
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

distribution.list <- c("bernoulli","adaboost")

shrinkage.list <- c(0.001, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.1)

balance <- 1

headers1<-cbind("samplesize", "run", "shrinkage", "LossFunc", "Predictors", "OptNumTrees", "TPR", "TNR", "Accuracy", "AUC","Concordance")
write.table(headers1, paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/12.BRFS_US_DS_Project/3.Documents/model_report/bf_ds_training_gbm_v1_', min.sample.size,'_', max.sample.size, '.csv'), 
            append=FALSE, sep=",",row.names=FALSE,col.names=FALSE)

headers2<-cbind("samplesize","run", "shrinkage" ,"LossFunc","SampleNumber", "Predictors", "OptNumTrees", "TestRun","TPR", "TNR", "AUC","Concordance")
write.table(headers2, paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/12.BRFS_US_DS_Project/3.Documents/model_report/bf_ds_testing_gbm_v1_', min.sample.size,'_', max.sample.size, '.csv'), 
            append=FALSE, sep=",",row.names=FALSE,col.names=FALSE)

total_obs =nrow(bf_data_all)
for(i in 1:length(sample.sizes))
{ 
  model_data_size <- round(0.5*total_obs)
  bf_data <- bf_data_all[bigsample(1:total_obs,size=model_data_size,replace = F),]
  names(bf_data) <- colnames(bf_data_all)
  rownames(bf_data) <- seq_len(model_data_size)
  for (s in 1:5)
  {
    if (balance==1)
    {
      bf_data.ones <- bf_data[bf_data[,2]==1,]
      index.ones.tra <- bigsample(1:nrow(bf_data.ones), size=0.5*sample.sizes[i], replace=F)
      tra_bf.ones <- bf_data.ones[ index.ones.tra,]
      tst_bf.ones <- bf_data.ones[-index.ones.tra,]
	  
      rm(bf_data.ones); gc();
      
      
      bf_data.zeroes <- bf_data[bf_data[,2]!=1,]
      index.zeroes.tra <- bigsample(1:nrow(bf_data.zeroes), size=0.5*sample.sizes[i], replace=F)
      tra_bf.zeroes <- bf_data.zeroes[ index.zeroes.tra,]
      tst_bf.zeroes <- bf_data.zeroes[-index.zeroes.tra,]
      
      rm(bf_data.zeroes); gc();
      
      tra_bf <- rbind(tra_bf.ones, tra_bf.zeroes)
      rm(tra_bf.ones, tra_bf.zeroes); gc();
      
    }
    
    if (balance==0)
    {
      index.tra <- bigsample(1:nrow(bf_data), size=sample.sizes[i], replace=F)
      tra_bf <- bf_data[ index.tra,]
      tst_bf.all <- bf_data[-index.tra,]
    }
    
    tra_bf <- tra_bf[c(-1)]
    
    prop <- sum(tra_bf[,1])/nrow(tra_bf)
    
    tra_bf <- data.frame(tra_bf)
    
    for(j in 1:length(shrinkage.list))
    {
      
      for (ls in 1:length(distribution.list))
      {
        
        gbm.object <- paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/12.BRFS_US_DS_Project/Model_Objects/gbm/gbm_', sample.sizes[i],'_', s, '_', 
                             distribution.list[ls], '_', shrinkage.list[j], '.RData')
       
        n <- names(tra_bf)
        f <- as.formula(paste("Response ~", paste(n[!n %in% "Response"], collapse = " + ")))
        
        gbm.model <- gbm(f, data=tra_bf, distribution=distribution.list[ls],  bag.fraction = 0.3, verbose=F,
                         n.trees=round(20/shrinkage.list[j]), cv.folds=10, shrinkage = shrinkage.list[j], interaction.depth = 2,
                         n.minobsinnode = 10)
        
        opt.num.trees <- gbm.perf(gbm.model)
        predictors <- sum(relative.influence(gbm.model, opt.num.trees)>0)
        
        prob_tra <- predict.gbm(object=gbm.model, data=data.frame(tra_bf), type='response', n.trees=opt.num.trees)
        
        confusion <- table(tra_bf[,1], prob_tra > 0.5)
        
        tpr <- confusion[1,1] / sum(confusion[1,])
        tnr <- confusion[2,2] / sum(confusion[2,])
        acc <- (confusion[1,1] + confusion[2,2])/nrow(tra_bf)
        
        print('------------------------------------------')
        print(gbm.model)
        print('------------------------------------------')
        
        prob_tra<- predict(gbm.model, tra_bf[c(-1)],type="response", n.trees=opt.num.trees)
        save(gbm.model, file=gbm.object)
        
        roc.area <- auc(roc(prob_tra, factor(tra_bf$Response)))
        
        concordance <- OptimisedConc(tra_bf$Response, prob_tra)[1]
        
        print(paste('SampleSize', 'Distribution', 'shrinkage', 'Run', 'Prior', 'Predictors', 'OptNumTrees', 'TPR', 'TNR', 'Accuracy', 'AUC', 'Concordance'))
        print(paste(sample.sizes[i],distribution.list[ls], shrinkage.list[j], s, prop, predictors, opt.num.trees, tpr, tnr, acc, roc.area, concordance))
        
        
        write.table(cbind(sample.sizes[i], s, shrinkage.list[j], distribution.list[ls], predictors, opt.num.trees, tpr, 
                          tnr, acc, roc.area, concordance),
                    paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/12.BRFS_US_DS_Project/3.Documents/model_report/bf_ds_training_gbm_v1_', min.sample.size,'_', max.sample.size, '.csv'), 
                    append=TRUE, sep=",",row.names=FALSE,col.names=FALSE)
        
        
        
        
        print('------------------------------------------')
        print('---------- Running Validations -----------')
        print('------------------------------------------')
        
        if (balance==1)
        {
          index.tst_bf <- sample(1:nrow(rbind(tst_bf.ones, tst_bf.zeroes)),
                                 size=10000*10, replace=F)
        }
        
        if (balance==0)
        {
          index.tst_bf <- sample(1:nrow(tst_bf.all),
                                 size=10000*10, replace=F)        
        }
        
        print(paste("samplesize","run", "shrinkage" ,"LossFunc","SampleNumber", "Predictors", "OptNumTrees", "TestRun","TPR", "TNR", "AUC","Concordance"))
        
        for (l in 1:10)
        {
          if (balance==1)
          {
            tst_bf <- rbind(tst_bf.ones, tst_bf.zeroes)[index.tst_bf[((l-1)*10000 + 1):(l*10000)],]
            tst_bf <- tst_bf[c(-1)]
            tst_bf <- data.frame(tst_bf)
          }
          
          if (balance==0)
          {
            tst_bf <- tst_bf.all[index.tst_bf[((l-1)*10000 + 1):(l*10000)],]
            tst_bf <- tst_bf[c(-1)]
            tst_bf <- data.frame(tst_bf)
          }
          
          prop.tst <- sum(tst_bf[,1])/nrow(tst_bf)
          
          gc()
          prob_tst <- predict(gbm.model, tst_bf[c(-1)],type="response", n.trees=opt.num.trees)
          tpr.test <- prop.table(table(tst_bf[,1], prob_tst > 0.5),1)[2, 2]
          tnr.test <- prop.table(table(tst_bf[,1],prob_tst > 0.5),1)[1, 1]
          roc.area.tst <- auc(roc(prob_tst, factor(tst_bf$Response)))
          
          concordance.tst <- OptimisedConc(tst_bf$Response,prob_tst)[1]
          
          print(paste(sample.sizes[i],s, shrinkage.list[j],distribution.list[ls], l, prop.tst, predictors, opt.num.trees,tpr.test,tnr.test, roc.area.tst, concordance.tst))
          
          write.table(cbind(sample.sizes[i], s, shrinkage.list[j], distribution.list[ls], predictors, opt.num.trees, l,
                            roc.area.tst, concordance.tst),
                      paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/12.BRFS_US_DS_Project/3.Documents/model_report/bf_ds_testing_gbm_v1_', min.sample.size,'_', max.sample.size, '.csv'), 
                      append=TRUE, sep=",",row.names=FALSE,col.names=FALSE)
          
          
          
        }
        
      }
      
    }
    
    rm(tst_bf.ones, tst_bf.zeroes)
    gc()
    
  }
  rm(bf_data) ; gc();
}

