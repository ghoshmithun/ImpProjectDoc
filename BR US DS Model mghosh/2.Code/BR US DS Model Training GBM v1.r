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




br_data_all <-read.table.ffdf(file="//10.8.8.51/lv0/Move to Box/Mithun/projects/9.BR_US_DS_Project/1.data/BR_data_full.txt",header = TRUE,VERBOSE = TRUE,
                          sep='|',colClasses = c(rep("numeric",51)))


br_data_all <- subset(br_data_all,select=c('customer_key','Response','percent_disc_last_12_mth','num_days_on_books','per_elec_comm','num_em_campaign','num_units_12mth','disc_ats',
'time_since_last_retail_purchase','avg_order_amt_last_6_mth','br_gp_net_sales_ratio','non_disc_ats','ratio_rev_wo_rewd_12mth','on_sales_item_rev_12mth',
'br_hit_ind_tot','br_on_sales_ratio','on_sales_rev_ratio_12mth','ratio_rev_rewd_12mth','num_order_num_last_6_mth','card_status','ratio_order_6_12_mth',
'ratio_order_units_6_12_mth','ratio_disc_non_disc_ats','sale_hit_ind_tot','num_disc_comm_responded','mobile_ind_tot','gp_hit_ind_tot','br_bf_net_sales_ratio',
'num_dist_catg_purchased','total_plcc_cards','on_hit_ind_tot','pct_off_hit_ind_tot','br_go_net_sales_ratio','at_hit_ind_tot','purchased','searchdex_ind_tot',
'factory_hit_ind_tot','clearance_hit_ind_tot','markdown_hit_ind_tot'))


min.sample.size <- 5000; max.sample.size <- 20000;

sample.sizes <- seq(min.sample.size, max.sample.size, by=1000)

distribution.list <- c("bernoulli","adaboost")

shrinkage.list <- c(0.001, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.1)

balance <- 1

headers1<-cbind("samplesize", "run", "shrinkage", "LossFunc", "Predictors", "OptNumTrees", "TPR", "TNR", "Accuracy", "AUC","Concordance")
write.table(headers1, paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/9.BR_US_DS_Project/3.Documents/model_report/br_ds_training_gbm_v1_', min.sample.size,'_', max.sample.size, '.csv'), 
            append=FALSE, sep=",",row.names=FALSE,col.names=FALSE)

headers2<-cbind("samplesize","run", "shrinkage" ,"LossFunc","SampleNumber", "Predictors", "OptNumTrees", "TestRun","TPR", "TNR", "AUC","Concordance")
write.table(headers2, paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/9.BR_US_DS_Project/3.Documents/model_report/br_ds_testing_gbm_v1_', min.sample.size,'_', max.sample.size, '.csv'), 
            append=FALSE, sep=",",row.names=FALSE,col.names=FALSE)

total_obs =nrow(br_data_all)
for(i in 1:length(sample.sizes))
{ 
  model_data_size <- round(0.5*total_obs)
  br_data <- br_data_all[bigsample(1:total_obs,size=model_data_size,replace = F),]
  names(br_data) <- colnames(br_data_all)
  rownames(br_data) <- seq_len(model_data_size)
  for (s in 1:5)
  {
    if (balance==1)
    {
      br_data.ones <- br_data[br_data[,2]==1,]
      index.ones.tra <- bigsample(1:nrow(br_data.ones), size=0.5*sample.sizes[i], replace=F)
      tra_br.ones <- br_data.ones[ index.ones.tra,]
      tst_br.ones <- br_data.ones[-index.ones.tra,]
	  
      rm(br_data.ones); gc();
      
      
      br_data.zeroes <- br_data[br_data[,2]!=1,]
      index.zeroes.tra <- bigsample(1:nrow(br_data.zeroes), size=0.5*sample.sizes[i], replace=F)
      tra_br.zeroes <- br_data.zeroes[ index.zeroes.tra,]
      tst_br.zeroes <- br_data.zeroes[-index.zeroes.tra,]
      
      rm(br_data.zeroes); gc();
      
      tra_br <- rbind(tra_br.ones, tra_br.zeroes)
      rm(tra_br.ones, tra_br.zeroes); gc();
      
    }
    
    if (balance==0)
    {
      index.tra <- bigsample(1:nrow(br_data), size=sample.sizes[i], replace=F)
      tra_br <- br_data[ index.tra,]
      tst_br.all <- br_data[-index.tra,]
    }
    
    tra_br <- tra_br[c(-1)]
    
    prop <- sum(tra_br[,1])/nrow(tra_br)
    
    tra_br <- data.frame(tra_br)
    
    for(j in 1:length(shrinkage.list))
    {
      
      for (ls in 1:length(distribution.list))
      {
        
        gbm.object <- paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/9.BR_US_DS_Project/Model_Objects/gbm/gbm_', sample.sizes[i],'_', s, '_', 
                             distribution.list[ls], '_', shrinkage.list[j], '.RData')
       
        n <- names(tra_br)
        f <- as.formula(paste("Response ~", paste(n[!n %in% "Response"], collapse = " + ")))
        
        gbm.model <- gbm(f, data=tra_br, distribution=distribution.list[ls],  bag.fraction = 0.3, verbose=F,
                         n.trees=round(20/shrinkage.list[j]), cv.folds=10, shrinkage = shrinkage.list[j], interaction.depth = 2,
                         n.minobsinnode = 10)
        
        opt.num.trees <- gbm.perf(gbm.model)
        predictors <- sum(relative.influence(gbm.model, opt.num.trees)>0)
        
        prob_tra <- predict.gbm(object=gbm.model, data=data.frame(tra_br), type='response', n.trees=opt.num.trees)
        
        confusion <- table(tra_br[,1], prob_tra > 0.5)
        
        tpr <- confusion[1,1] / sum(confusion[1,])
        tnr <- confusion[2,2] / sum(confusion[2,])
        acc <- (confusion[1,1] + confusion[2,2])/nrow(tra_br)
        
        print('------------------------------------------')
        print(gbm.model)
        print('------------------------------------------')
        
        prob_tra<- predict(gbm.model, tra_br[c(-1)],type="response", n.trees=opt.num.trees)
        save(gbm.model, file=gbm.object)
        
        roc.area <- auc(roc(prob_tra, factor(tra_br$Response)))
        
        concordance <- OptimisedConc(tra_br$Response, prob_tra)[1]
        
        print(paste('SampleSize', 'Distribution', 'shrinkage', 'Run', 'Prior', 'Predictors', 'OptNumTrees', 'TPR', 'TNR', 'Accuracy', 'AUC', 'Concordance'))
        print(paste(sample.sizes[i],distribution.list[ls], shrinkage.list[j], s, prop, predictors, opt.num.trees, tpr, tnr, acc, roc.area, concordance))
        
        
        write.table(cbind(sample.sizes[i], s, shrinkage.list[j], distribution.list[ls], predictors, opt.num.trees, tpr, 
                          tnr, acc, roc.area, concordance),
                    paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/9.BR_US_DS_Project/3.Documents/model_report/br_ds_training_gbm_v1_', min.sample.size,'_', max.sample.size, '.csv'), 
                    append=TRUE, sep=",",row.names=FALSE,col.names=FALSE)
        
        
        
        
        print('------------------------------------------')
        print('---------- Running Validations -----------')
        print('------------------------------------------')
        
        if (balance==1)
        {
          index.tst_br <- sample(1:nrow(rbind(tst_br.ones, tst_br.zeroes)),
                                 size=10000*10, replace=F)
        }
        
        if (balance==0)
        {
          index.tst_br <- sample(1:nrow(tst_br.all),
                                 size=10000*10, replace=F)        
        }
        
        print(paste("samplesize","run", "shrinkage" ,"LossFunc","SampleNumber", "Predictors", "OptNumTrees", "TestRun","TPR", "TNR", "AUC","Concordance"))
        
        for (l in 1:10)
        {
          if (balance==1)
          {
            tst_br <- rbind(tst_br.ones, tst_br.zeroes)[index.tst_br[((l-1)*10000 + 1):(l*10000)],]
            tst_br <- tst_br[c(-1)]
            tst_br <- data.frame(tst_br)
          }
          
          if (balance==0)
          {
            tst_br <- tst_br.all[index.tst_br[((l-1)*10000 + 1):(l*10000)],]
            tst_br <- tst_br[c(-1)]
            tst_br <- data.frame(tst_br)
          }
          
          prop.tst <- sum(tst_br[,1])/nrow(tst_br)
          
          gc()
          prob_tst <- predict(gbm.model, tst_br[c(-1)],type="response", n.trees=opt.num.trees)
          tpr.test <- prop.table(table(tst_br[,1], prob_tst > 0.5),1)[2, 2]
          tnr.test <- prop.table(table(tst_br[,1],prob_tst > 0.5),1)[1, 1]
          roc.area.tst <- auc(roc(prob_tst, factor(tst_br$Response)))
          
          concordance.tst <- OptimisedConc(tst_br$Response,prob_tst)[1]
          
          print(paste(sample.sizes[i],s, shrinkage.list[j],distribution.list[ls], l, prop.tst, predictors, opt.num.trees,tpr.test,tnr.test, roc.area.tst, concordance.tst))
          
          write.table(cbind(sample.sizes[i], s, shrinkage.list[j], distribution.list[ls], predictors, opt.num.trees, l,
                            roc.area.tst, concordance.tst),
                      paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/9.BR_US_DS_Project/3.Documents/model_report/br_ds_testing_gbm_v1_', min.sample.size,'_', max.sample.size, '.csv'), 
                      append=TRUE, sep=",",row.names=FALSE,col.names=FALSE)
          
          
          
        }
        
      }
      
    }
    
    rm(tst_br.ones, tst_br.zeroes)
    gc()
    
  }
  rm(br_data) ; gc();
}

