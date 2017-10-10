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




gp_data <-read.table.ffdf(file="//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/1. data/GP_full_data_base.txt",
                          header = TRUE,VERBOSE = TRUE,
                          sep='|',colClasses = c(rep("numeric",50)))


gp_data <- subset(gp_data,select=c('customer_key','Response','percent_disc_last_12_mth','percent_disc_last_6_mth','per_elec_comm',
'gp_hit_ind_tot','num_units_12mth','num_em_campaign','disc_ats','avg_order_amt_last_6_mth','Time_Since_last_disc_purchase',
'non_disc_ats','gp_on_net_sales_ratio','on_sales_rev_ratio_12mth','mobile_ind_tot','ratio_order_6_12_mth',
'pct_off_hit_ind_tot','ratio_rev_wo_rewd_12mth','ratio_disc_non_disc_ats','card_status','br_hit_ind_tot',
'gp_br_sales_ratio','ratio_order_units_6_12_mth','num_disc_comm_responded','purchased','ratio_rev_rewd_12mth',
'num_dist_catg_purchased','num_order_num_last_6_mth','at_hit_ind_tot','gp_go_net_sales_ratio','clearance_hit_ind_tot',
'searchdex_ind_tot','total_plcc_cards','factory_hit_ind_tot','gp_bf_net_sales_ratio','markdown_hit_ind_tot' ))


min.sample.size <- 5000; max.sample.size <- 20000;

sample.sizes <- seq(min.sample.size, max.sample.size, by=1000)

distribution.list <- c("bernoulli","adaboost")

shrinkage.list <- c(0.001, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.1)

balance <- 1

headers1<-cbind("samplesize", "run", "shrinkage", "LossFunc", "Predictors", "OptNumTrees", "TPR", "TNR", "Accuracy", "AUC","Concordance")
write.table(headers1, paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/gp_ds_training_gbm_v1_', min.sample.size,'_', max.sample.size, '_v3.csv'), 
            append=FALSE, sep=",",row.names=FALSE,col.names=FALSE)

headers2<-cbind("samplesize","run", "shrinkage" ,"LossFunc","SampleNumber", "Predictors", "OptNumTrees", "TestRun", "AUC","Concordance")
write.table(headers2, paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/gp_ds_testing_gbm_v1_', min.sample.size,'_', max.sample.size, '_v3.csv'), 
            append=FALSE, sep=",",row.names=FALSE,col.names=FALSE)


for(i in 1:length(sample.sizes))
{ 
  for (s in 1:5)
  {
    
    if (balance==1)
    {
      gp_data.ones <- gp_data[gp_data[,2]==1,]
      index.ones.tra <- bigsample(1:nrow(gp_data.ones), size=0.5*sample.sizes[i], replace=F)
      tra_gp.ones <- gp_data.ones[ index.ones.tra,]
      tst_gp.ones <- gp_data.ones[-index.ones.tra,]
	  
      rm(gp_data.ones); gc();
      
      
      gp_data.zeroes <- gp_data[gp_data[,2]!=1,]
      index.zeroes.tra <- bigsample(1:nrow(gp_data.zeroes), size=0.5*sample.sizes[i], replace=F)
      tra_gp.zeroes <- gp_data.zeroes[ index.zeroes.tra,]
      tst_gp.zeroes <- gp_data.zeroes[-index.zeroes.tra,]
      
      rm(gp_data.zeroes,tst_gp.zeroes); gc();
      
      tra_gp <- rbind(tra_gp.ones, tra_gp.zeroes)
      rm(tra_gp.ones, tra_gp.zeroes); gc();
      
    }
    
    if (balance==0)
    {
      index.tra <- bigsample(1:nrow(gp_data), size=sample.sizes[i], replace=F)
      tra_gp <- gp_data[ index.tra,]
      tst_gp.all <- gp_data[-index.tra,]
    }
    
    tra_gp <- tra_gp[c(-1)]
    
    prop <- sum(tra_gp[,1])/nrow(tra_gp)
    
    tra_gp <- data.frame(tra_gp)
    
    for(j in 1:length(shrinkage.list))
    {
      
      for (ls in 1:length(distribution.list))
      {
        
        gbm.object <- paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/Model_Objects/gbm/gbm_', sample.sizes[i],'_', s, '_', 
                             distribution.list[ls], '_', shrinkage.list[j], '.RData')
       
        n <- names(tra_gp)
        f <- as.formula(paste("Response ~", paste(n[!n %in% "Response"], collapse = " + ")))
        
        gbm.model <- gbm(f, data=tra_gp, distribution=distribution.list[ls],  bag.fraction = 0.3, verbose=F,
                         n.trees=round(20/shrinkage.list[j]), cv.folds=10, shrinkage = shrinkage.list[j], interaction.depth = 2,
                         n.minobsinnode = 10)
        
        opt.num.trees <- gbm.perf(gbm.model)
        predictors <- sum(relative.influence(gbm.model, opt.num.trees)>0)
        
        prob_tra <- predict.gbm(object=gbm.model, data=data.frame(tra_gp), type='response', n.trees=opt.num.trees)
        
        confusion <- table(tra_gp[,1], prob_tra > 0.5)
        
        tpr <- confusion[1,1] / sum(confusion[1,])
        tnr <- confusion[2,2] / sum(confusion[2,])
        acc <- (confusion[1,1] + confusion[2,2])/nrow(tra_gp)
        
        print('------------------------------------------')
        print(gbm.model)
        print('------------------------------------------')
        
        prob_tra<- predict(gbm.model, tra_gp[c(-1)],type="response", n.trees=opt.num.trees)
        save(gbm.model, file=gbm.object)
        
        roc.area <- auc(roc(prob_tra, factor(tra_gp$Response)))
        
        concordance <- OptimisedConc(tra_gp$Response, prob_tra)[1]
        
        print(paste('SampleSize', 'Distribution', 'shrinkage', 'Run', 'Prior', 'Predictors', 'OptNumTrees', 'TPR', 'TNR', 'Accuracy', 'AUC', 'Concordance'))
        print(paste(sample.sizes[i],distribution.list[ls], shrinkage.list[j], s, prop, predictors, opt.num.trees, tpr, tnr, acc, roc.area, concordance))
        
        
        write.table(cbind(sample.sizes[i], s, shrinkage.list[j], distribution.list[ls], predictors, opt.num.trees, tpr, 
                          tnr, acc, roc.area, concordance),
                    paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/gp_ds_training_gbm_v1_', min.sample.size,'_', max.sample.size, '_v3.csv'), 
                    append=TRUE, sep=",",row.names=FALSE,col.names=FALSE)
        
        
        
        
        print('------------------------------------------')
        print('---------- Running Validations -----------')
        print('------------------------------------------')
        
        if (balance==1)
        {
          index.tst_gp <- sample(1:nrow(rbind(tst_gp.ones, tst_gp.zeroes)),
                                 size=10000*10, replace=F)
        }
        
        if (balance==0)
        {
          index.tst_gp <- sample(1:nrow(tst_gp.all),
                                 size=10000*10, replace=F)        
        }
        
        print(paste('SampleSize', 'Distribution', 'shrinkage', 'Run', 'TestRun', 'Prior', 'Predictors', 'OptNumTrees', 'AUC', 'Concordance'))
        
        for (l in 1:10)
        {
          if (balance==1)
          {
            tst_gp <- rbind(tst_gp.ones, tst_gp.zeroes)[index.tst_gp[((l-1)*10000 + 1):(l*10000)],]
            tst_gp <- tst_gp[c(-1)]
            tst_gp <- data.frame(tst_gp)
          }
          
          if (balance==0)
          {
            tst_gp <- tst_gp.all[index.tst_gp[((l-1)*10000 + 1):(l*10000)],]
            tst_gp <- tst_gp[c(-1)]
            tst_gp <- data.frame(tst_gp)
          }
          
          prop.tst <- sum(tst_gp[,1])/nrow(tst_gp)
          
          gc()
          prob_tst <- predict(gbm.model, tst_gp[c(-1)],type="response", n.trees=opt.num.trees)
          
          roc.area.tst <- auc(roc(prob_tst, factor(tst_gp$Response)))
          
          concordance.tst <- OptimisedConc(tst_gp$Response,prob_tst)[1]
          
          print(paste(sample.sizes[i],distribution.list[ls], shrinkage.list[j], s, l, prop.tst, 
                      predictors, opt.num.trees, roc.area.tst, concordance.tst))
          
          write.table(cbind(sample.sizes[i], s, shrinkage.list[j], distribution.list[ls], predictors, opt.num.trees, l,
                            roc.area.tst, concordance.tst),
                      paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/gp_ds_testing_gbm_v1_', min.sample.size,'_', max.sample.size, '_v3.csv'), 
                      append=TRUE, sep=",",row.names=FALSE,col.names=FALSE)
          
          
          
        }
        
      }
      
    }
    
    rm(tst_gp.ones, tst_gp.zeroes)
    gc()
    
  }
  
}

