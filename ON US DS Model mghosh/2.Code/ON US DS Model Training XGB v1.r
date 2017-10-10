#install.packages("ada")
#install.packages("ffbase")
#install.packages("ff")
#install.packages("plyr")
#install.packages("VIF")
#install.packages("pracma")
#install.packages("verification")
#install.packages("ROCR")
#install.packages("pROC")
#install.packages("xgboost")
#install.packages("chron")

#rm(list=ls())
gc()

library(ff)
library(ffbase)
library(ada)
library(ffbase)
library(plyr)
library(pracma)
library(VIF)
library(verification)
library(ROCR)
library(pROC)
library(chron)
library(xgboost)



on_data_all <-read.table.ffdf(file="//10.8.8.51/lv0/Move to Box/Mithun/projects/8.ON_US_DS_Project/1.data/on_full_data_base.txt",
                              header = TRUE,VERBOSE = TRUE,
                              sep='|',colClasses = c(rep("numeric",53)))


on_data_all <- subset(on_data_all,select=c('customer_key','Response','Time_since_acquisition',
			'percent_disc_last_6_mth','non_disc_ats','ratio_rev_rewd_12mth','per_elec_comm','avg_order_amt_last_6_mth','Time_Since_last_Retail_purchase',
			'num_em_campaign','disc_ats','num_units_6mth','ratio_order_6_12_mth','Time_Since_last_Onl_purchase','ratio_disc_non_disc_ats',
			'num_disc_comm_responded','on_sales_rev_ratio_12mth','ratio_rev_wo_rewd_12mth','on_sales_item_rev_12mth','time_since_last_disc_purchase',
			'card_status','mobile_ind_tot','pct_off_hit_ind_tot','num_dist_catg_purchased','ratio_order_units_6_12_mth','gp_hit_ind_tot','on_gp_net_sales_ratio','clearance_hit_ind_tot','num_order_num_last_6_mth','purchased','br_hit_ind_tot','on_go_net_sales_ratio','on_br_sales_ratio','total_plcc_cards','at_hit_ind_tot','on_bf_net_sales_ratio','searchdex_ind_tot','factory_hit_ind_tot','markdown_hit_ind_tot' ))



#remove all except gp_data
rm(list=setdiff(ls(), "on_data_all"))
gc();


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









min.sample.size <- 10000; max.sample.size <- 20000;
sample.sizes <- seq(min.sample.size, max.sample.size, by=1000)

headers1<-cbind("Objective Parm","samplesize", "run", "TPR", "TNR", "Accuracy", "AUC","Concordance")
write.table(headers1, paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/8.ON_US_DS_Project/3.Documents/model/ON_US_ds_XGB_training_', min.sample.size,'_', max.sample.size, '.csv'), 
            append=FALSE, sep=",",row.names=FALSE,col.names=FALSE)

headers2<-cbind("Objective Parm","samplesize","run", "SampleNumber", "Prior", "AUC","Concordance")
write.table(headers2, paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/8.ON_US_DS_Project/3.Documents/model/ON_US_ds_XGB_testing_', min.sample.size,'_', max.sample.size, '.csv'), 
            append=FALSE, sep=",",row.names=FALSE,col.names=FALSE)


prior <- sum(on_data_all[,2])/nrow(on_data_all)

on_data_all.ones <- on_data_all[on_data_all[,2]==1,]
on_data_all.zeroes <- on_data_all[on_data_all[,2]!=1,]

cv.ind <- 1

for(i in 1:length(sample.sizes))
{ 
  gc();
  for (s in 1:10)
  {

    index.ones.tra <- bigsample(1:nrow(on_data_all.ones), size=0.5*sample.sizes[i], replace=F)
    index.zeroes.tra <- bigsample(1:nrow(on_data_all.zeroes), size=0.5*sample.sizes[i], replace=F)
    
    tra.on.ones <- on_data_all.ones[index.ones.tra,]
    tra.on.zeroes <- on_data_all.zeroes[index.zeroes.tra,]
    
    tra.on <- rbind(tra.on.ones, tra.on.zeroes)
    
    #create index of test dataset (rest from train)
    index.ones.tst <- c(1:nrow(on_data_all.ones))[-index.ones.tra]
    index.zeroes.tst <- c(1:nrow(on_data_all.zeroes))[-index.zeroes.tra]
    
    #remove customer_key
    tra.on <- tra.on[c(-1)]
    
    #print post prob (0.5)
    print(prop <- sum(tra.on[,1])/nrow(tra.on))
    
    #split into matrix X and vector y
    xmatrix <- as.matrix(tra.on[,2:ncol(tra.on)])
    yvec = as.matrix(tra.on[,1])
    
    
    param <- list("objective" = "binary:logistic",
                  "eval_metric" = "auc",
                  "num_class" = 2)
    
    if (cv.ind==1)
    {
      cv.nround <- 20
      cv.nfold <- 10
      cv.nthread <-  2
      
      bst.cv = xgb.cv(param=param, data = xmatrix, label = yvec, 
                      nfold = cv.nfold, nrounds = cv.nround, verbose=F)
    }
    
    
    xgb.object <- paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/8.ON_US_DS_Project/Model_objects/xgb/xgb_',
                         substr("binary:logistic",8,16),sample.sizes[i], '_', s, '.RData')
    
    xgb.model <- xgboost(data=xmatrix, label = yvec, objective = "binary:logistic",
                         verbose=F, nrounds=10)
    
    prob_tra <- predict(xgb.model, xmatrix)
    pred_tra <- ifelse(prob_tra > 0.5, 1, 0)
    tpr <- prop.table(table(yvec, pred_tra),1)[2, 2]
    tnr <- prop.table(table(yvec, pred_tra),1)[1, 1]
    acc <- sum(diag(prop.table(table(yvec, pred_tra))))
    roc.area <- auc(roc(factor(yvec), prob_tra))
    
    # keeps crashing here: what is this function doing?
    concordance <- OptimisedConc(yvec,prob_tra)[1]
    
    
    save(xgb.model, file=xgb.object)
    
    print(paste('Objective Parm','SampleSize', 'Run', 'Prior', 'TPR', 'TNR', 'Accuracy', 'AUC', 'Concordance'))
    print(paste("binary:logistic",sample.sizes[i], s, prop, tpr, tnr, acc, roc.area, concordance))
    
    write.table(cbind("binary:logistic",sample.sizes[i], s, tpr, tnr, acc, roc.area, concordance),
                paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/8.ON_US_DS_Project/3.Documents/model/ON_US_ds_XGB_training_', min.sample.size,'_', max.sample.size, '.csv'), 
                append=TRUE, sep=",",row.names=FALSE,col.names=FALSE)
    
    print('------------------------------------------')
    print('---------- Running Validations -----------')
    print('------------------------------------------')

    #create test dataset
    tst.on.ones <- on_data_all.ones[bigsample(index.ones.tst, size=round(prior*100000), replace=F),]
    tst.on.zeroes <- on_data_all.zeroes[bigsample(index.zeroes.tst, size=100000-nrow(tst.on.ones), replace=F),]
    index.tst.on <- sample(1:nrow(rbind(tst.on.ones, tst.on.zeroes)),size=100000, replace=F)
    
    print(paste('Objective Parm','SampleSize', 'Run', 'TestRun', 'Prior', 'AUC', 'Concordance'))
    
    for (l in 1:10)
    {
      
      tst.on <- rbind(tst.on.ones, tst.on.zeroes)[index.tst.on[((l-1)*10000 + 1):(l*10000)],]
      
      #remove customer_key
      tst.on <- tst.on[c(-1)]
      
      #split into matrix X and vector y
      xmatrix <- as.matrix(tst.on[,2:ncol(tst.on)])
      yvec = as.matrix(tst.on[,1])
      
      #prior prob
      prop.tst <- sum(tst.on[,1])/nrow(tst.on)
      
      gc()

      prob_tst <- predict(xgb.model, xmatrix)
      
      roc.area.tst <- auc(roc(factor(yvec), prob_tst))
      
      concordance.tst <- OptimisedConc(yvec,prob_tst)[1]
      
      print(paste("binary:logistic",sample.sizes[i], s, l, prop.tst, roc.area.tst, concordance.tst))
      
      write.table(cbind(sample.sizes[i], s, l, prop.tst, roc.area.tst, concordance.tst),
                  paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/8.ON_US_DS_Project/3.Documents/model/ON_US_ds_XGB_testing_', min.sample.size,'_', max.sample.size, '.csv'), 
                  append=TRUE, sep=",",row.names=FALSE,col.names=FALSE)
      
    }
  }
}






