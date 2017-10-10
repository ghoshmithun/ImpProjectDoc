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









require(data.table)
gp_data <-read.table.ffdf(file="//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/1. data/GP_full_data_base.txt",
                          header = TRUE,VERBOSE = TRUE,
                          sep='|',colClasses = c(rep("numeric",50)))

gp_data <- subset(gp_data,select=c('customer_key','Response',
'percent_disc_last_12_mth',
'percent_disc_last_6_mth',
'per_elec_comm',
'gp_hit_ind_tot',
'num_units_12mth',
'num_em_campaign',
'disc_ats',
'avg_order_amt_last_6_mth',
'Time_Since_last_disc_purchase',
'non_disc_ats',
'gp_on_net_sales_ratio',
'on_sales_rev_ratio_12mth',
'mobile_ind_tot',
'ratio_order_6_12_mth',
'pct_off_hit_ind_tot',
'ratio_rev_wo_rewd_12mth',
'ratio_disc_non_disc_ats',
'card_status',
'br_hit_ind_tot',
'gp_br_sales_ratio',
'ratio_order_units_6_12_mth',
'num_disc_comm_responded',
'purchased',
'ratio_rev_rewd_12mth',
'num_dist_catg_purchased',
'num_order_num_last_6_mth',
'at_hit_ind_tot',
'gp_go_net_sales_ratio',
'clearance_hit_ind_tot',
'searchdex_ind_tot',
'total_plcc_cards',
'factory_hit_ind_tot',
'gp_bf_net_sales_ratio',
'markdown_hit_ind_tot'))





#remove all except gp_data
rm(list=setdiff(ls(), "gp_data"))
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









min.sample.size <- 10000; max.sample.size <- 15000;
sample.sizes <- seq(min.sample.size, max.sample.size, by=1000)

headers1<-cbind("samplesize", "run", "TPR", "TNR", "Accuracy", "AUC","Concordance")
write.table(headers1, paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/gp_ds_training_xgb_', min.sample.size,'_', max.sample.size, '.csv'), 
            append=FALSE, sep=",",row.names=FALSE,col.names=FALSE)

headers2<-cbind("samplesize","run", "SampleNumber", "Prior", "AUC","Concordance")
write.table(headers2, paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/gp_ds_testing_xgb_', min.sample.size,'_', max.sample.size, '.csv'), 
            append=FALSE, sep=",",row.names=FALSE,col.names=FALSE)



prior <- sum(gp_data[,2])/nrow(gp_data)

gp_data.ones <- gp_data[gp_data[,2]==1,]
gp_data.zeroes <- gp_data[gp_data[,2]!=1,]

cv.ind <- 1


for(i in 1:length(sample.sizes))
{ 
  gc();
  for (s in 1:10)
  {

    index.ones.tra <- bigsample(1:nrow(gp_data.ones), size=0.5*sample.sizes[i], replace=F)
    index.zeroes.tra <- bigsample(1:nrow(gp_data.zeroes), size=0.5*sample.sizes[i], replace=F)
    
    tra.gp.ones <- gp_data.ones[index.ones.tra,]
    tra.gp.zeroes <- gp_data.zeroes[index.zeroes.tra,]
    
    tra.gp <- rbind(tra.gp.ones, tra.gp.zeroes)
    
    #create index of test dataset (rest from train)
    index.ones.tst <- c(1:nrow(gp_data.ones))[-index.ones.tra]
    index.zeroes.tst <- c(1:nrow(gp_data.zeroes))[-index.zeroes.tra]
    
    #remove customer_key
    tra.gp <- tra.gp[c(-1)]
    
    #print post prob (0.5)
    print(prop <- sum(tra.gp[,1])/nrow(tra.gp))
    
    #split into matrix X and vector y
    xmatrix <- as.matrix(tra.gp[,2:ncol(tra.gp)])
    yvec = as.matrix(tra.gp[,1])
    
    
    param <- list("objective" =  "binary:logistic",
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
    
    
    xgb.object <- paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/Model_Objects/xgb/',
                         sample.sizes[i], '_', s, '.RData')
    
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
    
    print(paste('SampleSize', 'Run', 'Prior', 'TPR', 'TNR', 'Accuracy', 'AUC', 'Concordance'))
    print(paste(sample.sizes[i], s, prop, tpr, tnr, acc, roc.area, concordance))
    
    write.table(cbind(sample.sizes[i], s, tpr, tnr, acc, roc.area, concordance),
                paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/gp_ds_training_xgb_', min.sample.size,'_', max.sample.size, '.csv'), 
                append=TRUE, sep=",",row.names=FALSE,col.names=FALSE)
    
    print('------------------------------------------')
    print('---------- Running Validations -----------')
    print('------------------------------------------')

    #create test dataset
    tst.gp.ones <- gp_data.ones[bigsample(index.ones.tst, size=round(prior*100000), replace=F),]
    tst.gp.zeroes <- gp_data.zeroes[bigsample(index.zeroes.tst, size=100000-nrow(tst.gp.ones), replace=F),]
    index.tst.gp <- sample(1:nrow(rbind(tst.gp.ones, tst.gp.zeroes)),size=100000, replace=F)
    
    print(paste('SampleSize', 'Run', 'TestRun', 'Prior', 'AUC', 'Concordance'))
    
    for (l in 1:10)
    {
      
      tst.gp <- rbind(tst.gp.ones, tst.gp.zeroes)[index.tst.gp[((l-1)*10000 + 1):(l*10000)],]
      
      #remove customer_key
      tst.gp <- tst.gp[c(-1)]
      
      #split into matrix X and vector y
      xmatrix <- as.matrix(tst.gp[,2:ncol(tst.gp)])
      yvec = as.matrix(tst.gp[,1])
      
      #prior prob
      prop.tst <- sum(tst.gp[,1])/nrow(tst.gp)
      
      gc()

      prob_tst <- predict(xgb.model, xmatrix)
      
      roc.area.tst <- auc(roc(factor(yvec), prob_tst))
      
      concordance.tst <- OptimisedConc(yvec,prob_tst)[1]
      
      print(paste(sample.sizes[i], s, l, prop.tst, roc.area.tst, concordance.tst))
      
      write.table(cbind(sample.sizes[i], s, l, prop.tst, roc.area.tst, concordance.tst),
                  paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/gp_ds_testing_xgb_', min.sample.size,'_', max.sample.size, '.csv'), 
                  append=TRUE, sep=",",row.names=FALSE,col.names=FALSE)
      
    }
  }
}






