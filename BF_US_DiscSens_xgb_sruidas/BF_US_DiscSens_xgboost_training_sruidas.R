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





bf_data_all <-read.table.ffdf(file="//10.8.8.51/lv0/Move to Box/Mithun/projects/12.BRFS_US_DS_Project/1.data/BF_data_full_transformed_disc20.txt",sep="|",header = T,VERBOSE = TRUE,colClasses = c(rep("numeric",50)))

#rawdata=read.table.ffdf(file="Z:/Shraddha/brfs_discsens/BF_data_full_transformed.txt",header=T, VERBOSE = TRUE,
#                      sep='|',colClasses = c(rep("numeric",50)))
#bf_data_all=data.frame(rawdata)
#rm(rawdata)
gc()
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

#remove all except bf_data
bf_data_all=data.frame(bf_data_all)


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









min.sample.size <- 30000; max.sample.size <- 110000;
sample.sizes <- seq(min.sample.size, max.sample.size, by=20000)
nrounds=c(200,250,500)
max.depth=c(3,4,5)
eta=c(0.05,0.15,0.25)

headers1<-cbind("samplesize", "run","nrounds","max_depth","eta", "TPR", "TNR", "Accuracy", "AUC","Concordance")
write.table(headers1, paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/12.BRFS_US_DS_Project/3.Documents/model_report/bf_ds20_training_xgb_newnew_', min.sample.size,'_', max.sample.size, '.csv'), 
            append=FALSE, sep=",",row.names=FALSE,col.names=FALSE)

headers2<-cbind("samplesize","run", "SampleNumber", "Prior", "AUC","Concordance")
write.table(headers2, paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/12.BRFS_US_DS_Project/3.Documents/model_report/bf_ds20_testing_xgb_newnew_', min.sample.size,'_', max.sample.size, '.csv'), 
            append=FALSE, sep=",",row.names=FALSE,col.names=FALSE)



prior <- sum(bf_data_all[,2])/nrow(bf_data_all)

bf_data.ones <- bf_data_all[bf_data_all[,2]==1,]
bf_data.zeroes <- bf_data_all[bf_data_all[,2]!=1,]

cv.ind <- 1


for(i in 1:length(sample.sizes))
{ 
  gc()
  for (s in 1:10)
  {
    
    index.ones.tra <- bigsample(1:nrow(bf_data.ones), size=0.5*sample.sizes[i], replace=F)
    index.zeroes.tra <- bigsample(1:nrow(bf_data.zeroes), size=0.5*sample.sizes[i], replace=F)
    
    tra.bf.ones <- bf_data.ones[index.ones.tra,]
    tra.bf.zeroes <- bf_data.zeroes[index.zeroes.tra,]
    
    tra.bf <- rbind(tra.bf.ones, tra.bf.zeroes)
    
    #create index of test dataset (rest from train)
    index.ones.tst <- c(1:nrow(bf_data.ones))[-index.ones.tra]
    index.zeroes.tst <- c(1:nrow(bf_data.zeroes))[-index.zeroes.tra]
    
    #remove customer_key
    tra.bf <- tra.bf[c(-1)]
    
    #print post prob (0.5)
    print(prop <- sum(tra.bf[,1])/nrow(tra.bf))
    
    #split into matrix X and vector y
    xmatrix <- as.matrix(tra.bf[,2:ncol(tra.bf)])
    yvec = as.matrix(tra.bf[,1])
    
    
    
    for(j in 1:length(nrounds)){
      for(k in 1:length(max.depth)){
        for(l in 1:length(eta)){
    params = list(objective = "binary:logistic",eta=eta[l],max.depth = max.depth[k], eval_metric="error", booster="gbtree",silent=1)
    
    xgb.object <- paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/12.BRFS_US_DS_Project/Model_Objects/xgb/xgb_disc20_new_',
                         sample.sizes[i], '_', s,'_',nrounds[j],'_',max.depth[k],'_',eta[l], '.RData')
    
    xgb.model <- xgboost(data=xmatrix, label = yvec, verbose=1, nrounds=nrounds[j],params=params)
    
    prob_tra <- predict(xgb.model, xmatrix)
    pred_tra <- ifelse(prob_tra > 0.5, 1, 0)
    tpr <- prop.table(table(yvec, pred_tra),1)[2, 2]
    tnr <- prop.table(table(yvec, pred_tra),1)[1, 1]
    acc <- sum(diag(prop.table(table(yvec, pred_tra))))
    roc.area <- auc(roc(response=factor(tra.bf$Response), predictor=prob_tra))
    
    # keeps crashing here: what is this function doing?
    concordance <- OptimisedConc(yvec,prob_tra)[1]
    
    
    save(xgb.model, file=xgb.object)
    
    print(paste('SampleSize', 'Run','nrounds','max_depth','eta', 'TPR', 'TNR', 'Accuracy', 'AUC', 'Concordance'))
    print(paste(sample.sizes[i], s,nrounds[j],max.depth[k],eta[l] , tpr, tnr, acc, roc.area, concordance))
    
    write.table(cbind(sample.sizes[i], s,nrounds[j],max.depth[k],eta[l],tpr, tnr, acc, roc.area, concordance),
                paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/12.BRFS_US_DS_Project/3.Documents/model_report/bf_ds20_training_xgb_new_', min.sample.size,'_', max.sample.size, '.csv'), 
                append=TRUE, sep=",",row.names=FALSE,col.names=FALSE)
    
    print('------------------------------------------')
    print('---------- Running Validations -----------')
    print('------------------------------------------')
    
    #create test dataset
    tst.bf.ones <- bf_data.ones[bigsample(index.ones.tst, size=round(prior*100000), replace=F),]
    tst.bf.zeroes <- bf_data.zeroes[bigsample(index.zeroes.tst, size=100000-nrow(tst.bf.ones), replace=F),]
    index.tst.bf <- sample(1:nrow(rbind(tst.bf.ones, tst.bf.zeroes)),size=100000, replace=F)
    
    print(paste('SampleSize', 'Run', 'TestRun', 'Prior', 'AUC', 'Concordance'))
    
    for (m in 1:10)
    {
      
      tst.bf <- rbind(tst.bf.ones, tst.bf.zeroes)[index.tst.bf[((m-1)*10000 + 1):(m*10000)],]
      
      #remove customer_key
      tst.bf <- tst.bf[c(-1)]
      
      #split into matrix X and vector y
      xmatrix1 <- as.matrix(tst.bf[,2:ncol(tst.bf)])
      yvec1 = as.matrix(tst.bf[,1])
      
      #prior prob
      prop.tst <- sum(tst.bf[,1])/nrow(tst.bf)
      
      gc()
      
      prob_tst <- predict(xgb.model, xmatrix1)
      
      roc.area.tst <- auc(roc(response=factor(tst.bf$Response), predictor=prob_tst))
      
      concordance.tst <- OptimisedConc(yvec1,prob_tst)[1]
      
      print(paste(sample.sizes[i], s, m, prop.tst, roc.area.tst, concordance.tst))
      
      write.table(cbind(sample.sizes[i], s, m, prop.tst, roc.area.tst, concordance.tst),
                  paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/12.BRFS_US_DS_Project/3.Documents/model_report/bf_ds20_testing_xgb_new_', min.sample.size,'_', max.sample.size, '.csv'), 
                  append=TRUE, sep=",",row.names=FALSE,col.names=FALSE)
    }
    
        }
      }
      
      
    }
  }

  }







