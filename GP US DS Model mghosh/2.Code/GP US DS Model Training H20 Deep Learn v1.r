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
library(ffbase)
library(plyr)
library(pracma)
library(VIF)
library(verification)
library(ROCR)
library(pROC)
library(party)
library(partykit)
library(caret)
library(AUC)
library(h2o)

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


#options(fftempdir = "C:/Users/mghosh/Documents/ffdf")

gp_data_all <-read.table.ffdf(file = "//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/1. data/GP_full_data_base.txt",
                          header = TRUE,VERBOSE = TRUE,
                          sep='|',colClasses = c(rep("numeric",50)))

gp_data_all <- subset(gp_data_all,select=c('customer_key','Response','percent_disc_last_12_mth','percent_disc_last_6_mth','per_elec_comm',
                                   'gp_hit_ind_tot','num_units_12mth','num_em_campaign','disc_ats','avg_order_amt_last_6_mth','Time_Since_last_disc_purchase',
                                   'non_disc_ats','gp_on_net_sales_ratio','on_sales_rev_ratio_12mth','mobile_ind_tot','ratio_order_6_12_mth',
                                   'pct_off_hit_ind_tot','ratio_rev_wo_rewd_12mth','ratio_disc_non_disc_ats','card_status','br_hit_ind_tot',
                                   'gp_br_sales_ratio','ratio_order_units_6_12_mth','num_disc_comm_responded','purchased','ratio_rev_rewd_12mth',
                                   'num_dist_catg_purchased','num_order_num_last_6_mth','at_hit_ind_tot','gp_go_net_sales_ratio','clearance_hit_ind_tot',
                                   'searchdex_ind_tot','total_plcc_cards','factory_hit_ind_tot','gp_bf_net_sales_ratio','markdown_hit_ind_tot' ))

min.sample.size <- 15000; max.sample.size <- 40000;

sample.sizes <- seq(min.sample.size, max.sample.size, by=5000)

act_func_list <- c("Rectifier", "Tanh","Maxout", "RectifierWithDropout", "TanhWithDropout", "MaxoutWithDropout")

balance <- 1

headers1<-cbind("samplesize", "run", "act_func", "TPR", "TNR", "Accuracy", "AUC", "Concordance")
write.table(headers1, paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/GP_US_h2o_training_', min.sample.size,'_', max.sample.size, '.csv'), 
            append=FALSE, sep=",",row.names=FALSE,col.names=FALSE)

headers2<-cbind('SampleSize', 'TrainRun','act_func', 'TestRun', 'Prior','TPR','TNR','Accuracy', 'AUC', 'Concordance')
write.table(headers2, paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/GP_US_h2o_testing_', min.sample.size,'_', max.sample.size, '.csv'), 
            append=FALSE, sep=",",row.names=FALSE,col.names=FALSE)

local.h2o <- h2o.init(ip = "localhost", port = 54321, startH2O = TRUE, nthreads=-1)
total_obs =nrow(gp_data_all)
for(i in 1:length(sample.sizes))
{ 
  model_data_size <- round(0.20*total_obs)
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
    trData<-as.h2o(tra.gp)
    
    for (act_func in act_func_list){
      h2o.model_path <- paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/Model_objects/h2o')
      h2o.model_name <- paste0('h2o_model_', sample.sizes[i],'_',act_func)
      
      
      # This returns the formula:
      tic <- proc.time()
      set.seed(1105)
      h20.model <- h2o.deeplearning(x = 2:38,
                                    y = 1,
                                    training_frame = trData,
                                    activation = act_func,
                                    input_dropout_ratio = 0.2,
                                    # hidden_dropout_ratios = c(0.5,0.5,0.5),
                                    balance_classes = TRUE, 
                                    hidden = c(27,18,10),
                                    epochs = 500)
      
      print(act_func)
      print(proc.time()-tic)      
      
      prob_tra <-  as.data.frame(h2o.predict(h20.model, trData))
      
      confusion <- table(tra.gp[,1] == 1, prob_tra[,1])
      
      tpr <- confusion[1,1] / sum(confusion[1,])
      tnr <- confusion[2,2] / sum(confusion[2,])
      acc <- (confusion[1,1] + confusion[2,2])/nrow(tra.gp)
      
      print('------------------------------------------')
      print(h20.model)
      print('------------------------------------------')
      
      h2o.saveModel(object=h20.model, path=h2o.model_path, force = TRUE)
      #a<-h2o.loadModel(path=paste0(h2o.model_path,"/","DeepLearning_model_R_1483283230165_3"))
      
      roc.area <- auc(roc( prob_tra[,3],tra.gp[,1]))
      
      concordance <- OptimisedConc(tra.gp[,1], prob_tra[,3])[1]
      
      print(paste('SampleSize', 'Run', "Act_func", 'Prior', 'TPR', 'TNR', 'Accuracy', 'AUC', 'Concordance'))
      print(paste(sample.sizes[i], s, act_func, prop, tpr, tnr, acc, roc.area, concordance))
      
      
      write.table(cbind(sample.sizes[i], s,act_func, tpr, tnr, acc, roc.area, concordance),
                  paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/GP_US_h2o_training_', min.sample.size,'_', max.sample.size, '.csv'), 
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
      
      print(paste('SampleSize', 'TrainRun','act_func', 'TestRun', 'Prior','TPR','TNR','Accuracy', 'AUC', 'Concordance'))
      
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
        tsData<-as.h2o(tst.gp)
        prob_tst <- as.data.frame(h2o.predict(h20.model, tsData))
        confusion.tst <- table(tst.gp[,1] == 1, prob_tst[,1])
        
        tpr.tst <- confusion.tst[1,1] / sum(confusion.tst[1,])
        tnr.tst <- confusion.tst[2,2] / sum(confusion.tst[2,])
        acc.tst <- (confusion.tst[1,1] + confusion.tst[2,2])/nrow(tst.gp)
        roc.area.tst <- auc(roc( prob_tst[,3], as.factor(tst.gp[,1])))
        
        concordance.tst <- OptimisedConc(tst.gp[,1], prob_tst[,3])[1]
        
        print(paste(sample.sizes[i], s, act_func, l, prop.tst,tpr.tst,tnr.tst,acc.tst, roc.area.tst, concordance.tst))
        
        write.table(cbind(sample.sizes[i], s, act_func, l, prop.tst,tpr.tst,tnr.tst,acc.tst, roc.area.tst, concordance.tst),
                    paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/GP_US_h2o_testing_', min.sample.size,'_', max.sample.size, '.csv'), 
                    append=TRUE, sep=",",row.names=FALSE,col.names=FALSE)
        
        
        rm(tst.gp,tsData)
      }
      
    }
    rm(tra.gp,trData)
    rm(tst.gp.ones, tst.gp.zeroes)
    gc()
    
  }
  rm(gp_data) ; gc();
}