install.packages("ada")
install.packages("ffbase")
install.packages("ff")
install.packages("plyr")
install.packages("VIF")
install.packages("pracma")
install.packages("verification")
install.packages("ROCR")
install.packages("pROC")
install.packages("randomForest")

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
library(randomForest)

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



options(fftempdir = "C:/Users/mghosh/Documents/ffdf")

gp_data <-read.table.ffdf(file="//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/1. data/GP_full_data_base.txt",
                              header = TRUE,VERBOSE = TRUE,
                              sep='|',colClasses = c(rep("numeric",50)))

gp_data <- subset(gp_data,select=c('customer_key','Response','Time_since_acquisition',
			'percent_disc_last_6_mth','non_disc_ats','ratio_rev_rewd_12mth','per_elec_comm','avg_order_amt_last_6_mth','Time_Since_last_Retail_purchase',
			'num_em_campaign','disc_ats','num_units_6mth','ratio_order_6_12_mth','Time_Since_last_Onl_purchase','ratio_disc_non_disc_ats',
			'num_disc_comm_responded','on_sales_rev_ratio_12mth','ratio_rev_wo_rewd_12mth','on_sales_item_rev_12mth','time_since_last_disc_purchase',
			'card_status','mobile_ind_tot','pct_off_hit_ind_tot','num_dist_catg_purchased','ratio_order_units_6_12_mth','gp_hit_ind_tot','on_gp_net_sales_ratio','clearance_hit_ind_tot','num_order_num_last_6_mth','purchased','br_hit_ind_tot','on_go_net_sales_ratio','on_br_sales_ratio','total_plcc_cards','at_hit_ind_tot','on_bf_net_sales_ratio','searchdex_ind_tot','factory_hit_ind_tot','markdown_hit_ind_tot' ))

min.sample.size <- 5000; max.sample.size <- 20000;

sample.sizes  <- seq(min.sample.size, max.sample.size, by=1000)

ntree.list    <- seq(100, 1000, by=100)

nodesize.list <- c(1, seq(10, 100, 10)) 
  
balance <- 1

headers1<-cbind("samplesize", "run", "ntree", "nodesize", "TPR", "TNR", "Accuracy", "AUC","Concordance")
write.table(headers1, paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/GP_DS_RFT_train_', min.sample.size,'_', max.sample.size, '_v3.csv'), 
            append=FALSE, sep=",",row.names=FALSE,col.names=FALSE)

headers2<-cbind("samplesize","run", "ntree", "nodesize", "SampleNumber", "AUC","Concordance")
write.table(headers2, paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/GP_DS_RFT_test_', min.sample.size,'_', max.sample.size, '_v3.csv'), 
            append=FALSE, sep=",",row.names=FALSE,col.names=FALSE)


for(i in 1:length(sample.sizes))
{ 
  for (s in 1:10)
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
    
    for(j in 1:length(ntree.list))
    {
      
      for (ls in 1:length(nodesize.list))
      {
        
        rft.object <- paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/Model_Objects/RandomForest/', sample.sizes[i],'_', s, '_', 
                             ntree.list[j], '_', nodesize.list[ls], '.RData')
        
        rft.model <- randomForest(y=as.factor(tra.gp[,1]), x=tra.gp[,c(-1)], ntree=100, 
                                  mtry=round(sqrt(ncol(tra.gp[,c(-1)]))), replace=T,
                                  nodesize=16, importance=T, proximity=T)
        
        tpr <- rft.model$confusion[1,1] / sum(rft.model$confusion[1,])
        tnr <- rft.model$confusion[2,2] / sum(rft.model$confusion[2,])
        acc <- (rft.model$confusion[1,1] + rft.model$confusion[2,2])/nrow(tra.gp)
        
        print('------------------------------------------')
        print(rft.model)
        print('------------------------------------------')
        
        prob_tra<- predict(rft.model, tra.gp[,c(-1)], type="prob")
        
        save(rft.model, file=rft.object)
        
        roc.area <- auc(roc(tra.gp[,1], prob_tra[,2]))
        
        concordance <- OptimisedConc(tra.gp$Response,prob_tra[,2])[1]
        
        print(paste('SampleSize', 'Run', 'ntree', 'nodesize','Prior', 'TPR', 'TNR', 'Accuracy', 'AUC', 'Concordance'))
        print(paste(sample.sizes[i], s, ntree.list[j], nodesize.list[ls], prop, tpr, tnr, acc, roc.area, concordance))
        
        
        write.table(cbind(sample.sizes[i], s, ntree.list[j], nodesize.list[ls], tpr, 
                          tnr, acc, roc.area, concordance),
                    paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/GP_DS_RFT_train_', min.sample.size,'_', max.sample.size, '_v3.csv'), 
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
        
        print(paste('SampleSize', 'Run', 'ntree', 'nodesize', 'TestRun', 'Prior', 'AUC', 'Concordance'))
        
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
          prob_tst <- predict(rft.model,tst.gp[,c(-1)],type="prob")
          
          roc.area.tst <- auc(roc(tst.gp[,1], prob_tst[,2]))
          
          concordance.tst <- OptimisedConc(tst.gp$Response,prob_tst[,2])[1]
          
          print(paste(sample.sizes[i], s, ntree.list[j], nodesize.list[ls], l, prop.tst, 
                      roc.area.tst, concordance.tst))
          
          write.table(cbind(sample.sizes[i], s, ntree.list[j], nodesize.list[ls], l,
                            roc.area.tst, concordance.tst),
                      paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/GP_DS_RFT_test_', min.sample.size,'_', max.sample.size, '_v3.csv'), 
                      append=TRUE, sep=",",row.names=FALSE,col.names=FALSE)
          
          
          
        }
        
      }
      
    }
    
    rm(tst.gp.ones, tst.gp.zeroes)
    gc()
    
  }
  
}

