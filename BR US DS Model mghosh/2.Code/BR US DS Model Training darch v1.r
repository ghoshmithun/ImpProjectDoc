install.packages("ada")
install.packages("ffbase")
install.packages("ff")
install.packages("plyr")
install.packages("VIF")
install.packages("pracma")
install.packages("verification")
install.packages("ROCR")
install.packages("pROC")
install.packages("darch",dep=TRUE)

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
library(darch)
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



#options(fftempdir = "C:/Users/mghosh/Documents/ffdf")

br_data_all <-read.table.ffdf(file="//10.8.8.51/lv0/Move to Box/Mithun/projects/9.BR_US_DS_Project/1.data/BR_data_full.txt",header = TRUE,VERBOSE = TRUE,
                          sep='|',colClasses = c(rep("numeric",51)))


br_data_all <- subset(br_data_all,select=c('customer_key','Response','percent_disc_last_12_mth','num_days_on_books','per_elec_comm','num_em_campaign','num_units_12mth','disc_ats',
'time_since_last_retail_purchase','avg_order_amt_last_6_mth','br_gp_net_sales_ratio','non_disc_ats','ratio_rev_wo_rewd_12mth','on_sales_item_rev_12mth',
'br_hit_ind_tot','br_on_sales_ratio','on_sales_rev_ratio_12mth','ratio_rev_rewd_12mth','num_order_num_last_6_mth','card_status','ratio_order_6_12_mth',
'ratio_order_units_6_12_mth','ratio_disc_non_disc_ats','sale_hit_ind_tot','num_disc_comm_responded','mobile_ind_tot','gp_hit_ind_tot','br_bf_net_sales_ratio',
'num_dist_catg_purchased','total_plcc_cards','on_hit_ind_tot','pct_off_hit_ind_tot','br_go_net_sales_ratio','at_hit_ind_tot','purchased','searchdex_ind_tot',
'factory_hit_ind_tot','clearance_hit_ind_tot','markdown_hit_ind_tot'))


min.sample.size <- 20000; max.sample.size <- 40000;

sample.sizes  <- seq(min.sample.size, max.sample.size, by=5000)

actFun <- c( 'sigmoidUnit',  'softplusUnit', 'softmaxUnit')
  
balance <- 1

headers1<-cbind('SampleSize', 'Run', 'ActivationFunction', 'Prior','TPR','TNR',  'Accuracy', 'AUC', 'Concordance')
write.table(headers1, paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/9.BR_US_DS_Project/3.Documents/model_report/BR_US_DS_darch_train_', min.sample.size,'_', max.sample.size, '.csv'), append=FALSE, sep=",",row.names=FALSE,col.names=FALSE)

headers2<-cbind('SampleSize', 'ModelRun', 'TestRun' , 'ActivationFunction', 'TestPrior','TPR','TNR',  'Accuracy', 'AUC', 'Concordance')
write.table(headers2, paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/9.BR_US_DS_Project/3.Documents/model_report/BR_US_DS_darch_test_', min.sample.size,'_', max.sample.size, '.csv'), append=FALSE, sep=",",row.names=FALSE,col.names=FALSE)


total_obs =nrow(br_data_all)
for(i in 1:length(sample.sizes))
{ 
  model_data_size <- round(0.3*total_obs)
  br_data <- br_data_all[bigsample(1:total_obs,size=model_data_size,replace = F),]
  names(br_data) <- colnames(br_data_all)
  rownames(br_data) <- seq_len(model_data_size)
  for (s in 1:2)
  {
    
    if (balance==1)
    {
      br_data.ones <- br_data[br_data[,2]==1,]
      index.ones.tra <- bigsample(1:nrow(br_data.ones), size=0.5*sample.sizes[i], replace=F)
      tra.br.ones <- br_data.ones[ index.ones.tra,]
      tst.br.ones <- br_data.ones[-index.ones.tra,]
      
      rm(br_data.ones); gc();
      
      
      br_data.zeroes <- br_data[br_data[,2]!=1,]
      index.zeroes.tra <- bigsample(1:nrow(br_data.zeroes), size=0.5*sample.sizes[i], replace=F)
      tra.br.zeroes <- br_data.zeroes[ index.zeroes.tra,]
      tst.br.zeroes <- br_data.zeroes[-index.zeroes.tra,]
      
      rm(br_data.zeroes); gc();
      
      tra.br <- rbind(tra.br.ones, tra.br.zeroes)
      rm(tra.br.ones, tra.br.zeroes); gc();
      
    }
    
    if (balance==0)
    {
      index.tra <- bigsample(1:nrow(br_data), size=sample.sizes[i], replace=F)
      tra.br <- br_data[ index.tra,]
      tst.br.all <- br_data[-index.tra,]
      
    }
    
    tra.br <- tra.br[,c(-1)]
    
    prop <- sum(tra.br[,1])/nrow(tra.br)
    
     for(act in actFun){     
        darch.object <- paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/9.BR_US_DS_Project/Model_Objects/darch/darch_', sample.sizes[i],'_', s, '_', act, '.RData')
		
        darch.model <- darch(x=tra.br[,-1], y=as.factor(tra.br[,1]), layers = c(27,18,12), darch.numEpochs = 100, darch.stopClassErr = 0, retainData = T, darch.returnBestModel=T, darch.unitFunction=act)
        
       
        pred_tra <- predict(darch.model, newdata=data.matrix(tra.br[,c(-1)]))[,2]
		    pred_class<- predict(darch.model, newdata=data.matrix(tra.br[,c(-1)]),type = "class")
		   
		   tpr <- prop.table(table(tra.br[,1], pred_class),1)[2, 2]
		   tnr <- prop.table(table(tra.br[,1], pred_class),1)[1, 1]
        
        acc <- sum(diag(table(pred_class,tra.br[,1])))/sample.sizes[i]
        
        print('------------------------------------------')
        bestmodel<- darchTest(darch.model)
        bestmodel
        print('------------------------------------------')
             
        
        save( darch.model,file=darch.object)
        
        roc.area <- auc(roc(pred_tra,as.factor(tra.br[,1])))
        
        concordance <- OptimisedConc(tra.br$Response,pred_tra)[1]
        
        print(paste('SampleSize', 'Run', 'ActivationFunction', 'Prior','TPR','TNR',  'Accuracy', 'AUC', 'Concordance'))
        print(paste(sample.sizes[i], s, act, prop, tpr,tnr, acc, roc.area, concordance))
        
        
        write.table(cbind(sample.sizes[i], s, act, prop, tpr,tnr, acc, roc.area, concordance),
                    paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/9.BR_US_DS_Project/3.Documents/model_report/BR_US_DS_darch_train_', min.sample.size,'_', max.sample.size, '.csv'), 
                    append=TRUE, sep=",",row.names=FALSE,col.names=FALSE)
        
                gc()
        
        print('------------------------------------------')
        print('---------- Running Validations -----------')
        print('------------------------------------------')
        
        if (balance==1)
        {
          index.tst.br <- sample(1:nrow(rbind(tst.br.ones, tst.br.zeroes)),
                                 size=10000*10, replace=F)
        }
        
        if (balance==0)
        {
          index.tst.br <- sample(1:nrow(tst.br.all),
                                 size=10000*10, replace=F)        
        }
        
        print(paste('SampleSize', 'ModelRun', 'TestRun' , 'ActivationFunction', 'TestPrior','TPR','TNR',  'Accuracy', 'AUC', 'Concordance'))
        
        for (l in 1:10)
        {
          if (balance==1)
          {
            tst.br <- rbind(tst.br.ones, tst.br.zeroes)[index.tst.br[((l-1)*10000 + 1):(l*10000)],]
            tst.br <- tst.br[,c(-1)]
            tst.br <- data.frame(tst.br)
          }
          
          if (balance==0)
          {
            tst.br <- tst.br.all[index.tst.br[((l-1)*10000 + 1):(l*10000)],]
            tst.br <- tst.br[c(-1)]
            tst.br <- data.frame(tst.br)
          }
          
          prop.tst <- sum(tst.br[,1])/nrow(tst.br)
          
          gc()
          pred_tst <- predict(darch.model, newdata=data.matrix(tst.br[,c(-1)]))[,2]
          pred_class.test<- predict(darch.model, newdata=data.matrix(tst.br[,c(-1)]),type = "class")
          
          tpr.test <- prop.table(table(tst.br[,1], pred_class.test),1)[2, 2]
          tnr.test <- prop.table(table(tst.br[,1], pred_class.test),1)[1, 1]
          
          acc.test <- sum(diag(table(pred_class.test,tst.br[,1])))/nrow(tst.br)
          
          roc.area.test <- auc(roc(pred_tst,as.factor(tst.br[,1])))
          
          concordance.test <- OptimisedConc(tst.br$Response,pred_tst)[1]
          
          print(paste(sample.sizes[i], s, l, act, prop.tst, tpr.test, tnr.test, acc.test,roc.area.test, concordance.test))
          
          write.table(cbind(sample.sizes[i], s, l, act, prop.tst, tpr.test, tnr.test, acc.test,roc.area.test, concordance.test),
                      paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/9.BR_US_DS_Project/3.Documents/model_report/BR_US_DS_darch_test_', min.sample.size,'_', max.sample.size, '.csv'), 
                      append=TRUE, sep=",",row.names=FALSE,col.names=FALSE)
          
          
          
        }
            
    }
	 rm(tst.br.ones, tst.br.zeroes)
    gc()
  }
  rm(br_data) ; gc();
}

