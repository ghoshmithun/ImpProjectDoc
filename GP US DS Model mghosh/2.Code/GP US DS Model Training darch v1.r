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

#rm(list = setdiff(ls(),"gp_data_all"))

min.sample.size <- 20000; max.sample.size <- 40000;

sample.sizes  <- seq(min.sample.size, max.sample.size, by=5000)

actFun <- c( 'sigmoidUnit',  'softplusUnit', 'softmaxUnit')
  
balance <- 1

headers1<-cbind('SampleSize', 'Run', 'ActivationFunction', 'Prior','TPR','TNR',  'Accuracy', 'AUC', 'Concordance')
write.table(headers1, paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/gp_US_DS_darch_train_', min.sample.size,'_', max.sample.size, '.csv'), append=FALSE, sep=",",row.names=FALSE,col.names=FALSE)

headers2<-cbind('SampleSize', 'ModelRun', 'TestRun' , 'ActivationFunction', 'TestPrior','TPR','TNR',  'Accuracy', 'AUC', 'Concordance')
write.table(headers2, paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/gp_US_DS_darch_test_', min.sample.size,'_', max.sample.size, '.csv'), append=FALSE, sep=",",row.names=FALSE,col.names=FALSE)


total_obs =nrow(gp_data_all)
for(i in 1:length(sample.sizes))
{ 
  model_data_size <- round(0.25*total_obs)
  gp_data <- gp_data_all[bigsample(1:total_obs,size=model_data_size,replace = F),]
  names(gp_data) <- colnames(gp_data_all)
  rownames(gp_data) <- seq_len(model_data_size)
  for (s in 1:2)
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
    
     for(act in actFun){     
        darch.object <- paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/Model_Objects/darch/gp_US_DS_darch_', sample.sizes[i],'_', s, '_', act, '.RData')
		
        darch.model <- darch(x=tra.gp[,-1], y=as.factor(tra.gp[,1]), layers = c(27,18,12), darch.numEpochs = 300, darch.stopClassErr = 0, retainData = T, darch.returnBestModel=T, darch.unitFunction=act)
        
       
        pred_tra <- predict(darch.model, newdata=data.matrix(tra.gp[,c(-1)]))[,2]
		    pred_class<- predict(darch.model, newdata=data.matrix(tra.gp[,c(-1)]),type = "class")
		   
		   tpr <- prop.table(table(tra.gp[,1], pred_class),1)[2, 2]
		   tnr <- prop.table(table(tra.gp[,1], pred_class),1)[1, 1]
        
        acc <- sum(diag(table(pred_class,tra.gp[,1])))/sample.sizes[i]
        
        print('---------------------------------------------------------------------')
        print('-----------------------------Model Selected--------------------------')
        bestmodel<- darchTest(darch.model)
        bestmodel
        print('----------------------------------------------------------------------')
             
        
        save( darch.model,file=darch.object)
        
        roc.area <- auc(roc(pred_tra,as.factor(tra.gp[,1])))
        
        concordance <- OptimisedConc(tra.gp$Response,pred_tra)[1]
        
        print(paste('SampleSize', 'Run', 'ActivationFunction', 'Prior','TPR','TNR',  'Accuracy', 'AUC', 'Concordance'))
        print(paste(sample.sizes[i], s, act, prop, tpr,tnr, acc, roc.area, concordance))
        
        
        write.table(cbind(sample.sizes[i], s, act, prop, tpr,tnr, acc, roc.area, concordance),
                    paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/gp_US_DS_darch_train_', min.sample.size,'_', max.sample.size, '.csv'), 
                    append=TRUE, sep=",",row.names=FALSE,col.names=FALSE)
        
      
        gc()
        
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
        
        print(paste('SampleSize', 'ModelRun', 'TestRun' , 'ActivationFunction', 'TestPrior','TPR','TNR',  'Accuracy', 'AUC', 'Concordance'))
        
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
          pred_tst <- predict(darch.model, newdata=data.matrix(tst.gp[,c(-1)]))[,2]
          pred_class.test<- predict(darch.model, newdata=data.matrix(tst.gp[,c(-1)]),type = "class")
          
          tpr.test <- prop.table(table(tst.gp[,1], pred_class.test),1)[2, 2]
          tnr.test <- prop.table(table(tst.gp[,1], pred_class.test),1)[1, 1]
          
          acc.test <- sum(diag(table(pred_class.test,tst.gp[,1])))/nrow(tst.gp)
          
          roc.area.test <- auc(roc(pred_tst,as.factor(tst.gp[,1])))
          
          concordance.test <- OptimisedConc(tst.gp$Response,pred_tst)[1]
          
          print(paste(sample.sizes[i], s, l, act, prop.tst, tpr.test, tnr.test, acc.test,roc.area.test, concordance.test))
          
          write.table(cbind(sample.sizes[i], s, l, act, prop.tst, tpr.test, tnr.test, acc.test,roc.area.test, concordance.test),
                      paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/gp_US_DS_darch_test_', min.sample.size,'_', max.sample.size, '.csv'), 
                      append=TRUE, sep=",",row.names=FALSE,col.names=FALSE)
          
          
          
        }
            
    
     }
    rm(tst.gp.ones, tst.gp.zeroes)
    gc()
  }
  rm(gp_data) ; gc();
}

