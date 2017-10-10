install.packages("ada")
install.packages("ffbase")
install.packages("ff")
install.packages("plyr")
install.packages("VIF")
install.packages("pracma")
install.packages("verification")
install.packages("ROCR")
install.packages("pROC")
install.packages("xgboost")
install.packages("chron")

rm(list=ls())
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


setwd('//10.8.8.51/lv0/Tanumoy/Datasets/Model Replication')


at.data <-read.csv.ffdf(file="//10.8.8.51/lv0/Tanumoy/Datasets/Model Replication/at_catalog_data_v4.csv",
                        header = T, VERBOSE = TRUE, sep=',')


colnames(at.data) <- c("customer_key",
                       "bought",
                       "net_sales",
                       "disc_p",
                       "avg_unt_rtl",         
                       "unt_per_txn",
                       "on_sale_items",
                       "recency",
                       "items_browsed",
                       "items_abandoned",
                       "num_cat",
                       "net_sales_sameprd",
                       "disc_p_sameprd",
                       "num_txn_sameprd",
                       "avg_unt_rtl_sameprd",
                       "unt_per_txn_sameprd",
                       "on_sale_items_sameprd",
                       "season"
                       )	

at.data.df <- data.frame(at.data)

seasons <- unique(at.data.df$season)
for (i in 1:length(seasons))
{
  temp <- at.data.df[at.data.df$season==seasons[i], ]
  prop.resp <- sum(temp$bought)/nrow(temp)
  print(prop.resp)
  rm(temp); gc();
}

rm(at.data.df); gc();

min.sample.size <- 5000; max.sample.size <- 20000;

sample.sizes <- seq(min.sample.size, max.sample.size, by=1000)


headers1<-cbind("samplesize", "run", "TPR", "TNR", "Accuracy", "AUC","Concordance")
write.table(headers1, paste0('//10.8.8.51/lv0/Tanumoy/Datasets//Model Results/Catalog Response Model/at_catalog_training_xgb_', min.sample.size,'_', max.sample.size, '_v4.csv'), 
            append=FALSE, sep=",",row.names=FALSE,col.names=FALSE)

headers2<-cbind("samplesize","run", "SampleNumber", "Prior", "AUC","Concordance")
write.table(headers2, paste0('//10.8.8.51/lv0/Tanumoy/Datasets/Model Results/Catalog Response Model/at_catalog_testing_xgb_', min.sample.size,'_', max.sample.size, '_v4.csv'), 
            append=FALSE, sep=",",row.names=FALSE,col.names=FALSE)


cv.ind <- 1


for(i in 1:length(sample.sizes))
{ 
  for (s in 1:10)
  {
    at.data.ones <- at.data[at.data[,2]==1,]
    index.ones.tra <- bigsample(1:nrow(at.data.ones), size=0.5*sample.sizes[i], replace=F)
    tra.at.ones <- at.data.ones[ index.ones.tra,]
    tst.at.ones <- at.data.ones[-index.ones.tra,]
    
    
    
    
    rm(at.data.ones); gc();
    
    
    at.data.zeroes <- at.data[at.data[,2]!=1,]
    index.zeroes.tra <- bigsample(1:nrow(at.data.zeroes), size=0.5*sample.sizes[i], replace=F)
    tra.at.zeroes <- at.data.zeroes[ index.zeroes.tra,]
    tst.at.zeroes <- at.data.zeroes[-index.zeroes.tra,]
    
    rm(at.data.zeroes); gc();
    
    
    tra.at <- rbind(tra.at.ones, tra.at.zeroes)
    rm(tra.at.ones, tra.at.zeroes); gc();
    
    tra.at <- tra.at[c(-1, -ncol(tra.at))]
    
    print(prop <- sum(tra.at[,1])/nrow(tra.at))
    
    
    xmatrix <- as.matrix(tra.at[,2:ncol(tra.at)])
    yvec = as.matrix(tra.at[,1])
    
    
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
    
    
    xgb.object <- paste0('//10.8.8.51/lv0/Tanumoy/Datasets/Parameter Files/AT Catalog Response/XGB/v4/',
                         sample.sizes[i], '_', s, '.RData')
    
    xgb.model <- xgboost(data=xmatrix, label = yvec, objective = "binary:logistic",
                         verbose=F, nrounds=10)
    
    prob_tra <- predict(xgb.model, xmatrix)
    pred_tra <- ifelse(prob_tra > 0.5, 1, 0)
    tpr <- prop.table(table(yvec, pred_tra),1)[2, 2]
    tnr <- prop.table(table(yvec, pred_tra),1)[1, 1]
    acc <- sum(diag(prop.table(table(yvec, pred_tra))))
    
    roc.area <- auc(roc(factor(yvec), prob_tra))
    
    concordance <- OptimisedConc(yvec,prob_tra)[1]
    
    
    save(xgb.model, file=xgb.object)
    
    print(paste('SampleSize', 'Run', 'Prior', 'TPR', 'TNR', 'Accuracy', 'AUC', 'Concordance'))
    print(paste(sample.sizes[i], s, prop, tpr, tnr, acc, roc.area, concordance))
    
    write.table(cbind(sample.sizes[i], s, tpr, tnr, acc, roc.area, concordance),
                paste0('//10.8.8.51/lv0/Tanumoy/Datasets/Model Results/Catalog Response Model/at_catalog_training_xgb_', min.sample.size,'_', max.sample.size, '_v4.csv'), 
                append=TRUE, sep=",",row.names=FALSE,col.names=FALSE)
    
    print('------------------------------------------')
    print('---------- Running Validations -----------')
    print('------------------------------------------')
    
    index.tst.at <- sample(1:nrow(rbind(tst.at.ones, tst.at.zeroes)),
                           size=10000*10, replace=F)
    
    print(paste('SampleSize', 'Run', 'TestRun', 'Prior', 'AUC', 'Concordance'))
    
    for (l in 1:10)
    {
      
      tst.at <- rbind(tst.at.ones, tst.at.zeroes)[index.tst.at[((l-1)*10000 + 1):(l*10000)],]
      tst.at <- tst.at[c(-1, -ncol(tst.at))]
      
      xmatrix <- as.matrix(tst.at[,2:ncol(tst.at)])
      yvec = as.matrix(tst.at[,1])
      
      prop.tst <- sum(tst.at[,1])/nrow(tst.at)
      
      gc()
      prob_tst <- predict(xgb.model, xmatrix)
      
      roc.area.tst <- auc(roc(factor(yvec), prob_tst))
      
      concordance.tst <- OptimisedConc(yvec,prob_tst)[1]
      
      print(paste(sample.sizes[i], s, l, prop.tst, roc.area.tst, concordance.tst))
      
      write.table(cbind(sample.sizes[i], s, l, prop.tst, roc.area.tst, concordance.tst),
                  paste0('//10.8.8.51/lv0/Tanumoy/Datasets/Model Results/Catalog Response Model/at_catalog_testing_xgb_', min.sample.size,'_', max.sample.size, '_v4.csv'), 
                  append=TRUE, sep=",",row.names=FALSE,col.names=FALSE)
      
    }
  }
}

