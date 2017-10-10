install.packages("ada")
install.packages("ffbase")
install.packages("ff")
install.packages("plyr")
install.packages("VIF")
install.packages("pracma")
install.packages("verification")
install.packages("ROCR")
install.packages("pROC")
install.packages("drat", repos="https://cran.rstudio.com")
drat:::addRepo("dmlc")
install.packages("mxnet")

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
library(mxnet)

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



bf_data_all <-read.table.ffdf(file="//10.8.8.51/lv0/Move to Box/Mithun/projects/12.BRFS_US_DS_Project/1.data/BF_data_full_transformed.txt",sep="|",header = TRUE,VERBOSE = TRUE,colClasses = c(rep("numeric",50)))


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

#rm(list = setdiff(ls(),"bf_data_all"))

min.sample.size <- 5000; max.sample.size <- 25000;

sample.sizes  <- seq(min.sample.size, max.sample.size, by=1000)

num_round_list    <- seq(50, 100, by=50)

learning_rate <- c(0.005,0.01,0.05,0.07,0.1) 
  
balance <- 1

headers1<-cbind("samplesize", "run", "Num_Round", "LearningRate", "Accuracy", "AUC","Concordance")
write.table(headers1, paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/12.BRFS_US_DS_Project/3.Documents/model_report/bf_US_DS_Conv_NNET_train_', min.sample.size,'_', max.sample.size, '.csv'), 
            append=FALSE, sep=",",row.names=FALSE,col.names=FALSE)

headers2<-cbind("SampleSize", "TrainRun", "NumRound", "LearningRate", "TestRun", "PropTest","tpr.test","tnr.test", "roc.area.tst", "concordance.tst")
write.table(headers2, paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/12.BRFS_US_DS_Project/3.Documents/model_report/bf_US_DS_Conv_NNET_test_', min.sample.size,'_', max.sample.size, '.csv'), 
            append=FALSE, sep=",",row.names=FALSE,col.names=FALSE)

devices <- mx.cpu()
total_obs =nrow(bf_data_all)
for(i in 1:length(sample.sizes))
{ 
  model_data_size <- round(0.12*total_obs)
  bf_data <- bf_data_all[sample(1:total_obs,size=model_data_size,replace = F),]
  names(bf_data) <- colnames(bf_data_all)
  rownames(bf_data) <- seq_len(model_data_size)
  for (s in 1:3)
  {
    
    if (balance==1)
    {
      bf_data.ones <- bf_data[bf_data[,2]==1,]
      index.ones.tra <- sample(1:nrow(bf_data.ones), size=0.5*sample.sizes[i], replace=F)
      tra.bf.ones <- bf_data.ones[ index.ones.tra,]
      tra.bf.ones <- bf_data.ones[-index.ones.tra,]
      
      rm(bf_data.ones); gc();
      
      
      bf_data.zeroes <- bf_data[bf_data[,2]!=1,]
      index.zeroes.tra <- sample(1:nrow(bf_data.zeroes), size=0.5*sample.sizes[i], replace=F)
      tra.bf.zeroes <- bf_data.zeroes[ index.zeroes.tra,]
      tra.bf.zeroes <- bf_data.zeroes[-index.zeroes.tra,]
      
      rm(bf_data.zeroes); gc();
      
      tra.bf <- rbind(tra.bf.ones, tra.bf.zeroes)
      rm(tra.bf.ones, tra.bf.zeroes); gc();
      
    }
    
    if (balance==0)
    {
      index.tra <- sample(1:nrow(bf_data), size=sample.sizes[i], replace=F)
      tra.bf <- bf_data[ index.tra,]
      tra.bf.all <- bf_data[-index.tra,]
      
    }
    
    tra.bf <- tra.bf[,c(-1)]
    
    prop <- sum(tra.bf[,1])/nrow(tra.bf)
    
    
    for(j in 1:length(num_round_list))
    {
      
      for (ls in 1:length(learning_rate))
      {
        
        Conv_NNET.object <- paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/12.BRFS_US_DS_Project/Model_Objects/conv_nnet/bf_US_DS_Conv_NNET_', sample.sizes[i],'_', s, '_', num_round_list[j], '_', learning_rate[ls], '.RData')
        
        train_x=data.matrix(tra.bf[c(-1)])
        train_y <- tra.bf[,1]
        
        Conv_NNET.model <- mx.mlp(data=train_x, label=train_y, hidden_node=c(50,25,15), out_node=2, out_activation="softmax",
                num.round=num_round_list[j], array.batch.size=30, learning.rate=learning_rate[ls], momentum=0.9, device = devices,
                eval.metric=mx.metric.accuracy)
        
		prob_tra<- t(predict(Conv_NNET.model, data.matrix(tra.bf[,c(-1)])))
# Assign Labels
        predicted_labels <- max.col(prob_tra)-1
        acc <- sum(diag(table(predicted_labels,tra.bf[,1])))/sample.sizes[i]
        
        print('------------------------------------------')
        print('------------------------------------------')
               
        
        mx.model.save(model=Conv_NNET.model, prefix = Conv_NNET.object ,iteration = num_round_list[j])
        
        roc.area <- auc(roc(tra.bf[,1], prob_tra[,2]))
        
        concordance <- OptimisedConc(tra.bf$Response,prob_tra[,2])[1]
        
        print(paste('SampleSize', 'Run', 'Num_Round', 'LearningRate','Prior',  'Accuracy', 'AUC', 'Concordance'))
        print(paste(sample.sizes[i], s, num_round_list[j], learning_rate[ls], prop,  acc, roc.area, concordance))
        
        
        write.table(cbind(sample.sizes[i], s, num_round_list[j], learning_rate[ls], acc, roc.area, concordance),
                    paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/12.BRFS_US_DS_Project/3.Documents/model_report/bf_US_DS_Conv_NNET_train_', min.sample.size,'_', max.sample.size, '.csv'), 
                    append=TRUE, sep=",",row.names=FALSE,col.names=FALSE)
        
        rm(train_x,train_y)
        gc()
        
        print('------------------------------------------')
        print('---------- Running Validations -----------')
        print('------------------------------------------')
        
        if (balance==1)
        {
          index.tra.bf <- sample(1:nrow(rbind(tra.bf.ones, tra.bf.zeroes)),
                                 size=10000*10, replace=F)
        }
        
        if (balance==0)
        {
          index.tra.bf <- sample(1:nrow(tra.bf.all),
                                 size=10000*10, replace=F)        
        }
        
        print(paste("SampleSize", "TrainRun", "NumRound", "LearningRate", "TestRun", "PropTest","tpr.test","tnr.test", "roc.area.tst", "concordance.tst"))
        
        for (l in 1:10)
        {
          if (balance==1)
          {
            tra.bf <- rbind(tra.bf.ones, tra.bf.zeroes)[index.tra.bf[((l-1)*10000 + 1):(l*10000)],]
            tra.bf <- tra.bf[,c(-1)]
            tra.bf <- data.frame(tra.bf)
          }
          
          if (balance==0)
          {
            tra.bf <- tra.bf.all[index.tra.bf[((l-1)*10000 + 1):(l*10000)],]
            tra.bf <- tra.bf[c(-1)]
            tra.bf <- data.frame(tra.bf)
          }
          
          prop.tst <- sum(tra.bf[,1])/nrow(tra.bf)
          
          gc()
          prob_tst <- t(predict(Conv_NNET.model, data.matrix(tra.bf[,c(-1)])))
# Assign Labels
        predicted_labels_test <- max.col(prob_tst)-1
        acc.test <- sum(diag(table(predicted_labels_test,tra.bf[,1])))/nrow(tra.bf)
          roc.area.tst <- auc(roc( tra.bf[,1],prob_tst[,2]))
		  tpr.test <- prop.table(table(tst_bf[,1], predicted_labels_test),1)[2, 2]
          tnr.test <- prop.table(table(tst_bf[,1],predicted_labels_test),1)[1, 1]
          
          concordance.tst <- OptimisedConc(tra.bf$Response,prob_tst[,2])[1]
          
          print(paste(sample.sizes[i], s, num_round_list[j], learning_rate[ls], l, prop.tst,tpr.test,tnr.test, roc.area.tst, concordance.tst))
          
          write.table(cbind(sample.sizes[i], s, num_round_list[j], learning_rate[ls], l, prop.tst, tpr.test,tnr.test, roc.area.tst, concordance.tst),
                      paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/12.BRFS_US_DS_Project/3.Documents/model_report/bf_US_DS_Conv_NNET_test_', min.sample.size,'_', max.sample.size, '.csv'), 
                      append=TRUE, sep=",",row.names=FALSE,col.names=FALSE)
          
          
          
        }
        
      }
      
    }

  }
  rm(bf_data) ; gc();
}

