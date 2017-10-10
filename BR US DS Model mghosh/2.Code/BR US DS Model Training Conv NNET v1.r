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

br_data_all <-read.table(file="//10.8.8.51/lv0/Move to Box/Mithun/projects/9.BR_US_DS_Project/1.data/BR_data_full.txt",header = TRUE,
                          sep='|',stringsAsFactors = F)


br_data_all <- subset(br_data_all,select=c('customer_key','Response','percent_disc_last_12_mth','num_days_on_books','per_elec_comm','num_em_campaign','num_units_12mth','disc_ats',
'time_since_last_retail_purchase','avg_order_amt_last_6_mth','br_gp_net_sales_ratio','non_disc_ats','ratio_rev_wo_rewd_12mth','on_sales_item_rev_12mth',
'br_hit_ind_tot','br_on_sales_ratio','on_sales_rev_ratio_12mth','ratio_rev_rewd_12mth','num_order_num_last_6_mth','card_status','ratio_order_6_12_mth',
'ratio_order_units_6_12_mth','ratio_disc_non_disc_ats','sale_hit_ind_tot','num_disc_comm_responded','mobile_ind_tot','gp_hit_ind_tot','br_bf_net_sales_ratio',
'num_dist_catg_purchased','total_plcc_cards','on_hit_ind_tot','pct_off_hit_ind_tot','br_go_net_sales_ratio','at_hit_ind_tot','purchased','searchdex_ind_tot',
'factory_hit_ind_tot','clearance_hit_ind_tot','markdown_hit_ind_tot'))

#rm(list = setdiff(ls(),"br_data_all"))

min.sample.size <- 5000; max.sample.size <- 25000;

sample.sizes  <- seq(min.sample.size, max.sample.size, by=1000)

num_round_list    <- seq(100, 500, by=100)

learning_rate <- c(0.005,0.01,0.03,0.05,0.07,0.1) 
  
balance <- 1

headers1<-cbind("samplesize", "run", "Num_Round", "LearningRate", "Accuracy", "AUC","Concordance")
write.table(headers1, paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/9.BR_US_DS_Project/3.Documents/model_report/BR_US_DS_Conv_NNET_train_', min.sample.size,'_', max.sample.size, '.csv'), 
            append=FALSE, sep=",",row.names=FALSE,col.names=FALSE)

headers2<-cbind("SampleSize", "TrainRun", "NumRound", "LearningRate", "TestRun", "PropTest","tpr.test","tnr.test", "roc.area.tst", "concordance.tst")
write.table(headers2, paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/9.BR_US_DS_Project/3.Documents/model_report/BR_US_DS_Conv_NNET_test_', min.sample.size,'_', max.sample.size, '.csv'), 
            append=FALSE, sep=",",row.names=FALSE,col.names=FALSE)

devices <- mx.cpu()
total_obs =nrow(br_data_all)
for(i in 1:length(sample.sizes))
{ 
  model_data_size <- round(0.12*total_obs)
  br_data <- br_data_all[sample(1:total_obs,size=model_data_size,replace = F),]
  names(br_data) <- colnames(br_data_all)
  rownames(br_data) <- seq_len(model_data_size)
  for (s in 1:3)
  {
    
    if (balance==1)
    {
      br_data.ones <- br_data[br_data[,2]==1,]
      index.ones.tra <- sample(1:nrow(br_data.ones), size=0.5*sample.sizes[i], replace=F)
      tra.br.ones <- br_data.ones[ index.ones.tra,]
      tra.br.ones <- br_data.ones[-index.ones.tra,]
      
      rm(br_data.ones); gc();
      
      
      br_data.zeroes <- br_data[br_data[,2]!=1,]
      index.zeroes.tra <- sample(1:nrow(br_data.zeroes), size=0.5*sample.sizes[i], replace=F)
      tra.br.zeroes <- br_data.zeroes[ index.zeroes.tra,]
      tra.br.zeroes <- br_data.zeroes[-index.zeroes.tra,]
      
      rm(br_data.zeroes); gc();
      
      tra.br <- rbind(tra.br.ones, tra.br.zeroes)
      rm(tra.br.ones, tra.br.zeroes); gc();
      
    }
    
    if (balance==0)
    {
      index.tra <- sample(1:nrow(br_data), size=sample.sizes[i], replace=F)
      tra.br <- br_data[ index.tra,]
      tra.br.all <- br_data[-index.tra,]
      
    }
    
    tra.br <- tra.br[,c(-1)]
    
    prop <- sum(tra.br[,1])/nrow(tra.br)
    
    
    for(j in 1:length(num_round_list))
    {
      
      for (ls in 1:length(learning_rate))
      {
        
        Conv_NNET.object <- paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/9.BR_US_DS_Project/Model_Objects/conv_nnet/BR_US_DS_Conv_NNET_', sample.sizes[i],'_', s, '_', num_round_list[j], '_', learning_rate[ls], '.RData')
        
        train_x=data.matrix(tra.br[c(-1)])
        train_y <- tra.br[,1]
        
        Conv_NNET.model <- mx.mlp(data=train_x, label=train_y, hidden_node=c(25,15), out_node=2, out_activation="softmax",
                num.round=num_round_list[j], array.batch.size=30, learning.rate=learning_rate[ls], momentum=0.9, device = devices,
                eval.metric=mx.metric.accuracy)
        
		prob_tra<- t(predict(Conv_NNET.model, data.matrix(tra.br[,c(-1)])))
# Assign Labels
        predicted_labels <- max.col(prob_tra)-1
        acc <- sum(diag(table(predicted_labels,tra.br[,1])))/sample.sizes[i]
        
        print('------------------------------------------')
        print(Conv_NNET.model)
        print('------------------------------------------')
               
        
        mx.model.save(model=Conv_NNET.model, prefix = Conv_NNET.object ,iteration = num_round_list[j])
        
        roc.area <- auc(roc(tra.br[,1], prob_tra[,2]))
        
        concordance <- OptimisedConc(tra.br$Response,prob_tra[,2])[1]
        
        print(paste('SampleSize', 'Run', 'Num_Round', 'LearningRate','Prior',  'Accuracy', 'AUC', 'Concordance'))
        print(paste(sample.sizes[i], s, num_round_list[j], learning_rate[ls], prop,  acc, roc.area, concordance))
        
        
        write.table(cbind(sample.sizes[i], s, num_round_list[j], learning_rate[ls], acc, roc.area, concordance),
                    paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/9.BR_US_DS_Project/3.Documents/model_report/BR_US_DS_Conv_NNET_train_', min.sample.size,'_', max.sample.size, '.csv'), 
                    append=TRUE, sep=",",row.names=FALSE,col.names=FALSE)
        
        rm(train_x,train_y)
        gc()
        
        print('------------------------------------------')
        print('---------- Running Validations -----------')
        print('------------------------------------------')
        
        if (balance==1)
        {
          index.tra.br <- sample(1:nrow(rbind(tra.br.ones, tra.br.zeroes)),
                                 size=10000*10, replace=F)
        }
        
        if (balance==0)
        {
          index.tra.br <- sample(1:nrow(tra.br.all),
                                 size=10000*10, replace=F)        
        }
        
        print(paste("SampleSize", "TrainRun", "NumRound", "LearningRate", "TestRun", "PropTest","tpr.test","tnr.test", "roc.area.tst", "concordance.tst"))
        
        for (l in 1:10)
        {
          if (balance==1)
          {
            tra.br <- rbind(tra.br.ones, tra.br.zeroes)[index.tra.br[((l-1)*10000 + 1):(l*10000)],]
            tra.br <- tra.br[,c(-1)]
            tra.br <- data.frame(tra.br)
          }
          
          if (balance==0)
          {
            tra.br <- tra.br.all[index.tra.br[((l-1)*10000 + 1):(l*10000)],]
            tra.br <- tra.br[c(-1)]
            tra.br <- data.frame(tra.br)
          }
          
          prop.tst <- sum(tra.br[,1])/nrow(tra.br)
          
          gc()
          prob_tst <- t(predict(Conv_NNET.model, data.matrix(tra.br[,c(-1)])))
# Assign Labels
        predicted_labels_test <- max.col(prob_tst)-1
        acc.test <- sum(diag(table(predicted_labels_test,tra.br[,1])))/nrow(tra.br)
          roc.area.tst <- auc(roc( tra.br[,1],prob_tst[,2]))
		  tpr.test <- prop.table(table(tst_br[,1], predicted_labels_test),1)[2, 2]
          tnr.test <- prop.table(table(tst_br[,1],predicted_labels_test),1)[1, 1]
          
          concordance.tst <- OptimisedConc(tra.br$Response,prob_tst[,2])[1]
          
          print(paste(sample.sizes[i], s, num_round_list[j], learning_rate[ls], l, prop.tst,tpr.test,tnr.test, roc.area.tst, concordance.tst))
          
          write.table(cbind(sample.sizes[i], s, num_round_list[j], learning_rate[ls], l, prop.tst, tpr.test,tnr.test, roc.area.tst, concordance.tst),
                      paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/9.BR_US_DS_Project/3.Documents/model_report/BR_US_DS_Conv_NNET_test_', min.sample.size,'_', max.sample.size, '.csv'), 
                      append=TRUE, sep=",",row.names=FALSE,col.names=FALSE)
          
          
          
        }
        
      }
      
    }
    
    rm(tra.br.ones, tra.br.zeroes)
    gc()
    
  }
  rm(br_data) ; gc();
}

