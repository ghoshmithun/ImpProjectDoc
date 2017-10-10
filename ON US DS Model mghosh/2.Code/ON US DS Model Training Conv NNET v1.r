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


on_data_all <-read.table.ffdf(file="//10.8.8.51/lv0/Move to Box/Mithun/projects/8.ON_US_DS_Project/1.data/on_full_data_base.txt",
                              header = TRUE,VERBOSE = TRUE,
                              sep='|',colClasses = c(rep("numeric",53)))


on_data_all <- subset(on_data_all,select=c('customer_key','Response','Time_since_acquisition',
			'percent_disc_last_6_mth','non_disc_ats','ratio_rev_rewd_12mth','per_elec_comm','avg_order_amt_last_6_mth','Time_Since_last_Retail_purchase',
			'num_em_campaign','disc_ats','num_units_6mth','ratio_order_6_12_mth','Time_Since_last_Onl_purchase','ratio_disc_non_disc_ats',
			'num_disc_comm_responded','on_sales_rev_ratio_12mth','ratio_rev_wo_rewd_12mth','on_sales_item_rev_12mth','time_since_last_disc_purchase',
			'card_status','mobile_ind_tot','pct_off_hit_ind_tot','num_dist_catg_purchased','ratio_order_units_6_12_mth','gp_hit_ind_tot','on_gp_net_sales_ratio','clearance_hit_ind_tot','num_order_num_last_6_mth','purchased','br_hit_ind_tot','on_go_net_sales_ratio','on_br_sales_ratio','total_plcc_cards','at_hit_ind_tot','on_bf_net_sales_ratio','searchdex_ind_tot','factory_hit_ind_tot','markdown_hit_ind_tot' ))

rm(list = setdiff(ls(),"on_data_all"))

min.sample.size <- 5000; max.sample.size <- 25000;

sample.sizes  <- seq(min.sample.size, max.sample.size, by=1000)

num_round_list    <- seq(100, 500, by=100)

learning_rate <- c(0.005,0.01,0.03,0.05,0.07,0.1) 
  
balance <- 1

headers1<-cbind("samplesize", "run", "Num_Round", "LearningRate", "Accuracy", "AUC","Concordance")
write.table(headers1, paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/8.ON_US_DS_Project/3.Documents/model/ON_US_DS_Conv_NNET_train_', min.sample.size,'_', max.sample.size, '.csv'), 
            append=FALSE, sep=",",row.names=FALSE,col.names=FALSE)

headers2<-cbind("samplesize","run", "Num_Round", "LearningRate", "SampleNumber", "TestAccuracy","AUC","Concordance")
write.table(headers2, paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/8.ON_US_DS_Project/3.Documents/model/ON_US_DS_Conv_NNET_test_', min.sample.size,'_', max.sample.size, '.csv'), 
            append=FALSE, sep=",",row.names=FALSE,col.names=FALSE)

devices <- mx.cpu()
total_obs =nrow(on_data_all)
for(i in 1:length(sample.sizes))
{ 
  model_data_size <- round(0.12*total_obs)
  on_data <- on_data_all[bigsample(1:total_obs,size=model_data_size,replace = F),]
  names(on_data) <- colnames(on_data_all)
  rownames(on_data) <- seq_len(model_data_size)
  for (s in 1:3)
  {
    
    if (balance==1)
    {
      on_data.ones <- on_data[on_data[,2]==1,]
      index.ones.tra <- bigsample(1:nrow(on_data.ones), size=0.5*sample.sizes[i], replace=F)
      tra.on.ones <- on_data.ones[ index.ones.tra,]
      tst.on.ones <- on_data.ones[-index.ones.tra,]
      
      rm(on_data.ones); gc();
      
      
      on_data.zeroes <- on_data[on_data[,2]!=1,]
      index.zeroes.tra <- bigsample(1:nrow(on_data.zeroes), size=0.5*sample.sizes[i], replace=F)
      tra.on.zeroes <- on_data.zeroes[ index.zeroes.tra,]
      tst.on.zeroes <- on_data.zeroes[-index.zeroes.tra,]
      
      rm(on_data.zeroes); gc();
      
      tra.on <- rbind(tra.on.ones, tra.on.zeroes)
      rm(tra.on.ones, tra.on.zeroes); gc();
      
    }
    
    if (balance==0)
    {
      index.tra <- bigsample(1:nrow(on_data), size=sample.sizes[i], replace=F)
      tra.on <- on_data[ index.tra,]
      tst.on.all <- on_data[-index.tra,]
      
    }
    
    tra.on <- tra.on[,c(-1)]
    
    prop <- sum(tra.on[,1])/nrow(tra.on)
    
    
    for(j in 1:length(num_round_list))
    {
      
      for (ls in 1:length(learning_rate))
      {
        
        Conv_NNET.object <- paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/8.ON_US_DS_Project/Model_Objects/conv_nnet/ON_US_DS_Conv_NNET_', sample.sizes[i],'_', s, '_', num_round_list[j], '_', learning_rate[ls], '.RData')
        
        train_x=data.matrix(tra.on[c(-1)])
        train_y <- tra.on[,1]
        
        Conv_NNET.model <- mx.mlp(data=train_x, label=train_y, hidden_node=c(25,15), out_node=2, out_activation="softmax",
                num.round=num_round_list[j], array.batch.size=30, learning.rate=learning_rate[ls], momentum=0.9, device = devices,
                eval.metric=mx.metric.accuracy)
        
		prob_tra<- t(predict(Conv_NNET.model, data.matrix(tra.on[,c(-1)])))
# Assign Labels
        predicted_labels <- max.col(prob_tra)-1
        acc <- sum(diag(table(predicted_labels,tra.on[,1])))/sample.sizes[i]
        
        print('------------------------------------------')
        print(Conv_NNET.model)
        print('------------------------------------------')
               
        
        mx.model.save(model=Conv_NNET.model, prefix = Conv_NNET.object ,iteration = num_round_list[j])
        
        roc.area <- auc(roc(tra.on[,1], prob_tra[,2]))
        
        concordance <- OptimisedConc(tra.on$Response,prob_tra[,2])[1]
        
        print(paste('SampleSize', 'Run', 'Num_Round', 'LearningRate','Prior',  'Accuracy', 'AUC', 'Concordance'))
        print(paste(sample.sizes[i], s, num_round_list[j], learning_rate[ls], prop,  acc, roc.area, concordance))
        
        
        write.table(cbind(sample.sizes[i], s, num_round_list[j], learning_rate[ls], acc, roc.area, concordance),
                    paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/8.ON_US_DS_Project/3.Documents/model/ON_US_DS_Conv_NNET_train_', min.sample.size,'_', max.sample.size, '.csv'), 
                    append=TRUE, sep=",",row.names=FALSE,col.names=FALSE)
        
        rm(train_x,train_y)
        gc()
        
        print('------------------------------------------')
        print('---------- Running Validations -----------')
        print('------------------------------------------')
        
        if (balance==1)
        {
          index.tst.on <- sample(1:nrow(rbind(tst.on.ones, tst.on.zeroes)),
                                 size=10000*10, replace=F)
        }
        
        if (balance==0)
        {
          index.tst.on <- sample(1:nrow(tst.on.all),
                                 size=10000*10, replace=F)        
        }
        
        print(paste('SampleSize', 'Run', 'Num_Round', 'LearningRate', 'TestRun', 'Prior',"TestAccuracy", 'AUC', 'Concordance'))
        
        for (l in 1:10)
        {
          if (balance==1)
          {
            tst.on <- rbind(tst.on.ones, tst.on.zeroes)[index.tst.on[((l-1)*10000 + 1):(l*10000)],]
            tst.on <- tst.on[,c(-1)]
            tst.on <- data.frame(tst.on)
          }
          
          if (balance==0)
          {
            tst.on <- tst.on.all[index.tst.on[((l-1)*10000 + 1):(l*10000)],]
            tst.on <- tst.on[c(-1)]
            tst.on <- data.frame(tst.on)
          }
          
          prop.tst <- sum(tst.on[,1])/nrow(tst.on)
          
          gc()
          prob_tst <- t(predict(Conv_NNET.model, data.matrix(tst.on[,c(-1)])))
# Assign Labels
        predicted_labels_test <- max.col(prob_tst)-1
        acc.test <- sum(diag(table(predicted_labels_test,tst.on[,1])))/nrow(tst.on)
          roc.area.tst <- auc(roc( tst.on[,1],prob_tst[,2]))
          
          concordance.tst <- OptimisedConc(tst.on$Response,prob_tst[,2])[1]
          
          print(paste(sample.sizes[i], s, num_round_list[j], learning_rate[ls], l, prop.tst, acc.test,
                      roc.area.tst, concordance.tst))
          
          write.table(cbind(sample.sizes[i], s, num_round_list[j], learning_rate[ls], l,acc.test, roc.area.tst, concordance.tst),
                      paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/8.ON_US_DS_Project/3.Documents/model/ON_US_DS_Conv_NNET_test_', min.sample.size,'_', max.sample.size, '.csv'), 
                      append=TRUE, sep=",",row.names=FALSE,col.names=FALSE)
          
          
          
        }
        
      }
      
    }
    
    rm(tst.on.ones, tst.on.zeroes)
    gc()
    
  }
  rm(on_data) ; gc();
}

