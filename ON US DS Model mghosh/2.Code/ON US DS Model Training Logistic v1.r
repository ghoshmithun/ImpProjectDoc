install.packages("ada")
install.packages("ffbase")
install.packages("ff")
install.packages("plyr")
install.packages("VIF")
install.packages("pracma")
install.packages("verification")
install.packages("ROCR")
install.packages("AUC")



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




on_data_all <-read.table.ffdf(file="//10.8.8.51/lv0/Move to Box/Mithun/projects/8.ON_US_DS_Project/1.data/on_full_data_base.txt",
                              header = TRUE,VERBOSE = TRUE,
                              sep='|',colClasses = c(rep("numeric",53)))


on_data_all <- subset(on_data_all,select=c('customer_key','Response','Time_since_acquisition',
			'percent_disc_last_6_mth','non_disc_ats','ratio_rev_rewd_12mth','per_elec_comm','avg_order_amt_last_6_mth','Time_Since_last_Retail_purchase',
			'num_em_campaign','disc_ats','num_units_6mth','ratio_order_6_12_mth','Time_Since_last_Onl_purchase','ratio_disc_non_disc_ats',
			'num_disc_comm_responded','on_sales_rev_ratio_12mth','ratio_rev_wo_rewd_12mth','on_sales_item_rev_12mth','time_since_last_disc_purchase',
			'card_status','mobile_ind_tot','pct_off_hit_ind_tot','num_dist_catg_purchased','ratio_order_units_6_12_mth','gp_hit_ind_tot','on_gp_net_sales_ratio','clearance_hit_ind_tot','num_order_num_last_6_mth','purchased','br_hit_ind_tot','on_go_net_sales_ratio','on_br_sales_ratio','total_plcc_cards','at_hit_ind_tot','on_bf_net_sales_ratio','searchdex_ind_tot','factory_hit_ind_tot','markdown_hit_ind_tot' ))


min.sample.size <- 15000; max.sample.size <- 40000;

sample.sizes <- seq(min.sample.size, max.sample.size, by=1000)

balance <- 1

headers1<-cbind("samplesize", "run", "Predictors",  "TPR", "TNR", "Accuracy", "AUC","Concordance","Predictors")
write.table(headers1, paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/8.ON_US_DS_Project/3.Documents/model/ON_US_ds_training_glm_lr_v1_', min.sample.size,'_', max.sample.size, '.csv'), 
            append=FALSE, sep=",",row.names=FALSE,col.names=FALSE)

headers2<-cbind("samplesize","run", "Predictors",  "TestRun", "AUC","Concordance")
write.table(headers2, paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/8.ON_US_DS_Project/3.Documents/model/ON_US_ds_testing_glm_lr_v1_', min.sample.size,'_', max.sample.size, '.csv'), 
            append=FALSE, sep=",",row.names=FALSE,col.names=FALSE)

total_obs =nrow(on_data_all)
for(i in 1:length(sample.sizes))
{ 
  model_data_size <- round(0.12*total_obs)
  on_data <- on_data_all[bigsample(1:total_obs,size=model_data_size,replace = F),]
  names(on_data) <- colnames(on_data_all)
  rownames(on_data) <- seq_len(model_data_size)
  for (s in 1:5)
  {
    if (balance==1)
    {
      on_data.ones <- on_data[on_data[,2]==1,]
      index.ones.tra <- bigsample(1:nrow(on_data.ones), size=0.5*sample.sizes[i], replace=F)
      tra_on.ones <- on_data.ones[ index.ones.tra,]
      tst_on.ones <- on_data.ones[-index.ones.tra,]
      
      rm(on_data.ones); gc();
      
      
      on_data.zeroes <- on_data[on_data[,2]!=1,]
      index.zeroes.tra <- bigsample(1:nrow(on_data.zeroes), size=0.5*sample.sizes[i], replace=F)
      tra_on.zeroes <- on_data.zeroes[ index.zeroes.tra,]
      tst_on.zeroes <- on_data.zeroes[-index.zeroes.tra,]
      
      rm(on_data.zeroes); gc();
      
      tra_on <- rbind(tra_on.ones, tra_on.zeroes)
      rm(tra_on.ones, tra_on.zeroes); gc();
      
    }
    
    if (balance==0)
    {
      index.tra <- bigsample(1:nrow(on_data), size=sample.sizes[i], replace=F)
      tra_on <- on_data[ index.tra,]
      tst_on.all <- on_data[-index.tra,]
    }
    
    tra_on <- tra_on[c(-1)]
    
    prop <- sum(tra_on[,1])/nrow(tra_on)
    
    tra_on <- data.frame(tra_on)
    
    
    glm_lr.object <- paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/8.ON_US_DS_Project/Model_objects/logistic/glm_lr_', sample.sizes[i],'_', s, '_', '.RData')
    
    n <- names(tra_on)
    f <- as.formula(paste("Response ~", paste(n[!n %in% "Response"], collapse = " + ")))
    
    glm_lr.model0 <- glm(f, data=tra_on, family=binomial(link='logit'))
    summary(glm_lr.model0)
    glm_lr.model <- step(glm_lr.model0)
    
    predictors <- length(glm_lr.model$coefficients)
    pred_name <- list(names(glm_lr.model$coefficients)[-1])
    prob_tra <- predict.glm(object=glm_lr.model, data=data.frame(tra_on), type='response')
    
    confusion <- table(tra_on[,1], prob_tra > 0.5)
    
    tpr <- confusion[1,1] / sum(confusion[1,])
    tnr <- confusion[2,2] / sum(confusion[2,])
    acc <- (confusion[1,1] + confusion[2,2])/nrow(tra_on)
    
    print('------------------------------------------')
    print(glm_lr.model)
    print('------------------------------------------')
    
    prob_tra<- predict(glm_lr.model, tra_on[c(-1)],type="response")
    save(glm_lr.model, file=glm_lr.object)
    
    roc.area <- auc(roc(prob_tra, factor(tra_on$Response)))
    
    concordance <- OptimisedConc(tra_on$Response, prob_tra)[1]
    
    print(paste('SampleSize',  'Run', 'Prior', 'Predictors',  'TPR', 'TNR', 'Accuracy', 'AUC', 'Concordance'))
    print(paste(sample.sizes[i], s, prop, predictors, tpr, tnr, acc, roc.area, concordance))
    
    
    write.table(cbind(sample.sizes[i], s,  predictors, tpr, 
                      tnr, acc, roc.area, concordance,t(pred_name)),
                paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/8.ON_US_DS_Project/3.Documents/model/ON_US_ds_training_glm_lr_v1_', min.sample.size,'_', max.sample.size, '.csv'), 
                append=TRUE, sep=",",row.names=FALSE,col.names=FALSE)
    
    
    
    
    print('------------------------------------------')
    print('---------- Running Validations -----------')
    print('------------------------------------------')
    
    if (balance==1)
    {
      index.tst_on <- sample(1:nrow(rbind(tst_on.ones, tst_on.zeroes)),
                             size=10000*10, replace=F)
    }
    
    if (balance==0)
    {
      index.tst_on <- sample(1:nrow(tst_on.all),
                             size=10000*10, replace=F)        
    }
    
    print(paste('SampleSize', 'Run', 'TestRun', 'Prior', 'Predictors',  'AUC', 'Concordance'))
    
    for (l in 1:10)
    {
      if (balance==1)
      {
        tst_on <- rbind(tst_on.ones, tst_on.zeroes)[index.tst_on[((l-1)*10000 + 1):(l*10000)],]
        tst_on <- tst_on[c(-1)]
        tst_on <- data.frame(tst_on)
      }
      
      if (balance==0)
      {
        tst_on <- tst_on.all[index.tst_on[((l-1)*10000 + 1):(l*10000)],]
        tst_on <- tst_on[c(-1)]
        tst_on <- data.frame(tst_on)
      }
      
      prop.tst <- sum(tst_on[,1])/nrow(tst_on)
      
      gc()
      prob_tst <- predict(glm_lr.model, tst_on[c(-1)],type="response", n.trees=opt.num.trees)
      
      roc.area.tst <- auc(roc(prob_tst, factor(tst_on$Response)))
      
      concordance.tst <- OptimisedConc(tst_on$Response,prob_tst)[1]
      
      print(paste(sample.sizes[i], s, l, prop.tst, 
                  predictors, roc.area.tst, concordance.tst))
      
      write.table(cbind(sample.sizes[i], s,  predictors, l, 
                        roc.area.tst, concordance.tst),
                  paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/8.ON_US_DS_Project/3.Documents/model/ON_US_ds_testing_glm_lr_v1_', min.sample.size,'_', max.sample.size, '.csv'), 
                  append=TRUE, sep=",",row.names=FALSE,col.names=FALSE)
      
      
      
      
    }
    
    rm(tst_on.ones, tst_on.zeroes)
    gc()
    
  }
  rm(on_data) ; gc();
}

