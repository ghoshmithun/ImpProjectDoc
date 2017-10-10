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


min.sample.size <- 15000; max.sample.size <- 25000;

sample.sizes <- seq(min.sample.size, max.sample.size, by=1000)

balance <- 1

headers1<-cbind("samplesize", "run", "Predictors",  "TPR", "TNR", "Accuracy", "AUC","Concordance","Predictors")
write.table(headers1, paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/gp_ds_training_glm_lr_v1_', min.sample.size,'_', max.sample.size, '.csv'), 
            append=FALSE, sep=",",row.names=FALSE,col.names=FALSE)

headers2<-cbind("samplesize","run", "Predictors",  "TestRun", "AUC","Concordance")
write.table(headers2, paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/gp_ds_testing_glm_lr_v1_', min.sample.size,'_', max.sample.size, '.csv'), 
            append=FALSE, sep=",",row.names=FALSE,col.names=FALSE)

total_obs =nrow(gp_data_all)
for(i in 1:length(sample.sizes))
{ 
  model_data_size <- round(0.3*total_obs)
  gp_data <- gp_data_all[bigsample(1:total_obs,size=model_data_size,replace = F),]
  names(gp_data) <- colnames(gp_data_all)
  rownames(gp_data) <- seq_len(model_data_size)
  for (s in 1:5)
  {
    if (balance==1)
    {
      gp_data.ones <- gp_data[gp_data[,2]==1,]
      index.ones.tra <- bigsample(1:nrow(gp_data.ones), size=0.5*sample.sizes[i], replace=F)
      tra_gp.ones <- gp_data.ones[ index.ones.tra,]
      tst_gp.ones <- gp_data.ones[-index.ones.tra,]
      
      rm(gp_data.ones); gc();
      
      
      gp_data.zeroes <- gp_data[gp_data[,2]!=1,]
      index.zeroes.tra <- bigsample(1:nrow(gp_data.zeroes), size=0.5*sample.sizes[i], replace=F)
      tra_gp.zeroes <- gp_data.zeroes[ index.zeroes.tra,]
      tst_gp.zeroes <- gp_data.zeroes[-index.zeroes.tra,]
      
      rm(gp_data.zeroes); gc();
      
      tra_gp <- rbind(tra_gp.ones, tra_gp.zeroes)
      rm(tra_gp.ones, tra_gp.zeroes); gc();
      
    }
    
    if (balance==0)
    {
      index.tra <- bigsample(1:nrow(gp_data), size=sample.sizes[i], replace=F)
      tra_gp <- gp_data[ index.tra,]
      tst_gp.all <- gp_data[-index.tra,]
    }
    
    tra_gp <- tra_gp[c(-1)]
    
    prop <- sum(tra_gp[,1])/nrow(tra_gp)
    
    tra_gp <- data.frame(tra_gp)
    
    
    glm_lr.object <- paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/Model_Objects/glm_lr/glm_lr_', sample.sizes[i],'_', s, '_', '.RData')
    
    n <- names(tra_gp)
    f <- as.formula(paste("Response ~", paste(n[!n %in% "Response"], collapse = " + ")))
    
    glm_lr.model0 <- glm(f, data=tra_gp, family=binomial(link='logit'))
    summary(glm_lr.model0)
    glm_lr.model <- step(glm_lr.model0)
    
    predictors <- length(glm_lr.model$coefficients)
    pred_name <- list(names(glm_lr.model$coefficients)[-1])
    prob_tra <- predict.glm(object=glm_lr.model, data=data.frame(tra_gp), type='response')
    
    confusion <- table(tra_gp[,1], prob_tra > 0.5)
    
    tpr <- confusion[1,1] / sum(confusion[1,])
    tnr <- confusion[2,2] / sum(confusion[2,])
    acc <- (confusion[1,1] + confusion[2,2])/nrow(tra_gp)
    
    print('------------------------------------------')
    print(glm_lr.model)
    print('------------------------------------------')
    
    prob_tra<- predict(glm_lr.model, tra_gp[c(-1)],type="response")
    save(glm_lr.model, file=glm_lr.object)
    
    roc.area <- auc(roc(prob_tra, factor(tra_gp$Response)))
    
    concordance <- OptimisedConc(tra_gp$Response, prob_tra)[1]
    
    print(paste('SampleSize',  'Run', 'Prior', 'Predictors',  'TPR', 'TNR', 'Accuracy', 'AUC', 'Concordance'))
    print(paste(sample.sizes[i], s, prop, predictors, tpr, tnr, acc, roc.area, concordance))
    
    
    write.table(cbind(sample.sizes[i], s,  predictors, tpr, 
                      tnr, acc, roc.area, concordance,pred_name),
                paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/gp_ds_training_glm_lr_v1_', min.sample.size,'_', max.sample.size, '.csv'), 
                append=TRUE, sep=",",row.names=FALSE,col.names=FALSE)
    
    
    
    
    print('------------------------------------------')
    print('---------- Running Validations -----------')
    print('------------------------------------------')
    
    if (balance==1)
    {
      index.tst_gp <- sample(1:nrow(rbind(tst_gp.ones, tst_gp.zeroes)),
                             size=10000*10, replace=F)
    }
    
    if (balance==0)
    {
      index.tst_gp <- sample(1:nrow(tst_gp.all),
                             size=10000*10, replace=F)        
    }
    
    print(paste('SampleSize', 'Run', 'TestRun', 'Prior', 'Predictors',  'AUC', 'Concordance'))
    
    for (l in 1:10)
    {
      if (balance==1)
      {
        tst_gp <- rbind(tst_gp.ones, tst_gp.zeroes)[index.tst_gp[((l-1)*10000 + 1):(l*10000)],]
        tst_gp <- tst_gp[c(-1)]
        tst_gp <- data.frame(tst_gp)
      }
      
      if (balance==0)
      {
        tst_gp <- tst_gp.all[index.tst_gp[((l-1)*10000 + 1):(l*10000)],]
        tst_gp <- tst_gp[c(-1)]
        tst_gp <- data.frame(tst_gp)
      }
      
      prop.tst <- sum(tst_gp[,1])/nrow(tst_gp)
      
      gc()
      prob_tst <- predict(glm_lr.model, tst_gp[c(-1)],type="response", n.trees=opt.num.trees)
      
      roc.area.tst <- auc(roc(prob_tst, factor(tst_gp$Response)))
      
      concordance.tst <- OptimisedConc(tst_gp$Response,prob_tst)[1]
      
      print(paste(sample.sizes[i], s, l, prop.tst, 
                  predictors, roc.area.tst, concordance.tst))
      
      write.table(cbind(sample.sizes[i], s,  predictors, l, 
                        roc.area.tst, concordance.tst),
                  paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/gp_ds_testing_glm_lr_v1_', min.sample.size,'_', max.sample.size, '.csv'), 
                  append=TRUE, sep=",",row.names=FALSE,col.names=FALSE)
      
      
      
      
    }
    
    rm(tst_gp.ones, tst_gp.zeroes)
    gc()
    
  }
  rm(gp_data) ; gc();
}

