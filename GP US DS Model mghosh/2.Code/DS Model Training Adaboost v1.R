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
  Data = cbind(indvar, fittedvalues) 
  ones = Data[Data[,1] == 1,]
  zeros = Data[Data[,1] == 0,]
  conc=matrix(0, dim(zeros)[1], dim(ones)[1])
  disc=matrix(0, dim(zeros)[1], dim(ones)[1])
  ties=matrix(0, dim(zeros)[1], dim(ones)[1])
  for (j in 1:dim(zeros)[1])
  {
    for (i in 1:dim(ones)[1])
    {
      if (ones[i,2]>zeros[j,2])
      {conc[j,i]=1}
      else if (ones[i,2]<zeros[j,2])
      {disc[j,i]=1}
      else if (ones[i,2]==zeros[j,2])
      {ties[j,i]=1}
    }
  }
  Pairs=dim(zeros)[1]*dim(ones)[1]
  PercentConcordance=(sum(conc)/Pairs)*100
  PercentDiscordance=(sum(disc)/Pairs)*100
  PercentTied=(sum(ties)/Pairs)*100
  return(list("Percent Concordance"=PercentConcordance,"Percent Discordance"=PercentDiscordance,"Percent Tied"=PercentTied,"Pairs"=Pairs))
}


require(data.table)
gp_data <-read.table.ffdf(file="//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/1. data/GP_full_data_base.txt",
                          header = TRUE,VERBOSE = TRUE,
                          sep='|',colClasses = c(rep("numeric",50)))

gp_data <- subset(gp_data,select=c('customer_key','Response','percent_disc_last_12_mth','percent_disc_last_6_mth','per_elec_comm',
'gp_hit_ind_tot','num_units_12mth','num_em_campaign','disc_ats','avg_order_amt_last_6_mth','Time_Since_last_disc_purchase',
'non_disc_ats','gp_on_net_sales_ratio','on_sales_rev_ratio_12mth','mobile_ind_tot','ratio_order_6_12_mth',
'pct_off_hit_ind_tot','ratio_rev_wo_rewd_12mth','ratio_disc_non_disc_ats','card_status','br_hit_ind_tot',
'gp_br_sales_ratio','ratio_order_units_6_12_mth','num_disc_comm_responded','purchased','ratio_rev_rewd_12mth',
'num_dist_catg_purchased','num_order_num_last_6_mth','at_hit_ind_tot','gp_go_net_sales_ratio','clearance_hit_ind_tot',
'searchdex_ind_tot','total_plcc_cards','factory_hit_ind_tot','gp_bf_net_sales_ratio','markdown_hit_ind_tot' ))

min.sample.size <- 10000; max.sample.size <- 15000;

sample.sizes <- seq(min.sample.size, max.sample.size, by=1000)

c.list <-c(seq(0.1,1, by=0.1))

loss <- c("exponential","logistic")

balance <- 1

headers1<-cbind("Loss Function","samplesize", "run", "nu", "TPR", "TNR", "Accuracy", "AUC","Concordance")
write.table(headers1, paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/gp_ds_training_ada_', min.sample.size,'_', max.sample.size, '.csv'), 
            append=FALSE, sep=",",row.names=FALSE,col.names=FALSE)

headers2<-cbind("Loss Function","samplesize","run", "nu" ,"SampleNumber","TPR", "TNR", "Accuracy", "AUC","Concordance")
write.table(headers2, paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/gp_ds_testing_ada_', min.sample.size,'_', max.sample.size, '.csv'), 
            append=FALSE, sep=",",row.names=FALSE,col.names=FALSE)


for (k in 1:length(loss))
{  
  for(i in 1:length(sample.sizes))
  { 
    for (s in 1:2){
      
      if (balance==1)
      {
        gp_data.ones <- gp_data[gp_data$Response ==1,]
        index.ones.tra <- bigsample(1:nrow(gp_data.ones), size=0.5*sample.sizes[i], replace=F)
        tra.gp.ones <- gp_data.ones[ index.ones.tra,]
        tst.gp.ones <- gp_data.ones[-index.ones.tra,]
        
        rm(gp_data.ones); gc();
        
        
        gp_data.zeroes <- gp_data[gp_data$Response !=1,]
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
      
      
      tra.gp <- tra.gp[c(-1)]
      
      prop <- sum(tra.gp[,1])/nrow(tra.gp)
      
      
      for(j in 1:length(c.list))
      {
        
        
        
        ada.object <- paste0('ada_',sample.sizes[i],'_',c.list[j],"_",loss[k],"_",s)
        
        ada.model <- ada(
          as.factor(Response) ~., data=tra.gp, type="discrete",
          loss=loss[k], bag.frac = 0.5, iter=100, verbose=T,
          nu=c.list[j]
        )
        
        tpr <- ada.model$confusion[1,1] / sum(ada.model$confusion[1,])
        tnr <- ada.model$confusion[2,2] / sum(ada.model$confusion[2,])
        acc <- (ada.model$confusion[1,1] + ada.model$confusion[2,2])/nrow(tra.gp)
        
        print('------------------------------------------')
        print(ada.model)
        print('------------------------------------------')
        
         save(ada.model,file = paste0("//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/Model_Objects/adaboost/" , ada.object , ".RData"))
        
        prob_tra<- predict(ada.model,tra.gp[c(-1)],type="probs")
        
        roc.area <- 
          auc(roc(prob_tra[,2], factor(tra.gp$Response)))
        
        concordance <- OptimisedConc(tra.gp$Response,prob_tra[,2])[1]
        
        print(paste("Run","Sample Size","nu","Loss Function",'Prior', 'TPR', 'TNR', 'Accuracy', 'AUC', 'Concordance'))
        print(paste(s,sample.sizes[i],c.list[j],loss[k],prop, tpr, tnr, acc, roc.area, concordance))
        
        
        write.table(cbind(loss[k],sample.sizes[i], s, c.list[j], tpr, 
                          tnr, acc, roc.area, concordance),
                    paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/gp_ds_training_ada_', min.sample.size,'_', max.sample.size, '.csv'), 
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
        
        print(paste("Run","Sample Size","nu","Loss Function",'Prior', 'TPR', 'TNR', 'Accuracy', 'AUC', 'Concordance'))
        
        for (l in 1:10)
        {
          if (balance==1)
          {
            tst.gp <- rbind(tst.gp.ones, tst.gp.zeroes)[index.tst.gp[((l-1)*10000 + 1):(l*10000)],]
            tst.gp <- tst.gp[c(-1)]
          }
          
          if (balance==0)
          {
            tst.gp <- tst.gp.all[index.tst.gp[((l-1)*10000 + 1):(l*10000)],]
            tst.gp <- tst.gp[c(-1)]
          }
          
          prop.tst <- sum(tst.gp[,1])/nrow(tst.gp)
          
          gc()
          prob_tst <- predict(ada.model,tst.gp[c(-1)],type="probs")
          vect.tst <- predict(ada.model,tst.gp[c(-1)],type="vector")
          tpr.tst <- prop.table(table(tst.gp[,1], vect.tst))[1,1]
          tnr.tst <- prop.table(table(tst.gp[,1], vect.tst))[2,2]
          acc.tst <- (table(tst.gp[,1], vect.tst)[1,1] +
                        table(tst.gp[,1], vect.tst)[2,2]) / nrow(tst.gp) 
          
          roc.area.tst <- 
            auc(roc(prob_tst[,2], factor(tst.gp$Response)))
          
          concordance.tst <- OptimisedConc(tst.gp$Response,prob_tst[,2])[1]
          
          print(paste(sample.sizes[i],c.list[j],loss[k],prop.tst, tpr.tst, tnr.tst, acc.tst, roc.area.tst, concordance.tst))
          
          write.table(cbind(s,loss[k],sample.sizes[i], s, c.list[j], l, tpr.tst, tnr.tst, 
                            acc.tst, roc.area.tst, concordance.tst),
                      paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/gp_ds_testing_ada_', min.sample.size,'_', max.sample.size, '.csv'), 
                      append=TRUE, sep=",",row.names=FALSE,col.names=FALSE)      
        }
        
      }
      
      rm(tst.gp.ones, tst.gp.zeroes)
      gc()
    }
  }
}