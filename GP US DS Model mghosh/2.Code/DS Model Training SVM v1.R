#install.packages(c("ff","kernlab","ffbase","pracma","AUC"),dep=T)

rm(list=ls())
gc()
library(ff)
library(kernlab)
library(ffbase)
library(plyr)
library(pracma)
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
  {50
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


min.sample.size <- 5000; max.sample.size <- 15000;

sample.sizes <- seq(min.sample.size, max.sample.size, by=1000)

c.list <-c(seq(0.1,1, by=0.3),1.5,3,5,10,20)



balance <- 1

headers1<-cbind("samplesize", "run", "nu",  "AUC","Concordance")
write.table(headers1, paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/GP_DS_SVM_train_', min.sample.size,'_', max.sample.size, '.csv'), 
            append=FALSE, sep=",",row.names=FALSE,col.names=FALSE)

headers2<-cbind("samplesize","run", "nu" ,"SampleNumber", "AUC","Concordance")
write.table(headers2, paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/GP_DS_SVM_test_', min.sample.size,'_', max.sample.size, '.csv'), 
            append=FALSE, sep=",",row.names=FALSE,col.names=FALSE)


  for(i in 1:length(sample.sizes))
  { 
    for (s in 1:1){
      
      if (balance==1)
      {
        gp_data.ones <- gp_data[gp_data$Response ==1,]
        index.ones.tra <- bigsample(1:nrow(gp_data.ones), size=0.5*sample.sizes[i], replace=F)
        tra_gp.ones <- gp_data.ones[ index.ones.tra,]
        tst_gp.ones <- gp_data.ones[-index.ones.tra,]
        
        rm(gp_data.ones); gc();
        
        
        gp_data.zeroes <- gp_data[gp_data$Response !=1,]
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
      srange<-sigest(Response ~.,  data=tra_gp)
      
      sigma<-srange[2]
      
      
      for(j in 1:length(c.list))
      {
        
        
        
        ksvm.object <- paste0('svm_active_',sample.sizes[i],'_',c.list[j],"_",round(sigma,2),"_",s)
        
        ksvm.model <- ksvm(
          as.factor(Response) ~., data=tra_gp, type="C-svc",
          kernel="rbfdot", kpar=list(sigma = sigma), C=c.list[j],
          cross=10,prob.model=TRUE   )
        
        
        print('------------------------------------------')
        print(ksvm.model)
        print('------------------------------------------')
        
         save(ksvm.model,file = paste0("//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/Model_Objects/svm/" , ksvm.object , ".RData"))
        
        prob_tra<- predict(ksvm.model,tra_gp[c(-1)],type="probabilities")
        
        roc.area <- 
          auc(roc(prob_tra[,2], factor(tra_gp$Response)))
        
        concordance <- OptimisedConc(tra_gp$Response,prob_tra[,2])[1]
        
        print(paste("Run","Sample Size","nu",'Prior', 'AUC', 'Concordance'))
        print(paste(s,sample.sizes[i],c.list[j], prop, roc.area, concordance))
        
        
        write.table(cbind(sample.sizes[i], s, c.list[j],  roc.area, concordance),
                    paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/GP_DS_SVM_train_', min.sample.size,'_', max.sample.size, '.csv'), 
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
        
        print(paste("Run","Sample Size","nu",'Prior',  'AUC', 'Concordance'))
        
        for (l in 1:10)
        {
          if (balance==1)
          {
            tst_gp <- rbind(tst_gp.ones, tst_gp.zeroes)[index.tst_gp[((l-1)*10000 + 1):(l*10000)],]
            tst_gp <- tst_gp[c(-1)]
          }
          
          if (balance==0)
          {
            tst_gp <- tst_gp.all[index.tst_gp[((l-1)*10000 + 1):(l*10000)],]
            tst_gp <- tst_gp[c(-1)]
          }
          
          prop.tst <- sum(tst_gp[,1])/nrow(tst_gp)
          
          gc()
          prob_tst <- predict(ksvm.model,tst_gp[c(-1)],type="probabilities")
         
          
          roc.area.tst <- 
            auc(roc(prob_tst[,2], factor(tst_gp$Response)))
          
          concordance.tst <- OptimisedConc(tst_gp$Response,prob_tst[,2])[1]
          
          print(paste(s,sample.sizes[i],c.list[j],prop.tst, roc.area.tst, concordance.tst))
          
          write.table(cbind(sample.sizes[i], s, c.list[j], l, roc.area.tst, concordance.tst),
                      paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/GP_DS_SVM_test_', min.sample.size,'_', max.sample.size, '.csv'), 
                      append=TRUE, sep=",",row.names=FALSE,col.names=FALSE)      
        }
        
      }
      
      rm(tst_gp.ones, tst_gp.zeroes)
      gc()
    }
}
