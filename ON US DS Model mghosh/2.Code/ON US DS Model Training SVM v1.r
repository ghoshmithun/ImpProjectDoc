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

on_data_all <-read.table.ffdf(file="//10.8.8.51/lv0/Move to Box/Mithun/projects/8.ON_US_DS_Project/1.data/on_full_data_base.txt",
                              header = TRUE,VERBOSE = TRUE,
                              sep='|',colClasses = c(rep("numeric",53)))


on_data_all <- subset(on_data_all,select=c('customer_key','Response','Time_since_acquisition',
			'percent_disc_last_6_mth','non_disc_ats','ratio_rev_rewd_12mth','per_elec_comm','avg_order_amt_last_6_mth','Time_Since_last_Retail_purchase',
			'num_em_campaign','disc_ats','num_units_6mth','ratio_order_6_12_mth','Time_Since_last_Onl_purchase','ratio_disc_non_disc_ats',
			'num_disc_comm_responded','on_sales_rev_ratio_12mth','ratio_rev_wo_rewd_12mth','on_sales_item_rev_12mth','time_since_last_disc_purchase',
			'card_status','mobile_ind_tot','pct_off_hit_ind_tot','num_dist_catg_purchased','ratio_order_units_6_12_mth','gp_hit_ind_tot','on_gp_net_sales_ratio','clearance_hit_ind_tot','num_order_num_last_6_mth','purchased','br_hit_ind_tot','on_go_net_sales_ratio','on_br_sales_ratio','total_plcc_cards','at_hit_ind_tot','on_bf_net_sales_ratio','searchdex_ind_tot','factory_hit_ind_tot','markdown_hit_ind_tot' ))


min.sample.size <- 5000; max.sample.size <- 15000;

sample.sizes <- seq(min.sample.size, max.sample.size, by=1000)

c.list <-c(seq(0.1,1, by=0.3),1.5,3,5,10,20)



balance <- 1

headers1<-cbind("samplesize", "run", "nu",  "AUC","Concordance")
write.table(headers1, paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/8.ON_US_DS_Project/3.Documents/model/on_DS_SVM_train_', min.sample.size,'_', max.sample.size, '.csv'), 
            append=FALSE, sep=",",row.names=FALSE,col.names=FALSE)

headers2<-cbind("samplesize","run", "nu" ,"SampleNumber", "AUC","Concordance")
write.table(headers2, paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/8.ON_US_DS_Project/3.Documents/model/on_DS_SVM_test_', min.sample.size,'_', max.sample.size, '.csv'), 
            append=FALSE, sep=",",row.names=FALSE,col.names=FALSE)


  for(i in 1:length(sample.sizes))
  { 
    model_data_size <- round(0.15*total_obs)
  on_data <- on_data_all[bigsample(1:total_obs,size=model_data_size,replace = F),]
  names(on_data) <- colnames(on_data_all)
  rownames(on_data) <- seq_len(model_data_size)
    for (s in 1:1){
      
      if (balance==1)
      {
        on_data.ones <- on_data[on_data$Response ==1,]
        index.ones.tra <- bigsample(1:nrow(on_data.ones), size=0.5*sample.sizes[i], replace=F)
        tra_on.ones <- on_data.ones[ index.ones.tra,]
        tst_on.ones <- on_data.ones[-index.ones.tra,]
        
        rm(on_data.ones); gc();
        
        
        on_data.zeroes <- on_data[on_data$Response !=1,]
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
      srange<-sigest(Response ~.,  data=tra_on)
      
      sigma<-srange[2]
      
      
      for(j in 1:length(c.list))
      {
        
        
        
        ksvm.object <- paste0('svm_active_',sample.sizes[i],'_',c.list[j],"_",round(sigma,2),"_",s)
        
        ksvm.model <- ksvm(
          as.factor(Response) ~., data=tra_on, type="C-svc",
          kernel="rbfdot", kpar=list(sigma = sigma), C=c.list[j],
          cross=10,prob.model=TRUE   )
        
        
        print('------------------------------------------')
        print(ksvm.model)
        print('------------------------------------------')
        
         save(ksvm.model,file = paste0("//10.8.8.51/lv0/Move to Box/Mithun/projects/8.ON_US_DS_Project/Model_Objects/svm/" , ksvm.object , ".RData"))
        
        prob_tra<- predict(ksvm.model,tra_on[c(-1)],type="probabilities")
        
        roc.area <- 
          auc(roc(prob_tra[,2], factor(tra_on$Response)))
        
        concordance <- OptimisedConc(tra_on$Response,prob_tra[,2])[1]
        
        print(paste("Run","Sample Size","nu",'Prior', 'AUC', 'Concordance'))
        print(paste(s,sample.sizes[i],c.list[j], prop, roc.area, concordance))
        
        
        write.table(cbind(sample.sizes[i], s, c.list[j],  roc.area, concordance),
                    paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/8.ON_US_DS_Project/3.Documents/model/on_DS_SVM_train_', min.sample.size,'_', max.sample.size, '.csv'), 
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
        
        print(paste("Run","Sample Size","nu",'Prior',  'AUC', 'Concordance'))
        
        for (l in 1:10)
        {
          if (balance==1)
          {
            tst_on <- rbind(tst_on.ones, tst_on.zeroes)[index.tst_on[((l-1)*10000 + 1):(l*10000)],]
            tst_on <- tst_on[c(-1)]
          }
          
          if (balance==0)
          {
            tst_on <- tst_on.all[index.tst_on[((l-1)*10000 + 1):(l*10000)],]
            tst_on <- tst_on[c(-1)]
          }
          
          prop.tst <- sum(tst_on[,1])/nrow(tst_on)
          
          gc()
          prob_tst <- predict(ksvm.model,tst_on[c(-1)],type="probabilities")
         
          
          roc.area.tst <- 
            auc(roc(prob_tst[,2], factor(tst_on$Response)))
          
          concordance.tst <- OptimisedConc(tst_on$Response,prob_tst[,2])[1]
          
          print(paste(l,sample.sizes[i],c.list[j],prop.tst, roc.area.tst, concordance.tst))
          
          write.table(cbind(sample.sizes[i], s, c.list[j], l, roc.area.tst, concordance.tst),
                      paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/8.ON_US_DS_Project/3.Documents/model/on_DS_SVM_test_', min.sample.size,'_', max.sample.size, '.csv'), 
                      append=TRUE, sep=",",row.names=FALSE,col.names=FALSE)      
        }
        
      }
      
      rm(tst_on.ones, tst_on.zeroes)
      gc()
    }
	  rm(on_data) ; gc();
}
