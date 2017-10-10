library(readr)

header <- cbind("Customer_key","Response","MeanScore","MedianScore")

write.table(x = header,file="//10.8.8.51/lv0/Move to Box/Mithun/projects/12.BRFS_US_DS_Project/1.data/BR_US_DS_Score.txt",sep=",",col.names=F,row.names=F,append=F)


BF_data_min_max_value <- read.table(file="//10.8.8.51/lv0/Move to Box/Mithun/projects/12.BRFS_US_DS_Project/1.data/full_data_min_max_value.txt",sep="|",header=T)

#Cap the  value
cap_value <- function(i){
  var_name <- model_predictorx[[i]]
  var <- BF_data[,var_name]
  max_val <- BF_data_min_max_value[which(BF_data_min_max_value$Var_Names==var_name),4]
  min_val <- BF_data_min_max_value[which(BF_data_min_max_value$Var_Names==var_name),2]
  mid_val <- BF_data_min_max_value[which(BF_data_min_max_value$Var_Names==var_name),3]
  var[is.na(var)] <- mid_val
  var[var > max_val] <- max_val
  var[var < min_val] <- min_val
  return((var-min_val)/(max_val-min_val))
  #return(var)
}


  
BF_data <- read.table(file = "//10.8.8.51/lv0/Move to Box/Mithun/projects/9.BR_US_DS_Project/1.data/Feb17/BR_disc_sens_Data_pull_Feb17.txt",
                    stringsAsFactors = F , sep = "|",header = T )



model_predictor <- c('percent_disc_last_12_mth', 'num_days_on_books','per_elec_comm','num_em_campaign','num_units_12mth','disc_ats','time_since_last_retail_purchase',
                     'avg_order_amt_last_6_mth','br_gp_net_sales_ratio','non_disc_ats','ratio_rev_wo_rewd_12mth','on_sales_item_rev_12mth','br_hit_ind_tot',
                     'br_on_sales_ratio','on_sales_rev_ratio_12mth','ratio_rev_rewd_12mth','num_order_num_last_6_mth','card_status','ratio_order_6_12_mth',
                     'ratio_order_units_6_12_mth','ratio_disc_non_disc_ats','sale_hit_ind_tot','num_disc_comm_responded','mobile_ind_tot','gp_hit_ind_tot',
                     'br_bf_net_sales_ratio','num_dist_catg_purchased','total_plcc_cards','on_hit_ind_tot','pct_off_hit_ind_tot','br_go_net_sales_ratio','at_hit_ind_tot',
                     'purchased','searchdex_ind_tot','factory_hit_ind_tot','clearance_hit_ind_tot','markdown_hit_ind_tot')
#model_predictor[!model_predictor %in% colnames(BF_data)]
#colnames(BF_data)[!colnames(BF_data) %in% model_predictor]

BR_data <- BR_data[,c('customer_key',"resp_disc_percent",model_predictor)]
BR_data_capped <- do.call("cbind.data.frame",lapply(seq_along(model_predictor),cap_value))

BR_data$Response <- ifelse(BR_data$resp_disc_percent < -0.35 , 1, 0)
BR_data$Response[is.na(BR_data$Response)] <- 0

BR_data_final <- cbind.data.frame(BR_data$customer_key,BR_data$Response,BR_data_capped)

names(BR_data_final) <- c('customer_key','Response',model_predictor)

rm(BR_data,BR_data_capped)
gc()
#Load the model to score

samplesize <- c('5000' ,'5000' ,'7000' ,'7000' ,'7000' ,'7000' ,'8000' ,'8000' ,'9000' ,'9000' ,'9000' ,'9000' ,'11000' ,
                '13000' ,'15000' ,'16000' ,'16000' ,'16000' ,'18000' ,'18000')

run	<- c('3' ,'3' ,'2' ,'5' ,'5' ,'5' ,'1' ,'3' ,'4' ,'4' ,'4' ,'4' ,'5' ,'2' ,'5' ,'1' ,'2' ,'2' ,'4' ,'4')
shrinkage <-	c('0.04' ,'0.09' ,'0.07' ,'0.04' ,'0.05' ,'0.09' ,'0.01' ,'0.05' ,'0.001' ,'0.03' ,'0.06' ,'0.07' ,'0.1' ,'0.09' ,
               '0.09' ,'0.1' ,'0.08' ,'0.1' ,'0.07' ,'0.1')
LossFunc	<- c('bernoulli' ,'bernoulli' ,'adaboost' ,'bernoulli' ,'bernoulli' ,'bernoulli' ,'bernoulli' ,'bernoulli' ,'bernoulli' ,
              'adaboost' ,'bernoulli' ,'adaboost' ,'bernoulli' ,'bernoulli' ,'bernoulli' ,'bernoulli' ,'bernoulli' ,'bernoulli' ,
              'bernoulli' ,'bernoulli')


score_file <- matrix(0,nrow=nrow(BF_data_final),ncol=24)
for (j in 1:20){
  require(gbm)
  load(paste0("//10.8.8.51/lv0/Move to Box/Mithun/projects/9.BR_US_DS_Project/Model_objects/gbm/gbm_" ,samplesize[j] , "_" , run[j] , "_" , LossFunc[j] , "_" , shrinkage[j] , ".RData"))
  opt.num.trees <- gbm.perf(gbm.model,plot.it =FALSE)
  score_file[,1]<- BR_data_final$customer_key
  score_file[,2]<- BR_data_final$Response
  score_file[,j+1] <- predict.gbm(object=gbm.model, newdata=data.frame(BR_data_final), type='response', n.trees=opt.num.trees)
  rm(gbm.model)
  gc()
  print(paste("scoring for model", j))
  }

score_file[,23]<- rowMeans(score_file[,3:22])
score_file[,24] <- apply(score_file[,3:22],1,median)

write.table(x = score_file[,c(1,2,23,24)],file="//10.8.8.51/lv0/Move to Box/Mithun/projects/12.BRFS_US_DS_Project/1.data/BR_US_DS_Score.txt",sep=",",col.names=F,row.names=F,append=T)



