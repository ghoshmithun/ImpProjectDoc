library(readr)

header <- cbind("Customer_key","Response","MeanScore","MedianScore")

write.table(x = header,file="//10.8.8.51/lv0/Move to Box/Mithun/projects/9.BR_US_DS_Project/1.data/Jun15/BR_US_DS_Score_Jun15.txt",sep=",",col.names=F,row.names=F,append=F)


BR_data_min_max_value <- read.table(file="//10.8.8.51/lv0/Move to Box/Mithun/projects/9.BR_US_DS_Project/1.data/full_data_min_max_value.txt",sep="|",header=T)

#Cap the  value
cap_value <- function(i){
  var_name <- model_predictorx[[i]]
  var <- BR_data[,var_name]
  max_val <- BR_data_min_max_value[which(BR_data_min_max_value$Var_Names==var_name),4]
  min_val <- BR_data_min_max_value[which(BR_data_min_max_value$Var_Names==var_name),2]
  mid_val <- BR_data_min_max_value[which(BR_data_min_max_value$Var_Names==var_name),3]
  var[is.na(var)] <- mid_val
  var[var > max_val] <- max_val
  var[var < min_val] <- min_val
  return((var-min_val)/(max_val-min_val))
  #return(var)
}


require(R.utils)
total_row_scoredata <- countLines("//10.8.8.51/lv0/Move to Box/Mithun/projects/9.BR_US_DS_Project/1.data/Jun15/BR_disc_sens_Data_pull_Jun15.txt")
#4722436
chunk_size <- 5000000
num_loop <- total_row_scoredata %/% chunk_size
reminder_size <- total_row_scoredata %% chunk_size
start_row <- 1
end_row <- 0
for (loop in 0:num_loop){
  if(loop==0){
    start_row <- 1
    end_row <- reminder_size
    }
  else {
    start_row <- end_row + 1
    end_row <- end_row + chunk_size
  }

  print("#################################################################################")
  print(paste("scoring start for  data chunk", loop , " , start_row:", start_row  , " , end_row :" , end_row))
  print("#################################################################################")
  
BR_data <- read.table(file = "//10.8.8.51/lv0/Move to Box/Mithun/projects/9.BR_US_DS_Project/1.data/Jun15/BR_disc_sens_Data_pull_Jun15.txt",
                    stringsAsFactors = F ,nrows = (end_row - start_row + 1), skip = start_row -1, sep = "|",header = F )

names(BR_data) <- c('customer_key','flag_employee','offline_last_txn_date','online_last_txn_date','offline_disc_last_txn_date','total_plcc_cards','acquisition_date',
                    'time_since_last_retail_purchase','time_since_last_online_purchase','time_since_last_disc_purchase','num_days_on_books','avg_order_amt_last_6_mth',
                    'num_order_num_last_6_mth','avg_order_amt_last_12_mth','ratio_order_6_12_mth','num_order_num_last_12_mth','ratio_order_units_6_12_mth',
                    'br_net_sales_amt_12_mth','br_on_sales_ratio','num_disc_comm_responded','percent_disc_last_6_mth','percent_disc_last_12_mth','br_go_net_sales_ratio',
                    'br_bf_net_sales_ratio','br_gp_net_sales_ratio','card_status','disc_ats','non_disc_ats','ratio_disc_non_disc_ats','num_dist_catg_purchased',
                    'num_units_6mth','num_txn_6mth','num_units_12mth','num_txn_12mth','ratio_rev_rewd_12mth','ratio_rev_wo_rewd_12mth','num_em_campaign',
                    'per_elec_comm','on_sales_item_rev_12mth','on_sales_rev_ratio_12mth','on_sale_item_qty_12mth','customerkey','masterkey','mobile_ind_tot',
                    'searchdex_ind_tot','gp_hit_ind_tot','br_hit_ind_tot','on_hit_ind_tot','at_hit_ind_tot','factory_hit_ind_tot','sale_hit_ind_tot','markdown_hit_ind_tot',
                    'clearance_hit_ind_tot','pct_off_hit_ind_tot','browse_hit_ind_tot','home_hit_ind_tot','bag_add_ind_tot','purchased','resp_disc_percent')

model_predictor <- c('percent_disc_last_12_mth', 'num_days_on_books','per_elec_comm','num_em_campaign','num_units_12mth','disc_ats','time_since_last_retail_purchase',
                     'avg_order_amt_last_6_mth','br_gp_net_sales_ratio','non_disc_ats','ratio_rev_wo_rewd_12mth','on_sales_item_rev_12mth','br_hit_ind_tot',
                     'br_on_sales_ratio','on_sales_rev_ratio_12mth','ratio_rev_rewd_12mth','num_order_num_last_6_mth','card_status','ratio_order_6_12_mth',
                     'ratio_order_units_6_12_mth','ratio_disc_non_disc_ats','sale_hit_ind_tot','num_disc_comm_responded','mobile_ind_tot','gp_hit_ind_tot',
                     'br_bf_net_sales_ratio','num_dist_catg_purchased','total_plcc_cards','on_hit_ind_tot','pct_off_hit_ind_tot','br_go_net_sales_ratio','at_hit_ind_tot',
                     'purchased','searchdex_ind_tot','factory_hit_ind_tot','clearance_hit_ind_tot','markdown_hit_ind_tot')
#model_predictor[!model_predictor %in% colnames(BR_data)]
#colnames(BR_data)[!colnames(BR_data) %in% model_predictor]

BR_data <- BR_data[,c('customer_key',"resp_disc_percent",model_predictor)]

BR_data_capped <- do.call("cbind.data.frame",lapply(seq_along(model_predictor),cap_value))

# Test the accuracy by 
#head(cbind(BR_data[,28],BR_data_capped[,27]))
#tail(cbind(BR_data[,28],BR_data_capped[,27]))
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

print(paste("start_row:",start_row,"end_row:",end_row))
score_file <- matrix(0,nrow=nrow(BR_data_final),ncol=24)
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
print(paste("start_row:",start_row ,"end_row:",end_row))

score_file[,23]<- rowMeans(score_file[,3:22])
score_file[,24] <- apply(score_file[,3:22],1,median)

write.table(x = score_file[,c(1,2,23,24)],file="//10.8.8.51/lv0/Move to Box/Mithun/projects/9.BR_US_DS_Project/1.data/Jun15/BR_US_DS_Score_Jun15.txt",sep=",",col.names=F,row.names=F,append=T)
print("##################################################################")
print(paste("scoring for data chunk", loop , "complete"))
print("##################################################################")
}


require(ffbase)
require(ff)
#Score_Data <- read.table.ffdf(file = "//10.8.8.51/lv0/Move to Box/Mithun/projects/9.BR_US_DS_Project/1.data/Jun15/BR_US_DS_Score_Jun15.txt",header=TRUE, sep=",",VERBOSE = T,colClasses=c(rep("numeric",23)))

Score_Data <- as.data.frame(score_file)
names(Score_Data) <- c("Customer_key",'Response','score1',	'score2',	'score3',	'score4',	'score5',	'score6',	'score7',	'score8',	'score9',	'score10',	'score11',	'score12',	'score13',	'score14',	'score15',	'score16',	'score17',	'score18',	'score19',	'score20',"MeanScore","MedianScore")

Score_Data <- Score_Data[,c(1,2,23,24)]


