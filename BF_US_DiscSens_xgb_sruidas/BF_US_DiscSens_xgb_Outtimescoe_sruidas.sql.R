 BF_data<-read.table(file = "Z:/Shraddha/brfs_discsens/bf_disc_sens_model6_outtime.txt",
                           sep = "|",header = FALSE)
#BF_data=data.frame(rawdata)
colnames(BF_data) <- c('customer_key',
                       'resp_disc_percent',
                       'ratio_rev_rewd_12mth',
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
                       'time_since_last_online_purchase')
#library(readr)

header <- cbind("Customer_key","Response","MeanScore","MedianScore")

write.table(x = header,file="//10.8.8.51/lv0/Move to Box/Mithun/projects/12.BRFS_US_DS_Project/3.Documents/model_report/bf_ds20_xgb_outtime_score.txt",sep=",",col.names=F,row.names=F,append=F)
BF_data_min_max_value <- read.table(file="//10.8.8.51/lv0/Move to Box/Mithun/projects/12.BRFS_US_DS_Project/1.data/full_data_min_max_value.txt",sep="|",header=T)
#Cap the  value
cap_value <- function(i){
  var_name <- model_predictor[[i]]
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
model_predictor <- c('ratio_rev_rewd_12mth',
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
                     'time_since_last_online_purchase')

BF_data <- BF_data[,colnames(BF_data) %in% c('customer_key',"resp_disc_percent",model_predictor)]

gc()
BF_data_capped <- do.call("cbind.data.frame",lapply(seq_along(model_predictor),cap_value))
BF_data$Response <- ifelse(BF_data$resp_disc_percent < -0.20 , 1, 0)
BF_data$Response[is.na(BF_data$Response)] <- 0

BF_data_final <- cbind.data.frame(BF_data$customer_key,BF_data$Response,BF_data_capped)

names(BF_data_final) <- c('customer_key','Response',model_predictor)
rm(BF_data,BF_data_capped)
gc()
bf_data <- subset(BF_data_final,select=c('customer_key','Response','ratio_rev_rewd_12mth',
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
#bf_data=data.frame(bf_data)
xmatrix <- as.matrix(bf_data[,3:ncol(bf_data)])
yvec = as.matrix(bf_data[,2])
sample.sizes=c(30000,30000,30000,50000,50000,70000,90000,90000,90000,90000,90000,90000,110000,
               110000,110000,110000,110000,110000,110000,110000)
run=c(5,5,5,7,7,3,1,1,8,8,8,8,4,4,4,5,5,6,7,7)
nrounds=c(200,250,500,200,250,200,200,250,200,200,250,500,200,200,250,200,250,250,200,200)
max.depth=c(3,3,3,5,4,3,3,3,3,4,3,3,3,4,5,3,3,3,3,4)
eta=rep(0.05,20)

score_file <- matrix(0,nrow=nrow(bf_data),ncol=24)
score_file[,1]<- bf_data$customer_key
score_file[,2]<- bf_data$Response
for (j in 1:20){
  load( paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/12.BRFS_US_DS_Project/Model_Objects/xgb/xgb_disc20_new_',
               sample.sizes[j], '_', run[j],'_',nrounds[j],'_',max.depth[j],'_',eta[j], '.RData'))
  score_file[,j+2] <- predict(xgb.model, xmatrix)
  rm(xgb.model)
  gc()
  print(paste("scoring for model", j))
}
score_file[,23]<- rowMeans(score_file[,3:22])
score_file[,24] <- apply(score_file[,3:22],1,median)
write.table(x = score_file[,c(1,2,23,24)],file="//10.8.8.51/lv0/Move to Box/Mithun/projects/12.BRFS_US_DS_Project/3.Documents/model_report/bf_ds20_xgb_outtime_score.txt",sep=",",col.names=F,row.names=F,append=T)
Score_Data <- read.table(file = "//10.8.8.51/lv0/Move to Box/Mithun/projects/12.BRFS_US_DS_Project/3.Documents/model_report/bf_ds20_xgb_outtime_score.txt",header=T,sep=",",stringsAsFactors=FALSE)

load( paste0('//10.8.8.51/lv0/Move to Box/Mithun/projects/12.BRFS_US_DS_Project/Model_Objects/xgb/xgb_disc20_new_',
        30000, '_', 5,'_',200,'_',3,'_',0.05, '.RData'))                     
