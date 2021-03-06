var_min_max_val <- function(var_name){
  var_name_val <- quantile(na.omit(var_name[is.finite(var_name)]), prob=c(0.005,0.5,0.995))
  return(var_name_val)
}

var_treat_median <- function(var_name){
  var_name_val <- quantile(na.omit(var_name[is.finite(var_name)]), prob=c(0.005,0.5,0.995))
  var_name[is.na(var_name)] <- var_name_val[2]
  var_name[var_name < var_name_val[1]] <- var_name_val[1]
  var_name[var_name > var_name_val[3]] <- var_name_val[3]
  return(var_name)
}

var_treat_max <- function(var_name){
  var_name_val <- quantile(na.omit(var_name[is.finite(var_name)]), prob=c(0.005,0.5,0.995))
  var_name_max <- max(var_name[is.finite(var_name)])
  var_name[is.na(var_name)] <- var_name_max 
  var_name[var_name < var_name_val[1]] <- var_name_val[1] 
  var_name[var_name > var_name_val[3]] <- var_name_val[3] 
  return(var_name)
} 


var_min_max_transform <- function(var_name){
  min_var_name <- min(var_name)
  max_var_name <- max(var_name)
  return((var_name-min_var_name)/(max_var_name-min_var_name))
}




library(readr)
library(ff)
require(ffbase)

data_full <- read.table.ffdf(file="//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/1. data/ON_disc_sens_Data_pull.txt",sep="|",VERBOSE = T,header=F)        
--27257261
names(data_full) <- c('customer_key',
                      'flag_employee',
                      'offline_last_txn_date',
                      'online_last_txn_date',
                      'offline_disc_last_txn_date',
                      'total_plcc_cards',
                      'acquisition_date',
                      'Time_Since_last_Retail_purchase',
                      'time_since_last_disc_purchase',
                      'num_days_on_books',
                      'avg_order_amt_last_6_mth',
                      'num_order_num_last_6_mth',
                      'avg_order_amt_last_12_mth',
                      'ratio_order_6_12_mth',
                      'num_order_num_last_12_mth',
                      'ratio_order_units_6_12_mth',
                      'on_net_sales_amt_12_mth',
                      'on_br_sales_ratio',
                      'num_disc_comm_responded',
                      'percent_disc_last_6_mth',
                      'percent_disc_last_12_mth',
                      'on_go_net_sales_ratio',
                      'on_bf_net_sales_ratio',
                      'on_gp_net_sales_ratio',
                      'card_status',
                      'disc_ats',
                      'non_disc_ats',
                      'ratio_disc_non_disc_ats',
                      'num_dist_catg_purchased',
                      'num_units_6mth',
                      'num_txn_6mth',
                      'num_units_12mth',
                      'num_txn_12mth',
                      'ratio_rev_rewd_12mth',
                      'ratio_rev_wo_rewd_12mth',
                      'num_em_campaign',
                      'per_elec_comm',
                      'on_sales_item_rev_12mth',
                      'on_sales_rev_ratio_12mth',
                      'on_sale_item_qty_12mth',
                      'customerkey',
                      'masterkey',
                      'mobile_ind_tot',
                      'searchdex_ind_tot',
                      'gp_hit_ind_tot',
                      'br_hit_ind_tot',
                      'on_hit_ind_tot',
                      'at_hit_ind_tot',
                      'factory_hit_ind_tot',
                      'sale_hit_ind_tot',
                      'markdown_hit_ind_tot',
                      'clearance_hit_ind_tot',
                      'pct_off_hit_ind_tot',
                      'browse_hit_ind_tot',
                      'home_hit_ind_tot',
                      'bag_add_ind_tot',
                      'purchased',
                      'resp_disc_percent')



####################################################################
## overall full data Base together
#Storing the min max value for use in future scoring
full_data_base <-data_full[,!colnames(data_full) %in% c("customerkey","masterkey","flag_employee",
                                                                  "offline_disc_last_txn_date", "offline_last_txn_date")]
full_data_base$online_last_txn_date[full_data_base$online_last_txn_date==""]<-'2014-10-31'
full_data_base$acquisition_date <- substr(full_data_base$acquisition_date,1,10)
full_data_base$acquisition_date[full_data_base$acquisition_date==""]<-'2010-10-31'

full_data_base$Time_Since_last_Onl_purchase <- as.numeric(as.Date("2015-11-01")-as.Date(full_data_base$online_last_txn_date),units = "days")
full_data_base$Time_since_acquisition <- as.numeric(as.Date('2015-11-01')-as.Date(full_data_base$acquisition_date),units = "days") /30

full_data_base <- full_data_base[,!colnames(full_data_base) %in% c("Resp_Disc_Percent","acquisition_date","offline_last_txn_date","online_last_txn_date","offline_disc_last_txn_date")]

pred <- colnames(full_data_base)[!colnames(full_data_base) %in% c("customer_key","Response","masterkey","flag_employee",
                                                                  "offline_disc_last_txn_date", "offline_last_txn_date")]
full_data_min_max_value <- as.data.frame(t(sapply(full_data_base[,pred],var_min_max_val)))
full_data_min_max_value <- cbind(rownames(full_data_min_max_value),full_data_min_max_value)
names(full_data_min_max_value) <- c("Var_Names","Min_Value","Mid_value","Max_Value")

write.table(full_data_min_max_value,"//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/1. data/on_full_data_min_max_value.txt",sep="|",col.names = T,row.names = F,append=F)


# Taking first 30 variables for space issue
data_base_part1 <-data_full[,1:30]
data_base_part1 <-data_base_part1[,!colnames(data_base_part1) %in% c("customerkey","masterkey","flag_employee",
                                                        "offline_disc_last_txn_date", "offline_last_txn_date")]
#--27257261

data_base_part1$online_last_txn_date[data_base_part1$online_last_txn_date==""]<-'2014-10-31'
data_base_part1$acquisition_date <- substr(data_base_part1$acquisition_date,1,10)
data_base_part1$acquisition_date[data_base_part1$acquisition_date==""]<-'2010-10-31'

data_base_part1$Time_Since_last_Onl_purchase <- as.numeric(as.Date("2015-11-01")-as.Date(data_base_part1$online_last_txn_date),units = "days")
data_base_part1$Time_since_acquisition <- as.numeric(as.Date('2015-11-01')-as.Date(data_base_part1$acquisition_date),units = "days") /30

data_base_part1 <- data_base_part1[,!colnames(data_base_part1) %in% c("Resp_Disc_Percent","acquisition_date","offline_last_txn_date","online_last_txn_date","offline_disc_last_txn_date")]

#List of predictors without time variable
predx <- colnames(data_base_part1)[!colnames(data_base_part1) %in% c("customer_key","resp_disc_percent","time_since_last_disc_purchase","offline_last_txn_date","offline_disc_last_txn_date"
                                                                   ,"Time_Since_last_Retail_purchase","Time_Since_last_Onl_purchase","Time_since_acquisition")]
data_base_part1[,predx] <- do.call("cbind.data.frame",lapply(data_base_part1[,predx],var_treat_median))
gc()
data_base_part1[,predx] <- do.call("cbind.data.frame",lapply(data_base_part1[,predx],var_min_max_transform))

#List of   time variable predictors
predy <- colnames(data_base_part1)[colnames(data_base_part1) %in% c("time_since_last_disc_purchase","Time_Since_last_Retail_purchase","Time_Since_last_Onl_purchase","Time_since_acquisition")]
data_base_part1[,predy] <- do.call("cbind.data.frame",lapply(data_base_part1[,predy],var_treat_max))
data_base_part1[,predy] <- do.call("cbind.data.frame",lapply(data_base_part1[,predy],var_min_max_transform))

data_base_part1 <- as.ffdf(data_base_part1)

# Taking next 28 variables for space issue
data_base_part2 <-data_full[,c(1,31:58)]
data_base_part2 <-data_base_part2[,!colnames(data_base_part2) %in% c("customerkey","masterkey","flag_employee",
                                                                  "offline_disc_last_txn_date", "offline_last_txn_date")]
#--27257261

data_base_part2 <- data_base_part2[,!colnames(data_base_part2) %in% c("acquisition_date","offline_last_txn_date","online_last_txn_date","offline_disc_last_txn_date")]

#List of predictors without time variable
predx <- colnames(data_base_part2)[!colnames(data_base_part2) %in% c("customer_key","resp_disc_percent","time_since_last_disc_purchase","offline_last_txn_date","offline_disc_last_txn_date"
                                                                   ,"Time_Since_last_Retail_purchase","Time_Since_last_Onl_purchase","Time_since_acquisition")]
data_base_part2[,predx] <- do.call("cbind.data.frame",lapply(data_base_part2[,predx],var_treat_median))
gc()
data_base_part2[,predx] <- do.call("cbind.data.frame",lapply(data_base_part2[,predx],var_min_max_transform))

#No  time variable predictors
predy <- colnames(data_base_part2)[colnames(data_base_part2) %in% c("time_since_last_disc_purchase","Time_Since_last_Retail_purchase","Time_Since_last_Onl_purchase","Time_since_acquisition")]
#data_base_part2[,predy] <- do.call("cbind.data.frame",lapply(data_base_part2[,predy],var_treat_max))
#data_base_part2[,predy] <- do.call("cbind.data.frame",lapply(data_base_part2[,predy],var_min_max_transform))



data_base_part2$resp_disc_percent[is.na(data_base_part2$resp_disc_percent)] <- 0
data_base_part2$Response <- ifelse(data_base_part2$resp_disc_percent < -0.35 , 1, 0)

data_base_part2 <- data_base_part2[,!colnames(data_base_part2) %in% c("acquisition_date","online_last_txn_date","resp_disc_percent")]
gc()

data_base_part2<- as.ffdf(data_base_part2)

chunk_size=5000000
tot_num_row=nrow(data_full)
no_loop=floor(tot_num_row/chunk_size)
reminder_row=tot_num_row-no_loop*chunk_size
read_start_row=1
read_end_row=1
for (i in 0:no_loop){
  if (i==0){
    read_end_row = reminder_row
    part1<- data_base_part1[read_start_row:read_end_row,]
    part2 <- data_base_part2[read_start_row:read_end_row,]
    write.table(merge(part1,part2,by="customer_key"),"//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/1. data/on_full_data_base.txt",sep="|",append = F,col.names = T,row.names = F)
    rm(part1,part2)
    gc()
  } else {
    read_start_row=reminder_row+(i-1)*chunk_size+1
    read_end_row=reminder_row + i*chunk_size
    part1<- data_base_part1[read_start_row:read_end_row,]
    part2 <- data_base_part2[read_start_row:read_end_row,]
    write.table(merge(part1,part2,by="customer_key"),"//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/1. data/on_full_data_base.txt",sep="|",append = T,col.names = F,row.names = F)
    rm(part1,part2)
    gc()
  }
}     
     

chunk_size=5000000
tot_num_row=nrow(data_full)
no_loop=floor(tot_num_row/chunk_size)
reminder_row=tot_num_row-no_loop*chunk_size
read_start_row=1
read_end_row=1
for (i in 0:no_loop){
  if (i==0){
    read_end_row = reminder_row
    print(paste("i :",i))
    print(paste("read_start_row :",read_start_row))
    print(paste("read_end_row: ",read_end_row))
    part1<- data_base_part1[read_start_row:read_end_row,]
    part2 <- data_base_part2[read_start_row:read_end_row,]
    write.table(merge(part1,part2,by="customer_key"),"//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/1. data/on_full_data_base2.txt",sep="|",append = F,col.names = T,row.names = F)
    rm(part1,part2)
    gc()
  } else {
    read_start_row=reminder_row+(i-1)*chunk_size+1
    read_end_row=reminder_row + i*chunk_size
    print(paste("i :",i))
    print(paste("read_start_row :",read_start_row))
    print(paste("read_end_row: ",read_end_row))
    part1<- data_base_part1[read_start_row:read_end_row,]
    part2 <- data_base_part2[read_start_row:read_end_row,]
    write.table(merge(part1,part2,by="customer_key"),"//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/1. data/on_full_data_base2.txt",sep="|",append = T,col.names = F,row.names = F)
    rm(part1,part2)
    gc()
  }
}     
     
    
require(R.utils)
num_rows=countLines("//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/1. data/on_full_data_base2.txt")
#27257262 
     