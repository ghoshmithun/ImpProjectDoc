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



full_data_base <-data_full[,!colnames(data_full) %in% c("customerkey","masterkey","flag_employee",
                                                        "offline_disc_last_txn_date", "offline_last_txn_date")]
#--27257261

full_data_base$online_last_txn_date[full_data_base$online_last_txn_date==""]<-'2014-10-31'
full_data_base$acquisition_date <- substr(full_data_base$acquisition_date,1,10)
full_data_base$acquisition_date[full_data_base$acquisition_date==""]<-'2010-10-31'

full_data_base$Time_Since_last_Onl_purchase <- as.numeric(as.Date("2015-11-01")-as.Date(full_data_base$online_last_txn_date),units = "days")
full_data_base$Time_since_acquisition <- as.numeric(as.Date('2015-11-01')-as.Date(full_data_base$acquisition_date),units = "days") /30


full_data_base$Response <- ifelse(full_data_base$resp_disc_percent < -0.35 , 1, 0)
full_data_base$Response[is.na(full_data_base$Response)] <- 0
full_data_base <- full_data_base[,!colnames(full_data_base) %in% c("acquisition_date","online_last_txn_date","resp_disc_percent")]
gc()

#Storing the min max value for use in future scoring
pred <- colnames(full_data_base)[!colnames(full_data_base) %in% c("customer_key","Response")]
full_data_min_max_value <- as.data.frame(t(sapply(full_data_base[,pred],var_min_max_val)))
full_data_min_max_value <- cbind(rownames(full_data_min_max_value),full_data_min_max_value)
names(full_data_min_max_value) <- c("Var_Names","Min_Value","Mid_value","Max_Value")

write.table(full_data_min_max_value,"//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/1. data/on_full_data_min_max_value.txt",sep="|",col.names = T,row.names = F,append=F)


full_data_base <- full_data_base[,!colnames(full_data_base) %in% c("Resp_Disc_Percent","offline_last_txn_date","offline_disc_last_txn_date")]

#List of predictors without time variable
predx <- colnames(full_data_base)[!colnames(full_data_base) %in% c("customer_key","Response","time_since_last_disc_purchase","offline_last_txn_date","offline_disc_last_txn_date"
                                                                  ,"Time_Since_last_Retail_purchase","Time_Since_last_Onl_purchase","Time_since_acquisition")]
full_data_base[,predx] <- do.call("cbind.data.frame",lapply(full_data_base[,predx],var_treat_median))

full_data_base[,predx] <- do.call("cbind.data.frame",lapply(full_data_base[,predx],var_min_max_transform))

#List of   time variable predictors
predy <- colnames(full_data_base)[colnames(full_data_base) %in% c("time_since_last_disc_purchase","Time_Since_last_Retail_purchase","Time_Since_last_Onl_purchase","Time_since_acquisition")]
full_data_base[,predy] <- do.call("cbind.data.frame",lapply(full_data_base[,predy],var_treat_max))
full_data_base[,predy] <- do.call("cbind.data.frame",lapply(full_data_base[,predy],var_min_max_transform))

write.table(full_data_base,"//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/1. data/on_full_data_base.txt",sep="|",append = F,col.names = T,row.names = F)


rf <- randomForest::randomForest(as.factor(Response)~.,data=full_data_base[sample(nrow(full_data_base),80000),-1])

important <- randomForest::importance(rf)
important_var_full_data <-as.data.frame(important)
important_var_full_data <- cbind(rownames(important_var_full_data),important_var_full_data)

rownames(important_var_full_data) <-NULL
names(important_var_full_data) <- c("Var","MeanDecreaseGini")
important_var_full_data <- important_var_full_data[order(-important_var_full_data$MeanDecreaseGini),]

write.table(important_var_full_data ,"//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/important_var_full_data.csv",sep=",",col.names=T,row.names = F,append = F)

require(ggplot2)

ggplot(data = important_var_full_data, aes(x = reorder(Var,-MeanDecreaseGini),y = MeanDecreaseGini)) +
  geom_bar(stat="identity") + theme(axis.text.x = element_text(angle=90)) + xlab("Var")

#full_data_base <- read.table("//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/1. data/GP_full_data_base.txt",sep="|",header=T)

form.alldata<-paste('Response ~',paste(colnames(full_data_base)[!colnames(full_data_base) %in% c("customer_key","Response")],collapse='+'))
mod1_fulldata<-lm(form.alldata,data=full_data_base[sample(nrow(full_data_base),80000),])
summary(mod1_fulldata)
require(VIF)
keep.dat<-vif_func(in_frame=full_data_base[!colnames(full_data_base) %in% c("customer_key","Response")],thresh=3,trace=F)
form.alldata2<-paste('Response ~',paste(keep.dat,collapse='+'))
mod2_fulldata<-lm(form.alldata2,data=full_data_base[sample(nrow(full_data_base),80000),])
summary(mod2_fulldata)
