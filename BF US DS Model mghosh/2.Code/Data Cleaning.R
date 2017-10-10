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

data_full <- read.table(file="//10.8.8.51/lv0/Shraddha/brfs_discsens/bf_disc_sens_model6.txt",sep="|",header=F)        

names(data_full) <- c('customer_key',
                      'flag_employee',
                      'offline_last_txn_date',
                      'online_last_txn_date',
                      'offline_disc_last_txn_date',
                      'total_plcc_cards',
                      'acquisition_date',
                      'time_since_last_retail_purchase',
                      'time_since_last_online_purchase',
                      'time_since_last_disc_purchase',
                      'num_days_on_books',
                      'avg_order_amt_last_6_mth',
                      'num_order_num_last_6_mth',
                      'avg_order_amt_last_12_mth',
                      'ratio_order_6_12_mth',
                      'num_order_num_last_12_mth',
                      'ratio_order_units_6_12_mth',
                      'bf_net_sales_amt_12_mth',
                      'bf_on_sales_ratio',
                      'num_disc_comm_responded',
                      'percent_disc_last_6_mth',
                      'percent_disc_last_12_mth',
                      'bf_go_net_sales_ratio',
                      'bf_br_net_sales_ratio',
                      'bf_gp_net_sales_ratio',
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


data_full <-data_full[,!names(data_full) %in% c('flag_employee', 
                                                'offline_last_txn_date','online_last_txn_date', 
                                                'offline_disc_last_txn_date','acquisition_date', 
                                                'addr_zip_code','customerkey','masterkey')]
#--3,342,975

gc()

data_full$Response <- ifelse(data_full$resp_disc_percent < -0.35 , 1, 0)
data_full$Response[is.na(data_full$Response)] <- 0


#Storing the min max value for use in future scoring
pred <- colnames(data_full)[!colnames(data_full) %in% c("customer_key","Response","resp_disc_percent")]
full_data_min_max_value <- as.data.frame(t(sapply(data_full[,pred],var_min_max_val)))
full_data_min_max_value <- cbind(rownames(full_data_min_max_value),full_data_min_max_value)
names(full_data_min_max_value) <- c("Var_Names","Min_Value","Mid_value","Max_Value")

write.table(full_data_min_max_value,"//10.8.8.51/lv0/Move to Box/Mithun/projects/12.BRFS_US_DS_Project/1.data/full_data_min_max_value.txt",sep="|",col.names = T,row.names = F,append=F)


#List of predictors without time related variable (which has to be replaced by maximum value)
predx <- colnames(data_full)[!colnames(data_full) %in% c("customer_key","Response","Resp_Disc_Percent",'time_since_last_retail_purchase',
                                                        'time_since_last_disc_purchase','num_days_on_books')]
data_full[,predx] <- do.call("cbind.data.frame",lapply(data_full[,predx],var_treat_median))

data_full[,predx] <- do.call("cbind.data.frame",lapply(data_full[,predx],var_min_max_transform))

#List of   time variable predictors
predy <- colnames(data_full)[colnames(data_full) %in% c("time_since_last_retail_purchase","time_since_last_disc_purchase","num_days_on_books")]
data_full[,predy] <- do.call("cbind.data.frame",lapply(data_full[,predy],var_treat_max))
data_full[,predy] <- do.call("cbind.data.frame",lapply(data_full[,predy],var_min_max_transform))

data_full <- data_full[,!colnames(data_full) %in% c("resp_disc_percent")]
#write.table(data_full,"//10.8.8.51/lv0/Move to Box/Mithun/projects/12.BRFS_US_DS_Project/1.data/BF_data_full_transformed.txt",sep="|",append = F,col.names = T,row.names = F)
write.table(data_full,"//10.8.8.51/lv0/Shraddha/brfs_discsens/BF_data_full_transformed.txt",sep="|",append = F,col.names = T,row.names = F)





data_full <- read.table(file="//10.8.8.51/lv0/Move to Box/Mithun/projects/12.BRFS_US_DS_Project/1.data/BF_data_full_transformed.txt",sep="|",stringsAsFactors = F,header = T)
data_full <- data_full[,!colnames(data_full) %in% c("total_plcc_cards")]

rf <- randomForest::randomForest(as.factor(Response) ~ . ,data=data_full[sample(nrow(data_full),80000),-1])

important <- randomForest::importance(rf)
important_var_full_data <-as.data.frame(important)
important_var_full_data <- cbind(rownames(important_var_full_data),important_var_full_data)

rownames(important_var_full_data) <-NULL
names(important_var_full_data) <- c("Var","MeanDecreaseGini")
important_var_full_data <- important_var_full_data[order(-important_var_full_data$MeanDecreaseGini),]

write.table(important_var_full_data ,"//10.8.8.51/lv0/Move to Box/Mithun/projects/12.BRFS_US_DS_Project/3.Documents/important_var_full_data.csv",sep=",",col.names=T,row.names = F,append = F)

require(ggplot2)
png("//10.8.8.51/lv0/Move to Box/Mithun/projects/12.BRFS_US_DS_Project/3.Documents/important_var_full_data.png")
ggplot(data = important_var_full_data, aes(x = reorder(Var,-MeanDecreaseGini),y = MeanDecreaseGini,color="var")) +
  geom_bar(stat="identity") + theme(axis.text.x = element_text(angle=90)) + xlab("Var")
dev.off()


# Variable selection based on VIF measure

form1<-paste('Response ~',paste(colnames(data_full)[!colnames(data_full) %in% c("customer_key","Response")],collapse='+'))
mod1_fulldata<-lm(form1,data=data_full[sample(nrow(data_full),80000),])
summary(mod1_fulldata)
require(VIF)
source("//10.8.8.51/lv0/Move to Box/Mithun/projects/12.BRFS_US_DS_Project/2.Code/VIF_FUNC.R")
keep.dat<-vif_func(in_frame=data_full[!colnames(data_full) %in% c("customer_key","Response")],thresh=3,trace=F)
form2<-paste('Response ~',paste(keep.dat,collapse='+'))
mod2_fulldata<-lm(form2,data=data_full[sample(nrow(data_full),80000),])
summary(mod2_fulldata)
