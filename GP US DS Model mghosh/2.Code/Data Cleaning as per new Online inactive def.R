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

data_full <- read.table.ffdf(file="//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/1. data/GP_disc_sens_Data_pull.txt",sep="|",VERBOSE = T,header=F)        
--10367377
names(data_full) <- c('customer_key',	'offline_last_txn_date',	'offline_disc_last_txn_date',	'total_plcc_cards',	
                      'avg_order_amt_last_6_mth',	'num_order_num_last_6_mth',	'avg_order_amt_last_12_mth',	
                      'ratio_order_6_12_mth',	'num_order_num_last_12_mth',	'ratio_order_units_6_12_mth',	
                      'gp_net_sales_amt_12_mth',	'gp_br_sales_ratio',	'num_disc_comm_responded',	
                      'percent_disc_last_6_mth',	'percent_disc_last_12_mth',	'gp_go_net_sales_ratio',	'gp_bf_net_sales_ratio',	
                      'gp_on_net_sales_ratio',	'card_status',	'disc_ats',	'non_disc_ats',	'ratio_disc_non_disc_ats',
                      'num_dist_catg_purchased',	'num_units_6mth',	'num_txn_6mth',	'num_units_12mth',	'num_txn_12mth',
                      'ratio_rev_rewd_12mth',	'ratio_rev_wo_rewd_12mth',	'num_em_campaign',	'per_elec_comm',	
                      'on_sales_item_rev_12mth',	'on_sales_rev_ratio_12mth',	'on_sale_item_qty_12mth',	'masterkey',
                      'mobile_ind_tot',	'searchdex_ind_tot',	'gp_hit_ind_tot',	'br_hit_ind_tot',	'on_hit_ind_tot',	
                      'at_hit_ind_tot',	'factory_hit_ind_tot',	'sale_hit_ind_tot',	'markdown_hit_ind_tot',	'clearance_hit_ind_tot',
                      'pct_off_hit_ind_tot',	'browse_hit_ind_tot',	'home_hit_ind_tot',	'bag_add_ind_tot',	'purchased',
                      'Resp_Disc_Percent')


#Online Inactive Base 

Onl_inact_base <-data_full[is.na(data_full$purchased) | (data_full$purchased == 0),]
#--6131845
Onl_inact_base <- Onl_inact_base[,]

Onl_inact_base$offline_last_txn_date[Onl_inact_base$offline_last_txn_date==""]<-'2014-10-31'
Onl_inact_base$offline_disc_last_txn_date[Onl_inact_base$offline_disc_last_txn_date==""]<-'2014-10-31' 

Onl_inact_base$Time_Since_last_purchase <- as.numeric(as.Date("2015-11-01")-as.Date(Onl_inact_base$offline_last_txn_date),units = "days")
Onl_inact_base$Time_Since_last_disc_purchase <- as.numeric(as.Date('2015-11-01')-as.Date(Onl_inact_base$offline_disc_last_txn_date),units = "days")

Onl_inact_base$Response <- ifelse(Onl_inact_base$Resp_Disc_Percent < -0.35 , 1, 0)
Onl_inact_base$Response[is.na(Onl_inact_base$Response)] <- 0

Onl_inact_base$purchased[is.na(Onl_inact_base$purchased)] <- 0

#Storing the min max value for use in future scoring
pred <- colnames(Onl_inact_base)[!colnames(Onl_inact_base) %in% c("customer_key","Response","masterkey","offline_last_txn_date","offline_disc_last_txn_date")]
Onl_inact_min_max_value <- as.data.frame(t(sapply(Onl_inact_base[,pred],var_min_max_val)))

Onl_inact_min_max_value <- cbind.data.frame(Onl_inact_min_max_value$`rownames(Onl_inact_min_max_value)`,
                                            as.numeric(Onl_inact_min_max_value$V1),as.numeric(Onl_inact_min_max_value$V2))
names(Onl_inact_min_max_value) <- c("Var_Names","Min_Value","Max_Value")

write.table(Onl_inact_min_max_value,"//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/1. data/Onl_inact_min_max_value.txt",sep="|",col.names = T,row.names = F,append=F)



Onl_inact_base <- Onl_inact_base[,!colnames(Onl_inact_base) %in% c("Resp_Disc_Percent","offline_last_txn_date","offline_disc_last_txn_date","masterkey","purchased")]

#List of predictors without time variable
pred <- colnames(Onl_inact_base)[!colnames(Onl_inact_base) %in% c("customer_key","Response","Time_Since_last_purchase","Time_Since_last_disc_purchase")]
Onl_inact_base[,pred] <- do.call("cbind.data.frame",lapply(Onl_inact_base[,pred],var_treat_median))

Onl_inact_base[,pred] <- do.call("cbind.data.frame",lapply(Onl_inact_base[,pred],var_min_max_transform))


#List of   time variable predictors
pred <- colnames(Onl_inact_base)[colnames(Onl_inact_base) %in% c("Time_Since_last_purchase","Time_Since_last_disc_purchase")]
Onl_inact_base[,pred] <- do.call("cbind.data.frame",lapply(Onl_inact_base[,pred],var_treat_max))
Onl_inact_base[,pred] <- do.call("cbind.data.frame",lapply(Onl_inact_base[,pred],var_min_max_transform))

write.table(Onl_inact_base,"//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/1. data/GP_Onl_inact_base.txt",sep="|",col.names = T,row.names = F,append=F)


require(randomForest)

rf <- randomForest::randomForest(as.factor(Response)~.,data=Onl_inact_base[sample(nrow(Onl_inact_base),40000),-1])

important <- randomForest::importance(rf)
important_var_inact <-as.data.frame(important)
important_var_inact <- cbind(rownames(important_var_inact),important_var_inact)
rownames(important_var_inact) <-NULL
names(important_var_inact) <- c("Var","MeanDecreaseGini")
important_var_inact <- important_var_inact[order(-important_var_inact$MeanDecreaseGini),]

write.table(important_var_inact ,"//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/important_var_inactive.csv",sep=",",col.names=T,row.names = F,append = F)
require(ggplot2)

ggplot(data = important_var_inact, aes(x = reorder(Var,-MeanDecreaseGini),y = MeanDecreaseGini)) +
  geom_bar(stat="identity") + theme(axis.text.x = element_text(angle=90)) + xlab("Var")

#Onl_inact_base <- read.table("//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/1. data/GP_Onl_inact_base.txt",sep="|",header=T)

form.inactive<-paste('Response ~',paste(colnames(Onl_inact_base)[!colnames(Onl_inact_base) %in% c("customer_key","Response")],collapse='+'))
mod1_inact<-lm(form.inactive,data=Onl_inact_base[sample(nrow(Onl_inact_base),40000),])
summary(mod1_inact)
require(VIF)
keep.dat.inactive<-vif_func(in_frame=Onl_inact_base[sample(nrow(Onl_inact_base),40000),!colnames(Onl_inact_base) %in% c("customer_key","Response")],thresh=3,trace=F)
form.inactive2<-paste('Response ~',paste(keep.dat.inactive,collapse='+'))
mod2_inact<-lm(form.inactive2,data=Onl_inact_base[sample(nrow(Onl_inact_base),40000),])
summary(mod2_inact)


####################################################################################
#.. Online Active Base

Onl_act_base <-data_full[(data_full$purchased > 0),]
#--4235532
Onl_act_base <- Onl_act_base[,!colnames(Onl_act_base) %in% c("masterkey")]

Onl_act_base$offline_last_txn_date[Onl_act_base$offline_last_txn_date==""]<-'2014-10-31'
Onl_act_base$offline_disc_last_txn_date[Onl_act_base$offline_disc_last_txn_date==""]<-'2014-10-31'

Onl_act_base$Time_Since_last_purchase <- as.numeric(as.Date("2015-11-01")-as.Date(Onl_act_base$offline_last_txn_date),units = "days")
Onl_act_base$Time_Since_last_disc_purchase <- as.numeric(as.Date('2015-11-01')-as.Date(Onl_act_base$offline_disc_last_txn_date),units = "days")

Onl_act_base$Response <- ifelse(Onl_act_base$Resp_Disc_Percent < -0.35 , 1, 0)
Onl_act_base$Response[is.na(Onl_act_base$Response)] <- 0


#Storing the min max value for use in future scoring
pred <- colnames(Onl_act_base)[!colnames(Onl_act_base) %in% c("customer_key","Response","masterkey","offline_last_txn_date","offline_disc_last_txn_date")]
Onl_act_min_max_value <- as.data.frame(t(sapply(Onl_act_base[,pred],var_min_max_val)))
Onl_act_min_max_value <- cbind(rownames(Onl_act_min_max_value),Onl_act_min_max_value)
Onl_act_min_max_value <- cbind.data.frame(Onl_act_min_max_value$`rownames(Onl_act_min_max_value)`,
                                            as.numeric(Onl_act_min_max_value$V1),as.numeric(Onl_act_min_max_value$V2))
names(Onl_act_min_max_value) <- c("Var_Names","Min_Value","Max_Value")

write.table(Onl_act_min_max_value,"//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/1. data/Onl_act_min_max_value.txt",sep="|",col.names = T,row.names = F,append=F)


Onl_act_base <- Onl_act_base[,!colnames(Onl_act_base) %in% c("Resp_Disc_Percent","offline_last_txn_date","offline_disc_last_txn_date")]

#List of predictors without time variable
pred <- colnames(Onl_act_base)[!colnames(Onl_act_base) %in% c("customer_key","Response","Time_Since_last_purchase","Time_Since_last_disc_purchase","Time_Since_last_purchase","Time_Since_last_disc_purchase")]
Onl_act_base[,pred] <- do.call("cbind.data.frame",lapply(Onl_act_base[,pred],var_treat_median))

Onl_act_base[,pred] <- do.call("cbind.data.frame",lapply(Onl_act_base[,pred],var_min_max_transform))

#List of   time variable predictors
pred <- colnames(Onl_act_base)[colnames(Onl_act_base) %in% c("Time_Since_last_purchase","Time_Since_last_disc_purchase")]
Onl_act_base[,pred] <- do.call("cbind.data.frame",lapply(Onl_act_base[,pred],var_treat_max))
Onl_act_base[,pred] <- do.call("cbind.data.frame",lapply(Onl_act_base[,pred],var_min_max_transform))

write.table(Onl_act_base,"//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/1. data/GP_Onl_act_base.txt",sep="|",append = F,col.names = T,row.names = F)



rf <- randomForest::randomForest(as.factor(Response)~.,data=Onl_act_base[sample(nrow(Onl_act_base),60000),-1])

important <- randomForest::importance(rf)
important_var_act <-as.data.frame(important)
important_var_act <- cbind(rownames(important_var_act),important_var_act)

rownames(important_var_act) <-NULL
names(important_var_act) <- c("Var","MeanDecreaseGini")
important_var_act <- important_var_act[order(-important_var_act$MeanDecreaseGini),]

write.table(important_var_act ,"//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/important_var_active.csv",sep=",",col.names=T,row.names = F,append = F)

require(ggplot2)

ggplot(data = important_var_act, aes(x = reorder(Var,-MeanDecreaseGini),y = MeanDecreaseGini)) +
  geom_bar(stat="identity") + theme(axis.text.x = element_text(angle=90)) + xlab("Var")

#Onl_act_base <- read.table("//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/1. data/GP_Onl_act_base.txt",sep="|",header=T)

form.active<-paste('Response ~',paste(colnames(Onl_act_base)[-c(1,50)],collapse='+'))
mod1_act<-lm(form.active,data=Onl_act_base[sample(nrow(Onl_act_base),60000),])
summary(mod1_act)
require(VIF)
keep.dat.active<-vif_func(in_frame=Onl_act_base[sample(nrow(Onl_act_base),60000),!colnames(Onl_act_base) %in% c("customer_key","Response")],thresh=3,trace=T)
form.active2<-paste('Response ~',paste(keep.dat.active,collapse='+'))
mod2_act<-lm(form.active2,data=Onl_act_base[sample(nrow(Onl_act_base),60000),])
summary(mod2_act)



####################################################################
## overall full data Base together



full_data_base <-data_full[,]
#--10367377
full_data_base <- full_data_base[,!colnames(full_data_base) %in% c("masterkey")]

full_data_base$offline_last_txn_date[full_data_base$offline_last_txn_date==""]<-'2014-10-31'
full_data_base$offline_disc_last_txn_date[full_data_base$offline_disc_last_txn_date==""]<-'2014-10-31'

full_data_base$Time_Since_last_purchase <- as.numeric(as.Date("2015-11-01")-as.Date(full_data_base$offline_last_txn_date),units = "days")
full_data_base$Time_Since_last_disc_purchase <- as.numeric(as.Date('2015-11-01')-as.Date(full_data_base$offline_disc_last_txn_date),units = "days")

full_data_base$Response <- ifelse(full_data_base$Resp_Disc_Percent < -0.35 , 1, 0)
full_data_base$Response[is.na(full_data_base$Response)] <- 0


#Storing the min max value for use in future scoring
pred <- colnames(full_data_base)[!colnames(full_data_base) %in% c("customer_key","Response","masterkey","offline_last_txn_date","offline_disc_last_txn_date")]
full_data_min_max_value <- as.data.frame(t(sapply(full_data_base[,pred],var_min_max_val)))
full_data_min_max_value <- cbind(rownames(full_data_min_max_value),full_data_min_max_value)
names(full_data_min_max_value) <- c("Var_Names","Min_Value","Mid_value","Max_Value")

write.table(full_data_min_max_value,"//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/1. data/full_data_min_max_value.txt",sep="|",col.names = T,row.names = F,append=F)


full_data_base <- full_data_base[,!colnames(full_data_base) %in% c("Resp_Disc_Percent","offline_last_txn_date","offline_disc_last_txn_date")]

#List of predictors without time variable
pred <- colnames(full_data_base)[!colnames(full_data_base) %in% c("customer_key","Response","Resp_Disc_Percent","offline_last_txn_date","offline_disc_last_txn_date")]
full_data_base[,pred] <- do.call("cbind.data.frame",lapply(full_data_base[,pred],var_treat_median))

full_data_base[,pred] <- do.call("cbind.data.frame",lapply(full_data_base[,pred],var_min_max_transform))

#List of   time variable predictors
pred <- colnames(full_data_base)[colnames(full_data_base) %in% c("Time_Since_last_purchase","Time_Since_last_disc_purchase")]
full_data_base[,pred] <- do.call("cbind.data.frame",lapply(full_data_base[,pred],var_treat_max))
full_data_base[,pred] <- do.call("cbind.data.frame",lapply(full_data_base[,pred],var_min_max_transform))

write.table(full_data_base,"//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/1. data/GP_full_data_base.txt",sep="|",append = F,col.names = T,row.names = F)


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

form.active<-paste('Response ~',paste(colnames(full_data_base)[!colnames(full_data_base) %in% c("customer_key","Response")],collapse='+'))
mod1_fulldata<-lm(form.active,data=full_data_base[sample(nrow(full_data_base),80000),])
summary(mod1_fulldata)
require(VIF)
keep.dat<-vif_func(in_frame=full_data_base[!colnames(full_data_base) %in% c("customer_key","Response")],thresh=3,trace=F)
form.active2<-paste('Response ~',paste(keep.dat,collapse='+'))
mod2_fulldata<-lm(form.active2,data=full_data_base[sample(nrow(full_data_base),80000),])
summary(mod2_fulldata)
