var_treat <- function(var_name){
  var_name_val <- quantile(na.omit(var_name), prob=c(0.01,0.5,0.99))
  var_name[is.na(var_name)] <- var_name_val[2]
  var_name[var_name < var_name_val[1]] <- var_name_val[1]
  var_name[var_name > var_name_val[3]] <- var_name_val[3]
  return(var_name)
}

var_min_max_transform <- function(var_name){
  min_var_name <- min(var_name)
  max_var_name <- max(var_name)
  return((var_name-min_var_name)/(max_var_name-min_var_name))
}

require(ggplot2)
require(dplyr)
var_plot <- function(var_name,Response){ 
  Response[is.na(Response)] <- 0
  df <- cbind.data.frame(var_name,Response)
  df <- df[order(df$var_name),]
  df$bin <- cut(var_name, breaks = unique(c(min(var_name)-1,quantile(na.omit(var_name), prob=seq(0,1,by=0.1)))))
  plot_data <- df %>% group_by(bin) %>% summarise(Num_Customer=n(),Num_Responder=sum(Response))
  plot_data2 <- melt(plot_data,id.vars = "bin") 
  plot_data$resp_rate <- plot_data$Num_Responder/plot_data$Num_Customer
  plot_data3 <- melt(plot_data[,c("bin","resp_rate")],id.vars = "bin")
  
  plot_data2$facet <- "#Customer and #Responder"
  plot_data3$facet <- "Response Rate"
  
  p<- ggplot(rbind(plot_data2,plot_data3)) +  facet_grid(facet ~., scale="free") +
    geom_bar(stat="identity",aes(bin, value, fill=variable), position="dodge") +
    geom_density(stat="identity",aes(bin, value)) 
  
  return(p)
  
}

library(readr)
library(ff)
require(ffbase)
        
data_full <- read.table.ffdf(file="//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/1. data/GP_disc_sens_Data_pull.txt",sep="|",VERBOSE = T,header=F)        

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

Onl_inact_base <-data_full[is.na(data_full$browse_hit_ind_tot),]

#Online Inactive Train Data Analysis
inactive_data<- data_full[is.na(data_full$browse_hit_ind_tot),]
onl_inact_trn_data_index <- bigsample(x = nrow(inactive_data),size = 0.1*nrow(data_full),replace = F)
data_train <- inactive_data[onl_inact_trn_data_index,]

#data_full <- data_full[order(data_full$masterkey),]
data_train$offline_last_txn_date[data_train$offline_last_txn_date==""]<-'2014-10-31'
data_train$offline_disc_last_txn_date[data_train$offline_disc_last_txn_date==""]<-'2014-10-31'

data_train$Time_Since_last_purchase <- as.numeric(as.Date("2015-11-01")-as.Date(data_train$offline_last_txn_date),units = "days")
data_train$Time_Since_last_disc_purchase <- as.numeric(as.Date('2015-11-01')-as.Date(data_train$offline_disc_last_txn_date),units = "days")

require(dplyr)

data_train$Response <- ifelse(data_train$Resp_Disc_Percent < -0.35 , 1, 0)
data_train$Response[is.na(data_train$Response)] <- 0
data_train <- data_train[,!colnames(data_train) %in% c("masterkey","Resp_Disc_Percent","offline_last_txn_date","offline_disc_last_txn_date",
                                                       'mobile_ind_tot',	'searchdex_ind_tot',	'gp_hit_ind_tot',	'br_hit_ind_tot',	'on_hit_ind_tot',	
                                                       'at_hit_ind_tot',	'factory_hit_ind_tot',	'sale_hit_ind_tot',	'markdown_hit_ind_tot',	'clearance_hit_ind_tot',
                                                       'pct_off_hit_ind_tot',	'browse_hit_ind_tot',	'home_hit_ind_tot',	'bag_add_ind_tot',	'purchased')]

data_train_var_types <- sapply(data_train,class) 


# 
# p2<- ggplot(plot_data2, aes(bin, value)) + geom_point()
# 
# ggplot() + 
#   geom_bar(data=plot_data2,aes(bin, value, fill=variable) ,stat="identity", position="dodge") +
#   geom_point(data=plot_data3, aes(bin, value))+ scale_y_continuous(sec.axis = sec_axis(~.*sum(plot_data3$value)/sum(plot_data2$value)),name = "resp_rate")
# 
# ggplot(rbind(plot_data2,plot_data3)) +  facet_grid(facet ~., scale="free") +
#   geom_bar(stat="identity",aes(bin, value, fill=variable), position="dodge") +
#   geom_density(stat="identity",aes(bin, value)) 

#List of predictors
pred <- colnames(data_train)[!colnames(data_train) %in% c("customer_key","Response")]
data_train[,pred] <- do.call("cbind.data.frame",lapply(data_train[,pred],var_treat))

data_train[,pred] <- do.call("cbind.data.frame",lapply(data_train[,pred],var_min_max_transform))

require(randomForest)

rf <- randomForest::randomForest(as.factor(Response)~.,data=data_train[sample(nrow(data_train),20000),-1])

important <- randomForest::importance(rf)
important_var <-as.data.frame(important)
important_var <- cbind(rownames(important_var),important_var)

rownames(important_var) <-NULL
names(important_var) <- c("Var","MeanDecreaseGini")
important_var_inactive <- important_var[order(-important_var$MeanDecreaseGini),]
write.table(important_var_inactive ,"//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/important_var_inactive.csv",sep=",",col.names=T,row.names = F)
require(ggplot2)

ggplot(data = important_var_inactive, aes(x = reorder(Var,-MeanDecreaseGini),y = MeanDecreaseGini)) +
         geom_bar(stat="identity") + theme(axis.text.x = element_text(angle=90)) +xlab("Var")

form.inactive<-paste('Response ~',paste(colnames(data_train)[-c(1,35)],collapse='+'))
mod1<-lm(form.inactive,data=data_train)
summary(mod1)
require(VIF)
keep.dat.inactive<-vif_func(in_frame=data_train[-c(1,35)],thresh=15,trace=F)
form.in<-paste('y ~',paste(keep.dat.inactive,collapse='+'))
mod2<-lm(form.in,data=data_train)
summary(mod2)



#Online Active Train Data Analysis
active_data <- data_full[!is.na(data_full$browse_hit_ind_tot),]
active_trn_data_index <- bigsample(x = nrow(active_data),size = 0.1*nrow(data_full),replace = F)
act_data_train <- active_data[active_trn_data_index,]

#data_full <- data_full[order(data_full$masterkey),]
act_data_train$offline_last_txn_date[act_data_train$offline_last_txn_date==""]<-'2014-10-31'
act_data_train$offline_disc_last_txn_date[act_data_train$offline_disc_last_txn_date==""]<-'2014-10-31'

act_data_train$Time_Since_last_purchase <- as.numeric(as.Date("2015-11-01")-as.Date(act_data_train$offline_last_txn_date),units = "days")
act_data_train$Time_Since_last_disc_purchase <- as.numeric(as.Date('2015-11-01')-as.Date(act_data_train$offline_disc_last_txn_date),units = "days")

require(dplyr)

act_data_train$Response <- ifelse(act_data_train$Resp_Disc_Percent < -0.35 , 1, 0)
act_data_train$Response[is.na(act_data_train$Response)] <- 0
act_data_train <- act_data_train[,!colnames(act_data_train) %in% c("masterkey","Resp_Disc_Percent","offline_last_txn_date","offline_disc_last_txn_date" )]

act_data_train_var_types <- sapply(act_data_train,class) 


#List of predictors
pred <- colnames(act_data_train)[!colnames(act_data_train) %in% c("customer_key","Response")]
act_data_train[,pred] <- do.call("cbind.data.frame",lapply(act_data_train[,pred],var_treat))

act_data_train[,pred] <- do.call("cbind.data.frame",lapply(act_data_train[,pred],var_min_max_transform))

require(randomForest)

rf <- randomForest::randomForest(as.factor(Response)~.,data=act_data_train[sample(nrow(act_data_train),20000),-1])

important <- randomForest::importance(rf)
important_var <-as.data.frame(important)
important_var <- cbind(rownames(important_var),important_var)

rownames(important_var) <-NULL
names(important_var) <- c("Var","MeanDecreaseGini")
important_var_active <- important_var[order(-important_var$MeanDecreaseGini),]

write.table(important_var_active ,"//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/important_var_active.csv",sep=",",col.names=T,row.names = F)
require(ggplot2)

ggplot(data = important_var_active, aes(x = reorder(Var,-MeanDecreaseGini),y = MeanDecreaseGini)) +
  geom_bar(stat="identity") + theme(axis.text.x = element_text(angle=90)) + xlab("Var")


form.active<-paste('Response ~',paste(colnames(act_data_train)[-c(1,50)],collapse='+'))
mod1_act<-lm(form.active,data=act_data_train)
summary(mod1_act)
require(VIF)
keep.dat.active<-vif_func(in_frame=act_data_train[-c(1,50)],thresh=5,trace=F)
form.active2<-paste('Response ~',paste(keep.dat.active,collapse='+'))
mod2_act<-lm(form.active2,data=act_data_train)
summary(mod2_act)
