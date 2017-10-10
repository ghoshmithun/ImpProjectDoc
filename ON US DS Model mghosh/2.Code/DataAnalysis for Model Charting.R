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


var_plot <- function(var_names,Response=data_full2$Response,data=data_full2,path_to_save="Plot.pdf"){ 
  require(ggplot2)
  require(dplyr)
  p<-list()
  for (var_name in var_names){
    variable <- data[,var_name] 
    df <- cbind.data.frame(variable,Response)
    names(df) <- c(var_name,"Response")
    df <- df[order(df[,var_name]),]
    if (class(variable) %in% c("numeric","integer")){
      df$bin <- cut(df[,var_name], breaks = unique(c(min(na.omit(df[,var_name]))-1,quantile(na.omit(df[,var_name]), prob=seq(0,1,by=0.05)))))
    } else {
      df$bin <- df[,var_name]
    }
    plot_data <- df %>% group_by(bin) %>% summarise(Num_Customer=n(),Num_Responder=sum(Response)) %>% 
      mutate(Num_Non_Responder=Num_Customer-Num_Responder,Resp_rate=Num_Responder/Num_Customer, Odd=Num_Non_Responder/Num_Responder)
    require(reshape2)
    plot_data2 <- melt(data = plot_data[,c("bin","Num_Customer","Num_Responder")],id.vars = "bin")
    
    p[[var_name]][[1]] <- ggplot(data = plot_data2, aes(x=bin, y=value, fill=factor(variable))) + geom_bar(stat = 'identity',position = "dodge") + theme(legend.position="bottom") + scale_fill_discrete(name="") +
      xlab(var_name ) + ylab("Num Customers") + ggtitle(paste("Distribution of response across", var_name)) + theme(axis.text.x = element_text(angle = 90, hjust = 1))
    
    plot_data3 <- melt(data = plot_data[,c("bin","Resp_rate","Odd")],id.vars = "bin")
    
    p[[var_name]][[2]] <- ggplot(data = plot_data3,aes(x=bin,y=value,group=factor(variable),color=factor(variable))) + geom_line(size=1)+ geom_point(size=2) + theme(legend.position="bottom")  +
      xlab(var_name ) + ylab("Response Rate and Odd Ratio") + ggtitle(paste("Odd ratio & Response rate across", var_name)) + theme(axis.text.x = element_text(angle = 90, hjust = 1))
    
    rm(df, plot_data, plot_data2, plot_data3)
    gc()
  }
  library(gridExtra)
  
  pdf(path_to_save, onefile = TRUE)
  for (i in seq(length(p))) {
    do.call("grid.arrange", p[[i]])  
  }
  dev.off()
}

library(readr)
library(ff)
require(ffbase)
        
data_full <- read.table(file="//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/1. data/GP_disc_sens_Data_pull.txt",sep="|", nrows = 10000,header=F,stringsAsFactors = F)        

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



data_full$offline_last_txn_date[data_full$offline_last_txn_date==""]<-'2014-10-31'
data_full$offline_disc_last_txn_date[data_full$offline_disc_last_txn_date==""]<-'2014-10-31'

data_full$Time_Since_last_purchase <- as.numeric(as.Date("2015-11-01")-as.Date(data_full$offline_last_txn_date),units = "days")
data_full$Time_Since_last_disc_purchase <- as.numeric(as.Date('2015-11-01')-as.Date(data_full$offline_disc_last_txn_date),units = "days")

require(dplyr)

data_full$Response <- ifelse(data_full$Resp_Disc_Percent < -0.35 , 1, 0)
data_full$Response[is.na(data_full$Response)] <- 0
data_full2 <- data_full[,!colnames(data_full) %in% c("masterkey","Resp_Disc_Percent","offline_last_txn_date","offline_disc_last_txn_date",
                                                       'mobile_ind_tot',	'searchdex_ind_tot',	'gp_hit_ind_tot',	'br_hit_ind_tot',	'on_hit_ind_tot',	
                                                       'at_hit_ind_tot',	'factory_hit_ind_tot',	'sale_hit_ind_tot',	'markdown_hit_ind_tot',	'clearance_hit_ind_tot',
                                                       'pct_off_hit_ind_tot',	'browse_hit_ind_tot',	'home_hit_ind_tot',	'bag_add_ind_tot',	'purchased')]

data_full_var_types <- sapply(data_full2,class) 
var_names <- setdiff(colnames(data_full2),c("customer_key","Response"))

write.table(x=data_full,file="C:/Users/mghosh/Documents/data_full.csv",sep=",",col.names=T,row.names=F)

var_use <- setdiff(colnames(data_full),c("customer_key","Response","masterkey","Resp_Disc_Percent","offline_last_txn_date","offline_disc_last_txn_date"))
data_full_var_types <- sapply(data_full[,var_use],class)

var_plot(var_names=var_use , Response=data_full$Response,data=data_full,path_to_save="Variable_Response_Plot.pdf" )
