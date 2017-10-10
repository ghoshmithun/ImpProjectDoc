

data_for_distribution <- function(var_name,data=data){
require(ggplot2)
require(dplyr)
p<-list()
  variable <- data[,var_name] 
  variable <- sort(variable)
  if (class(variable) %in% c("numeric","integer")){
    df <- as.data.frame(variable)
    names(df) <- var_name
    df$bin <- cut(df[,var_name], breaks = unique(c(min(na.omit(df[,var_name]))-1,quantile(na.omit(df[,var_name]), prob=seq(0,1,by=0.01)))))
  } else {
    df$bin <- df[,var_name]
  }
  plot_data <- df %>% group_by(bin) %>% summarise(Num_Customer=n()/length(variable)) 
  
  return(plot_data)
  
}

BR_train_data <- read.table(file = "//10.8.8.51/lv0/Move to Box/Mithun/projects/9.BR_US_DS_Project/1.data/BR_disc_sens_Data_pull.txt",stringsAsFactors = F ,sep = "|",header = F )
names(BR_train_data) <- c('customer_key', 
                      'flag_employee', 
                      'offline_last_txn_date', 
                      'online_last_txn_date', 
                      'offline_disc_last_txn_date', 
                      'total_plcc_cards', 
                      'acquisition_date', 
                      'time_since_last_retail_purchase', 
                      'time_since_last_disc_purchase', 
                      'num_days_on_books', 
                      'avg_order_amt_last_6_mth', 
                      'num_order_num_last_6_mth', 
                      'avg_order_amt_last_12_mth', 
                      'ratio_order_6_12_mth', 
                      'num_order_num_last_12_mth', 
                      'ratio_order_units_6_12_mth', 
                      'br_net_sales_amt_12_mth', 
                      'br_on_sales_ratio', 
                      'num_disc_comm_responded', 
                      'percent_disc_last_6_mth', 
                      'percent_disc_last_12_mth', 
                      'br_go_net_sales_ratio', 
                      'br_bf_net_sales_ratio', 
                      'br_gp_net_sales_ratio', 
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
                      'addr_zip_code', 
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



model_predictor <- c('percent_disc_last_12_mth', 'num_days_on_books','per_elec_comm','num_em_campaign','num_units_12mth','disc_ats','time_since_last_retail_purchase',
                     'avg_order_amt_last_6_mth','br_gp_net_sales_ratio','non_disc_ats','ratio_rev_wo_rewd_12mth','on_sales_item_rev_12mth','br_hit_ind_tot',
                     'br_on_sales_ratio','on_sales_rev_ratio_12mth','ratio_rev_rewd_12mth','num_order_num_last_6_mth','card_status','ratio_order_6_12_mth',
                     'ratio_order_units_6_12_mth','ratio_disc_non_disc_ats','sale_hit_ind_tot','num_disc_comm_responded','mobile_ind_tot','gp_hit_ind_tot',
                     'br_bf_net_sales_ratio','num_dist_catg_purchased','total_plcc_cards','on_hit_ind_tot','pct_off_hit_ind_tot','br_go_net_sales_ratio','at_hit_ind_tot',
                     'purchased','searchdex_ind_tot','factory_hit_ind_tot','clearance_hit_ind_tot','markdown_hit_ind_tot')

BR_train_data <- subset(x = BR_train_data , select = model_predictor)

BR_data <- subset(x = BR_data, select = model_predictor)

BR_data_distibution_list <- lapply(model_predictor, function(x) {data_for_distribution(x,BR_data)})



BR_train_data_distibution_list <- lapply(model_predictor, function(x) {data_for_distribution(x,BR_train_data)})

p <-list()
for ( i in 1:length(model_predictor)) {
p[[model_predictor[i]]][[1]] <- ggplot2::ggplot(data = BR_train_data_distibution_list[[i]]) + 
                      geom_line(aes(y=Num_Customer,x=bin),stat = "identity",group=1,color="blue") +
                      theme(legend.position="bottom",axis.text.x = element_text(angle = 45, hjust = 1)) + 
                      ylab("% Customer Train Data") + xlab(paste(model_predictor[i]," Range")) + ggtitle(paste(model_predictor[i], "distribution")) + 
                      scale_y_continuous(labels = scales::percent)

p[[model_predictor[i]]][[2]] <- ggplot2::ggplot(data = BR_data_distibution_list[[i]]) + 
                       geom_line(aes(y=Num_Customer,x=bin),stat = "identity",group=1,color="red") + 
                       theme(legend.position="bottom",axis.text.x = element_text(angle = 45, hjust = 1)) + 
                       ylab("% Customer Score Data") + xlab(paste(model_predictor[i]," Range")) + ggtitle(paste(model_predictor[i], "distribution")) + 
                       scale_y_continuous(labels = scales::percent)
  
}
  require(gridExtra)
  pdf(file = "C:/Users/mghosh/Box Sync/10.DS_New_Training_BR/Variable_Distro.pdf", onefile = TRUE)
  for (i in seq(length(p))) {
    do.call("grid.arrange", p[[i]])  
  }
  dev.off()


  
 