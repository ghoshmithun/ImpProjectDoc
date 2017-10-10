


mydata <- read.table(file="//10.8.8.51/lv0/Shraddha/brfs_discsens/bf_disc_sens_model6.txt",sep="|",header=F,stringsAsFactors = F)        

names(mydata) <- c('customer_key',
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

mydata <-mydata[,!names(mydata) %in% c('offline_last_txn_date','online_last_txn_date', 
                                                'offline_disc_last_txn_date','acquisition_date', 
                                                'addr_zip_code','customerkey','masterkey')]
require(dplyr)

mydata$Response <- ifelse(mydata$resp_disc_percent < -0.35 , 1, 0)
mydata$Response[is.na(mydata$Response)] <- 0

var_names <- setdiff(colnames(mydata),c("customer_key","Response","total_plcc_cards","masterkey","online_last_txn_date","Resp_Disc_Percent","offline_last_txn_date","offline_disc_last_txn_date","customerkey"))
mydata_var_types <- sapply(mydata[,var_names],class) 

mydata$card_status <- as.factor(mydata$card_status)



  var_treat_median <- function(var_name){
    var_name_val <- quantile(na.omit(var_name[is.finite(var_name)]), prob=c(0.05,0.5,0.95))
    #var_name[is.na(var_name)] <- var_name_val[2]
    var_name[var_name < var_name_val[1]] <- var_name_val[1]
    var_name[var_name > var_name_val[3]] <- var_name_val[3]
    return(var_name)
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
      
      p[[var_name]][[1]] <- ggplot(data = plot_data2, aes(x=bin, y=value, fill=factor(variable))) + geom_bar(stat = 'identity',position = "dodge") + theme(legend.position="bottom") + 
        xlab(var_name ) + ylab("Num Customers") + ggtitle(paste("Total Customer and Response")) + theme(axis.text.x = element_text(angle = 90, hjust = 1))
      
      plot_data3 <- melt(data = plot_data[,c("bin","Resp_rate","Odd")],id.vars = "bin")
      
      p[[var_name]][[2]] <- ggplot(data = plot_data3,aes(x=bin,y=value,group=factor(variable),color=factor(variable))) + geom_line(size=1)+ geom_point(size=2) + theme(legend.position="bottom")  +
        xlab(var_name ) + ylab("Response Rate and Odd Ratio") + ggtitle(paste("Odd ratio & Response rate")) + theme(axis.text.x = element_text(angle = 90, hjust = 1))
      
      if (class(variable) %in% c("numeric","integer")){     
        p[[var_name]][[3]] <- ggplot(df, aes(x=as.factor(Response),y=df[[var_name]]))+geom_boxplot(colour=c("blue","green"),outlier.colour = "red")+xlab("Response")+ylab(var_name)+ggtitle(paste("BoxPlot:",var_name))+theme(legend.position="bottom",axis.text.x = element_text(angle = 90, hjust = 1))
        
        p[[var_name]][[4]] <- ggplot(df,aes(x=df[,var_name],color=as.factor(Response))) +  geom_freqpoly() +xlab(var_name)+ylab("count")+ggtitle(paste("FreqPlot:",var_name))+ theme(legend.position="bottom",axis.text.x = element_text(angle = 90, hjust = 1))
      }
      #rm(df, plot_data, plot_data2, plot_data3)
      gc()
    }
    
    library(gridExtra)
    
    pdf(path_to_save, onefile = TRUE)
    plot(0:10, type = "n", xaxt="n", yaxt="n", bty="n", xlab = "", ylab = "")
    text(5, 8, "BF Variable Exploration Chart")
    text(5, 7, paste("Analysis made by" ,Sys.getenv("USERNAME"),"on"))
    text(5, 6, Sys.Date())
    for (i in seq(length(p))) {
      do.call("grid.arrange", p[[i]])  
    }
    dev.off()
  }
  
  
  var_plot(var_names=var_names , Response=mydata$Response,data=mydata ,path_to_save = "Z:/Move to Box/Mithun/projects/12.BRFS_US_DS_Project/3.Documents/BRFS_Variable_Response_v2.pdf")
  
  