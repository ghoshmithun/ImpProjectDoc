library(readr)

header <- cbind("Customer_key",'score1',	'score2',	'score3',	'score4',	'score5',	'score6',	'score7',	'score8',	'score9',	'score10',	'score11',	'score12',	'score13',	'score14',	'score15',	'score16',	'score17',	'score18',	'score19',	'score20',"MeanScore","MedianScore")

write.table(x = header,file="//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/1. data/Jan17_data/GP_US_DS_Score.txt",sep=",",col.names=F,row.names=F,append=F)


gp_data_min_max_value <- read.table(file="//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/1. data/full_data_min_max_value.txt",sep="|",header=T)

#Cap the  value
cap_value <- function(i){
  var_name <- model_predictorx[[i]]
  var <- gp_data[,var_name]
  max_val <- gp_data_min_max_value[which(gp_data_min_max_value$Var_Names==var_name),4]
  min_val <- gp_data_min_max_value[which(gp_data_min_max_value$Var_Names==var_name),2]
  mid_val <- gp_data_min_max_value[which(gp_data_min_max_value$Var_Names==var_name),3]
  var[is.na(var)] <- mid_val
  var[var > max_val] <- max_val
  var[var < min_val] <- min_val
  return((var-min_val)/(max_val-min_val))
  #return(var)
}


require(R.utils)
total_row_scoredata <- countLines("//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/1. data/Jan17_data/GP_disc_sens_Data_pull.txt")
#8728606
chunk_size <- 2000000
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
  
gp_data <- read.table(file = "//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/1. data/Jan17_data/GP_disc_sens_Data_pull.txt",
                    stringsAsFactors = F ,nrows = (end_row - start_row), skip = start_row -1, sep = "|",header = F )

names(gp_data) <- c('customer_key','offline_last_txn_date','offline_disc_last_txn_date','acquisition_date','total_plcc_cards',
                    'avg_order_amt_last_6_mth','num_order_num_last_6_mth','ratio_order_6_12_mth','ratio_order_units_6_12_mth',
                    'gp_br_sales_ratio','num_disc_comm_responded','percent_disc_last_6_mth','percent_disc_last_12_mth',
                    'gp_go_net_sales_ratio','gp_bf_net_sales_ratio','gp_on_net_sales_ratio','card_status','disc_ats',
                    'non_disc_ats','ratio_disc_non_disc_ats','num_dist_catg_purchased','num_units_12mth','ratio_rev_rewd_12mth',
                    'ratio_rev_wo_rewd_12mth','num_em_campaign','per_elec_comm','on_sales_rev_ratio_12mth','mobile_ind_tot',
                    'searchdex_ind_tot','gp_hit_ind_tot','br_hit_ind_tot','at_hit_ind_tot','factory_hit_ind_tot','markdown_hit_ind_tot',
                    'clearance_hit_ind_tot','pct_off_hit_ind_tot','purchased')


model_predictor <- c('customer_key','percent_disc_last_12_mth','percent_disc_last_6_mth','per_elec_comm',
                    'gp_hit_ind_tot','num_units_12mth','num_em_campaign','disc_ats','avg_order_amt_last_6_mth','Time_Since_last_disc_purchase',
                    'non_disc_ats','gp_on_net_sales_ratio','on_sales_rev_ratio_12mth','mobile_ind_tot','ratio_order_6_12_mth',
                    'pct_off_hit_ind_tot','ratio_rev_wo_rewd_12mth','ratio_disc_non_disc_ats','card_status','br_hit_ind_tot',
                    'gp_br_sales_ratio','ratio_order_units_6_12_mth','num_disc_comm_responded','purchased','ratio_rev_rewd_12mth',
                    'num_dist_catg_purchased','num_order_num_last_6_mth','at_hit_ind_tot','gp_go_net_sales_ratio','clearance_hit_ind_tot',
                    'searchdex_ind_tot','total_plcc_cards','factory_hit_ind_tot','gp_bf_net_sales_ratio','markdown_hit_ind_tot' )

#model_predictor[!model_predictor %in% colnames(gp_data)] 
#Time_Since_last_disc_purchase - This is the only derived variable (dont count customer_key)

gp_data$offline_disc_last_txn_date <- ifelse(gp_data$offline_disc_last_txn_date=='','2016-01-01',gp_data$offline_disc_last_txn_date)
gp_data$Time_Since_last_disc_purchase <- as.numeric(as.Date('2017-01-01')-as.Date(gp_data$offline_disc_last_txn_date),units = "days")

gp_data <- gp_data[,model_predictor]


model_predictorx <- model_predictor[-1]
gp_data_capped <- do.call("cbind.data.frame",lapply(seq_along(model_predictorx),cap_value))

# Test the accuracy by 
#head(cbind(gp_data[,28],gp_data_capped[,27]))
#tail(cbind(gp_data[,28],gp_data_capped[,27]))

gp_data_final <- cbind.data.frame(gp_data$customer_key,gp_data_capped)

names(gp_data_final) <- model_predictor

rm(gp_data,gp_data_capped)
gc()

#Load the model to score

samplesize <- c('5000',	'11000',	'13000',	'14000',	'15000',	'15000',	'17000',	'17000',	'17000',
                '20000',	'20000',	'20000',	'20000',	'20000',	'20000',	'20000',	'20000',	'20000',
                '20000',	'20000')

run	<- c('5',	'2',	'2',	'3',	'4',	'4',	'2',	'3',	'3',	'1',	'1',	'2',	'3',	'3',	'3',
         '3',	'3',	'3',	'3',	'3')
shrinkage <-	c('0.07',	'0.09',	'0.1',	'0.1',	'0.06',	'0.08',	'0.08',	'0.09',	'0.1',	'0.09',	
               '0.1',	'0.05',	'0.04',	'0.04',	'0.05',	'0.07',	'0.08',	'0.09',	'0.1',	'0.1')
LossFunc	<- c('adaboost',	'adaboost',	'bernoulli',	'bernoulli',	'bernoulli',	'adaboost',	
              'bernoulli',	'adaboost',	'adaboost',	'adaboost',	'adaboost',	'adaboost',
              'adaboost',	'bernoulli',	'adaboost',	'adaboost',	'bernoulli',	'bernoulli',
              'adaboost',	'bernoulli')
print(paste("start_row:",start_row,"end_row:",end_row))
score_file <- matrix(0,nrow=nrow(gp_data_final),ncol=23)
for (j in 1:20){
  require(gbm)
  load(paste0("//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/Model_Objects/gbm/gbm_" ,samplesize[j] , "_" , run[j] , "_" , LossFunc[j] , "_" , shrinkage[j] , ".RData"))
  opt.num.trees <- gbm.perf(gbm.model,plot.it =FALSE)
  score_file[,1]<- gp_data_final$customer_key
  score_file[,j+1] <- predict.gbm(object=gbm.model, newdata=data.frame(gp_data_final), type='response', n.trees=opt.num.trees)
  rm(gbm.model)
  gc()
  print(paste("scoring for model", j))
  }
print(paste("start_row:",start_row ,"end_row:",end_row))

score_file[,22]<- rowMeans(score_file[,2:21])
score_file[,23] <- apply(score_file[,2:21],1,median)

write.table(x = score_file,file="//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/1. data/Jan17_data/GP_US_DS_Score.txt",sep=",",col.names=F,row.names=F,append=T)
print("##################################################################")
print(paste("scoring for data chunk", loop , "complete"))
print("##################################################################")
}


require(ffbase)
require(ff)
Score_Data <- read.table.ffdf(file = "//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/1. data/Jan17_data/GP_US_DS_Score.txt",header=TRUE,sep=",",VERBOSE = T,colClasses=c(rep("numeric",23)))
