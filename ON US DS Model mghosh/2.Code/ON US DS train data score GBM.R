require(ff)
require(ffbase)

on_data_all <-read.table.ffdf(file="//10.8.8.51/lv0/Move to Box/Mithun/projects/8.ON_US_DS_Project/1.data/on_full_data_base.txt",
                              header = TRUE,VERBOSE = TRUE,
                              sep='|',colClasses = c(rep("numeric",53)))


on_data_all <- subset(on_data_all,select=c('customer_key','Response','Time_since_acquisition',
                                           'percent_disc_last_6_mth','non_disc_ats','ratio_rev_rewd_12mth','per_elec_comm','avg_order_amt_last_6_mth','Time_Since_last_Retail_purchase',
                                           'num_em_campaign','disc_ats','num_units_6mth','ratio_order_6_12_mth','Time_Since_last_Onl_purchase','ratio_disc_non_disc_ats',
                                           'num_disc_comm_responded','on_sales_rev_ratio_12mth','ratio_rev_wo_rewd_12mth','on_sales_item_rev_12mth','time_since_last_disc_purchase',
                                           'card_status','mobile_ind_tot','pct_off_hit_ind_tot','num_dist_catg_purchased','ratio_order_units_6_12_mth','gp_hit_ind_tot','on_gp_net_sales_ratio','clearance_hit_ind_tot','num_order_num_last_6_mth','purchased','br_hit_ind_tot','on_go_net_sales_ratio','on_br_sales_ratio','total_plcc_cards','at_hit_ind_tot','on_bf_net_sales_ratio','searchdex_ind_tot','factory_hit_ind_tot','markdown_hit_ind_tot' ))


header <- cbind("Customer_key",'score1',	'score2',	'score3',	'score4',	'score5',	'score6',	'score7',	'score8',	'score9',	'score10',	'score11',	'score12',	'score13',	'score14',	'score15',	'score16',	'score17',	'score18',	'score19',	'score20',"MeanScore","MedianScore")

write.table(x = header,file="//10.8.8.51/lv0/Move to Box/Mithun/projects/8.ON_US_DS_Project/1.data/ON_US_DS_train_Score.txt",sep=",",col.names=F,row.names=F,append=F)

total_row_scoredata <- nrow(on_data_all)
#27257261
chunk_size <- 3000000
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
  print(paste("scoring start for  data chunk", loop , " , start_row:", start_row , " , end_row :" , end_row))
  print("#################################################################################")
  
  on_data_final <- on_data_all[start_row:end_row,]
  #Load the model to score
  
  
  samplesize <- c('6000',	'6000',	'6000',	'6000',	'8000',	'9000',	'9000',	'9000',
                  '9000',	'9000',	'9000',	'9000',	'9000',	'9000',	'9000',	'9000',	'9000',	'9000',	'9000',	'11000')
  
  run	<- c('4',	'4',	'4',	'4',	'3',	'1',	'2',	'2',	'2',	'2',	
           '2',	'2',	'2',	'2',	'2',	'2',	'5',	'5',	'5',	'1')
  shrinkage <-	c('0.01',	'0.04',	'0.05',	'0.07',	'0.08',	'0.08',	'0.001',	'0.01',	'0.02',	'0.03',
                 '0.04',	'0.05',	'0.06',	'0.07',	'0.08',	'0.09',	'0.001',	'0.08',	'0.09',	'0.08')
  LossFunc	<- c('bernoulli',	'bernoulli',	'bernoulli',	'bernoulli',	'bernoulli',	'bernoulli',	'bernoulli',
                'bernoulli',	'bernoulli',	'bernoulli',	'bernoulli',	'adaboost',	'bernoulli',
                'bernoulli',	'adaboost',	'bernoulli',	'bernoulli',	'bernoulli',	'bernoulli',	'bernoulli')
  
  print(paste("start_row:",start_row,"end_row:",end_row))
  score_file <- matrix(0,nrow=nrow(on_data_final),ncol=23)
  for (j in 1:20){
    require(gbm)
    load(paste0("//10.8.8.51/lv0/Move to Box/Mithun/projects/8.ON_US_DS_Project/Model_Objects/gbm/gbm_" ,samplesize[j] , "_" , run[j] , "_" , LossFunc[j] , "_" , shrinkage[j] , ".RData"))
    opt.num.trees <- gbm.perf(gbm.model,plot.it =FALSE)
    score_file[,1]<- on_data_final$customer_key
    score_file[,j+1] <- predict.gbm(object=gbm.model, newdata=data.frame(on_data_final), type='response', n.trees=opt.num.trees)
    rm(gbm.model)
    gc()
    print(paste("scoring for model", j))
  }
  print(paste("start_row:",start_row,"end_row:",end_row))
  
  score_file[,22]<- rowMeans(score_file[,2:21])
  score_file[,23] <- apply(score_file[,2:21],1,median)
  
  write.table(x = score_file,file="//10.8.8.51/lv0/Move to Box/Mithun/projects/8.ON_US_DS_Project/1.data/ON_US_DS_train_Score.txt",sep=",",col.names=F,row.names=F,append=T)
  print("##################################################################")
  print(paste("scoring for data chunk", loop , "complete"))
  print("##################################################################")
}

#countLines("//10.8.8.51/lv0/Move to Box/Mithun/projects/8.ON_US_DS_Project/1.data/ON_US_DS_train_Score.txt")
# 10367378
ON_US_train_score <- read.table.ffdf(file="//10.8.8.51/lv0/Move to Box/Mithun/projects/8.ON_US_DS_Project/1.data/ON_US_DS_train_Score.txt",sep=",",header = T,VERBOSE = TRUE,
                                colClasses = c(rep("numeric",23)))

Score_data <- merge(on_data_all[,1:2],ON_US_train_score[,c(1,22,23)],by.x = "customer_key" ,by.y = "Customer_key")


