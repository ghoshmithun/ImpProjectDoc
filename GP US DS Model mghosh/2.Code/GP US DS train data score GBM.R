require(ff)
require(ffbase)

gp_data_all <-read.table.ffdf(file="//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/1. data/GP_full_data_base.txt",
                              header = TRUE,VERBOSE = TRUE,
                              sep='|',colClasses = c(rep("numeric",50)))

gp_data_all <- subset(gp_data_all,select=c('customer_key','Response','percent_disc_last_12_mth','percent_disc_last_6_mth','per_elec_comm',
                                           'gp_hit_ind_tot','num_units_12mth','num_em_campaign','disc_ats','avg_order_amt_last_6_mth','Time_Since_last_disc_purchase',
                                           'non_disc_ats','gp_on_net_sales_ratio','on_sales_rev_ratio_12mth','mobile_ind_tot','ratio_order_6_12_mth',
                                           'pct_off_hit_ind_tot','ratio_rev_wo_rewd_12mth','ratio_disc_non_disc_ats','card_status','br_hit_ind_tot',
                                           'gp_br_sales_ratio','ratio_order_units_6_12_mth','num_disc_comm_responded','purchased','ratio_rev_rewd_12mth',
                                           'num_dist_catg_purchased','num_order_num_last_6_mth','at_hit_ind_tot','gp_go_net_sales_ratio','clearance_hit_ind_tot',
                                           'searchdex_ind_tot','total_plcc_cards','factory_hit_ind_tot','gp_bf_net_sales_ratio','markdown_hit_ind_tot' ))

header <- cbind("Customer_key",'score1',	'score2',	'score3',	'score4',	'score5',	'score6',	'score7',	'score8',	'score9',	'score10',	'score11',	'score12',	'score13',	'score14',	'score15',	'score16',	'score17',	'score18',	'score19',	'score20',"MeanScore","MedianScore")

write.table(x = header,file="//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/1. data/GP_US_DS_train_Score.txt",sep=",",col.names=F,row.names=F,append=F)

total_row_scoredata <- nrow(gp_data_all)
#8728606
chunk_size <- 1500000
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
  
  gp_data_final <- gp_data_all[start_row:end_row,]
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
  print(paste("start_row:",start_row,"end_row:",end_row))
  
  score_file[,22]<- rowMeans(score_file[,2:21])
  score_file[,23] <- apply(score_file[,2:21],1,median)
  
  write.table(x = score_file,file="//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/1. data/GP_US_DS_train_Score.txt",sep=",",col.names=F,row.names=F,append=T)
  print("##################################################################")
  print(paste("scoring for data chunk", loop , "complete"))
  print("##################################################################")
}

#countLines("//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/1. data/GP_US_DS_train_Score.txt")
# 10367378
GP_US_train_score <- read.table.ffdf(file="//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/1. data/GP_US_DS_train_Score.txt",sep=",",header = T,VERBOSE = TRUE,
                                colClasses = c(rep("numeric",23)))

Score_data <- merge(gp_data_all[,1:2],GP_US_train_score[,c(1,22,23)],by.x = "customer_key" ,by.y = "Customer_key")


