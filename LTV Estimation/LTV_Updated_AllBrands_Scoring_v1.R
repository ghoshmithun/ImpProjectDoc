score.ltv = function(modelpath, datapath, outpath)
           
{
  #--------------------------------- Load BTYD (Buy TIll You Die) Package --------------------------------------------
  library(BTYD)
  
  #-------------------------------- Import the R Workspace with the LTV Model details---------------------------------
  
  load(modelpath)
  
  rm(cal.cbs.compressed,
     exp_ltv_18,
     forecasted_txn_12,
     x,
     customer_key,
     forecasted_avg_spend_12,
     forecasted_txn_18,
     T.cal,
     exp_ltv_12,
     forecasted_avg_spend_18,
     m.x,
     t.x
    )
  
  
  
  filelist <- list.files(path=datapath, full.names=T)  
  
  print(filelist)
  
  for(i in seq(1, length(filelist), by=1))
    
  {
    
    filename = filelist[i]
    
    print(filename)
    
    data <- read.table(file=filename, sep="|",head=FALSE)
    
    spend <- data[,5]; transaction_days <- data[,4]; customer_key <- data[,1]
    
    x <- data[,10]
    
    t.x <- data[,8]
    
    T.cal <- data[,9]
    
    m.x <- data[,11]
    
    rm(data);
    
    gc()
    
    #-----------------Forecast the no. of txns and avg spend in next 12 months/52 weeks for the customers ----------------
    
    T.star <- 52
    
    forecasted_txn_12 <- pnbd.ConditionalExpectedTransactions(ptc, T.star, x, t.x, T.cal)
    
    forecasted_avg_spend_12 <- spend.expected.value(psc, m.x, x)
    
    gc()
    
    exp_ltv_12 <- forecasted_txn_12 * forecasted_avg_spend_12
    
    
    #-----------------Forecast the no. of txns and avg spend in next 18 months/78 weeks for the customers ----------------
    
    T.star <- 78
    
    forecasted_txn_18 <- pnbd.ConditionalExpectedTransactions(ptc, T.star, x, t.x, T.cal)
    
    forecasted_avg_spend_18 <- spend.expected.value(psc, m.x, x)
    
    gc()
    
    exp_ltv_18 <- forecasted_txn_18 * forecasted_avg_spend_18
    
    print(summary(exp_ltv_12)); print(summary(exp_ltv_18));
    
    scored_file <- cbind(
                          customer_key,
                          transaction_days,
                          spend,
                          forecasted_txn_12,
                          forecasted_avg_spend_12,
                          exp_ltv_12,
                          forecasted_txn_18, 
                          forecasted_avg_spend_18,
                          exp_ltv_18
                        )
    
    print(summary(scored_file))
    
    if (i==1)
    { 
      print(i); print("Creating file ...");
      write.table(scored_file, file = outpath, append=FALSE,
                  sep= "|", row.names= FALSE, col.names= TRUE)
      
    }
    
    if (i>1)
    {
      print(i); print("Appending to file ...");
      write.table(scored_file, file = outpath, append=TRUE,
                  sep= "|", row.names= FALSE, col.names= FALSE)
    }
    
    
    rm(customer_key,
       transaction_days,
       spend,
       forecasted_txn_12,
       forecasted_avg_spend_12,
       exp_ltv_12,
       forecasted_txn_18, 
       forecasted_avg_spend_18,
       exp_ltv_18,
       x,
       t.x,
       m.x,
       T.cal,
       T.star);
    
    gc();
    
  }
  
}


score.ltv(modelpath="//10.8.8.51/lv0/Tanumoy/Datasets/Model Replication/BR LTV Model.RData",
          datapath="//10.8.8.51/lv0/Tanumoy/Datasets/From Hive/BR_20150301_LTV_CALIB_TRANSACTION_TXT",
          outpath="//10.8.8.51/lv0/Tanumoy/Datasets/Model Replication/BR_20150301_LTV_SCORED_FILE.TXT")

score.ltv(modelpath="//10.8.8.51/lv0/Tanumoy/Datasets/Model Replication/GP LTV Model.RData",
          datapath="//10.8.8.51/lv0/Tanumoy/Datasets/From Hive/GP_20150301_LTV_CALIB_TRANSACTION_TXT",
          outpath="//10.8.8.51/lv0/Tanumoy/Datasets/Model Replication/GP_20150301_LTV_SCORED_FILE.TXT")

score.ltv(modelpath="//10.8.8.51/lv0/Tanumoy/Datasets/Model Replication/ON LTV Model.RData",
          datapath="//10.8.8.51/lv0/Tanumoy/Datasets/From Hive/ON_20150301_LTV_CALIB_TRANSACTION_TXT",
          outpath="//10.8.8.51/lv0/Tanumoy/Datasets/Model Replication/ON_20150301_LTV_SCORED_FILE.TXT")
