library(shiny)
library(ggplot2)
install.packages("usdm")
library(usdm)
library(pROC)
data<-read.csv("//10.8.8.51/lv0/Saumya/Datasets/at_combined.csv",header=FALSE)
colnames(data)<-c('customer_key',
                  'event_type',
                  'cell_key',
                  'cell_label',
                  'treatment_code',
                  'start_dt',
                  'end_dt',
                  'net_sales_amt_6mo_sls',
                  'net_sales_amt_12mo_sls',
                  'net_sales_amt_plcb_6mo_sls',
                  'net_sales_amt_plcb_12mo_sls',
                  'discount_amt_6mo_sls',
                  'discount_amt_12mo_sls',
                  'net_margin_6mo_sls',
                  'net_margin_12mo_sls',
                  'item_qty_6mo_sls',
                  'item_qty_12mo_sls',
                  'item_qty_onsale_6mo_sls',
                  'item_qty_onsale_12mo_sls',
                  'num_txns_6mo_sls',
                  'num_txns_12mo_sls',
                  'num_txns_plcb_6mo_sls',
                  'num_txns_plcb_12mo_sls',
                  'net_sales_amt_6mo_sls_rtl',
                  'net_sales_amt_12mo_sls_rtl',
                  'net_sales_amt_plcb_6mo_sls_rtl',
                  'net_sales_amt_plcb_12mo_sls_rtl',
                  'discount_amt_6mo_sls_rtl',
                  'discount_amt_12mo_sls_rtl',
                  'net_margin_6mo_sls_rtl',
                  'net_margin_12mo_sls_rtl',
                  'item_qty_6mo_sls_rtl',
                  'item_qty_12mo_sls_rtl',
                  'item_qty_onsale_6mo_sls_rtl',
                  'item_qty_onsale_12mo_sls_rtl',
                  'num_txns_6mo_sls_rtl',
                  'num_txns_12mo_sls_rtl',
                  'num_txns_plcb_6mo_sls_rtl',
                  'num_txns_plcb_12mo_sls_rtl',
                  'net_sales_amt_6mo_sls_onl',
                  'net_sales_amt_12mo_sls_onl',
                  'net_sales_amt_plcb_6mo_sls_onl',
                  'net_sales_amt_plcb_12mo_sls_onl',
                  'discount_amt_6mo_sls_onl',
                  'discount_amt_12mo_sls_onl',
                  'net_margin_6mo_sls_onl',
                  'net_margin_12mo_sls_onl',
                  'item_qty_6mo_sls_onl',
                  'item_qty_12mo_sls_onl',
                  'item_qty_onsale_6mo_sls_onl',
                  'item_qty_onsale_12mo_sls_onl',
                  'num_txns_6mo_sls_onl',
                  'num_txns_12mo_sls_onl',
                  'num_txns_plcb_6mo_sls_onl',
                  'num_txns_plcb_12mo_sls_onl',
                  'net_sales_amt_6mo_rtn',
                  'net_sales_amt_12mo_rtn',
                  'net_sales_amt_6mo_rtn_rtl',
                  'net_sales_amt_12mo_rtn_rtl',
                  'net_sales_amt_6mo_rtn_onl',
                  'net_sales_amt_12mo_rtn_onl',
                  'item_qty_6mo_rtn',
                  'item_qty_12mo_rtn',
                  'item_qty_6mo_rtn_rtl',
                  'item_qty_12mo_rtn_rtl',
                  'item_qty_6mo_rtn_onl',
                  'item_qty_12mo_rtn_onl',
                  'num_txns_6mo_rtn',
                  'num_txns_12mo_rtn',
                  'num_txns_6mo_rtn_rtl',
                  'num_txns_12mo_rtn_rtl',
                  'num_txns_6mo_rtn_onl',
                  'num_txns_12mo_rtn_onl',
                  'net_sales_amt_6mo_sls_cp',
                  'net_sales_amt_12mo_sls_cp',
                  'net_sales_amt_plcb_6mo_sls_cp',
                  'net_sales_amt_plcb_12mo_sls_cp',
                  'item_qty_6mo_sls_cp',
                  'item_qty_12mo_sls_cp',
                  'item_qty_onsale_6mo_sls_cp',
                  'item_qty_onsale_12mo_sls_cp',
                  'num_txns_6mo_sls_cp',
                  'num_txns_12mo_sls_cp',
                  'num_txns_plcb_6mo_sls_cp',
                  'num_txns_plcb_12mo_sls_cp',
                  'net_sales_amt_6mo_sls_rtl_cp',
                  'net_sales_amt_12mo_sls_rtl_cp',
                  'net_sales_amt_plcb_6mo_sls_rtl_cp',
                  'net_sales_amt_plcb_12mo_sls_rtl_cp',
                  'item_qty_6mo_sls_rtl_cp',
                  'item_qty_12mo_sls_rtl_cp',
                  'item_qty_onsale_6mo_sls_rtl_cp',
                  'item_qty_onsale_12mo_sls_rtl_cp',
                  'num_txns_6mo_sls_rtl_cp',
                  'num_txns_12mo_sls_rtl_cp',
                  'num_txns_plcb_6mo_sls_rtl_cp',
                  'num_txns_plcb_12mo_sls_rtl_cp',
                  'net_sales_amt_6mo_sls_onl_cp',
                  'net_sales_amt_12mo_sls_onl_cp',
                  'net_sales_amt_plcb_6mo_sls_onl_cp',
                  'net_sales_amt_plcb_12mo_sls_onl_cp',
                  'item_qty_6mo_sls_onl_cp',
                  'item_qty_12mo_sls_onl_cp',
                  'item_qty_onsale_6mo_sls_onl_cp',
                  'item_qty_onsale_12mo_sls_onl_cp',
                  'num_txns_6mo_sls_onl_cp',
                  'num_txns_12mo_sls_onl_cp',
                  'num_txns_plcb_6mo_sls_onl_cp',
                  'num_txns_plcb_12mo_sls_onl_cp',
                  'net_sales_amt_6mo_rtn_cp',
                  'net_sales_amt_12mo_rtn_cp',
                  'net_sales_amt_6mo_rtn_cp_rtl',
                  'net_sales_amt_12mo_rtn_cp_rtl',
                  'net_sales_amt_6mo_rtn_cp_onl',
                  'net_sales_amt_12mo_rtn_cp_onl',
                  'item_qty_6mo_rtn_cp',
                  'item_qty_12mo_rtn_cp',
                  'item_qty_6mo_rtn_cp_rtl',
                  'item_qty_12mo_rtn_cp_rtl',
                  'item_qty_6mo_rtn_cp_onl',
                  'item_qty_12mo_rtn_cp_onl',
                  'num_txns_6mo_rtn_cp',
                  'num_txns_12mo_rtn_cp',
                  'num_txns_6mo_rtn_cp_rtl',
                  'num_txns_12mo_rtn_cp_rtl',
                  'num_txns_6mo_rtn_cp_onl',
                  'num_txns_12mo_rtn_cp_onl',
                  'visasig_flag',
                  'basic_flag',
                  'silver_flag',
                  'sister_flag',
                  'card_status',
                  'days_last_pur',
                  'days_last_pur_onl',
                  'days_last_pur_rtl',
                  'days_last_pur_cp',
                  'days_last_pur_cp_onl',
                  'days_last_pur_cp_rtl',
                  'days_on_books',
                  'days_on_books_cp',
                  'div_shp',
                  'div_shp_onl',
                  'div_shp_rtl',
                  'div_shp_cp',
                  'div_shp_cp_onl',
                  'div_shp_cp_rtl',
                  'at_promotions_received_12mo',
                  'at_promotions_responded_12mo',
                  'response_rate_12mo',
                  'num_txns_sls_promo_12mo',
                  'gross_sales_sls_promo_12mo',
                  'discount_sls_promo_12mo',
                  'net_sales_sls_promo_12mo',
                  'responder')


data<-data[ , which(names(data) %in% c("discount_amt_6mo_sls_onl","num_txns_plcb_12mo_sls_onl",      
                                       "net_sales_amt_plcb_12mo_sls_rtl","net_sales_amt_plcb_6mo_sls_onl",  
                                       "net_sales_amt_12mo_rtn_onl","net_margin_12mo_sls_onl",          
                                       "net_margin_6mo_sls_rtl","num_txns_12mo_rtn_rtl",            
                                       "discount_amt_12mo_sls_rtl","num_txns_6mo_rtn_onl",            
                                       "response_rate_12mo","net_sales_sls_promo_12mo",         
                                       "days_last_pur_onl","item_qty_onsale_6mo_sls_onl",    
                                       "discount_sls_promo_12mo","at_promotions_received_12mo",      
                                       "days_last_pur_rtl","item_qty_onsale_12mo_sls_rtl",    
                                       "item_qty_6mo_rtn_rtl","div_shp_rtl",                      
                                       "item_qty_onsale_6mo_sls_rtl","div_shp_onl",                     
                                       "div_shp","days_on_books",                   
                                       "num_txns_6mo_rtn_cp_onl","net_sales_amt_12mo_rtn_cp_onl",    
                                       "days_last_pur_cp_onl","net_sales_amt_plcb_6mo_sls_onl_cp",
                                       "num_txns_plcb_12mo_sls_cp","net_sales_amt_12mo_rtn_cp_rtl",   
                                       "item_qty_6mo_sls_onl_cp","item_qty_6mo_rtn_cp_rtl","responder","start_dt"))]


tr<-data[data[,1]!="2017-03-09_00-00-00_EST",]
te<-data[data[,1]=="2017-03-09_00-00-00_EST",]

mini<-NULL
maxi<-NULL

for (var in c("discount_amt_6mo_sls_onl","num_txns_plcb_12mo_sls_onl",      
              "net_sales_amt_plcb_12mo_sls_rtl","net_sales_amt_plcb_6mo_sls_onl",  
              "net_sales_amt_12mo_rtn_onl","net_margin_12mo_sls_onl",          
              "net_margin_6mo_sls_rtl","num_txns_12mo_rtn_rtl",            
              "discount_amt_12mo_sls_rtl","num_txns_6mo_rtn_onl",            
              "response_rate_12mo","net_sales_sls_promo_12mo",         
              "days_last_pur_onl","item_qty_onsale_6mo_sls_onl",    
              "discount_sls_promo_12mo","at_promotions_received_12mo",      
              "days_last_pur_rtl","item_qty_onsale_12mo_sls_rtl",    
              "item_qty_6mo_rtn_rtl","div_shp_rtl",                      
              "item_qty_onsale_6mo_sls_rtl","div_shp_onl",                     
              "div_shp","days_on_books",                   
              "num_txns_6mo_rtn_cp_onl","net_sales_amt_12mo_rtn_cp_onl",    
              "days_last_pur_cp_onl","net_sales_amt_plcb_6mo_sls_onl_cp",
              "num_txns_plcb_12mo_sls_cp","net_sales_amt_12mo_rtn_cp_rtl",   
              "item_qty_6mo_sls_onl_cp","item_qty_6mo_rtn_cp_rtl"))
{
  min_var <-quantile(tr[,var],0.01)  
  mini<- rbind(mini,min_var)
  max_var <- quantile(tr[,var],0.99)
  maxi<-rbind(maxi,max_var)
  tr[,var]<-ifelse(tr[,var]< min_var,min_var,ifelse(tr[,var] > max_var,max_var,tr[,var]))
  tr[,var]<-(tr[,var]-min_var)/(max_var-min_var)
}



v<-c("discount_amt_6mo_sls_onl","num_txns_plcb_12mo_sls_onl",      
     "net_sales_amt_plcb_12mo_sls_rtl","net_sales_amt_plcb_6mo_sls_onl",  
     "net_sales_amt_12mo_rtn_onl","net_margin_12mo_sls_onl",          
     "net_margin_6mo_sls_rtl","num_txns_12mo_rtn_rtl",            
     "discount_amt_12mo_sls_rtl","num_txns_6mo_rtn_onl",            
     "response_rate_12mo","net_sales_sls_promo_12mo",         
     "days_last_pur_onl","item_qty_onsale_6mo_sls_onl",    
     "discount_sls_promo_12mo","at_promotions_received_12mo",      
     "days_last_pur_rtl","item_qty_onsale_12mo_sls_rtl",    
     "item_qty_6mo_rtn_rtl","div_shp_rtl",                      
     "item_qty_onsale_6mo_sls_rtl","div_shp_onl",                     
     "div_shp","days_on_books",                   
     "num_txns_6mo_rtn_cp_onl","net_sales_amt_12mo_rtn_cp_onl",    
     "days_last_pur_cp_onl","net_sales_amt_plcb_6mo_sls_onl_cp",
     "num_txns_plcb_12mo_sls_cp","net_sales_amt_12mo_rtn_cp_rtl",   
     "item_qty_6mo_sls_onl_cp","item_qty_6mo_rtn_cp_rtl")

mini<-data.frame(mini)
maxi<-data.frame(maxi)
v<-data.frame(v)
min_max<-data.frame(cbind(v,mini,maxi))
colnames(min_max) <- c("var","min","max")

save(min_max,file="//10.8.8.51/lv0/Saumya/Datasets/at_dm_32var_min_max.Rds")

for (i  in 1:nrow(min_max))
  {var<-as.character(min_max[i,1])
  min_var<-min_max[i,2]
  max_var<-min_max[i,3]
  te[,var]<-ifelse(te[,var]< min_var,min_var,ifelse(te[,var] > max_var,max_var,te[,var]))
te[,var]<-(te[,var]-min_var)/(max_var-min_var)
}

headers1<-cbind("samplesize", "eta","depth","nrounds", "TPR", "TNR", "Accuracy", "AUC")
write.table(headers1, paste0('//10.8.8.51/lv0/Saumya/Datasets/at_dm_training_xgb32_5000_20000.csv'), 
            append=FALSE, sep=",",row.names=FALSE,col.names=FALSE)

headers2<-cbind("samplesize","eta","depth","nrounds","sampleno", "IN_AUC","OUT_AUC")
write.table(headers2, paste0('//10.8.8.51/lv0/Saumya/Datasets/at_dm_testing_xgb32_5000_20000.csv'), 
            append=FALSE, sep=",",row.names=FALSE,col.names=FALSE)


sample.sizes <- seq(5000,20000,5000)
eta<-seq(0.01,0.1,0.01)
depth<-seq(3,6,1)
nround<-c(50,250,500)

for (i in 1:length(sample.sizes))
{
  for (j in 1:length(eta))
   { 
    for(k in 1:length(depth))
     {
      for(l in 1:length(nround))
      {  
        ones <- tr[tr$responder==1,]
      index.ones.tra <- sample(1:nrow(ones), size=0.5*sample.sizes[i], replace=F)
      tra.at.ones <- ones[ index.ones.tra,]
      tst.at.ones <- ones[-index.ones.tra,]
      rm(ones); gc();
      
      
      zeroes <- tr[tr$responder!=1,]
      index.zeroes.tra <- sample(1:nrow(zeroes), size=0.5*sample.sizes[i], replace=F)
      tra.at.zeroes <- zeroes[ index.zeroes.tra,]
      tst.at.zeroes <- zeroes[-index.zeroes.tra,]
      
      rm(zeroes); gc();
      
      
      tra.at <- rbind(tra.at.ones, tra.at.zeroes)
      rm(tra.at.ones, tra.at.zeroes); gc();
      
      
      
      xmatrix <- as.matrix(tra.at[,-c(1,34)])
      yvec = as.matrix(tra.at[,34])
      
      
  
      xgb.object <- paste0('//10.8.8.51/lv0/Saumya/Datasets/XGB_32VAR/',
                           sample.sizes[i], '_',eta[j],'_',depth[k],'_',nround[l],'.RData')
      
      xgb.model <- xgboost(data=xmatrix, label = yvec, objective = "binary:logistic",eta=eta[j],metrics="auc",
                           verbose=F,max_depth=depth[k], nrounds=nround[l])
      
      prob_tra <- predict(xgb.model, xmatrix)
      pred_tra <- ifelse(prob_tra > 0.5, 1, 0)
      tpr <- prop.table(table(yvec, pred_tra),1)[2, 2]
      tnr <- prop.table(table(yvec, pred_tra),1)[1, 1]
      acc <- sum(diag(prop.table(table(yvec, pred_tra))))
      
      roc.area <- as.numeric(auc(roc(prob_tra,factor(yvec))))
      
      
      
      save(xgb.model, file=xgb.object)
      
      write.table(cbind(sample.sizes[i], eta[j],depth[k],nround[l],roc.area),
                  paste0('//10.8.8.51/lv0/Saumya/Datasets/at_dm_training_xgb32_5000_20000.csv'), 
                  append=TRUE, sep=",",row.names=FALSE,col.names=FALSE)
    
      index.tst.at <- sample(1:nrow(te),
                             size=10000*7, replace=F)
      index.in.at <- sample(1:nrow(rbind(tst.at.ones, tst.at.zeroes)),
                            size=10000*7, replace=F)
      
      for (m in 1:7)
      {
        
        tst.at <- te[index.tst.at[((m-1)*10000 + 1):(m*10000)],]
        in.at <- rbind(tst.at.ones, tst.at.zeroes)[index.in.at[((m-1)*10000 + 1):(m*10000)],]
        
        
        
        xmatrix <- as.matrix(in.at[,-c(1,34)])
        yvec = as.matrix(in.at[,34])
        
        
        
        gc()
        prob_tst <- predict(xgb.model, xmatrix)
        
        roc.area.in <- as.numeric(auc(roc( prob_tst,factor(yvec))))
        
        xmatrix <- as.matrix(tst.at[,-c(1,34)])
        yvec = as.matrix(tst.at[,34])
        
        
        
        gc()
        prob_tst <- predict(xgb.model, xmatrix)
        
        roc.area.tst <- as.numeric(auc(roc(prob_tst,factor(yvec))))      
        
        
        
        write.table(cbind(sample.sizes[i], eta[j],depth[k],nround[l],m,roc.area.in,roc.area.tst),
                    paste0('//10.8.8.51/lv0/Saumya/Datasets/at_dm_testing_xgb32_5000_20000.csv'), 
                    append=TRUE, sep=",",row.names=FALSE,col.names=FALSE)
        
        } 
      }
    }
  }
}

mod_file<-read.csv('//10.8.8.51/lv0/Saumya/Datasets/at_dm_best50.csv',header=TRUE)

te_val<-te[sample(1:nrow(te),size=50000,replace=FALSE),]
tr_val<-tr[sample(1:nrow(tr),size=50000,replace=FALSE),]
val<-rbind(te_val,tr_val)
mat<-matrix(0,nrow(val),50)
o<-0.064
s<-0.5

for (i in 1:50)
{load(paste0('//10.8.8.51/lv0/Saumya/Datasets/XGB_32VAR/',
             mod_file[i,1], '_',mod_file[i,2],'_',mod_file[i,3],'_',mod_file[i,4],'.RData'))
  
  prob_1<- predict(object=xgb.model, as.matrix(val[,c(-1,-34)]))
  prob_0<-1-prob_1
  
  a<-prob_1/(s/o)
  b<-prob_0/((1-s)/(1-o))
  
  adj_p1<-a/(a+b)
  
  mat[,i]<-adj_p1
  
  
  
  
}

med<-rowMedians(mat)
roc.area.val <- as.numeric(auc(roc(med,factor(val[,34]))))   #0.72
mea<-rowMeans(mat)
roc.area.val <- as.numeric(auc(roc(mea,factor(val[,34])))) #0.73





tr_mat<-matrix(0,nrow(tr_val),50)

for (i in 1:50)
{load(paste0('//10.8.8.51/lv0/Saumya/Datasets/XGB_32VAR/',
             mod_file[i,1], '_',mod_file[i,2],'_',mod_file[i,3],'_',mod_file[i,4],'.RData'))
  
  prob_1<- predict(object=xgb.model, as.matrix(tr_val[,c(-1,-34)]))
  prob_0<-1-prob_1
  
  a<-prob_1/(s/o)
  b<-prob_0/((1-s)/(1-o))
  
  adj_p1<-a/(a+b)
  
  tr_mat[,i]<-adj_p1
  
  
  
  
}

med<-rowMedians(tr_mat)
roc.area.tr <- as.numeric(auc(roc(med,factor(tr_val[,34])))) #0.804  
mea<-rowMeans(tr_mat)
roc.area.tr <- as.numeric(auc(roc(mea,factor(tr_val[,34])))) #0.806


te_mat<-matrix(0,nrow(te_val),50)

for (i in 1:50)
{load(paste0('//10.8.8.51/lv0/Saumya/Datasets/XGB_32VAR/',
             mod_file[i,1], '_',mod_file[i,2],'_',mod_file[i,3],'_',mod_file[i,4],'.RData'))
  
  prob_1<- predict(object=xgb.model, as.matrix(te_val[,c(-1,-34)]))
  prob_0<-1-prob_1
  
  a<-prob_1/(s/o)
  b<-prob_0/((1-s)/(1-o))
  
  adj_p1<-a/(a+b)
  
  te_mat[,i]<-adj_p1
  
  
  
  
}

med<-rowMedians(te_mat)
roc.area.te <- as.numeric(auc(roc(med,factor(te_val[,34])))) #0.714
mea<-rowMeans(te_mat)
roc.area.te <- as.numeric(auc(roc(mea,factor(te_val[,34])))) #0.718
