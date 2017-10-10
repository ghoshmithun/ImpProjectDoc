library(readr)
library(mass)
library(ffbase)
library(BTYD)

rawdata=read_delim(file="C:/Users/sruidas/Desktop/cltv_vm/ON_20151201_return_sales_calibfinal.txt",col_names=FALSE, 
                       delim="|", n_max=-1)
colnames(rawdata)=c("customer_key","acqui_date","trans_days","rept_trans_days",
                        "spend","rept_spend","cust_duration","study_duration",
                        "rep_avg_spend","acqui_date_ret","trans_days_ret",
                    "rep_trans_days_ret","spend_ret","rep_spend_ret",
                    "cust_duration_ret","study_duration_ret", "rep_avg_spend_ret")

summary(rawdata)
sapply(rawdata,class)
capout <- function(x){
  x[ x > quantile(x,.9975) ] <- quantile(x,.9975)
  x
}
acqui_date=substr(rawdata$acqui_date,1,10)
cap_rept_trans_days=apply(rawdata[,4],2,capout)
cap_rept_avg_spend=apply(rawdata[,9],2,capout)
cap_rept_trans_days_ret=apply(rawdata[,11],2,capout)
cap_rept_avg_spend_ret=apply(rawdata[,17],2,capout)
#sales
calib=data.frame(cbind(rawdata$customer_key,acqui_date,cap_rept_trans_days,
                       rawdata$cust_duration,rawdata$study_duration,cap_rept_avg_spend))
calib_ret=data.frame(cbind(rawdata$customer_key,acqui_date,cap_rept_trans_days_ret,
                           rawdata$cust_duration_ret,rawdata$study_duration_ret,
                           cap_rept_avg_spend_ret))

rm(cap_rept_avg_spend_ret,cap_rept_avg_spend,rawdata,
   cap_rept_trans_days,cap_rept_trans_days_ret,rawdata)
gc()
#colnames(calib)=c("cust_key","acqui_date","x","t.x","T.cal","m.x")

calibdata=calib[(as.Date(calib$acqui_date))>=as.Date("2014-12-01"),]
#colnames(calibdata)=c("cust_key","acqui_date","x","t.x","T.cal","m.x")

calib_trandata=data.frame(calibdata[,c(3,4,5)])
colnames(calib_trandata)=c("x","t.x","T.cal")
cols = c(1,2,3)    
calib_trandata[,cols] = apply(calib_trandata[,cols], 2, 
                              function(x) as.numeric(as.character(x)))

calib_expndata=data.frame(calibdata[,c(6,3)])
colnames(calib_expndata)=c("m.x","x")
cols = c(1,2)    
calib_expndata[,cols] = apply(calib_expndata[,cols], 2, 
                              function(x) as.numeric(as.character(x)))
rm(calib,calibdata)
gc()
#return


calibdata_ret=calib_ret[(as.Date(calib_ret$acqui_date))>=as.Date("2014-12-01"),]
#colnames(calibdata_ret)=c("cust_key","acqui_date","x","t.x","T.cal","m.x")
calib_trandata_ret=data.frame(calibdata_ret[,c(3,4,5)])
colnames(calib_trandata_ret)=c("x","t.x","T.cal")
cols = c(1,2,3)    
calib_trandata_ret[,cols] = apply(calib_trandata_ret[,cols], 2, 
                              function(x) as.numeric(as.character(x)))
calib_expndata_ret=data.frame(calibdata_ret[,c(6,3)])
colnames(calib_expndata_ret)=c("m.x","x")
cols = c(1,2)    
calib_expndata_ret[,cols] = apply(calib_expndata_ret[,cols], 2, 
                                  function(x) as.numeric(as.character(x)))
rm(calib_ret,calibdata_ret,cap_rept_avg_spend_ret,cap_rept_avg_spend,
   cap_rept_trans_days,cap_rept_trans_days_ret,rawdata)
gc()
#sales estimation
#compressing for ease of estimation
cbs=pnbd.compress.cbs(calib_trandata)
cbs1=data.frame(cbs)
#pareto nbd estimation of parameters
param=pnbd.EstimateParameters(cbs1)
param
#0.8549137 18.5299606  0.2291164  1.3588024
param1<-spend.EstimateParameters(calib_expndata$m.x, calib_expndata$x)
param1
#1.844649   6.382486 154.064392
saveRDS(param,"C:/Users/sruidas/Desktop/cltv_vm/sales_ret/pareto_nbd_parameter.rds")
saveRDS(param1,"C:/Users/sruidas/Desktop/cltv_vm/sales_ret/pareto_parameter.rds")
rm(cbs,cbs1)
gc()
#returns estimation
#compressing for ease of estimation
cbs_ret=pnbd.compress.cbs(calib_trandata_ret)
cbs1_ret=data.frame(cbs_ret)
#pareto nbd estimation of parameters
param_ret=pnbd.EstimateParameters(cbs1_ret)
param_ret
# 
param1_ret<-spend.EstimateParameters(calib_expndata_ret$m.x, calib_expndata_ret$x)
param1_ret

saveRDS(param_ret,"C:/Users/sruidas/Desktop/cltv_vm/sales_ret/pareto_nbd_parameter_ret.rds")
saveRDS(param1_ret,"C:/Users/sruidas/Desktop/cltv_vm/sales_ret/pareto_parameter_ret.rds")

