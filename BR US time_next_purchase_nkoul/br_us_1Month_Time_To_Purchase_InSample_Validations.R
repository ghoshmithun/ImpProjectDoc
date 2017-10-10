suppressPackageStartupMessages({library(dplyr);library(pROC);library(ggplot2);library(plotly)})

#save(validation.data,file="GP_Time_To_Next_Purchase_Validation_data.Rdata")

load("validation_3m.Rdata")
validation.data <- validation.data[order( - validation.data$sixmonthprob),]
validation.data$Decile <- as.numeric(cut(1:nrow(validation.data),breaks=quantile(0:nrow(validation.data),probs=seq(0,1,by=0.1)), labels=1:10,right=T))
summary <- validation.data %>% group_by(Decile) %>% summarise(Num_Cust=n(),Num_Resp=sum(purchase_in_target_window)) %>% mutate(Resp_rate=Num_Resp/Num_Cust ,Per_Resp_Cap = cumsum(Num_Resp)/sum(Num_Resp) , Lift = cumsum(Num_Resp)/cumsum(Num_Cust))


# ROC Curve
roc_obj <- roc(response = validation.data$purchase_in_3month_target_window, predictor = validation.data$threemonthprob,plot=TRUE) 

# Area under the Curve
auc(roc_obj)


validation.data <- validation.data[order( - validation.data$threemonthprob),]
validation.data$Percentile <- as.numeric(cut(1:nrow(validation.data),breaks=quantile(0:nrow(validation.data),probs=seq(0,1,by=0.01)), labels=1:100,right=T))
summary_percentile <- validation.data %>% group_by(Percentile) %>% summarise(Num_Cust=n(),Num_Resp=sum(purchase_in_3month_target_window)) %>% mutate(Resp_rate=Num_Resp/Num_Cust ,Per_Resp_Cap = cumsum(Num_Resp)/sum(Num_Resp) , Lift = cumsum(Num_Resp)/cumsum(Num_Cust))

#Baseline purchase_in_target_window rate 
bau_rate<- sum(validation.data$purchase_in_3month_target_window)/nrow(validation.data) #0.6988367
summary_percentile$baseline_gain <- seq(0.01,1,by =0.01)
summary_percentile$gain_over_bau <- summary_percentile$Lift/bau_rate


# Cumulative Gain Chart
ggplot(data=summary_percentile,mapping = aes(x=Percentile))+geom_line(aes(y=baseline_gain, color="baseline_gain")) + 
    geom_line(aes(y=Per_Resp_Cap, color="Per_Resp_Cap")) + ylab("% of all responders Captured") + xlab("% Customer Contacted") +ggtitle("Cumulative Gain Chart ")


# Cumulative Lift Chart
ggplot(data=summary_percentile,mapping = aes(x=Percentile)) + geom_line(aes(y=gain_over_bau,color="Lift")) +geom_line(aes(y=1, color="bau rate")) +
    ylab("Lift achieved over average response  rate") + xlab("% customers contacted") + ggtitle("Cumulative Lift chart  ")

# Response rate across deciles
ggplot(data=summary,aes(x = Decile)) + geom_bar(aes(y=Resp_rate),stat="identity")  +geom_line(aes(y=bau_rate))+ylab("Response rate")+xlab("Decile")+ggtitle("Response Rate across deciles ")

# Response rate across percentiles
ggplot(data=summary_percentile,aes(x = Percentile)) + geom_bar(aes(y=Resp_rate),stat="identity") +geom_line(aes(y=bau_rate))+ylab("Response rate")+xlab("Percentile")+ggtitle("Response Rate across percentile ")
