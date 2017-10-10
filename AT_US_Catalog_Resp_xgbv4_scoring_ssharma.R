library(matrixStats)
library(xgboost)
library(ffbase)


d<-read.table.ffdf(file="//10.8.8.51/lv0/Saumya/Datasets/at_summary.txt",
                   header = F,VERBOSE = TRUE,
                   sep='|')
d<-as.data.frame(d)

colnames(d)<-c("customer_key","net_sales","disc_p","avg_unt_rtl","unt_per_txn","on_sale_items",       "recency","items_browsed","items_abandoned","num_cat","net_sales_sameprd","disc_p_sameprd","num_txn_sameprd","avg_unt_rtl_sameprd","unt_per_txn_sameprd","on_sale_items_sameprd","season" )



for(i in 2:(ncol(d)-1))
{
  d[,i]<-(d[,i]-min(d[,i]))/(max(d[,i])-min(d[,i]))
  
}

filnames<-list.files('//10.8.8.51/lv0/Tanumoy/Datasets/Parameter Files/AT Catalog Response/XGB/v4/')

l<-nrow(d)
top<-matrix(0,l,2)
top[,1]<-d[,1]

mat<-matrix(0,l,20)

s<-0.5
o<-0.1

for(i in 1:20)
{load(paste0('//10.8.8.51/lv0/Tanumoy/Datasets/Parameter Files/AT Catalog Response/XGB/v4/',filnames[i]))
  
  prob_1<- predict(object=xgb.model, as.matrix(d[,c(-1,-17)]))
  prob_0<-1-prob_1
  
  a<-prob_1/(s/o)
  b<-prob_0/((1-s)/(1-o))
  
  adj_p1<-a/(a+b)
  
  mat[,i]<-adj_p1
}

top[,2]<-rowMedians(mat)

colnames(top)<-c("customer_key","prob")

top<-as.data.frame(top)

top <- top[order(-top$prob), ] 

rownames(top)<-NULL
top$rowno<-seq(1:nrow(top))

top$dec<-floor(top$rowno*10/nrow(top))+1

top$dec<-ifelse(top$dec==11,10,top$dec)


write.table(cbind("customer_key","prob","decile"),'//10.8.8.51/lv0/Saumya/Datasets/at_med.csv',append=FALSE,sep=",",row.names=FALSE,col.names=FALSE)

write.table(cbind(top[,1],top[,2],top[,4]),'//10.8.8.51/lv0/Saumya/Datasets/at_med.csv', append=TRUE,sep=",",row.names=FALSE,col.names=FALSE)
