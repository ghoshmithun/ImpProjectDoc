
install.packages("h2o", repos=(c("http://s3.amazonaws.com/h2o-release/h2o/master/1497/R", getOption("repos"))))
library(h2o)
library(h2o)
 
#start a local h2o cluster
local.h2o <- h2o.init(ip = "localhost", port = 54321, startH2O = TRUE, nthreads=-1)

#Load the training and testing datasets

training <- read.csv ("train-data.csv") 
testing  <- read.csv ("test-data.csv")
 
 # convert digit labels to factor for classification
training[,1]<-as.factor(training[,1])
 
# pass dataframe from inside of the R environment to the H2O instance
trData<-as.h2o(training)
tsData<-as.h2o(testing)

# Step 4: Train the model	
res.dl <- h2o.deeplearning(x = 2:785, y = 1, trData, activation = "Tanh", hidden=rep(160,5),epochs = 20)

# Step 5: Use the model to predict
#use model to predict testing dataset
pred.dl<-h2o.predict(object=res.dl, newdata=tsData[,-1])
pred.dl.df<-as.data.frame(pred.dl)
 
summary(pred.dl)
test_labels<-testing[,1]
 
#calculate number of correct prediction
sum(diag(table(test_labels,pred.dl.df[,1])))


#Another Examplem -2

library(h2o)
localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE, Xmx = '3g')
train <- h2o.importFile(localH2O, path = "data/train.csv")
train <- cbind(train[,1],train[,-1]/255.0)
test <- h2o.importFile(localH2O, path = "data/test.csv")
test <- test/255.0

s <- proc.time()
set.seed(1105)
model <- h2o.deeplearning(x = 2:785,
                          y = 1,
                          data = train,
                          activation = "RectifierWithDropout",
                          input_dropout_ratio = 0.2,
                          hidden_dropout_ratios = c(0.5,0.5),
                          balance_classes = TRUE, 
                          hidden = c(800,800),
                          epochs = 500)
e <- proc.time()
d <- e - s
d
model

yhat <- h2o.predict(model, test)
ImageId <- as.numeric(seq(1,28000))
names(ImageId)[1] <- "ImageId"
predictions <- cbind(as.data.frame(ImageId),as.data.frame(yhat[,1]))
names(predictions)[2] <- "Label"
write.table(as.matrix(predictions), file="DNN_pred.csv", row.names=FALSE, sep=",")



