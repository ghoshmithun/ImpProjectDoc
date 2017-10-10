install.packages("drat", repos="https://cran.rstudio.com")
drat:::addRepo("dmlc")
install.packages("mxnet")

#Setting up Network http://mxnet.io/tutorials/r/mnistCompetition.html

  data <- mx.symbol.Variable("data")
   fc1 <- mx.symbol.FullyConnected(data, name="fc1", num_hidden=128)
   act1 <- mx.symbol.Activation(fc1, name="relu1", act_type="relu")
   fc2 <- mx.symbol.FullyConnected(act1, name="fc2", num_hidden=64)
   act2 <- mx.symbol.Activation(fc2, name="relu2", act_type="relu")
   fc3 <- mx.symbol.FullyConnected(act2, name="fc3", num_hidden=10)
   softmax <- mx.symbol.SoftmaxOutput(fc3, name="sm")
   
  devices <- mx.cpu()
  model <- mx.model.FeedForward.create(softmax, X=train.x, y=train.y,
                                        ctx=devices, num.round=10, array.batch.size=100,
                                        learning.rate=0.07, momentum=0.9,  eval.metric=mx.metric.accuracy,
                                        initializer=mx.init.uniform(0.07),
                                           epoch.end.callback=mx.callback.log.train.metric(100))
										   
										   
										   
#LeNet	Usage
# input
data <- mx.symbol.Variable('data')
# first conv
conv1 <- mx.symbol.Convolution(data=data, kernel=c(5,5), num_filter=20)
tanh1 <- mx.symbol.Activation(data=conv1, act_type="tanh")
pool1 <- mx.symbol.Pooling(data=tanh1, pool_type="max",
                          kernel=c(2,2), stride=c(2,2))
# second conv
conv2 <- mx.symbol.Convolution(data=pool1, kernel=c(5,5), num_filter=50)
tanh2 <- mx.symbol.Activation(data=conv2, act_type="tanh")
pool2 <- mx.symbol.Pooling(data=tanh2, pool_type="max",
                          kernel=c(2,2), stride=c(2,2))
# first fullc
flatten <- mx.symbol.Flatten(data=pool2)
fc1 <- mx.symbol.FullyConnected(data=flatten, num_hidden=500)
tanh3 <- mx.symbol.Activation(data=fc1, act_type="tanh")
# second fullc
fc2 <- mx.symbol.FullyConnected(data=tanh3, num_hidden=10)
# loss
lenet <- mx.symbol.SoftmaxOutput(data=fc2)

train.array <- train.x
dim(train.array) <- c(28, 28, 1, ncol(train.x))
test.array <- test
dim(test.array) <- c(28, 28, 1, ncol(test))

n.gpu <- 1
device.cpu <- mx.cpu()
device.gpu <- lapply(0:(n.gpu-1), function(i) {
  mx.gpu(i)
})

   mx.set.seed(0)
   tic <- proc.time()
   model <- mx.model.FeedForward.create(lenet, X=train.array, y=train.y,
                                    ctx=device.cpu, num.round=1, array.batch.size=100,
                                    learning.rate=0.05, momentum=0.9, wd=0.00001,
                                    eval.metric=mx.metric.accuracy,
                                      epoch.end.callback=mx.callback.log.train.metric(100))

#Train on a GPU:
 mx.set.seed(0)
   tic <- proc.time()
   model <- mx.model.FeedForward.create(lenet, X=train.array, y=train.y,
                                    ctx=device.gpu, num.round=5, array.batch.size=100,
                                    learning.rate=0.05, momentum=0.9, wd=0.00001,
                                    eval.metric=mx.metric.accuracy,
                                      epoch.end.callback=mx.callback.log.train.metric(100))			

#Develop a Neural Network with MXNet in Five Minutes http://mxnet.io/tutorials/r/fiveMinutesNeuralNetwork.html
 require(mlbench)
require(mxnet)
   data(Sonar, package="mlbench")

   Sonar[,61] = as.numeric(Sonar[,61])-1
   train.ind = c(1:50, 100:150)
   train.x = data.matrix(Sonar[train.ind, 1:60])
   train.y = Sonar[train.ind, 61]
   test.x = data.matrix(Sonar[-train.ind, 1:60])
   test.y = Sonar[-train.ind, 61]
   
# mx.mlp requires the following parameters:
# Training data and label
# Number of hidden nodes in each hidden layer
# Number of nodes in the output layer
# Type of the activation
# Type of the output loss
# The device to train (GPU or CPU)
# Other parameters for mx.model.FeedForward.create

   mx.set.seed(0)
   model <- mx.mlp(train.x, train.y, hidden_node=10, out_node=2, out_activation="softmax",
               num.round=20, array.batch.size=15, learning.rate=0.07, momentum=0.9, 
               eval.metric=mx.metric.accuracy)
			   

# To get an idea of what is happening, view the computation graph from R:
graph.viz(model$symbol$as.json())