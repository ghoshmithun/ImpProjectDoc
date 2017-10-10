
 # https://www.mql5.com/en/articles/1628

# Generating the datasets
inputs <- matrix(c(0,0,0,1,1,0,1,1),ncol=2,byrow=TRUE)
outputs <- matrix(c(0,1,1,0),nrow=4)

# Generating the darch
darch <- newDArch(c(2,4,1),batchSize=2)

# Pre-Train the darch
darch <- preTrainDArch(darch,inputs,maxEpoch=1000)

# Prepare the layers for backpropagation training for
# backpropagation training the layer functions must be
# set to the unit functions which calculates the also
# derivatives of the function result.
layers <- getLayers(darch)
for(i in length(layers):1){
  layers[[i]][[2]] <- sigmoidUnitDerivative
}
setLayers(darch) <- layers
rm(layers)

actFun <- list(sig = sigmoidUnitDerivative,
               tnh = tanSigmoidUnitDerivative,
               lin = linearUnitDerivative,
               soft = softmaxUnitDerivative)
# Setting and running the Fine-Tune function
setFineTuneFunction(darch) <- backpropagation

darch <- fineTuneDArch(darch,inputs,outputs,maxEpoch=1000)

# Running the darch
darch <-  getExecuteFunction(darch)(darch,inputs)
outputs <- getExecOutputs(darch)
cat(outputs[[length(outputs)]])

out <- predict(obj, newdata = x, type = typ) 

# 1. We create the deep architecture object named 'Darch' using the constructor with necessary parameters
newDArch(layers, batchSize, ff=FALSE, logLevel=INFO, genWeightFunc=generateWeights)

# layers: array indicating the number of layers and neurons in each layer. For example: layers = c(5,10,10,2) – an input layer with 5 neurons (visible), two hidden layers with 10 neurons each, and one output layer with 2 outputs.
# BatchSize: size of the mini-sample during training.
# ff: indicates whether the ff format should be used for weights, deviations and exits. The ff format is applied for saving large volumes of data with compression.
# LogLevel: level of logging and output when performing this function.
# GenWeightFunction: function for generating the matrix of RBM weights. There is an opportunity to use the user's activation function.

#2. Function of pre-training the darch-object
preTrainDArch(darch, dataSet, numEpoch = 1, numCD = 1, ..., trainOutputLayer = F),

# darch: instance of the 'Darch' class;
# dataSet: data set for training;
# numEpoch: number of training epochs;
# numCD : number of sampling iterations. Normally, one is sufficient;
# ... : additional parameters that can be transferred to the trainRBM function;
# trainOutputLayer: logical value that shows whether the output layer of RBM should be trained.
# The function performs the trainRBM() training function for every RBM, by copying after training weights and biases to the relevant neural network layers of the darch-object.

#3. Fine-tune function of the darch-object
fineTuneDArch(darch, dataSet, dataSetValid = NULL, numEpochs = 1, bootstrap = T, 
              isBin = FALSE, isClass = TRUE, stopErr = -Inf, stopClassErr = 101, 
              stopValidErr = -Inf, stopValidClassErr = 101, ...)

# darch: sample of the 'Darch' class;
# dataSet: set of data for training (can be used for validation) and testing;
# dataSetValid : set of data used for validation;
# numxEpoch: number of training epochs;
# bootstrap: logical, is it needed to apply bootstrap when creating validation data;
# isBin:indicates if output data should be interpreted as logical values. By default — FALSE. If TRUE, every value above 0.5 is interpreted as 1, and below — as 0.
# isClass : indicates if the network is trained for classification. If TRUE, then statistics for classifications will be determined. TRUE by default.
# stopErr : criterion for stopping the training of neural network due to error occurred during training. -Inf by default;
# stopClassErr : criterion for stopping the training of neural network due to classification error occurred during training. 101 by default;
# stopValidErr : criterion for stopping the neural network due to error in validation data. -Inf by default;
# stopValidClassErr : criterion for stopping the neural network due to classification error occurred during validation. 101 by default;
# ... : additional parameters that can be passed to the training function.

# Functions of the neural network's fine-tune — backpropagation by default, resilient-propagation rpropagation is also available in four variations ("Rprop+", "Rprop-", "iRprop+", "iRprop-") and minimizeClassifier (this function is trained by the Darch network classifier using the nonlinear conjugate gradient method). For the last two algorithms and for those who have a deep knowledge of the subject, a separate implementation of the neural network's fine-tune with a configuration of their multiple parameters is provided.


rpropagation(darch, trainData, targetData, method="iRprop+",
             decFact=0.5, incFact=1.2, weightDecay=0, initDelta=0.0125,
             minDelta=0.000001, maxDelta=50, ...),

# darch – darch-object for training;
# trainData – input data set for training;
# targetData – expected output for the training set;
# method – training method. "iRprop+" by default. "Rprop+", "Rprop-", "iRprop-" are possible;
# decFact – decreasing factor for training. 0.5 by default;
# incFact - increasing factor for training. 1.2 by default;
# weightDecay – decreasing weight at the training. 0 by default;
# initDelta – initialization value at the update. 0.0125 by default;
# minDelta – minimum border for the step size. 0.000001 by default;
# maxDelta – upper border for the step size. 50 by default.

# 3.2. Formation of training and testing samples.
# We have already formed the initial sample of data. Now, we need to divide it into training, validating and testing samples. The ratio by default is 2/3. Various packages have many functions that are used to divide samples. I use rminer::holdout() that calculates indexes for breaking down the initial sample into training and testing samples.

rminer::holdout(y, ratio = 2/3, internalsplit = FALSE, mode = "stratified", iter = 1,
        seed = NULL, window=10, increment=1),

# y – desired target variable, numeric vector or factor, in this case, the stratified separation is applied (i.e. proportions between the classes are the same for all parts);
# ratio – ratio of separation (in percentage — the size of the training sample is established; or in the total number of samples — the size of the testing sample is established);
# internalsplit – if TRUE, then training data is once again separated into training and validation samples. The same ratio is applied for the internal separation;
# mode – sampling mode. Options available:
# stratified – stratified random division (if у factor; otherwise standard random division);
# random – standard random division;
# order – static mode, when first examples are used for training, and the remaining ones — for testing (widely applied for time series);
# rolling – rolling window more commonly known as a sliding window (widely applied at the prediction of stock and financial markets), similarly order, except that window refers to window size, iter — rolling iteration and increment — number of samples the window slides forward at every iteration. The size of the training sample for every iteration is fixed with window, while the testing sample is equivalent to ratio, except for the last iteration (where it could be less).
# incremental – incremental mode of re-training, also known as an increasing window, same as order, except that window is an initial window size, iter — incremental iterations and increment — number of examples added at every iteration. The size of the training sample grows (+increment) at every iteration, whereas the size of the testing set is equivalent to ratio, except for the last iteration, where it can be smaller.
# iter – number of iterations of the incremental mode of re-training (used only when mode = "rolling" or "incremental", iter is usually set in a loop).
# seed – if NULL, then random seed is used, otherwise seed is fixed (further calculations will always have the same result returned);
# window – size of training window (if mode = "rolling") or the initial size of training window (if mode = "incremental");
# increment – number of examples added to the training window at every iteration (if mode="incremental" or mode="rolling").


#We will write a fine-tune function that will train and generate two neural networks: first — trained using the backpropagation function, second — with rpropagation:

#-----9-----------------------------------------
fineMod <- function(variant=1, dbnin, dS, 
                    hd = 0.5, id = 0.2,
                    act = c(2,1), nE = 10)
{
  setDropoutOneMaskPerEpoch(dbnin) <- FALSE
  setDropoutHiddenLayers(dbnin) <- hd
  setDropoutInputLayer(dbnin) <- id
  layers <<- getLayers(dbnin)
  stopifnot(length(layers)==length(act))
  if(variant < 0 || variant >2) {variant = 1}
  for(i in 1:length(layers)){
    fun <- actFun %>% extract2(act[i])
    layers[[i]][[2]] <- fun
  }
  setLayers(dbnin) <- layers
  if(variant == 1 || variant == 2){ # backpropagation
    if(variant == 2){# rpropagation
      #setDropoutHiddenLayers(dbnin) <- 0.0
      setFineTuneFunction(dbnin) <- rpropagation
    }
    mod = fineTuneDArch(darch = dbnin, 
                        dataSet = dS, 
                        numEpochs = nE,
                        bootstrap = T)
    return(mod)
  }
}


# variant - selection of fine-tune function (1- backpropagation, 2- rpropagation).
# dbnin - model of receipt resulted from pre-training.
# dS - data set for fine-tune (dataSet).
# hd - coefficient of sampling (hiddenDropout) in hidden layers of neural network.
# id - coefficient of sampling (inputDropout) in input layer of neural network.
# act - vector with indication of function of neuron activation in every layer of neural network. The length of vector is one unit shorter than the number of layers.
# nE - number of training epochs.
# dataSet — a new variable that appeared in this version. I don't really understand the reason behind its appearance. Normally, the language has two ways of transferring variables to a model — using a pair (x, y) or a formula (y~., data). The introduction of this variable doesn't improve the quality, but confuses the users instead. However, the author may have his reasons that are unknown to me.

