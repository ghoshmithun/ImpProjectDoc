import tensorflow as tf
import keras as keras
from keras.models import Sequential
from keras.layers import Dense, Activation
from keras.optimizers import SGD
from keras.utils.np_utils import to_categorical
from keras.regularizers import l2, activity_l2
from keras.callbacks import ModelCheckpoint
from keras.models import model_from_json


import pylab as pl
import pandas as pd
import csv as csv

import sklearn as sklearn
from sklearn import svm, datasets, model_selection
from sklearn.neural_network import MLPClassifier
from sklearn.utils import shuffle
from sklearn.metrics import roc_curve, auc
from sklearn.svm import SVC
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import StratifiedKFold
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import KFold
from sklearn.metrics import precision_recall_fscore_support


import numpy as np
import matplotlib.pyplot as plt

import os
import shutil
from sklearn.externals import joblib


def savemodel(dirpath):
    if not os.path.exists(dirpath):
        os.makedirs(dirpath)
    else:
        shutil.rmtree(dirpath)  # removes all the subdirectories!
        os.makedirs(dirpath)


at_data = pd.read_csv('//10.8.8.51/lv0/Tanumoy/Datasets/Model Replication/at_catalog_data_v4.csv',
                      sep=',', header=0)

at_data.columns = [
                    "customer_key",
                    "bought",
                    "net_sales",
                    "disc_p",
                    "avg_unt_rtl",
                    "unt_per_txn",
                    "on_sale_items",
                    "recency",
                    "items_browsed",
                    "items_abandoned",
                    "num_cat",
                    "net_sales_sameprd",
                    "disc_p_sameprd",
                    "num_txn_sameprd",
                    "avg_unt_rtl_sameprd",
                    "unt_per_txn_sameprd",
                    "on_sale_items_sameprd",
                    "season"
                 ]

# Define the function to calculate Concordance----------------------------------------------------------------
def OptimisedConc(responsevalues, fittedvalues):  # responsevalues and fittedvalues are one-dimensional arrays

    z = np.vstack((fittedvalues, responsevalues)).T

    zeroes = z[z[:, 1] == 0,]
    ones = z[z[:, 1] == 1,]

    concordant = 0
    discordant = 0
    ties = 0
    totalpairs = len(zeroes) * len(ones)

    for k in list(range(0, len(ones))):
        diffx = np.repeat(ones[k, 0], len(zeroes)) - zeroes[:, 0]
        concordant = concordant + np.sum(np.sign(diffx) == 1)
        discordant = discordant + np.sum(np.sign(diffx) == -1)
        ties = ties + np.sum(np.sign(diffx) == 0)

    concordance = concordant / totalpairs
    discordance = discordant / totalpairs
    percentties = 1 - concordance - discordance

    return [concordance, discordance, percentties]



# Define the function to calculate cross-validation scores for a keras MLP ------------------------------------
# Call the keras_crossvalidation only after compiling the deep learning network -------------------------------
def keras_crossvalidation(Xmatrix, Yvector, folds, seed, num_epochs, size_batch):
    skf = StratifiedKFold(n_splits=folds, shuffle=True, random_state=seed)
    X = Xmatrix
    Y = Yvector
    
    cvmatrix = np.zeros((folds, 3))
    split = 0
    for train_index, test_index in skf.split(X, Y):
       
       X_train, X_test = X[train_index], X[test_index]
       Y_train, Y_test = Y[train_index], Y[test_index]
       
       classifier.fit(X_train, to_categorical(Y_train), nb_epoch=500, batch_size=100, 
                      verbose=0)
       probs_test = classifier.predict_proba(X_test, verbose=0)
                
       # Compute ROC curve and area the curve
       fpr, tpr, thresholds = roc_curve(Y_test, probs_test[:, 1]) 
       roc_auc_cv = auc(fpr, tpr)
        
       concordance_cv = OptimisedConc(responsevalues=Y_test, fittedvalues=probs_test[:, 1])[0]
       
       cvmatrix[split, 0] = split
       cvmatrix[split, 1] = roc_auc_cv
       cvmatrix[split, 2] = concordance_cv
       
       print("Split:", split+1, "AUC:", roc_auc_cv, "Concordance:", concordance_cv)
       
       split = split + 1 
       
    return(np.asarray(cvmatrix))
    



colnames = list(at_data.columns.values)

at_data = pd.DataFrame.as_matrix(at_data, columns=colnames)

at_data_ones = np.array(at_data[at_data[:, 1] == 1])

at_data_zeroes = at_data[at_data[:, 1] == 0]


size_list = list(range(5000, 21000, 1000));
testing_size = 20000;
validation_size = 20000;

prop = 0.5;
plot_roc = 0;
hidden_layers = (12, 9, 6);

epochs = 1000
batch = 100
activationfunction = "tanh"

with open('//10.8.8.51/lv0/Tanumoy/Datasets/Model Results/Catalog Response Model/at_catalog_training_tfl_' + str(
        min(size_list)) + '_' + str(max(size_list)) + '_v4.txt', 'w',
          newline='') as fp:
    a = csv.writer(fp, delimiter='|')
    data = [['SampleSize', 'Layers', 'TrainingRun', 'TrainingAccuracy', 'TrainingTPR', 'TrainingTNR', 
             'TrainingAUC', 'valF1Score', 'TrainingConcordance', 'TrainingPrecision', 'TrainingF1Score']]
    a.writerows(data)

with open('//10.8.8.51/lv0/Tanumoy/Datasets/Model Results/Catalog Response Model/at_catalog_testing_tfl_' + str(
        min(size_list)) + '_' + str(max(size_list)) + '_v4.txt', 'w',
          newline='') as fp:
    a = csv.writer(fp, delimiter='|')
    data = [['SampleSize', 'Layers', 'TrainingRun', 'TestingRun', 'TestingAUC', 'TestingConcordance']]
    a.writerows(data)



for i in list(range(0, len(size_list))):

    size = size_list[i];

    for run in list(range(0, 10)):

        at_data_ones = shuffle(at_data_ones)
        at_data_zeroes = shuffle(at_data_zeroes)
        
        at_data_train = np.vstack([at_data_ones[0:int(size * prop)], at_data_zeroes[0:int(size * (1 - prop))]])
        at_data_test = np.vstack([at_data_ones[int(size * prop):], at_data_zeroes[int(size * (1 - prop)):]])
        
        at_data_train = np.delete(at_data_train, 0, 1)
        at_data_test = np.delete(at_data_test, 0, 1)
        
        at_data_train = np.delete(at_data_train, np.shape(at_data_train)[1] - 1, 1)
        at_data_test = np.delete(at_data_test, np.shape(at_data_test)[1] - 1, 1)
        
        at_data_train = np.array(at_data_train, dtype=float)
        at_data_test = np.array(at_data_test, dtype=float)

        classifier = Sequential()

        classifier.add(Dense(output_dim=12, input_dim=15,
                             W_regularizer=l2(0.01), 
                             activity_regularizer=activity_l2(0.01)))
        classifier.add(Activation(activationfunction))
        classifier.add(Dense(output_dim=9,
                             W_regularizer=l2(0.01), 
                             activity_regularizer=activity_l2(0.01)))
        classifier.add(Activation(activationfunction))
        classifier.add(Dense(output_dim=6,
                             W_regularizer=l2(0.01), 
                             activity_regularizer=activity_l2(0.01)))
        classifier.add(Activation(activationfunction))
        classifier.add(Dense(output_dim=2))
        classifier.add(Activation("softmax"))
        
        modelfolderpath = "//10.8.8.51/lv0/Tanumoy/Datasets/Parameter Files/AT Catalog Response/TFL/v4/"
        modelfilepathjson = modelfolderpath + str(int(size)) + "_" + activationfunction + "_" + str(int(run+1)) + ".json"
        modelfilepathh5 = modelfolderpath + str(int(size)) + "_" + activationfunction + "_" + str(int(run+1)) + ".h5"
        
        checkpoint = ModelCheckpoint(modelfilepathh5, monitor='val_loss', verbose=1, 
                                     save_best_only=True, mode='auto', 
                                     save_weights_only=True)
        callbacks_list = [checkpoint]

        classifier.compile(loss='binary_crossentropy', metrics=['fmeasure'],
                           optimizer=SGD(lr=0.01, momentum=0.9, nesterov=True))
        
        at_data_val = (shuffle(at_data_test))[0:validation_size, :]
        X_val = at_data_val[: , 1:np.shape(at_data_val)[1]]
        Y_val = at_data_val[: , 0]
        
        print('SampleSize', 'Layers', 'Run', 'Prior', 'TrainingAccuracy', 'TPR', 'TNR', 'AUC',
              'valF1Score', 'Concordance', 'Precision', 'F1Score')

        X_train = at_data_train[: , 1:np.shape(at_data_train)[1]]

        Y_train = at_data_train[: , 0]
        
        classifier_fit = classifier.fit(X_train, to_categorical(Y_train), nb_epoch=epochs, 
                                        callbacks=callbacks_list, batch_size=batch, verbose=1,
                                        validation_data=(X_val, to_categorical(Y_val)))
        
        # serialize model to JSON
        classifier_json = classifier.to_json()
        with open(modelfilepathjson, "w") as json_file:
             json_file.write(classifier_json)
        
             
        # load json and create model
        json_file = open(modelfilepathjson, 'r')
        classifier_json = json_file.read()
        json_file.close()
        classifier = model_from_json(classifier_json)
        # load weights into new model
        classifier.load_weights(modelfilepathh5)
        print("Loaded model from disk")
        
        classifier.evaluate(X_val, to_categorical(Y_val), batch_size=batch,
                            sample_weight = classifier.get_weights())
        
        del(classifier)
        
        # load json and create model
        json_file = open(modelfilepathjson, 'r')
        classifier_json = json_file.read()
        json_file.close()
        classifier = model_from_json(classifier_json)
        # load weights into new model
        classifier.load_weights(modelfilepathh5)
        print("Loaded model from disk")

        classifier.evaluate(X_val, to_categorical(Y_val), batch_size=batch)
        
        # Compute ROC curve and area the curve
        probs_train = classifier.predict_proba(X_train, verbose=0)
        
        fpr_train, tpr_train, thresholds_train = roc_curve(at_data_train[:,0], probs_train[:, 1])
        
        roc_grid = np.c_[thresholds_train, tpr_train, fpr_train]
        
        roc_auc_train = auc(fpr_train, tpr_train)
        
        concordance_train = OptimisedConc(responsevalues=at_data_train[:, 0], fittedvalues=probs_train[:, 1])[0]
        
        # Compute TPR, TNR, Accuracy, Precision and F1 Score
        
        classes = classifier.predict_classes(X_train, verbose=0)
        
        cfmatrix = np.array(pd.crosstab(at_data_train[:,0], classes), dtype=float)
        
        tpr_train         = cfmatrix[1,1] / np.sum(cfmatrix[1,:])
        
        tnr_train         = cfmatrix[0,0] / np.sum(cfmatrix[0,:])
        
        TrainingAccuracy  = (cfmatrix[0,0] + cfmatrix[1,1]) / np.sum(cfmatrix)
        
        precision_train   = np.sum((classes + at_data_train[:, 0]) == 2)/np.sum(classes == 1)
        
        f1score_train     = 2 * precision_train * tpr_train / (precision_train + tpr_train) 
        
        print('SampleSize', 'Layers', 'Activation', 'Run', 'Prior', 'TrainingAccuracy', 'TPR', 'TNR', 'cvAUC',
              'cvConcordance', 'AUC', 'Concordance', 'Precision', 'F1Score')
        print(size, hidden_layers, activationfunction, run+1, prop, TrainingAccuracy, tpr_train, tnr_train, 
              valf1score, roc_auc_train, concordance_train, precision_train, f1score_train)

        with open('//10.8.8.51/lv0/Tanumoy/Datasets/Model Results/Catalog Response Model/at_catalog_training_tfl_' + str(min(size_list)) + '_' + str(max(size_list)) + '_v4.txt', 'a', newline='') as fp:
                a = csv.writer(fp, delimiter='|')
                data = [[size, hidden_layers, activationfunction, run+1, TrainingAccuracy, tpr_train, tnr_train,
                         valf1score, roc_auc_train, concordance_train, precision_train, f1score_train]]
                a.writerows(data)
                
        modelfilepathjson = modelfolderpath + str(int(size)) + "_" + activationfunction + "_" + str(int(run+1)) + ".json"
        
        
        for k in list(range(0,10)):
            
            at_data_test = shuffle(at_data_test)
            probs_test = classifier.predict_proba(at_data_test[0:testing_size, 1:(at_data_train.shape[1])], verbose=0)
            
            # Compute ROC curve and area the curve
            fpr, tpr, thresholds = roc_curve(at_data_test[0:testing_size, 0], probs_test[:, 1])
            roc_grid = np.c_[thresholds, tpr, fpr]
            
            roc_auc_test = auc(fpr, tpr)
            
            pred_test = classifier.predict(at_data_test[0:testing_size, 1:(at_data_train.shape[1])])
    
            concordance_test = OptimisedConc(responsevalues=at_data_test[0:testing_size, 0], fittedvalues=probs_test[:, 1])[0]
            
            print("Size:", size, "Layers", hidden_layers, " Activation: ", activationfunction, 
                  " Training Run: ", run+1, " Testing Run:", k+1, "  AUC : %f" % roc_auc_test,
                  " Concordance:", concordance_test)
            
            with open('//10.8.8.51/lv0/Tanumoy/Datasets/Model Results/Catalog Response Model/at_catalog_testing_tfl_' + str(min(size_list)) + '_' + str(max(size_list)) + '_v4.txt', 'a', newline='') as fp:
                a = csv.writer(fp, delimiter='|')
                data = [[size, hidden_layers, activationfunction, run+1, k+1, roc_auc_test, concordance_test]]
                a.writerows(data)