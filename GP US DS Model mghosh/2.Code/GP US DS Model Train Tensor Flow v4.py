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
import random as random

def savemodel(dirpath):
    if not os.path.exists(dirpath):
        os.makedirs(dirpath)
    else:
        shutil.rmtree(dirpath)  # removes all the subdirectories!
        os.makedirs(dirpath)

filename = "//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/1. data/GP_full_data_base.txt"
n = sum(1 for line in open(filename)) - 1 #number of records in file (excludes header)
s = int(0.2*n) #desired sample size
skip = sorted(random.sample(range(1,n+1),n-s)) #the 0-indexed header will not be included in the skip list
gp_data = pd.read_csv(filename, skiprows=skip,sep='|', header=0)


gp_data = gp_data[['customer_key','Response','percent_disc_last_12_mth','percent_disc_last_6_mth','per_elec_comm',
                                           'gp_hit_ind_tot','num_units_12mth','num_em_campaign','disc_ats','avg_order_amt_last_6_mth','Time_Since_last_disc_purchase',
                                           'non_disc_ats','gp_on_net_sales_ratio','on_sales_rev_ratio_12mth','mobile_ind_tot','ratio_order_6_12_mth',
                                           'pct_off_hit_ind_tot','ratio_rev_wo_rewd_12mth','ratio_disc_non_disc_ats','card_status','br_hit_ind_tot',
                                           'gp_br_sales_ratio','ratio_order_units_6_12_mth','num_disc_comm_responded','purchased','ratio_rev_rewd_12mth',
                                           'num_dist_catg_purchased','num_order_num_last_6_mth','at_hit_ind_tot','gp_go_net_sales_ratio','clearance_hit_ind_tot',
                                           'searchdex_ind_tot','total_plcc_cards','factory_hit_ind_tot','gp_bf_net_sales_ratio','markdown_hit_ind_tot' ]]

                                           
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
    



colnames = list(gp_data.columns.values)

gp_data = pd.DataFrame.as_matrix(gp_data, columns=colnames)

gp_data_ones = np.array(gp_data[gp_data[:, 1] == 1])
gp_data_ones = np.delete(gp_data_ones,0,1)
gp_data_zeroes = gp_data[gp_data[:, 1] == 0]
gp_data_zeroes= np.delete(gp_data_zeroes,0,1)

size_list = list(range(5000, 21000, 1000));
testing_size = 20000;
validation_size = 20000;

prop = 0.5;
plot_roc = 0;
hidden_layers = (35 , 22, 14 , 9) 
epochs = 1000
batch = 100
activationfunction = "tanh"

with open('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/gp_ds_training_tfl_' + str(
        min(size_list)) + '_' + str(max(size_list)) + '_v4.txt', 'w',
          newline='') as fp:
    a = csv.writer(fp, delimiter='|')
    data = [['SampleSize', 'Layers', 'TrainingRun', 'TrainingAccuracy', 'TrainingTPR', 'TrainingTNR', 
             'TrainingAUC', 'valF1Score', 'TrainingConcordance', 'TrainingPrecision', 'TrainingF1Score']]
    a.writerows(data)

with open('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/gp_ds_testing_tfl_' + str(
        min(size_list)) + '_' + str(max(size_list)) + '_v4.txt', 'w',
          newline='') as fp:
    a = csv.writer(fp, delimiter='|')
    data = [['SampleSize', 'Layers', 'TrainingRun', 'TestingRun', 'TestingAUC', 'TestingConcordance']]
    a.writerows(data)



for i in list(range(0, len(size_list))):

    size = size_list[i];

    for run in list(range(0, 10)):

        gp_data_ones = shuffle(gp_data_ones)
        gp_data_zeroes = shuffle(gp_data_zeroes)
        
        gp_data_train = np.vstack([gp_data_ones[0:int(size * prop)], gp_data_zeroes[0:int(size * (1 - prop))]])
        gp_data_test = np.vstack([gp_data_ones[int(size * prop):], gp_data_zeroes[int(size * (1 - prop)):]])
                

        classifier = Sequential()

        classifier.add(Dense(output_dim=22, input_dim=34,
                             W_regularizer=l2(0.01), 
                             activity_regularizer=activity_l2(0.01)))
        classifier.add(Activation(activationfunction))
        classifier.add(Dense(output_dim=14,
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
        
        modelfolderpath = "//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/Model_Objects/tfl"
        modelfilepathjson = modelfolderpath + str(int(size)) + "_" + activationfunction + "_" + str(int(run+1)) + ".json"
        modelfilepathh5 = modelfolderpath + str(int(size)) + "_" + activationfunction + "_" + str(int(run+1)) + ".h5"
        
        checkpoint = ModelCheckpoint(modelfilepathh5, monitor='val_loss', verbose=1, 
                                     save_best_only=True, mode='auto', 
                                     save_weights_only=True)
        for k in list(range(0,10)):
            
            gp_data_test = shuffle(gp_data_test)
            probs_test = classifier.predict_proba(gp_data_test[0:testing_size, 1:(gp_data_train.shape[1])], verbose=0)
        
            # Compute ROC curve and area the curve
            fpr, tpr, thresholds = roc_curve(gp_data_test[0:testing_size, 0], probs_test[:, 1])
            roc_grid = np.c_[thresholds, tpr, fpr]
            
            roc_auc_test = auc(fpr, tpr)
            
            pred_test = classifier.predict(gp_data_test[0:testing_size, 1:(gp_data_train.shape[1])])
    
            concordance_test = OptimisedConc(responsevalues=gp_data_test[0:testing_size, 0], fittedvalues=probs_test[:, 1])[0]
            
            print("Size:", size, "Layers", hidden_layers, " Activation: ", activationfunction, 
                  " Training Run: ", run+1, " Testing Run:", k+1, "  AUC : %f" % roc_auc_test,
                  " Concordance:", concordance_test)
            
            with open('//10.8.8.51/lv0/Move to Box/Mithun/projects/7. DS_New Training/3.Documents/gp_ds_testing_tfl_' + str(min(size_list)) + '_' + str(max(size_list)) + '_v4.txt', 'a', newline='') as fp:
                a = csv.writer(fp, delimiter='|')
                data = [[size, hidden_layers, activationfunction, run+1, k+1, roc_auc_test, concordance_test]]
                a.writerows(data)