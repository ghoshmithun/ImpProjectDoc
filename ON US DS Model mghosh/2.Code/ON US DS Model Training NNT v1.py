import pylab as pl
import pandas as pd
import csv as csv
import sklearn as sklearn
import random


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


def savemodel (dirpath):
    if not os.path.exists(dirpath):
        os.makedirs(dirpath)
    else:
        shutil.rmtree(dirpath)           #removes all the subdirectories!
        os.makedirs(dirpath)

filename = "//10.8.8.51/lv0/Move to Box/Mithun/projects/8.ON_US_DS_Project/1.data/on_full_data_base.txt"
n = sum(1 for line in open(filename)) - 1 #number of records in file (excludes header)
s = int(0.15*n) #desired sample size
skip = sorted(random.sample(xrange(1,n+1),n-s)) #the 0-indexed header will not be included in the skip list
on_data = pd.read_csv(filename, skiprows=skip,sep='|', header=0)


on_data = on_data[['customer_key','Response','Time_since_acquisition',
			'percent_disc_last_6_mth','non_disc_ats','ratio_rev_rewd_12mth','per_elec_comm','avg_order_amt_last_6_mth','Time_Since_last_Retail_purchase',
			'num_em_campaign','disc_ats','num_units_6mth','ratio_order_6_12_mth','Time_Since_last_Onl_purchase','ratio_disc_non_disc_ats',
			'num_disc_comm_responded','on_sales_rev_ratio_12mth','ratio_rev_wo_rewd_12mth','on_sales_item_rev_12mth','time_since_last_disc_purchase',
			'card_status','mobile_ind_tot','pct_off_hit_ind_tot','num_dist_catg_purchased','ratio_order_units_6_12_mth','gp_hit_ind_tot','on_gp_net_sales_ratio','clearance_hit_ind_tot','num_order_num_last_6_mth','purchased','br_hit_ind_tot','on_go_net_sales_ratio','on_br_sales_ratio','total_plcc_cards','at_hit_ind_tot','on_bf_net_sales_ratio','searchdex_ind_tot','factory_hit_ind_tot','markdown_hit_ind_tot' ]]

def OptimisedConc(responsevalues,fittedvalues): #responsevalues and fittedvalues are one-dimensional arrays
    
  z = np.vstack((fittedvalues,responsevalues)).T
  
  zeroes = z[z[:,1]==0, ]
  ones   = z[z[:,1]==1, ]

  concordant = 0
  discordant = 0
  ties = 0
  totalpairs = len(zeroes) * len(ones)
  
  
  for k in list(range(0, len(ones))):    
    diffx = np.repeat(ones[k,0], len(zeroes)) - zeroes[:,0]
    concordant = concordant + np.sum(np.sign(diffx) ==  1)
    discordant = discordant + np.sum(np.sign(diffx) == -1)
    ties       = ties       + np.sum(np.sign(diffx) ==  0)
    
  concordance = concordant/totalpairs
  discordance = discordant/totalpairs
  percentties = 1-concordance-discordance
  
  return [concordance, discordance, percentties]
  
  
  
  
colnames = list(on_data.columns.values)

on_data = pd.DataFrame.as_matrix(on_data, columns=colnames)

on_data_ones = on_data[on_data[:,1] == 1]
                       
on_data_zeroes = on_data[on_data[:,1] == 0]

size_list = list(range(5000,31000,1000)); testing_size=20000;

with open('//10.8.8.51/lv0/Move to Box/Mithun/projects/8.ON_US_DS_Project/3.Documents/model/on_ds_training_NNT_v1_' + str(min(size_list)) + '_' + str(max(size_list)) + '_v3.txt', 'w', 
          newline='') as fp:
    a = csv.writer(fp, delimiter='|')
    data = [['SampleSize', 'Layers', 'TrainingRun', 'TrainingAccuracy', 'cvAUC', 'TrainingTPR', 'TrainingTNR', 
             'TrainingAUC', 'TrainingConcordance', 'TrainingPrecision', 'TrainingF1Score' ]]
    a.writerows(data)
    
    

with open('//10.8.8.51/lv0/Move to Box/Mithun/projects/8.ON_US_DS_Project/3.Documents/model/on_ds_testing_NNT_v1_' + str(min(size_list)) + '_' + str(max(size_list)) + '_v3.txt', 'w', 
          newline='') as fp:
    a = csv.writer(fp, delimiter='|')
    data = [['SampleSize', 'Layers', 'TrainingRun', 'TestingRun', 'TestingAUC', 'TestingConcordance']]
    a.writerows(data)


prop = 0.5; plot_roc=0; hidden_layers = (30,22,14,9,6);

for i in list(range(0,len(size_list))):
    
    size = size_list[i];     
    
    print('SampleSize', 'Layers', 'Run', 'Prior', 'TrainingAccuracy', 'cvAUC', 'TPR', 'TNR', 'AUC',
          'Concordance', 'Precision', 'F1Score')
                
    for run in list(range(0,20)):
            
        on_data_ones = shuffle(on_data_ones)
        on_data_zeroes = shuffle(on_data_zeroes)
        
        
        on_data_train = np.vstack([on_data_ones[0:int(size * prop)], on_data_zeroes[0:int(size * (1-prop))]])
        on_data_test  = np.vstack([on_data_ones[int(size * prop):], on_data_zeroes[int(size * (1-prop)):]])
        
        on_data_train = np.delete(on_data_train, 0, 1)
        on_data_test  = np.delete(on_data_test,  0, 1)
        
        
        classifier = MLPClassifier(solver='lbfgs', alpha=1e-5, 
                                   hidden_layer_sizes=hidden_layers, random_state=1)
        
        cvscores = cross_val_score(classifier, on_data_train[:,1:on_data_train.shape[1]],
                                   on_data_train[:,0], cv=10, scoring='roc_auc')

        classifier_fit = classifier.fit(on_data_train[:,1:on_data_train.shape[1]], on_data_train[:,0])
        
        probs_train = classifier.predict_proba(on_data_train[0:size, 1:len(on_data_train)])
        
        # Compute ROC curve and area the curve
        fpr_train, tpr_train, thresholds_train = roc_curve(on_data_train[:, 0], probs_train[:, 1])
        roc_grid = np.c_[thresholds_train, tpr_train, fpr_train]
        
        roc_auc_train = auc(fpr_train, tpr_train)
        
        pred_train = classifier.predict(on_data_train[0:size, 1:on_data_train.shape[1]])
    
        TrainingAccuracy  = np.sum(pred_train == on_data_train[:, 0]) / size
         
        tpr_train         = np.sum((pred_train + on_data_train[:, 0]) == 2)/np.sum(on_data_train[:, 0] == 1)
    
        tnr_train         = np.sum((pred_train + on_data_train[:, 0]) == 0)/np.sum(on_data_train[:, 0] == 0)
        
        precision_train   = np.sum((pred_train + on_data_train[:, 0]) == 2)/np.sum(pred_train == 1)
        
        f1score_train     = 2 * precision_train * tpr_train / (precision_train + tpr_train)                            
                                     
        concordance_train = OptimisedConc(responsevalues=on_data_train[:, 0], fittedvalues=probs_train[:, 1])[0]
                         
        print(size, hidden_layers, run+1, prop, TrainingAccuracy, np.mean(cvscores), tpr_train, tnr_train, 
              roc_auc_train, concordance_train, precision_train, f1score_train)
        
        with open('//10.8.8.51/lv0/Move to Box/Mithun/projects/8.ON_US_DS_Project/3.Documents/model/on_ds_training_NNT_v1_' + str(min(size_list)) + '_' + str(max(size_list)) + '_v3.txt', 'a', newline='') as fp:
                a = csv.writer(fp, delimiter='|')
                data = [[size, hidden_layers, run+1, TrainingAccuracy, np.mean(cvscores),tpr_train,tnr_train,
                         roc_auc_train, concordance_train, precision_train, f1score_train]]
                a.writerows(data)
                
        modelfolderpath = "//10.8.8.51/lv0/Move to Box/Mithun/projects/8.ON_US_DS_Project/Model_Objects/NNT/"
        modelfilepath = modelfolderpath + str(int(size)) + "_" + str(int(run+1)) + ".pkl"
        joblib.dump(classifier, modelfilepath)

            
        for k in list(range(0,20)):
            
            on_data_test = shuffle(on_data_test)
            probs_test = classifier.predict_proba(on_data_test[0:testing_size, 1:on_data_train.shape[1]])
            
            # Compute ROC curve and area the curve
            fpr, tpr, thresholds = roc_curve(on_data_test[0:testing_size, 0], probs_test[:, 1])
            roc_grid = np.c_[thresholds, tpr, fpr]
            
            roc_auc_test = auc(fpr, tpr)
            
            pred_test = classifier.predict(on_data_test[0:testing_size, 1:on_data_train.shape[1]])
    
            TestingAccuracy  = np.sum(pred_test == on_data_test[0:testing_size, 0]) / testing_size
            
            tpr_test         = np.sum((pred_test + on_data_test[0:testing_size, 0]) == 2)/np.sum(on_data_test[0:testing_size, 0] == 1)
    
            tnr_test         = np.sum((pred_test + on_data_test[0:testing_size, 0]) == 0)/np.sum(on_data_test[0:testing_size, 0] == 0)
            
            concordance_test = OptimisedConc(responsevalues=on_data_test[0:testing_size, 0], fittedvalues=probs_test[:, 1])[0]
            
            print("Size:", size, "Layers", hidden_layers, " Training Run: ", run+1, 
                  " Testing Run:", k+1, "  AUC : %f" % roc_auc_test,
                  " Concordance:", concordance_test)
            
            with open('//10.8.8.51/lv0/Move to Box/Mithun/projects/8.ON_US_DS_Project/3.Documents/model/on_ds_testing_NNT_v1_' + str(min(size_list)) + '_' + str(max(size_list)) + '_v3.txt', 'a', newline='') as fp:
                a = csv.writer(fp, delimiter='|')
                data = [[size, hidden_layers, run+1, k+1, roc_auc_test, concordance_test]]
                a.writerows(data)