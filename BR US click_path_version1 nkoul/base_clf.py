import sys
import datetime
from collections import defaultdict
import random

import numpy as np
import scipy as sp
from scipy.sparse import csr_matrix
from scipy.sparse import vstack

import networkx as nwx

import sklearn as sk
import sklearn.feature_extraction.text as txt
import sklearn.decomposition as dc
import sklearn.feature_selection as fs
from sklearn.feature_selection import chi2
from sklearn.utils import shuffle as sk_shuffle

import sklearn.linear_model as lm
import sklearn.svm as svm
import sklearn.ensemble as ensmbl
import sklearn.cluster as clstr

import sklearn.grid_search as gs
import sklearn.cross_validation as cv
import sklearn.metrics as metrics
import sklearn.pipeline as pipeline

# Parse to extract times as well as reconstruct the clickstream
def pipeline1(all_dat):
	# Create tuples of time,prop33 & pagenames
	dat_parse = []
	for item in all_dat:
		striped_dat = str(item).replace('[','').replace(']','')
		csv_sess = striped_dat.split('\x02')
	for item in csv_sess:
		spl_line = item.split('__')
		if len(spl_line) == 3:
			dat_parse.append((spl_line[2],spl_line[0],spl_line[1]))
		else:
			dat_parse.append(('','',''))
			print spl_line 
	# Replace the raw timestamps by datetime objects
	for index,item in enumerate(dat_parse):
		tmstmp = item[0].split(' ')
		if len(tmstmp) < 2:
			dat_parse[index] = (datetime.datetime.strptime('0:0:0','%H:%M:%S'),item[1],item[2])
		else:
			dat_parse[index] = (datetime.datetime.strptime(tmstmp[1],'%H:%M:%S'),item[1],item[2])
	# Sort the clickstream as per time
	sorted_dat_parse = sorted(dat_parse, key=lambda x:x[0])
	# Get the time on various pages
	ptimes = []
	for it1,it2 in zip(sorted_dat_parse,sorted_dat_parse[1:]):
		ptimes.append((it2[0]-it1[0]).seconds)
	if len(ptimes) !=0:
		mean_time = sum(ptimes)/len(ptimes)
	else:
		mean_time = 0
	ptimes.append(mean_time)
	# Replace timestamp by time on page
	for index,item in enumerate(zip(ptimes,sorted_dat_parse)):
		sorted_dat_parse[index] = (str(item[0]),item[1][1],item[1][2])
	# Format for output
	flat_list = []
	for item in sorted_dat_parse:
		flat_list = flat_list + [it1 for it1 in item]
	return sorted_dat_parse

# Return separate lists of pagenames and pagetimes
# Clean the pagenames,prop33 values by removing spaces 
def proc_split_raw_dat(filename):
	raw_dat_x_p = [];  raw_dat_x_t = []; raw_dat_y = []
	fh = open(filename,'r')
	for line in fh:
		str_line = line.rstrip()
		spl_line_l1 = str_line.split('\x01')
		try:
			rv = pipeline1([spl_line_l1[3]])
			rv1 = [item[0] for item in rv]
			rv2 = []
			for item in rv:
				if item[1] != '':
					rm_sp = item[1].replace(' ','_')
					rv2.append(rm_sp)
					#rv2.append(item[1])
				else:
					#sp_nm = item[2].split(':')
					#sp_nm_c = sp_nm.replace(' ','_')
					#rv2.append(sp_nm[-1])
					rm_sp2 = item[2].replace(' ','_')
					rv2.append(rm_sp2)
					#rv2.append(item[2])
			raw_dat_x_p.append(rv2); raw_dat_x_t.append(rv1)
			raw_dat_y.append(spl_line_l1[4])
		except:
			print 'Ecxeption handled'
	fh.close()
	return raw_dat_x_p,raw_dat_x_t,raw_dat_y

# Read the data for retraining picked from the specified clusters.
def load_rt_data(filename):
	dat_x = []; dat_y = []
	fh = open(filename,'r')
	for line in fh:
		sln = line.rstrip().split(',')
		dat_x.append(sln[0]); dat_y.append(sln[1])
	fh.close()
	return dat_x,dat_y

def to_lof_strings_alt(dat):
	los = []
	for item in dat:
		los.append(' '.join(item))
	return los

def to_lof_strings(dat):
	los = []
	for item in dat:
		ftr_item = [it2 for it2 in item if (it2!='orderConfirm'and it2!='shipping') and it2!='billing'\
		and it2!='gifting' and it2!='orderPlacement' and it2 !='order_gateway_sign_in' and it2!=\
		'order_status_detail' and it2!='shipment_detail' and it2!='br:checkout:Checkout' and it2.lower()\
		!='completeregistration' and it2.lower() !='address_book' and it2.lower() !='br:checkout:checkout:module:shipping'\
		and it2.lower()!='single_order_detail' and it2.lower() !='br:profile:order_status_shipment_detail' and it2.lower() !='rewards']
		#los.append(' '.join(item))
		los.append(' '.join(ftr_item))
	return los

def filter_sents_weights(sent,sent_wt):
	f_sent=[]; f_sent_wt=[]
	for word,wt in zip(f_sent,f_sent_wt):
		if (word!='orderConfirm' and word!='shipping' and word!='billing' and word!='gifting' and \
		word!='orderPlacement' and word!='order_gateway_sign_in' and word!='order_status_detail' and \
		word !='shipment_detail' and word !='br:checkout:checkout' and word.lower()!='completeregistration' and \
		word.lower() !='address_book' and word.lower() != 'br:checkout:checkout:module:shipping'and word.lower()!=\
		'single_order_detail' and word.lower() !='br:profile:order_status_shipment_detail' and word.lower() !='rewards'):
			f_sent.append(word); f_sent_wt.append(wt)
	return f_sent,f_sent_wt

def basic_trans_dat(dat_xp,ftr_fg=0):
	def cust_token(sent):
		return sent.split(' ')
	vctrz = txt.CountVectorizer(ngram_range=(1,3),tokenizer=cust_token)
	#vctrz = txt.TfidfVectorizer(ngram_range=(1,5),tokenizer=cust_token)
	if ftr_fg == 0:
		dat_sent_form = to_lof_strings(dat_xp)
	elif ftr_fg == 1:
		dat_sent_form = to_lof_strings_alt(dat_xp)
	#vctrz.fit(dat_sent_form[0:int(len(dat_sent_form)/2)])
	vctrz.fit(dat_sent_form)
	return vctrz

# sent,sent_wgts - pagenames,time of page; min_v, max_v - min and max range of ngram
def generate_wgtd_ng(sent,sent_wgts,min_v,max_v):
	ng = defaultdict(list)
	for ix in range(0,(max_v-min_v)+1):
		for ix2 in range(0,len(sent)):
			if len(sent[ix2:ix2+ix+1]) == ix+1:ng[' '.join(sent[ix2:ix2+ix+1])].append(sent_wgts[ix2:ix2+ix+1])
	return ng

def sent_weight_modification(sent,sent_wgts,min_v,max_v,spr_sent,vctr):
	def compose_weights(ll_wgts):
		sum_all = 0
		for item in ll_wgts:
			sum_all += sum([int(wgt) for wgt in item])
		return c_wgt
	fsent,fsent_wgts= filter_sents_weights(sent,sent_wgts)
	ng_rep = generate_wgtd_ng(fsent,fsent_wgts,min_v,max_v)
	arr_spr_sent = spr_sent.toarray()
	for key,value in ng_rep.iteritems():
		try:
			ridx = vctr.vocabulary_[key.lower().decode()]
			arr_spr_sent[0][ridx] = compose_weights(value)
		except:
			pass
	mod_spr_sent = csr_matrix(arr_spr_sent)
	return mod_spr_sent

def modify_ngram_weights(sents,sents_wgts,min_v,max_v,spr_sents,vctr):
	mod_vec = sent_weight_modification(sents[0],sents_wgts[0],min_v,max_v,spr_sents[0],vctr) 
	iter_tracker = 0;count = 0
	for it1,it2,it3 in zip(sents[1:],sents_wgts[1:],spr_sents[1:]):
		rv = sent_weight_modification(it1,it2,min_v,max_v,it3,vctr)
		mod_vec = vstack([mod_vec,rv])
		if count == 10000:
			print iter_tracker; count = 0
		iter_tracker +=1; count +=1
	return mod_vec

def modify_ngram_weights_alt(sents,sents_wgts,min_v,max_v,spr_sents,vctr):
	mod_vec = []
	iter_tracker = 0; count =0
	for it1,it2,it3 in zip(sents,sents_wgts,spr_sents):
		mod_vec.append(sent_weight_modification(it1,it2,min_v,max_v,it3,vctr))
		if count == 10000:
			print iter_tracker; count = 0
		iter_tracker +=1; count +=1
	mod_vec_np = np.asarray(mod_vec)
	return csr_matrix(mod_vec_np)

def transform_labels(labels):
	trans_labels = []
	for label in labels:
		if label != '':trans_labels.append('P')
		#if label != '':trans_labels.append(1)
		else:trans_labels.append('NP')
		#else:trans_labels.append(0)
	return trans_labels

# Retain items with no bag interaction, Remove items pertaining to customer services, short sessions and bounces
def filter_dataset_1(tr_dat_xp,tr_dat_xt,tr_dat_y):
	f_tr_dat_xp = []; f_tr_dat_xt = []; f_tr_dat_y = []
	for it1,it2,it3 in zip(tr_dat_xp,tr_dat_xt,tr_dat_y):
		if ('inlineBagAdd' not in it1 and 'shopping_bag' not in it1 and 'br:buy:Shopping_Bag' not in it1\
		and 'br: Store WiFi splash page with email entry' not in it1\
		and 'br:customerService:home' not in it1 and len(it1)> 2):
			f_tr_dat_xp.append(it1);f_tr_dat_xt.append(it2);f_tr_dat_y.append(it3)
	return f_tr_dat_xp, f_tr_dat_xt, f_tr_dat_y

# Retain items with bag interaction, Remove items pertaining to customer services, short sessions and bounces
def filter_dataset_2(tr_dat_xp,tr_dat_xt,tr_dat_y):
	f_tr_dat_xp = []; f_tr_dat_xt = []; f_tr_dat_y = []
	for it1,it2,it3 in zip(tr_dat_xp,tr_dat_xt,tr_dat_y):
		if ('inlineBagAdd' in it1 or 'shopping_bag' in it1 or 'br:buy:Shopping_Bag' in it1\
		and 'br: Store WiFi splash page with email entry' not in it1\
		and 'br:customerService:home' not in it1 and len(it1)> 2):
			f_tr_dat_xp.append(it1);f_tr_dat_xt.append(it2);f_tr_dat_y.append(it3)
	return f_tr_dat_xp, f_tr_dat_xt, f_tr_dat_y

# Lengths distribution associated with purchases
def purchase_len_dist(p_tr_dat_x,p_tr_dat_y):
	p_len_dist = [len(it1) for it1,it2 in zip(p_tr_dat_x,p_tr_dat_y) if it2 != '']
	len_dist = {}
	for item in p_len_dist:
		len_dist[item] = 0
	for item in p_len_dist:
		len_dist[item] += 1
	fh = open('len_dist.txt','w')
	for key,value in len_dist.items():
		fh.write(str(key)+'\t'+str(value)+'\n')
	fh.close()
	return len_dist

def purchase_no_purchase_ratio(p_tr_dat_x,p_tr_dat_y):
	p_len_dist = [len(it1) for it1,it2 in zip(p_tr_dat_x,p_tr_dat_y) if it2 != '']
	np_len_dist = [len(it1) for it1,it2 in zip(p_tr_dat_x,p_tr_dat_y) if it2 == '']
	len_dist = {}
	for item in p_len_dist:
		len_dist[item] = 0
	for item in p_len_dist:
		len_dist[item] += 1
	n_len_dist = {}
	for item in np_len_dist:
		n_len_dist[item] = 0
	for item in np_len_dist:
		n_len_dist[item] += 1
	len_rat = {}
	for item in p_len_dist:
		len_rat[item] = 0
	rkeys = len_dist.keys()
	for item in rkeys:
		try:
			len_rat[item]= (1.0*len_dist[item])/n_len_dist[item]
		except KeyError:
			pass
	fh = open('len_rat.txt','w')
	for key,value in len_rat.items():
		fh.write(str(key)+'\t'+str(value)+'\n')
	fh.close()
	return len_rat

# Get the feature names
def feature_names(ftr_sltr,vctr):
	def invert_dict(ip_dict):
		return dict([[v,k] for k,v in ip_dict.items()])
	ftr_names = []
	ftr_idx = ftr_sltr.get_support()
	inv_vocab = invert_dict(vctr.vocabulary_)
	for index,item in enumerate(ftr_idx):
		if item == True:ftr_names.append(inv_vocab[index])
	return ftr_names

# Get the sorted feature names
def sorted_feature_names(ftr_sltr,vctr):
	def invert_dict(ip_dict):
		return dict([[v,k] for k,v in ip_dict.items()])
	ftr_names = []
	inv_vocab = invert_dict(vctr.vocabulary_)
	ftr_pres = ftr_sltr.get_support()
	ftr_scores = ftr_sltr.scores_
	ftr_idx = [k for k,v in inv_vocab.items()]
	ls_tp = []
	for v1,v2,v3 in zip(ftr_scores,ftr_pres,ftr_idx):
		if np.isnan(v1):ls_tp.append((0,v2,v3))
		else:ls_tp.append((v1,v2,v3))
	srt_ls_tp = sorted(ls_tp,reverse=True)
	for index,item in enumerate(srt_ls_tp):
		if item[1] == True:ftr_names.append(inv_vocab[item[2]])
	return ftr_names

def save_top_features(ftr_names):
	fh = open('top_features.txt','w')
	for item in ftr_names:
		fh.write(item+'\n')
	fh.close()
	return None

# Cross Session Page Transition Count Matrix
def construct_transition_matrix(raw_dat,vctr=None,vocab=None):
	def prop33_pagename_set(raw_dat):
		p33_set = set()
		for item in raw_dat:
			for it2 in item:
				p33_set.add(it2)
		return list(p33_set)
	p33_set = prop33_pagename_set(raw_dat)
	p33_dict = {}; idx = 0
	for item in p33_set:
		p33_dict[item] = idx; idx +=1
	tr_mat = np.zeros((len(p33_set),len(p33_set)))
	for session in raw_dat:
		for pg1,pg2 in zip(session,session[1:]):
			idx1 = p33_dict[pg1]; idx2 = p33_dict[pg2]
			tr_mat[idx1][idx2] +=1
	return tr_mat

####--------Cluster Properties--------####
# Get a sample of data points belonging to a cluster
def get_cluster_dat(raw_dat,cpreds,cno=0,smp_rate=1.0):
	ftr_dat = []
	for it1,it2 in zip(cpreds,raw_dat):
		if it1 == cno and random.random()< smp_rate:
			ftr_dat.append(it2)
	return ftr_dat

# Cluster data points + purchase labels
def get_lbl_cluster_dat(raw_dat_x,raw_dat_y,cpreds,cno=0,smp_rate=1.0):
	ftr_dat_x = []; ftr_dat_y = []
	for it1,it2,it3 in zip(cpreds,raw_dat_x,raw_dat_y):
		if it1 == cno and random.random()< smp_rate:
			ftr_dat_x.append(it2)
			ftr_dat_y.append(it3)
	return ftr_dat_x,ftr_dat_y

# Percentage of purchasers in each cluster
def cluster_purchase_counts(raw_dat_x,raw_dat_y,cpreds,ncst,smp_rate=1.0):
	ctrs = [get_lbl_cluster_dat(raw_dat_x,raw_dat_y,cpreds,itr,smp_rate) for itr in range(0,ncst)]
	prch_pct = []
	for ctr in ctrs:
		count = 0.0
		for item,lbl in zip(ctr[0],ctr[1]):
			if lbl != '':count+=1
		prch_pct.append(count/len(ctr[1]))
	return prch_pct

# Cluster sizes and cluster element sizes
def clst_elem_sizes(raw_dat,cpreds,ncst,smp_rate=1.0):
	def mean_sz(clstr):
		mnsz=0.0
		szs =[len(sess) for sess in clstr]
		mnsz = sum(szs)/len(clstr)
		return mnsz
	ctrs =  [get_cluster_dat(raw_dat,cpreds,itr,smp_rate) for itr in range(0,ncst)]
	lngts = [len(item) for item in ctrs]
	mn_sz = [mean_sz(item) for item in ctrs]
	return lngts,mn_sz

# Cluster probability scores
def cluster_probability_scores(raw_dat,cpreds,cno,vctr,tr_pred,smp_rate=1.0):
	ctr_x = get_cluster_dat(raw_dat,cpreds,cno,smp_rate)
	trs_dat = vctr.transform(to_lof_strings(ctr_x))
	preds = tr_pred.predict_proba(trs_dat)
	return ctr_x,preds

# Cluster Predictions
def cluster_preds(raw_dat,cpreds,cno,vctr,tr_pred,smp_rate=1.0):
	ctr_x = get_cluster_dat(raw_dat,cpreds,cno,smp_rate)
	trs_dat= vctr.transform(to_lof_strings(ctr_x))
	preds = tr_pred.predict(trs_dat)
	return ctr_x,preds

# Get the miscalssified examples from each cluster
# clst_typ1 = 1/0 for P/NP cluster
def err_clst_pts(clst_type,cls_dat,cls_preds):
	rq_set_x = []; rq_set_y = []
	for item,pred in zip(cls_dat,cls_preds):
		if clst_type == 1:
			if pred == 'NP':
				rq_set_x.append(item)
				rq_set_y.append('P')
		else:
			if pred == 'P':
				rq_set_x.append(item)
				rq_set_y.append('NP')
	return rq_set_x, rq_set_y

# Write the identified cluster data for model retraining
def save_cluster_sample(sb_st1_x,sb_st1_y,sb_st2_x,sb_st2_y):
	sb_st1_x.extend(sb_st2_x); sb_st1_y.extend(sb_st2_y)
	shuffled_dat_x,shuffled_dat_y = sk_shuffle(sb_st1_x,sb_st1_y)
	fh = open('dat_f_retraining.txt','w')
	for item,label in zip(shuffled_dat_x,shuffled_dat_y):
		fh.write(str(' '.join(item))+','+str(label)+'\n')
	fh.close()
	return None

def divide_4_parts(data_x,data_y):
	p1_x=[];p1_y=[];p2_x=[];p2_y=[];p3_x=[];p3_y=[];p4_x=[];p4_y=[]
	for elem,lbl in zip(data_x,data_y):
		if (len(elem) > 5):
			p1_x.append(elem[0:int(len(elem)*0.20)]); p1_y.append(lbl)
			p2_x.append(elem[0:int(len(elem)*0.40)]); p2_y.append(lbl)
			p3_x.append(elem[0:int(len(elem)*0.60)]); p3_y.append(lbl)
			#p3_x.append(elem[int(len(elem)*0.75):int(len(elem)*1.0)]); p3_y.append(lbl)
			p4_x.append(elem[0:int(len(elem)*0.80)]); p4_y.append(lbl)
	return (p1_x,p1_y),(p2_x,p2_y),(p3_x,p3_y),(p4_x,p4_y)

################--------------------------------################

# Classifier Classes
clf1 = svm.LinearSVC(C=0.1,penalty='l2')
clf11 = svm.LinearSVC(C=0.1,penalty='l2')
clf2 = lm.ElasticNetCV(l1_ratio=0.3,n_jobs=1)
clf3 = lm.LogisticRegression(penalty='l1')
clf4 = lm.SGDClassifier(loss='hinge',n_jobs=1,n_iter=100,penalty='elasticnet')
clf5 = svm.SVC(C=4.0,kernel='rbf',degree=3,probability=True)
clf6 = ensmbl.AdaBoostClassifier(n_estimators=100,learning_rate=1.0)
clf7 = ensmbl.RandomForestClassifier(n_estimators=10,criterion='gini')

# Feature Selection Classes
fs1 = fs.SelectKBest(chi2,k=1000)
fs2 = fs.RFECV(clf1,step=1000,cv=5)
fs3 = fs.RFE(clf1)

# Load Training Data
train_dat = '000003_0'; test_dat = '000015_0'
p_tr_dat_xp, p_tr_dat_xt,p_tr_dat_y = proc_split_raw_dat(train_dat)
vctr = basic_trans_dat(p_tr_dat_xp)

dat_subset1 = filter_dataset_1(p_tr_dat_xp,p_tr_dat_xt,p_tr_dat_y)
dat_subset2 = filter_dataset_2(p_tr_dat_xp,p_tr_dat_xt,p_tr_dat_y)

# Load Retraining Data
retrain_dat = 'dat_f_retraining.txt'
rt_dat_xp,rt_dat_yp = load_rt_data(retrain_dat)
rt_dat_xsp = [item.split(' ') for item in rt_dat_xp]
rt_tr_dat_x = vctr.transform(to_lof_strings(rt_dat_xsp[0:int(len(rt_dat_xsp)/2)]))
rt_ts_dat_x = vctr.transform(to_lof_strings(rt_dat_xsp[int(len(rt_dat_xsp)/2):]))

# Experiment 1 -  Classifier learnt on Instances with a bag interaction + Counts/tfidf as weights
# Train Test decompositon + Predicted accuracy
tfr_labels_e1 = transform_labels(dat_subset2[2])
tr_dat_y_e1 =  np.asarray(tfr_labels_e1,dtype='string')

tr_dat_x_e1 = vctr.transform(to_lof_strings(dat_subset2[0][0:int(len(dat_subset2[0])/2)]))
fs1.fit(tr_dat_x_e1,tr_dat_y_e1[0:int(len(tr_dat_y_e1)/2)])
#top1000 = feature_names(fs1,vctr)
#fs2.fit_transform(tr_dat_x_e1,tr_dat_y_e1[0:int(len(tr_dat_y_e1)/2)])
predictor_e1 = clf1.fit(tr_dat_x_e1,tr_dat_y_e1[0:int(len(tr_dat_y_e1)/2)])
#predictor_rf = clf7.fit(tr_dat_x_e1,tr_dat_y_e1[0:int(len(tr_dat_y_e1)/2)])
preds_e1 = predictor_e1.predict(vctr.transform(to_lof_strings(dat_subset2[0][int(len(dat_subset2[0])/2):])))

print 'accuracy'+':'+str(metrics.accuracy_score(y_true=tr_dat_y_e1[int(len(tr_dat_y_e1)/2):],y_pred=preds_e1))
print 'f1_score'+':'+str(metrics.f1_score(y_true=tr_dat_y_e1[int(len(tr_dat_y_e1)/2):],y_pred=preds_e1,pos_label=None))
print 'confusion_matrix'+':'+str(metrics.confusion_matrix(y_true=tr_dat_y_e1[int(len(tr_dat_y_e1)/2):],y_pred=preds_e1))

# Prediction with varying session lengths
'''sb_ts = divide_4_parts(dat_subset2[0][0:int(len(dat_subset2[0])/2)],dat_subset2[2][0:int(len(dat_subset2[0])/2)])
tr_sbts_x = vctr.transform(to_lof_strings(sb_ts[0][0]))
tr_sbts_y_p = transform_labels(sb_ts[0][1]); tr_sbts_y = np.asarray(tr_sbts_y_p)
predictor_e2 = clf11.fit(tr_sbts_x,tr_sbts_y)

sb1,sb2,sb3,sb4 = divide_4_parts(dat_subset2[0][int(len(dat_subset2[0])/2):],dat_subset2[2][int(len(dat_subset2[0])/2):])
sbx = sb1
tr_sb1_x = vctr.transform(to_lof_strings(sbx[0]))
tr_sb1_y_p = transform_labels(sbx[1]); tr_sb1_y = np.asarray(tr_sb1_y_p,dtype='string')
preds_part = predictor_e1.predict(tr_sb1_x)
print 'accuracy'+':'+str(metrics.accuracy_score(y_true=tr_sb1_y,y_pred=preds_part))
print 'f1_score'+':'+str(metrics.f1_score(y_true=tr_sb1_y,y_pred=preds_part,pos_label=None))
cf_mt = metrics.confusion_matrix(y_true=tr_sb1_y,y_pred=preds_part)
print 'precision'+':'+str(float(cf_mt[0][0])/(cf_mt[0][0]+cf_mt[1][0]))
print 'recall'+':'+str(float(cf_mt[0][0])/(cf_mt[0][0]+cf_mt[0][1]))'''

# Cross validated accuracy
'''cv_accuracy = cv.cross_val_score(clf1,vctr.transform(to_lof_strings(dat_subset2[0])),tr_dat_y_e1,cv=5)
print cv_accuracy
#print 'mean cv accuracy test set'+':'+str(mean(cv_accuracy))

# Predict on the subset1 using the model trained on subset2
tfr_labels_e1_s1 = transform_labels(dat_subset1[2])
tr_dat_y_e1_s1 =  np.asarray(tfr_labels_e1_s1,dtype='string')
cv_accuracy_2 = cv.cross_val_score(clf1,vctr.transform(to_lof_strings(dat_subset1[0])),tr_dat_y_e1_s1,cv=5)'''

# Clustering Experiment 1 - Cluster non purchasers with a shopping bag interaction
kmc = clstr.KMeans(n_clusters=5,init='k-means++',max_iter=500)
aglmc = clstr.AgglomerativeClustering(n_clusters=5,affinity='euclidean',linkage='ward')
fsdat = fs1.transform(tr_dat_x_e1)
kmc.fit(fsdat)
cpreds = kmc.predict(fsdat)

# Clustering Experiment 2 - Cluster purchasers
np_subset_x = [it1 for it1,it2 in zip(dat_subset2[0],dat_subset2[2]) if it2 != '']
#np_subset_x = [it1 for it1,it2 in zip(dat_subset2[0],dat_subset2[2])]
kmc1 = clstr.KMeans(n_clusters=5,init='k-means++',max_iter=500)
aglmc = clstr.AgglomerativeClustering(n_clusters=5,affinity='euclidean',linkage='ward')
tr_dat_x_cls = vctr.transform(to_lof_strings(np_subset_x))
fsdat1 = fs1.transform(tr_dat_x_cls)
kmc1.fit(fsdat1)
cpreds1 = kmc1.predict(fsdat1)

cls_dat,cls_preds = cluster_preds(np_subset_x,cpreds1,0,vctr,predictor_e1,1.0)
pos_preds = [index for index,item in enumerate(cls_preds) if item == 'P']
rds_x_p1,rds_y_p1 = err_clst_pts(0,cls_dat,cls_preds)

# Comparison of per cluster classifier with the single classifier.
#cls_test_data = vctr.transform(to_lof_strings(dat_subset2[0][int(len(dat_subset2[0])/2):]))
cls_test_data = vctr.transform(to_lof_strings(dat_subset2[0]))
tr_cls_test_data = fs1.transform(cls_test_data)
cpreds_all = kmc1.predict(tr_cls_test_data)
ctrs = [get_lbl_cluster_dat(dat_subset2[0],tr_dat_y_e1,cpreds_all,itr,1.0) for itr in range(0,5)]
cvacc_per_cls = [cv.cross_val_score(clf1,vctr.transform(to_lof_strings(item[0])),np.asarray(item[1],dtype='string'),cv=3) for item in ctrs]

# Identify clusters for retraining and save the data
cls_lng,cls_mnsz = clst_elem_sizes(np_subset_x,cpreds1,5,1.0)
cls_dat,cls_preds = cluster_preds(np_subset_x,cpreds1,2,vctr,predictor_e1,1.0)
cls_Px,cls_Py = err_clst_pts(1,cls_dat,cls_preds)
cls_NPx,cls_NPy = err_clst_pts(0,cls_dat,cls_preds)
#save_cluster_sample(cls_Px,cls_Py,cls_NPx,cls_NPy)

# Retrain on the identified subset.
predictor_e3 = clf11.fit(rt_tr_dat_x,rt_dat_yp[0:int(len(rt_dat_yp)/2)])
preds_rt1 = predictor_e3.predict(rt_ts_dat_x)
print 'RT_accuracy'+':'+str(metrics.accuracy_score(y_true=rt_dat_yp[int(len(rt_dat_yp)/2):],y_pred=preds_rt1))
print 'RT_f1_score'+':'+str(metrics.f1_score(y_true=rt_dat_yp[int(len(rt_dat_yp)/2):],y_pred=preds_rt1,pos_label=None))
print 'RT_confusion_matrix'+':'+str(metrics.confusion_matrix(y_true=rt_dat_yp[int(len(rt_dat_yp)/2):],y_pred=preds_rt1))


# Clustering Experiment 3 - Cluster the top1000 features.
top_ftrs = feature_names(fs1,vctr)
top_ftrs_str = map(unicode.encode,top_ftrs)
kmc2 = clstr.KMeans(n_clusters=5,init='k-means++',max_iter=500)
vctr2 = basic_trans_dat(top_ftrs_str)
enc_top_ftrs = vctr2.transform(to_lof_strings_alt(top_ftrs_str))
kmc2.fit(enc_top_ftrs)
cpreds2 = kmc2.predict(enc_top_ftrs)

#aglmc.fit(fsdat1)
#cpreds2 = aglmc.predict(fsdat1)

# Experiment 2 -  Classifier learnt on Instances with a bag interaction + Time on page as weights
'''tr_dat_x_e2 = modify_ngram_weights(dat_subset2[0][0:int(len(dat_subset2[0])/2)],dat_subset2[1][0:int(len(dat_subset2[0])/2)],1,3,tr_dat_x_e1,vctr)
tr_dat_y_e2 = tr_dat_y_e1
predictor_e2 = clf1.fit(tr_dat_x_e2,tr_dat_y_e2[0:int(len(tr_dat_y_e2)/2)])
ts_dat_x_e2 = modify_ngram_weights(dat_subset2[0][int(len(dat_subset2[0])/2):],dat_subset2[1][int(len(dat_subset2[0])/2):],1,3,\
vctr.transform(to_lof_strings(dat_subset2[0][int(len(dat_subset2[0])/2):])),vctr)
preds_e2 = predictor_e2.predict(ts_dat_x_e2)
print 'accuracy'+':'+str(metrics.accuracy_score(y_true=tr_dat_y_e2[int(len(tr_dat_y_e2)/2):],y_pred=preds_e2))
print 'f1_score'+':'+str(metrics.f1_score(y_true=tr_dat_y_e2[int(len(tr_dat_y_e2)/2):],y_pred=preds_e2,pos_label=None))'''

# Test on held out data Experiment 1
'''p_ts_dat_xp, p_ts_dat_xt,p_ts_dat_y = proc_split_raw_dat(test_dat)
dat_subset1 = filter_dataset_1(p_ts_dat_xp,p_ts_dat_xt,p_ts_dat_y)
dat_subset2 = filter_dataset_2(p_ts_dat_xp,p_ts_dat_xt,p_ts_dat_y)

tfr_labels_e1 = transform_labels(dat_subset2[2])
tr_dat_y_e1 =  np.asarray(tfr_labels_e1,dtype='string')
tr_dat_x_e1 = vctr.transform(to_lof_strings(dat_subset2[0][0:int(len(dat_subset2[0])/2)]))

cv_accuracy = cv.cross_val_score(clf1,vctr.transform(to_lof_strings(dat_subset2[0])),tr_dat_y_e1,cv=5)
print cv_accuracy'''
#print 'mean cv accuracy holdout'+':'+str(mean(cv_accuracy))









