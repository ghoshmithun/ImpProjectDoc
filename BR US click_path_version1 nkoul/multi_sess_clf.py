import sys
import glob
import time
import datetime
from collections import defaultdict
import random
import pickle

import numpy as np
import scipy as sp
from scipy.sparse import csr_matrix
from scipy.sparse import vstack

import sklearn as sk
import sklearn.feature_extraction.text as txt
import sklearn.decomposition as dc
import sklearn.feature_selection as fs
from sklearn.tree import DecisionTreeClassifier as dtree
from sklearn.tree import ExtraTreeClassifier as etree
from sklearn.naive_bayes import MultinomialNB as mnb
from sklearn.feature_selection import chi2
from sklearn.utils import shuffle as sk_shuffle

import sklearn.linear_model as lm
import sklearn.svm as svm
import sklearn.ensemble as ensmbl
from sklearn.ensemble import RandomForestClassifier as rfc
import sklearn.cluster as clstr

import sklearn.grid_search as gs
import sklearn.cross_validation as cv
import sklearn.metrics as metrics
import sklearn.pipeline as pipeline

# Single line parser
def parse_pname_time(all_dat):
	dat_parse = []
	csv_sess = all_dat.split('\x02')
	for item in csv_sess:
		spl_line = item.split('__')
		if len(spl_line) == 3:
			dat_parse.append((spl_line[2],spl_line[0],spl_line[1]))
		else:
			dat_parse.append(('','',''))
			print spl_line
	for index,item in enumerate(dat_parse):
		tmstmp = item[0].split(' ')
		if len(tmstmp) < 2:
			dat_parse[index] = (datetime.datetime.strptime('0:0:0','%H:%M:%S'),item[1],item[2],datetime.datetime.strptime('1/1/2015 0:0:0','%d/%m/%Y %H:%M:%S'))
		else:
			dat_parse[index] = (datetime.datetime.strptime(tmstmp[1],'%H:%M:%S'),item[1],item[2],datetime.datetime.strptime(tmstmp[0]+' '+tmstmp[1],'%d/%m/%Y %H:%M:%S'))
	sorted_dat_parse = sorted(dat_parse, key=lambda x:x[0])
	ptimes = []
	for it1,it2 in zip(sorted_dat_parse,sorted_dat_parse[1:]):
		ptimes.append((it2[0]-it1[0]).seconds)
	if len(ptimes) !=0:
		mean_time = sum(ptimes)/len(ptimes)
	else:
		mean_time = 0
	ptimes.append(mean_time)
	for index,item in enumerate(zip(ptimes,sorted_dat_parse)):
		sorted_dat_parse[index] = (str(item[0]),item[1][1],item[1][2],item[1][3])
	return sorted_dat_parse

# Load data for single/multi session classification
def cross_sess_dat_parse(filename):
	raw_dat_id=[]; raw_dat_date=[]; raw_dat_xp=[]; raw_dat_xt=[]; raw_dat_y = []
	fh = open(filename,'r')
	for line in fh:
		str_line = line.rstrip()
		spl_line = str_line.split('\x01')
		try:
			rv = parse_pname_time(spl_line[1])
			rv0 = [item[3] for item in rv]
			rv1 = [item[0] for item in rv]
			rv2 = []
			for item in rv:
				if item[1] != '':
					rm_sp = item[1].replace(' ','_')
					rv2.append(rm_sp)
				else:
					rm_sp2 = item[2].replace(' ','_')
					rv2.append(rm_sp2)
			raw_dat_id.append(spl_line[0]);raw_dat_date.append(rv0[0])
			raw_dat_xp.append(rv2);raw_dat_xt.append(rv1);raw_dat_y.append(spl_line[3])
		except:
			pass
	fh.close()
	return (raw_dat_id,raw_dat_date,raw_dat_xp,raw_dat_xt,raw_dat_y)

# Aggregrate for a customer
# all_dat_tuple = (id,date,pname,ptime,prch_fg)
def construct_customer_view(all_dat_tuple):
	cust_dict = {}
	for item in all_dat_tuple[0]:
		cust_dict[item] = []
	for it1,it2,it3,it4,it5 in zip(all_dat_tuple[0],all_dat_tuple[1],all_dat_tuple[2],all_dat_tuple[3],all_dat_tuple[4]):
		cust_dict[it1].append((it2,it3,it4,it5))
	sessions = cust_dict.values()
	sorted_sessions = [sorted(item,key=lambda x:x[0]) for item in sessions]
	return sorted_sessions

# Return sorted sessions along with customer identifiers
def construct_customer_view_alt(all_dat_tuple):
	cust_dict = {}
	for item in all_dat_tuple[0]:
		cust_dict[item] = []
	for it1,it2,it3,it4,it5 in zip(all_dat_tuple[0],all_dat_tuple[1],all_dat_tuple[2],all_dat_tuple[3],all_dat_tuple[4]):
		cust_dict[it1].append((it2,it3,it4,it5))
	for key,value in cust_dict.iteritems():
		cust_dict[key] = sorted(value,key=lambda x:x[0])
	return cust_dict

# Flatten the customer data to a single session level
def build_single_sess_dat(sorted_sessions):
	ss_dat_x = [];ss_dat_y=[]
	for u_sns in sorted_sessions:
		for sess in u_sns:
			ss_dat_x.append(sess[1])
			ss_dat_y.append(sess[3])
	return ss_dat_x,ss_dat_y

# Using classification scores & labels of previous sessions
# pfg=1 probs used, pfg=0 labels used
def build_multi_sess_dat_t1(clf,vctr,sorted_sessions,pfg=0):
	if pfg == 0:
		all_preds_x = []; all_preds_y = []
		for u_sns in sorted_sessions:
			s_preds_x = []; s_preds_y = []
			for sess in u_sns:
				pdc = clf.predict_proba(vctr.transform(to_lof_strings([sess[1]])))
				s_preds_x.append(pdc[0][1])
				if sess[3] != '':
					s_preds_y.append(1.0)
				else:
					s_preds_y.append(0.0)
			all_preds_x.append(s_preds_x);all_preds_y.append(s_preds_y)
	elif pfg == 1:
		all_preds_x = []; all_preds_y = []
		for u_sns in sorted_sessions:
			s_preds_x = []; s_preds_y = []
			for sess in u_sns:
				pdc = clf.predict(vctr.transform(to_lof_strings([sess[1]])))
				s_preds_x.append(pdc[0])
				if sess[3] != '':
					s_preds_y.append('P')
				else:
					s_preds_y.append('NP')
			all_preds_x.append(s_preds_x);all_preds_y.append(s_preds_y)
	#return ms_dat_x,ms_dat_y
	return all_preds_x,all_preds_y

# Using session ngrams of previous sessions
def build_multi_sess_dat_t2(sorted_sessions):
	ms_dat_x = [];ms_dat_y = []
	for u_sns in sorted_sessions:
		conc_sess = [];cl = ''
		for sess in u_sns:
			conc_sess.extend(sess[1])
			cl = sess[3]
		ms_dat_x.append(conc_sess);ms_dat_y.append(cl)
	return ms_dat_x,ms_dat_y

# Filter leakage variables
def to_lof_strings(dat):
	los = []
	for item in dat:
		ftr_item = [it2 for it2 in item if (it2!='orderConfirm'and it2!='shipping') and it2!='billing'\
		and it2!='gifting' and it2!='orderPlacement' and it2 !='order_gateway_sign_in' and it2!=\
		'order_status_detail' and it2!='shipment_detail' and it2!='br:checkout:Checkout' and it2.lower()\
		!='completeregistration' and it2.lower() !='address_book' and it2.lower() !='br:checkout:checkout:module:shipping'\
		and it2.lower()!='single_order_detail' and it2.lower() !='br:profile:order_status_shipment_detail' and it2.lower() !='rewards']
		los.append(' '.join(ftr_item))
	return los

# Filter leakage variables
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

# Vectorizer
'''def basic_trans_dat(dat_xp,ftr_fg=0):
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
	return vctrz'''

# Tokenizer
def cust_token(sent):
	return sent.split(' ')

# Initialize Vectorizer
def basic_trans_dat(vctrz,dat_xp,ftr_fg=0):
	#vctrz = txt.CountVectorizer(ngram_range=(1,3),tokenizer=cust_token)
	#vctrz = txt.TfidfVectorizer(ngram_range=(1,5),tokenizer=cust_token)
	if ftr_fg == 0:
		dat_sent_form = to_lof_strings(dat_xp)
	elif ftr_fg == 1:
		dat_sent_form = to_lof_strings_alt(dat_xp)
	#vctrz.fit(dat_sent_form[0:int(len(dat_sent_form)/2)])
	vctrz.fit(dat_sent_form)
	return None

# Change Label Names to P for purchaser and NP otherwise
def transform_labels(labels):
	trans_labels = []
	for label in labels:
		if label != '':trans_labels.append('P')
		#if label != '':trans_labels.append(1)
		else:trans_labels.append('NP')
		#else:trans_labels.append(0)
	return trans_labels

# Cart interaction Sessions
# tp1=id,tp2=date,tp3=xp;tp4=xt;tp5=label
def filter_dataset_1(tp1,tp2,tp3,tp4,tp5):
	raw_dat_id=[]; raw_dat_date=[]; raw_dat_xp=[]; raw_dat_xt=[]; raw_dat_y = []
	for it1,it2,it3,it4,it5 in zip(tp1,tp2,tp3,tp4,tp5):
		if ('inlineBagAdd' in it3 or 'shopping_bag' in it3 or 'br:buy:Shopping_Bag' in it3\
		and 'br: Store WiFi splash page with email entry' not in it3\
		and 'br:customerService:home' not in it3 and len(it3)> 5):
			raw_dat_id.append(it1);raw_dat_date.append(it2);raw_dat_xp.append(it3);raw_dat_xt.append(it4);raw_dat_y.append(it5)
	return (raw_dat_id,raw_dat_date,raw_dat_xp,raw_dat_xt,raw_dat_y)

# Sessions sans cart interaction
def filter_dataset_2(tp1,tp2,tp3,tp4,tp5):
	raw_dat_id=[]; raw_dat_date=[]; raw_dat_xp=[]; raw_dat_xt=[]; raw_dat_y = []
	for it1,it2,it3,it4,it5 in zip(tp1,tp2,tp3,tp4,tp5):
		if ('inlineBagAdd' not in it3 and 'shopping_bag' not in it3 and 'br:buy:Shopping_Bag' not in it3\
		and 'br: Store WiFi splash page with email entry' not in it3\
		and 'br:customerService:home' not in it3 and len(it3)> 5):
			raw_dat_id.append(it1);raw_dat_date.append(it2);raw_dat_xp.append(it3);raw_dat_xt.append(it4);raw_dat_y.append(it5)
	return (raw_dat_id,raw_dat_date,raw_dat_xp,raw_dat_xt,raw_dat_y)

# Length based filter and bounce removal
def filter_dataset_3(tp1,tp2,tp3,tp4,tp5):
	raw_dat_id=[]; raw_dat_date=[]; raw_dat_xp=[]; raw_dat_xt=[]; raw_dat_y = []
	for  it1,it2,it3,it4,it5 in zip(tp1,tp2,tp3,tp4,tp5):
		if ('br: Store WiFi splash page with email entry' not in it3 and 'br:customerService:home' not in it3 and len(it3) > 5):
			raw_dat_id.append(it1);raw_dat_date.append(it2);raw_dat_xp.append(it3);raw_dat_xt.append(it4);raw_dat_y.append(it5)
	return (raw_dat_id,raw_dat_date,raw_dat_xp,raw_dat_xt,raw_dat_y)

# Click No Distribution where a shopping cart is created
def shopping_cart_dist(sess_x,sess_y):
	p_sess_x = [it1 for it1,it2 in zip(sess_x,sess_y) if it2 != '']
	np_sess_x = [it1 for it1,it2 in zip(sess_x,sess_y) if it2 == '']
	p_sc_dist = {}; np_sc_dist = {}
	p_sc_dist_k=[]; p_sc_dist_v=[];np_sc_dist_k=[]; np_sc_dist_v=[];
	min_length = min([len(item) for item in sess_x])
	max_length = max([len(item) for item in sess_x])
	for ix in range(0,max_length+1):
		p_sc_dist[ix]=0;np_sc_dist[ix]=0
	for sess in p_sess_x:
		for index,click in enumerate(sess):
			if (click == 'shopping_bag' or click == 'br:buy:Shopping_Bag'):
				p_sc_dist[index] +=1;break
	for sess in np_sess_x:
		for index,click in enumerate(sess):
			if (click == 'shopping_bag' or click == 'br:buy:Shopping_Bag'):
				np_sc_dist[index] +=1;break
	for key,value in p_sc_dist.iteritems():
		p_sc_dist_k.append(key);p_sc_dist_v.append(value)
	for key,value in np_sc_dist.iteritems():
		np_sc_dist_k.append(key);np_sc_dist_v.append(value)
	cum_sum_p = []; cum_sum_p.append(p_sc_dist_v[0])
	for item in p_sc_dist_v[1:]:
		cum_sum_p.append(cum_sum_p[-1]+item)
	for key,value in p_sc_dist.iteritems():
		p_sc_dist_k.append(key);p_sc_dist_v.append(value)
	cum_sum_np = []; cum_sum_np.append(np_sc_dist_v[0])
	for item in np_sc_dist_v[1:]:
		cum_sum_np.append(cum_sum_np[-1]+item)
	per_p = [(1.0*item)/cum_sum_p[-1] for item in cum_sum_p]
	per_np = [(1.0*item)/cum_sum_np[-1] for item in cum_sum_np]
	return p_sc_dist,np_sc_dist,per_p,per_np

# Save the cart creation distribution
def export_cartdist(sess_x,sess_y):
	rv1,rv2,rv3,rv4 = shopping_cart_dist(sess_x,sess_y)
	dist_file_p = 'dist_file_p.txt'; dist_file_np = 'dist_file_np.txt'
	fh = open(dist_file_p,'w')
	for index,item in enumerate(rv3):
		fh.write(str(index)+','+str(item)+'\n')
	fh.close()
	fh = open(dist_file_np,'w')
	for index,item in enumerate(rv4):
		fh.write(str(index)+','+str(item)+'\n')
	fh.close()
	return None

# Divide sessions in increments of 20%
def divide_4_parts(data_x,data_y):
	p1_x=[];p1_y=[];p2_x=[];p2_y=[];p3_x=[];p3_y=[];p4_x=[];p4_y=[]
	for elem,lbl in zip(data_x,data_y):
		if (len(elem) > 10):
			p1_x.append(elem[0:int(len(elem)*0.20)]); p1_y.append(lbl)
			p2_x.append(elem[0:int(len(elem)*0.40)]); p2_y.append(lbl)
			p3_x.append(elem[0:int(len(elem)*0.60)]); p3_y.append(lbl)
			#p3_x.append(elem[int(len(elem)*0.75):int(len(elem)*1.0)]); p3_y.append(lbl)
			p4_x.append(elem[0:int(len(elem)*0.80)]); p4_y.append(lbl)
	return (p1_x,p1_y),(p2_x,p2_y),(p3_x,p3_y),(p4_x,p4_y)

# Make a prediction following each/n cart interaction events
def divide_on_cart_events(sess_x,sess_y):
	def check_cart_event(click):
		check = False
		if click in ['inlineBagAdd','shopping_bag','br:buy:Shopping_Bag']:
			check = True
		return check
	sess_views_x = []; sess_views_y = []
	for it1,it2 in zip(sess_x,sess_y):
		sess_view = [];sub_sess = []
		cart_fg = False
		for clk in it1:
			if check_cart_event(clk):
				sub_sess.append(clk);sess_view.append(sub_sess)
				sub_sess = []
			else:
				sub_sess.append(clk)
		sess_view.append(sub_sess)
		sess_views_x.append(sess_view);sess_views_y.append(it2)
	return sess_views_x,sess_views_y

#
def prediction_metrics(preds,actuals):
	print 'accuracy'+':'+str(metrics.accuracy_score(y_true=actuals,y_pred=preds))
	print 'f1_score'+':'+str(metrics.f1_score(y_true=actuals,y_pred=preds,pos_label=None))
	print 'confusion_matrix'+':'+str(metrics.confusion_matrix(y_true=actuals,y_pred=preds))
	return None

# Sessions untill a certain percentage of cart interactions
def concatenate_on_count_prct(data_x,data_y,cop):
	def concatenate_lists(lol):
		c_lol = lol[0]
		for item in lol[1:]:
			c_lol.extend(item)
		return c_lol
	tr_dat_x = []; tr_dat_y = []
	for it1,it2 in zip(data_x,data_y):
		if type(cop) == int:
			if len(it1) < cop:
				#continue
				tr_dat_x.append(concatenate_lists(it1)); tr_dat_y.append(it2)
			else:
				tr_dat_x.append(concatenate_lists(it1[0:cop]))
				tr_dat_y.append(it2)
		else:
			tr_dat_x.append(concatenate_lists(it1[0:int(len(it1)*cop)]))
			tr_dat_y.append(it2)
	return tr_dat_x,tr_dat_y

# Train based on no of cart interactions based sub sessions
def train_on_cart_cop(data_x,data_y,cop,vctr,clf):
	a_data_x,a_data_y = concatenate_on_count_prct(data_x,data_y,cop)
	dat_x = vctr.transform(to_lof_strings(a_data_x))
	p_data_y = transform_labels(a_data_y);dat_y = np.asarray(p_data_y,dtype='string')
	tr_clf = clf.fit(dat_x,dat_y)
	return tr_clf

# Predict based on no of cart interactions based sub sessions
def predict_on_cart_cop(data_x,data_y,cop,vctr,clf):
	a_data_x,a_data_y = concatenate_on_count_prct(data_x,data_y,cop)
	dat_x = vctr.transform(to_lof_strings(a_data_x))
	p_data_y = transform_labels(a_data_y);dat_y = np.asarray(p_data_y,dtype='string')
	preds = clf.predict(dat_x)
	prediction_metrics(preds,dat_y)
	return preds

# Version 1 based on cart addition length
def data_for_ensemble_cart(data_x,data_y,len_set):
	tr_dat_x = []; tr_dat_y = []
	lb = 0
	for item in len_set:
		temp_x = [];temp_y = []
		for it1,it2 in zip(data_x,data_y): 
			if len(it1[0]) > lb and len(it1[0]) <= item:
				temp_x.append(it1); temp_y.append(it2)
		tr_dat_x.append(temp_x); tr_dat_y.append(temp_y)
		lb = item
	temp_x = [];temp_y = []
	for it1,it2 in zip(data_x,data_y):
		if len(it1[0]) >= len_set[-1]:
			temp_x.append(it1); temp_y.append(it2)
	tr_dat_x.append(temp_x); tr_dat_y.append(temp_y)
	return tr_dat_x,tr_dat_y

# Version 2 based on entire session length
def data_for_ensemble_lng(data_x,data_y,len_set):
	tr_dat_x = []; tr_dat_y = []
	lb = 0
	for item in len_set:
		temp_x = []; temp_y = []
		for it1,it2 in zip(data_x,data_y):
			if len(it1) > lb and len(it1) <= item:
				temp_x.append(it1); temp_y.append(it2)
		tr_dat_x.append(temp_x); tr_dat_y.append(temp_y)
		lb = item
	temp_x = [];temp_y = []
	for it1,it2 in zip(data_x,data_y):
		if len(it1) >= len_set[-1]:
			temp_x.append(it1); temp_y.append(it2)
	tr_dat_x.append(temp_x); tr_dat_y.append(temp_y)
	return tr_dat_x,tr_dat_y

# Train separate classifiers based on length of sessions
def train_length_ensemble(tr_data_x,tr_data_y,len_set,clf_ls,vctr):
	def train_unit(dat_x,dat_y,vctr,clf):
		tr_dat_x = vctr.transform(to_lof_strings(dat_x))
		p_data_y = transform_labels(dat_y);tr_dat_y = np.asarray(p_data_y,dtype='string')
		tr_clf = clf.fit(tr_dat_x,tr_dat_y)
		return tr_clf
	#tr_data_x,tr_data_y = data_for_ensemble_lng(data_x,data_y,len_set)
	tr_clf_ls = []
	for it1,it2,it3 in zip(tr_data_x,tr_data_y,clf_ls):
		tr_clf_ls.append(train_unit(it1,it2,vctr,it3))
	return tr_clf_ls

# Prediction metrics for length based classifier set
def predict_unit(a_dat_x,a_dat_y,vctr,clf):
	dat_x = vctr.transform(to_lof_strings(a_dat_x))
	p_data_y = transform_labels(a_dat_y);dat_y = np.asarray(p_data_y,dtype='string')
	preds = clf.predict(dat_x)
	prediction_metrics(preds,dat_y)
	return None

# Predict using set of length based classifiers
def predict_with_lng_ens(ts_dat_x,ts_dat_y,clf_ls,len_set,vctr):
	ts_dat_x_a,ts_dat_y_a = data_for_ensemble_lng(ts_dat_x,ts_dat_y,len_set)
	for it1,it2,it3 in zip(ts_dat_x_a,ts_dat_y_a,clf_ls):
		predict_unit(it1,it2,vctr,it3)
	return None

# Comparsions of confidence scores for correct and incorrect predictions
def examine_predictions(preds,cfs,lbls,smps):
	ls_tup = []
	for it1,it2,it3,it4 in zip(preds,cfs,lbls,smps):
		if it1 == it3 and it3 == 'P':
			ls_tup.append((it2,it4))
	return ls_tup

# Distribution of confidence scores with increasing percentage of a session
def correction_dist(preds1,preds2,conf1,conf2,actuals):
	rval = []
	for p1,p2,c1,c2,av in zip(preds1,preds2,conf1,conf2,actuals):
		if p1 != av and p2 == av:
			rval.append((c1,c2))
	return rval

# 
def potential_purchaser_labeling(dat_x,dat_y,fg=1):
	ct_nckt_x = []; ckt_x = []; prch_x = []
	ct_nckt_y = []; ckt_y = []; prch_y = []
	all_dat_x = []; all_dat_y = []
	for it1,it2 in zip(dat_x,dat_y):
		if it2 == '':
			if ('shopping_bag' in it1 or 'inlineBagAdd' in it1 or 'br:buy:Shopping_Bag' in it1) and 'br:checkout:Checkout' not in it1:
				ct_nckt_x.append(it1);
				ct_nckt_y.append('NP')
			if 'br:checkout:Checkout' in it1:
				ckt_x.append(it1)
				ckt_y.append('NP')
		else:
			prch_x.append(it1)
			prch_y.append('P')
	if fg == 1:
		all_dat_x.extend(ct_nckt_x);all_dat_x.extend(prch_x)
		all_dat_y.extend(ct_nckt_y);all_dat_y.extend(prch_y)
		#all_dat_x.extend(ckt_x);all_dat_y.extend(ckt_y)
		rv_x,rv_y = sk_shuffle(all_dat_x,all_dat_y)
		rv = (rv_x,rv_y)
	else:
		rv = ((ct_nckt_x,ct_nckt_y),(ckt_x,ckt_y),(prch_x,prch_y))
	return rv

# 
def sequential_prediction_correction(predictor,preds,dat_x):
	new_preds = []
	for data,prediction in zip(dat_x,preds):
		if prediction == 'NP':
			new_preds.append(prediction)
		else:
			new_preds.append(predictor.predict(data)[0])
	return new_preds

# Predict with a sequence of two classifiers
def sequential_prediction(predictor1,predictor2,dat_x):
	preds = predictor1.predict(dat_x);
	new_preds = sequential_prediction_correction(predictor2,preds,dat_x)
	return new_preds

# Decompose a session into subsessions of size inc
def dec_sess_view(sess,inc):
	dec_sess = []; intv = (len(sess)/float(inc))
	lb = 0; ub = inc
	for itr in range(0,int(intv)+1):
		dec_sess.append(sess[lb:ub])
		lb = ub; ub += inc
	return dec_sess

# Predict for all subsessions of a session
def mltpl_sess_preds(predictor,vctr,sess_view):
	cip = [];preds = []
	for item in sess_view:
		cip.extend(item)
		preds.append(predictor.predict(vctr.transform(to_lof_strings([cip])))[0])
	return preds

# Predict for all subsessions using the sequential predictor
def mltpl_sess_preds_alt(predictor1,predictor2,vctr,sess_view):
	cip = [];preds=[]
	for item in sess_view:
		cip.extend(item)
		preds.append(sequential_prediction(predictor1,predictor2,vctr.transform(to_lof_strings([cip])))[0])
	return preds

# Predict with increasing session data
def predict_x_clicks(predictor,data_x,vctr,inc):
	all_preds = []
	for sess in data_x:
		dec_view = dec_sess_view(sess,inc)
		all_preds.append(mltpl_sess_preds(predictor,vctr,dec_view))
	return all_preds

# Predict with increasing session data with sequential predictor
def predict_x_clicks_alt(predictor1,predictor2,data_x,vctr,inc):
	all_preds = []
	for sess in data_x:
		dec_view = dec_sess_view(sess,inc)
		all_preds.append(mltpl_sess_preds_alt(predictor1,predictor2,vctr,dec_view))
	return all_preds


# Accuracy per n clicks
def per_click_accuracy(preds,labels):
	preds_copy = []; acc_ls = []
	lns = [len(item) for item in preds]
	max_lng = max(lns)
	for index,item in enumerate(preds):
		ext_val = max_lng -len(item)
		v_to_app = item[-1]
		to_append = [v_to_app for it in range(0,ext_val)]
		#preds[index].extend(to_append)
		item.extend(to_append)
		preds_copy.append(item)
	for itr in range(0,max_lng):
		temp_pred = []
		for item in preds_copy:
			temp_pred.append(item[itr])
		acc_ls.append(metrics.accuracy_score(y_true=labels,y_pred=temp_pred))
	return acc_ls

# Identify the checkout abandoned session version 1
def checkout_abandoned_sessions(sorted_multi_sess,predictor,vctr):
	preds = []; flags = []; apreds = []
	for item in sorted_multi_sess:
		tpred = []; tflags = []; tapred = []
		for sess in item:
			tpred.append(predictor.predict(vctr.transform(to_lof_strings([sess[1]])))[0])
			if tpred[-1] == 'P' and sess[3] == '':
				tflags.append(1);
			else:
				tflags.append(0);
			if sess[3] == '':tapred.append('NP')
			else:tapred.append('P')
		preds.append(tpred)
		flags.append(tflags)
		apreds.append(tapred)
	return preds,flags,apreds

# Identify the checkout abandoned session version 2
def checkout_abandoned_sessions_act(sorted_multi_sess):
	ck_flags = []; apreds = []; times = []
	for item in sorted_multi_sess:
		tflag = []; tapred = []; ttimes = []
		for sess in item:
			if 'br:checkout:Checkout' in sess[1]:tflag.append(1)
			else:tflag.append(0)
			if sess[3] == '':tapred.append('NP')
			else:tapred.append('P')
			ttimes.append(sess[0])
		ck_flags.append(tflag)
		apreds.append(tapred)
		times.append(ttimes)
	return ck_flags,apreds,times

# Create dataset for correcting for cart abandoned/resumed sessions
def data_for_checkout_correction(sorted_multi_sess,cor_fg=0,tm_lim=1):
	sess_set1 = [item for item in sorted_multi_sess if len(item) == 1]
	sess_set2 = [item for item in sorted_multi_sess if len(item) == 2]
	dat_x = []; dat_y = []
	if cor_fg == 0:
		for sess in sorted_multi_sess:
			for in_sess in sess:
				if in_sess[3] == '':
					if 'br:checkout:Checkout' in in_sess[1]:
						dat_x.append(in_sess[1]);dat_y.append('NP')
				else:
					dat_x.append(in_sess[1]);dat_y.append('P')
	else:
		for sess in sess_set1:
			for in_sess in sess:
				if in_sess[3] == '':
					if 'br:checkout:Checkout' in in_sess[1]:
						dat_x.append(in_sess[1]);dat_y.append('NP')
				else:
					dat_x.append(in_sess[1]);dat_y.append('P')
		for sess in sess_set2:
			if sess[0][3] == '':
				if 'br:checkout:Checkout' in sess[0][1]:
					if sess[1][3] != '' and int((sess[1][0]-sess[0][0]).seconds/60.0) < tm_lim*60:
						dat_x.append(sess[0][1]); dat_y.append('P')
					else:
						dat_x.append(sess[0][1]); dat_y.append('NP')
			else:
				dat_x.append(sess[0][1]); dat_y.append('P')
			if sess[1][3] == '':
				if 'br:checkout:Checkout' in sess[1][1]:
					dat_x.append(sess[1][1]);dat_y.append('NP')
			else:
				dat_x.append(sess[1][1]);dat_y.append('P')
	return dat_x,dat_y

# Conversion counts after cart resumption
def sec_sess_conv_stats(ckflags,prflags,times):
	ftr_ckflags = [item for item in ckflags if len(item) == 2]
	ftr_prflags = [item for item in prflags if len(item) == 2]
	ftr_times = [item for item in times if len(item) == 2]
	count = 0.0; tdeltas = []; count1 = 0.0
	for it1,it2,it3 in zip(ftr_ckflags,ftr_prflags,ftr_times):
		if it1[0] ==1 and it2[0] == 'NP' and it2[1] == 'NP' and it1[1] == 1:
			count +=1; tdeltas.append(int((it3[1]-it3[0]).seconds/3600.0))
	'''for it1,it2,it3 in zip(ftr_ckflags,ftr_prflags,ftr_times):
		if int((it3[1]-it3[0]).seconds/60.0) < 1*60:
			count1 +=1
			if it1[0] == 1 and it2[0] == 'NP' and it2[1] =='P':
				count +=1'''
	return count,len(ftr_ckflags),tdeltas
	#return count,count1,tdeltasd

# Time distribution of second sessions
def dist_time(tdeltas):
	dist_o = {}
	rv = set(tdeltas)
	for item in rv:
		dist_o[item] = 0.0
	for item in tdeltas:
		dist_o[item] += 1
	for item in rv:
		dist_o[item] /= len(tdeltas)
	return dist_o

# Prediction after correcting Cart abandoned sessions with a following converting session
def corrected_data_prediction(sorted_sessions,fg,tm_rng,clf,vctr):
	comp_corr_x,comp_corr_y = data_for_checkout_correction(sorted_sessions,fg,tm_rng)
	sh_comp_corr_x,sh_comp_corr_y = sk_shuffle(comp_corr_x,comp_corr_y)
	tr_sh_comp_corr_y = np.asarray(sh_comp_corr_y,dtype='string')
	clf.fit(vctr.transform(to_lof_strings(sh_comp_corr_x)),tr_sh_comp_corr_y)
	cv_accuracy_comp = cv.cross_val_score(clf,vctr.transform(to_lof_strings(sh_comp_corr_x)),tr_sh_comp_corr_y,cv=5)
	print cv_accuracy_comp
	return cv_accuracy_comp

# Holdout accuracy of the cart abandoned corrected classifier
def corrected_holdout_prediction(sorted_sessions,fg,tm_rng,clf,vctr):
	comp_corr_x,comp_corr_y = data_for_checkout_correction(sorted_sessions,fg,tm_rng)
	sh_comp_corr_x,sh_comp_corr_y = sk_shuffle(comp_corr_x,comp_corr_y)
	tr_sh_comp_corr_y = np.asarray(sh_comp_corr_y,dtype='string')
	preds = clf.predict(vctr.transform(to_lof_strings(sh_comp_corr_x)))
	print 'accuracy corrected holdout :'  + str(metrics.accuracy_score(y_true=tr_sh_comp_corr_y,y_pred=preds))
	return None

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

# Length Distribution
def cum_leng_dist(data):
	leng_dist = {}
	all_lngs = [len(item) for item in data]
	min_lng = min(all_lngs); max_lng = max(all_lngs)
	lng_incs = []; iv = min_lng
	for item in range(0,int(max_lng/5)+1):
		lng_incs.append(iv)
		iv+=5
	for item in lng_incs:
		leng_dist[item] = 0
	for item in lng_incs:
		for item2 in data:
			if len(item2)>= item:
				leng_dist[item] +=1
	den = float(len(data))
	for item in lng_incs:
		leng_dist[item] /=den
	return leng_dist

# Load data across multiple files, filter the cart sessions and create a customer view
def cross_source_data_load(filenames):
	all_dat = []
	for fl_nm in filenames:
		ps_rv = cross_sess_dat_parse(fl_nm)
		tv = filter_dataset_1(ps_rv[0],ps_rv[1],ps_rv[2],ps_rv[3],ps_rv[4])
		cv_rv = construct_customer_view(tv)
		all_dat.extend(cv_rv)
	return all_dat

################################################################
################################################################

# Load and Sessionize data
#train_dat = 'Cross_Session_Data_A/000000_0a'
train_dat = 'Cross_Session_Data_2/000000_0'
train_dat_path = 'Cross_Session_Data_2/*_0'
tr_files = glob.glob(train_dat_path)
ext_dat = cross_sess_dat_parse(train_dat)
ftr_ext_dat = filter_dataset_1(ext_dat[0],ext_dat[1],ext_dat[2],ext_dat[3],ext_dat[4])
sorted_sessions = construct_customer_view(ftr_ext_dat)
#sorted_sessions = cross_source_data_load(tr_files)
#sorted_sessions_wk = construct_customer_view_alt(ftr_ext_dat)

ftr2_ext_dat = filter_dataset_2(ext_dat[0],ext_dat[1],ext_dat[2],ext_dat[3],ext_dat[4])
sorted_sessions_ftr2 = construct_customer_view(ftr2_ext_dat)

ftr3_ext_dat = filter_dataset_3(ext_dat[0],ext_dat[1],ext_dat[2],ext_dat[3],ext_dat[4])
sorted_sessions_lng = construct_customer_view(ftr3_ext_dat)

# Classifier Classes
clf1 = svm.LinearSVC(C=0.05,penalty='l2')
clf2 = lm.ElasticNetCV(l1_ratio=0.3,n_jobs=1)
clf3 = lm.LogisticRegression(penalty='l1')
clf4 = lm.SGDClassifier(loss='hinge',n_jobs=1,n_iter=100,penalty='elasticnet')
clf5 = svm.SVC(C=4.0,kernel='rbf',degree=3,probability=True)
bclf1 = dtree(max_depth=10)
bclf2 = svm.SVC(C=4.0,kernel='rbf',degree=3,probability=True)
bclf3 = mnb(alpha=1.0,fit_prior=True,class_prior=None)
#clf6 = ensmbl.AdaBoostClassifier(base_estimator=bclf1,n_estimators=100,learning_rate=1.0)
clf7 = ensmbl.RandomForestClassifier(n_estimators=10,criterion='gini')
clf8 = gs.GridSearchCV(svm.LinearSVC(penalty='l2'),{'C':[0.005,0.01,0.05,0.1,0.2,0.3]},cv=3)
clf9 = gs.GridSearchCV(svm.LinearSVC(penalty='l2'),{'C':[0.005,0.01,0.05,0.1,0.2,0.3]},cv=3)
clf10 = gs.RandomizedSearchCV(svm.LinearSVC(penalty='l2'),{'C':[0.0001,0.001,0.01,0.1,0.25]},cv=3)
clf11 = gs.RandomizedSearchCV(svm.LinearSVC(penalty='l2'),{'C':[1.0,2.0,5.0,10.0,20.0]},cv=3)
#clf9 = gs.GridSearchCV(svm.SVC(kernel='poly',degree='3'),{'C':[0.005,0.01,0.05,0.1,0.2,0.3]},cv=3)
#clf9 = gs.GridSearchCV(svm.SVC(),{'C':[0.3,0.5,1.0,2.0,3.0]},cv=3)
#clf9 = gs.GridSearchCV(lm.SGDClassifier(penalty='elasticnet',loss='log',n_iter=1000,n_jobs=-1,shuffle=True),{'l1_ratio':[0.1,0.5,0.7,0.9]},cv=3)
clf_ls = [gs.GridSearchCV(svm.LinearSVC(penalty='l2'),{'C':[0.005,0.01,0.05,0.1,0.2,0.3]},cv=3) for it in range(0,4)]

# Feature Selection Classes
fs1 = fs.SelectKBest(chi2,k=100)
fs2 = fs.RFECV(clf1,step=1000,cv=5)
fs3 = fs.RFE(clf1)

# Build single sesssion classifier on Cart Interaction Sessions
ss_ptrdat_x1,ss_ptr_dat_y1 = build_single_sess_dat(sorted_sessions)
ss_ptrdat_x,ss_ptr_dat_y = sk_shuffle(ss_ptrdat_x1,ss_ptr_dat_y1)
trans_starttime = time.time()
vctr = txt.CountVectorizer(ngram_range=(1,3),tokenizer=cust_token)
basic_trans_dat(vctr,ss_ptrdat_x)
trans_endtime = time.time()
print 'Vectorization time ' + ':' + str(trans_endtime-trans_starttime);
tfr_labels_e1 = transform_labels(ss_ptr_dat_y)
tr_dat_y_e1 =  np.asarray(tfr_labels_e1,dtype='string')
tr_dat_x_e1 = vctr.transform(to_lof_strings(ss_ptrdat_x[0:int(len(ss_ptrdat_x)*0.8)]))
predictor_e1 = clf1.fit(tr_dat_x_e1,tr_dat_y_e1[0:int(len(tr_dat_y_e1)*0.8)])
#predictor_e2 = clf5.fit(tr_dat_x_e1,tr_dat_y_e1[0:int(len(tr_dat_y_e1)*0.8)])
pred_starttime = time.time()
preds_e1 = predictor_e1.predict(vctr.transform(to_lof_strings(ss_ptrdat_x[int(len(ss_ptrdat_x)*0.8):])))
pred_endtime = time.time()
conf_score_e1 = predictor_e1.decision_function(vctr.transform(to_lof_strings(ss_ptrdat_x[int(len(ss_ptrdat_x)*0.8):])))

print 'prediction time' +':' + str(pred_endtime-pred_starttime)
print 'accuracy of lsvc cart only'+':'+str(metrics.accuracy_score(y_true=tr_dat_y_e1[int(len(tr_dat_y_e1)*0.8):],y_pred=preds_e1))
print 'f1_score of lsvc cart only'+':'+str(metrics.f1_score(y_true=tr_dat_y_e1[int(len(tr_dat_y_e1)*0.8):],y_pred=preds_e1,pos_label=None))
print 'confusion_matrix of lsvc cart only'+':'+str(metrics.confusion_matrix(y_true=tr_dat_y_e1[int(len(tr_dat_y_e1)*0.8):],y_pred=preds_e1))

# Boosting
'''bclfx1 = dtree(max_depth=None)
bclfx1a = etree()
bclfx1b = rfc(n_estimators=10)
bclfx2 = svm.LinearSVC(C=0.05,penalty='l2')
clf_bt1 = ensmbl.AdaBoostClassifier(base_estimator=bclfx1,n_estimators=100,learning_rate=0.5)
clf_bt2 = ensmbl.AdaBoostClassifier(base_estimator=bclfx2,n_estimators=100,learning_rate=0.5,algorithm='SAMME')
clf_bt3 = ensmbl.AdaBoostClassifier(base_estimator=lm.SGDClassifier(n_iter=100),n_estimators=50,learning_rate=1.0,algorithm='SAMME')
clf_bt4 = ensmbl.GradientBoostingClassifier()
param_grid1 = {"base_estimator__max_depth":[None,2,3,4],"n_estimators":[100,200,500,1000],"learning_rate":[0.05,0.1,0.25,0.5,0.75]}
param_grid2 = {"base_estimator__C":[0.05,0.1,0.25,0.5],"n_estimators":[100,200,500,1000],"learning_rate":[0.05,0.1,0.25,0.5,0.75]}
param_grid3 = {"learning_rate":[0.1,0.25,0.5]}
param_grid4 = {"base_estimator__criterion":["gini", "entropy"],"learning_rate":[0.1,0.25,0.5]}
clf_bt_fs1 = gs.GridSearchCV(ensmbl.AdaBoostClassifier(base_estimator=bclfx1),param_grid=param_grid1,cv=3)
clf_bt_fs2 = gs.GridSearchCV(ensmbl.AdaBoostClassifier(base_estimator=bclfx2,algorithm='SAMME'),param_grid=param_grid2,cv=3)
bt_predictor1 = clf_bt_fs1.fit(tr_dat_x_e1.toarray(),tr_dat_y_e1[0:int(len(tr_dat_y_e1)*0.8)])
bt_preds1 = bt_predictor1.predict(vctr.transform(to_lof_strings(ss_ptrdat_x[int(len(ss_ptrdat_x)*0.8):])).toarray())
bt_predictor2 = clf_bt_fs2.fit(tr_dat_x_e1,tr_dat_y_e1[0:int(len(tr_dat_y_e1)*0.8)])
bt_preds2 = bt_predictor2.predict(vctr.transform(to_lof_strings(ss_ptrdat_x[int(len(ss_ptrdat_x)*0.8):])))

bt_predsx = bt_preds1
print 'accuracy'+':'+str(metrics.accuracy_score(y_true=tr_dat_y_e1[int(len(tr_dat_y_e1)*0.8):],y_pred=bt_predsx))
print 'f1_score'+':'+str(metrics.f1_score(y_true=tr_dat_y_e1[int(len(tr_dat_y_e1)*0.8):],y_pred=bt_predsx,pos_label=None))
print 'confusion_matrix'+':'+str(metrics.confusion_matrix(y_true=tr_dat_y_e1[int(len(tr_dat_y_e1)*0.8):],y_pred=bt_predsx))'''

# Clasification while also including Non Cart Sessions
ftr3_ptrdat_x,ftr3_ptrdat_y = build_single_sess_dat(sorted_sessions_lng)
vctr2 = txt.CountVectorizer(ngram_range=(1,3),tokenizer=cust_token)
basic_trans_dat(vctr2,ftr3_ptrdat_x)
ftr3_lbls = transform_labels(ftr3_ptrdat_y)
tr_dat_y_e3 = np.asarray(ftr3_lbls,dtype='string')
tr_dat_x_e3 = vctr2.transform(to_lof_strings(ftr3_ptrdat_x[0:int(len(ss_ptrdat_x)*0.8)]))
predictor_e3 = clf4.fit(tr_dat_x_e3,tr_dat_y_e3[0:int(len(tr_dat_y_e1)*0.8)])
#preds_e3 = predictor_e3.predict(vctr2.transform(to_lof_strings(ftr3_ptrdat_x[int(len(ftr3_ptrdat_x)*0.8):])))
preds_e3 = predictor_e3.predict(vctr2.transform(to_lof_strings(ftr3_ptrdat_x[int(len(ftr3_ptrdat_x)*0.8):])))
print 'accuracy cart and non cart'+':'+str(metrics.accuracy_score(y_true=tr_dat_y_e3[int(len(tr_dat_y_e3)*0.8):],y_pred=preds_e3))
print 'f1_score cart and non cart'+':'+str(metrics.f1_score(y_true=tr_dat_y_e3[int(len(tr_dat_y_e3)*0.8):],y_pred=preds_e3,pos_label=None))
print 'confusion_matrix cart and non cart'+':'+str(metrics.confusion_matrix(y_true=tr_dat_y_e3[int(len(tr_dat_y_e3)*0.8):],y_pred=preds_e3))

# Classification on cart only data using the classifier trained with non filtered data
preds_e1 = predictor_e3.predict(vctr2.transform(to_lof_strings(ss_ptrdat_x[int(len(ss_ptrdat_x)*0.8):])))
print 'accuracy on nc training on all data'+':'+str(metrics.accuracy_score(y_true=tr_dat_y_e1[int(len(tr_dat_y_e1)*0.8):],y_pred=preds_e1))

# Performance of both classifiers on non cart data
ftr2_ptrdat_x,ftr2_ptrdat_y = build_single_sess_dat(sorted_sessions_ftr2)
ftr2_lbls = transform_labels(ftr2_ptrdat_y)
tr_dat_y_e4 = np.asarray(ftr2_lbls,dtype='string')
preds_e4 = predictor_e3.predict(vctr2.transform(to_lof_strings(ftr2_ptrdat_x[int(len(ftr2_ptrdat_x)*0.8):])))
print 'accuracy on non cart data alternate'+':'+str(metrics.accuracy_score(y_true=tr_dat_y_e4[int(len(tr_dat_y_e4)*0.8):],y_pred=preds_e4))

# Classification with partial sessions on non cart data
#sb1,sb2,sb3,sb4 = divide_4_parts(ftr2_ptrdat_x[int(len(ftr2_ptrdat_x)*0.8):],ftr2_ptrdat_y[int(len(ftr2_ptrdat_y)*0.8):])
#sb1,sb2,sb3,sb4 = divide_4_parts(ftr3_ptrdat_x[int(len(ftr3_ptrdat_x)*0.8):],ftr3_ptrdat_y[int(len(ftr3_ptrdat_y)*0.8):])
sb1,sb2,sb3,sb4 = divide_4_parts(ss_ptrdat_x[int(len(ss_ptrdat_x)*0.8):],ss_ptr_dat_y[int(len(ss_ptr_dat_y)*0.8):])
sbx = sb1
tr_sb1_x = vctr.transform(to_lof_strings(sbx[0]))
tr_sb1_y_p = transform_labels(sbx[1]); tr_sb1_y = np.asarray(tr_sb1_y_p,dtype='string')
preds_part = predictor_e1.predict(tr_sb1_x)
conf_score = predictor_e1.decision_function(tr_sb1_x)
'''sbx = sb2
tr_sb1_x = vctr.transform(to_lof_strings(sbx[0]))
tr_sb1_y_p = transform_labels(sbx[1]); tr_sb1_y = np.asarray(tr_sb1_y_p,dtype='string')
preds_part2 = predictor_e1.predict(tr_sb1_x)
conf_score2 = predictor_e1.decision_function(tr_sb1_x)'''
print 'accuracy with 20%'+':'+str(metrics.accuracy_score(y_true=tr_sb1_y,y_pred=preds_part))
print 'f1_score with 20%'+':'+str(metrics.f1_score(y_true=tr_sb1_y,y_pred=preds_part,pos_label=None))
cf_mt = metrics.confusion_matrix(y_true=tr_sb1_y,y_pred=preds_part)
print 'precision with 20%'+':'+str(float(cf_mt[0][0])/(cf_mt[0][0]+cf_mt[1][0]))
print 'recall with 20%'+':'+str(float(cf_mt[0][0])/(cf_mt[0][0]+cf_mt[0][1]))
print 'confusion_matrix'+':'+str(metrics.confusion_matrix(y_true=tr_sb1_y,y_pred=preds_part))

# Classification based on the number/percentage of cart events
crt_tr_x,crt_tr_y = divide_on_cart_events(ss_ptrdat_x[int(len(ss_ptrdat_x)*0.8):],ss_ptr_dat_y[int(len(ss_ptr_dat_y)*0.8):])
crt_tr_x1,crt_tr_y1 = divide_on_cart_events(ss_ptrdat_x[0:int(len(ss_ptrdat_x)*0.8)],ss_ptr_dat_y[0:int(len(ss_ptr_dat_y)*0.8)])
predictor_e5 = train_on_cart_cop(crt_tr_x1,crt_tr_y1,1,vctr,clf8)
print 'metrics pertaining to percentage of cart events begin'
predict_on_cart_cop(crt_tr_x,crt_tr_y,1,vctr,predictor_e5)
print 'metrics pertaining to percentage of cart events end'

# Cart Position based Classifier Ensemble
'''len_set1 = [10,25,40,65]
dat_cltr_x,dat_cltr_y = data_for_ensemble_cart(crt_tr_x1,crt_tr_y1,len_set1)'''

# Length Based Classifier Ensemble
len_set2 = [30,60,90]
dat_ltr_x,dat_ltr_y = data_for_ensemble_lng(ss_ptrdat_x[0:int(len(ss_ptrdat_x)*0.8)],ss_ptr_dat_y[0:int(len(ss_ptr_dat_y)*0.8)],len_set2)
lng_clf_ls = train_length_ensemble(dat_ltr_x,dat_ltr_y,len_set2,clf_ls,vctr)
#lng_clf_ls2 = [predictor_e5 for it in range(0,4)]
print 'metrics pertaining to length ensemble begin'
predict_with_lng_ens(ss_ptrdat_x[int(len(ss_ptrdat_x)*0.8):],ss_ptr_dat_y[int(len(ss_ptr_dat_y)*0.8):],lng_clf_ls,len_set2,vctr)
print 'metrics  pertaining to length ensemble end'

# Training after removing abandoned checkout sessions
ftr_dat_xy = potential_purchaser_labeling(ss_ptrdat_x,ss_ptr_dat_y)
ftr_dat_y_ar = np.asarray(ftr_dat_xy[1],dtype='string')
cv_accuracy = cv.cross_val_score(clf1,vctr.transform(to_lof_strings(ftr_dat_xy[0])),ftr_dat_y_ar,cv=5)
print 'cv_accuracy after removing checkout abandoned sessions'
print cv_accuracy
# Compare with the classifier trained on entire data
ftr_dat_xy_comp = potential_purchaser_labeling(ss_ptrdat_x[int(len(ss_ptrdat_x)*0.8):],ss_ptr_dat_y[int(len(ss_ptr_dat_y)*0.8):])
ftr_dat_y_ar_comp = np.asarray(ftr_dat_xy_comp[1],dtype='string')
comp_preds = predictor_e1.predict(vctr.transform(to_lof_strings(ftr_dat_xy_comp[0])))
print 'Compare accuracy with the classifier trained on entire data'
print 'accuracy'+':'+str(metrics.accuracy_score(y_true=ftr_dat_y_ar_comp,y_pred=comp_preds))
# Check the predictions for abandoned checkout sessions
predictor_sub = clf8.fit(vctr.transform(to_lof_strings(ftr_dat_xy[0][0:int(len(ftr_dat_xy[0])*0.8)])),ftr_dat_y_ar[0:int(len(ftr_dat_y_ar)*0.8)])
subset_xy = potential_purchaser_labeling(ss_ptrdat_x,ss_ptr_dat_y,0)
preds_sub1 = predictor_sub.predict(vctr.transform(to_lof_strings(subset_xy[1][0])))
print 'accuracy for checkout abandoned sessions'+':'+str(metrics.accuracy_score(y_true=subset_xy[1][1],y_pred=preds_sub1))

# Train to identify checkout abandoned sessions
ca_pdat_x = []; ca_pdat_x.extend(subset_xy[1][0][0:int(len(subset_xy[1][0])*0.8)]); ca_pdat_x.extend(subset_xy[2][0][0:int(len(subset_xy[2][0])*0.8)])
ca_pdat_y = []; ca_pdat_y.extend(subset_xy[1][1][0:int(len(subset_xy[1][1])*0.8)]); ca_pdat_y.extend(subset_xy[2][1][0:int(len(subset_xy[2][1])*0.8)])
ca_dat_x,ca_dat_y = sk_shuffle(ca_pdat_x,ca_pdat_y)
ca_dat_y_ar = np.asarray(ca_dat_y,dtype='string')
predictor_seq = clf9.fit(vctr.transform(to_lof_strings(ca_dat_x)),ca_dat_y_ar)
'''fs1.fit(vctr.transform(to_lof_strings(ca_dat_x)),ca_dat_y_ar)
top1000 = sorted_feature_names(fs1,vctr)'''
cv_accuracy1 = cv.cross_val_score(clf9,vctr.transform(to_lof_strings(ca_dat_x)),ca_dat_y_ar,cv=5)
#cv_f1 = cv.cross_val_score(clf9,vctr.transform(to_lof_strings(ca_dat_y_ar)),ca_dat_y_ar,cv=5,scoring='f1')
print 'accuracy of identifying checkout abandoned sessions'
print cv_accuracy1

# Use predictor_sub for partial session classification
sb1,sb2,sb3,sb4 = divide_4_parts(ftr_dat_xy[0][int(len(ftr_dat_xy[0])*0.8):],ftr_dat_y_ar[int(len(ftr_dat_xy[1])*0.8):])

# Performance of predictor_sub on the entire dataset.
ts_sub_x = []; ts_sub_y = []
ts_sub_x.extend(ftr_dat_xy[0][int(len(ftr_dat_xy[0])*0.8):])
ts_sub_x.extend(subset_xy[1][0][int(len(subset_xy[1][0])*0.8):])
ts_sub_y.extend(ftr_dat_y_ar[int(len(ftr_dat_y_ar)*0.8):])
ts_sub_y.extend(subset_xy[1][1][int(len(subset_xy[1][1])*0.8):])

ts_preds = predictor_sub.predict(vctr.transform(to_lof_strings(ts_sub_x)))
print 'accuracy of predictor trained after removing chkt abd data on entire data'+':'+str(metrics.accuracy_score(y_true=ts_sub_y,y_pred=ts_preds))
# Seqential prediction using predictor_sub(sub1+3) and predictor_seq(sub2+3)
new_preds = sequential_prediction_correction(predictor_seq,ts_preds,vctr.transform(to_lof_strings(ts_sub_x)))
print 'accuracy after correction with second classifier'+':'+str(metrics.accuracy_score(y_true=ts_sub_y,y_pred=new_preds))

# Corrections using Sessions following checkout abandoned sessions
comp_corr_x,comp_crr_y = data_for_checkout_correction(sorted_sessions,1,3)
sh_comp_corr_x,sh_comp_corr_y = sk_shuffle(comp_corr_x,comp_crr_y)
tr_sh_comp_corr_y = np.asarray(sh_comp_corr_y,dtype='string')
cv_accuracy_comp = cv.cross_val_score(clf9,vctr.transform(to_lof_strings(sh_comp_corr_x)),tr_sh_comp_corr_y,cv=5)
print 'cv accuracy after relabeling checkout abandoned sessions'
print cv_accuracy_comp

# Per(n)Click accuracy with single classifiers
new_preds_pc = predict_x_clicks(clf1,ss_ptrdat_x[int(len(ss_ptrdat_x)*0.8):],vctr,5)
pc_acc = per_click_accuracy(new_preds_pc,tr_dat_y_e1[int(len(tr_dat_y_e1)*0.8):])
# Per(n)Click accuracy with sequential classifiers
new_preds_pcs = predict_x_clicks_alt(predictor_sub,predictor_seq,ss_ptrdat_x[int(len(ss_ptrdat_x)*0.8):],vctr,5)
pcs_acc = per_click_accuracy(new_preds_pcs,tr_dat_y_e1[int(len(tr_dat_y_e1)*0.8):])

# Build multiple session classifier
'''ms_ptrdat_x,ms_ptr_dat_y = build_multi_sess_dat_t2(sorted_sessions)
tfr_labels_e2 = transform_labels(ms_ptr_dat_y)
tr_dat_y_e2 =  np.asarray(tfr_labels_e2,dtype='string')
tr_dat_x_e2 = vctr.transform(to_lof_strings(ms_ptrdat_x[0:int(len(ms_ptrdat_x)*0.8)]))
predictor_e12 = clf1.fit(tr_dat_x_e2,tr_dat_y_e2[0:int(len(tr_dat_y_e2)*0.8)])
#predictor_e22 = clf5.fit(tr_dat_x_e2,tr_dat_y_e2[0:int(len(tr_dat_y_e2)*0.8)])
preds_e2 = predictor_e12.predict(vctr.transform(to_lof_strings(ms_ptrdat_x[int(len(ms_ptrdat_x)*0.8):])))
print 'accuracy'+':'+str(metrics.accuracy_score(y_true=tr_dat_y_e2[int(len(tr_dat_y_e2)*0.8):],y_pred=preds_e2))
print 'f1_score'+':'+str(metrics.f1_score(y_true=tr_dat_y_e2[int(len(tr_dat_y_e2)*0.8):],y_pred=preds_e2,pos_label=None))
print 'confusion_matrix'+':'+str(metrics.confusion_matrix(y_true=tr_dat_y_e2[int(len(tr_dat_y_e2)*0.8):],y_pred=preds_e2))'''



