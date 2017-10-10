import time
import datetime

import numpy as np
import scipy as sp

import cPickle as pickle
import matplotlib.pyplot as plt

import sklearn.metrics as metrics
import sklearn.linear_model as lm
import sklearn.cross_validation as cv

def data_parser(data_file,count_min=0,count_max=1000000):
	def parse_history(prch_list):
		rval = []
		for item in prch_list:
			spl_item = item.split('__')
			if len(spl_item) == 3:
				spl_item.append('0.0')
			alt_spl_item = []
			alt_spl_item.append(datetime.datetime.strptime(spl_item[0]+' '+'0:0:0','%Y-%m-%d %H:%M:%S'))
			for datx in spl_item[1:]:
				alt_spl_item.append(float(datx))
			if len(alt_spl_item) == 4:
				rval.append(alt_spl_item)
		return sorted(rval,key = lambda x:x[0])
	def combine_same_day_prchs(prch_list):
		def pos_ws_sum(value):
			return list(sum(np.asarray(value)))
		red_prch_list = []
		date_dict = {}
		for pitem in prch_list:
			date_dict[pitem[0].strftime('%Y-%m-%d')] = []
		for pitem in prch_list:
			date_dict[pitem[0].strftime('%Y-%m-%d')].append(pitem[1:])
		for key,value in date_dict.iteritems():
			if len(value) > 1:
				iv = [datetime.datetime.strptime(key+' '+'0:0:0','%Y-%m-%d %H:%M:%S')]
				sum_arr = pos_ws_sum(value)
				iv.extend(sum_arr)
				red_prch_list.append(iv)
			else:
				iv = [datetime.datetime.strptime(key+' '+'0:0:0','%Y-%m-%d %H:%M:%S')]
				iv.extend(value[0])
				red_prch_list.append(iv)
		return sorted(red_prch_list, key = lambda x:x[0])
	def remove_zero_events(events):
		new_list = []
		for event in events:
			if event[1] > 0:
				new_list.append(event)
		return new_list
	raw_dat = {}; ccount = 0.0
	fh = open(data_file,'r')
	for line in fh:
		if ccount >= count_min and ccount <= count_max:
			rs_line = line.rstrip()
			spl_line = rs_line.split('\x01')
			spl_line_l2 = spl_line[1].split('\x02')
			alt_spl_line_l2 = parse_history(spl_line_l2)
			proc_hist = combine_same_day_prchs(alt_spl_line_l2)
			proc_hist_nz = remove_zero_events(proc_hist)
			if len(proc_hist_nz) >= 1:
				raw_dat[spl_line[0]] = proc_hist_nz
				ccount += 1.0;
		else:
			ccount += 1.0
	fh.close()
	return raw_dat

#
def data_parser_alt(data_file,rkeys,count_min=0,count_max=1000000):
	def parse_history(prch_list):
		rval = []
		for item in prch_list:
			spl_item = item.split('__')
			if len(spl_item) == 3:
				spl_item.append('0.0')
			alt_spl_item = []
			alt_spl_item.append(datetime.datetime.strptime(spl_item[0]+' '+'0:0:0','%Y-%m-%d %H:%M:%S'))
			for datx in spl_item[1:]:
				alt_spl_item.append(float(datx))
			if len(alt_spl_item) == 4:
				rval.append(alt_spl_item)
		return sorted(rval,key = lambda x:x[0])
	def combine_same_day_prchs(prch_list):
		def pos_ws_sum(value):
			return list(sum(np.asarray(value)))
		red_prch_list = []
		date_dict = {}
		for pitem in prch_list:
			date_dict[pitem[0].strftime('%Y-%m-%d')] = []
		for pitem in prch_list:
			date_dict[pitem[0].strftime('%Y-%m-%d')].append(pitem[1:])
		for key,value in date_dict.iteritems():
			if len(value) > 1:
				iv = [datetime.datetime.strptime(key+' '+'0:0:0','%Y-%m-%d %H:%M:%S')]
				sum_arr = pos_ws_sum(value)
				iv.extend(sum_arr)
				red_prch_list.append(iv)
			else:
				iv = [datetime.datetime.strptime(key+' '+'0:0:0','%Y-%m-%d %H:%M:%S')]
				iv.extend(value[0])
				red_prch_list.append(iv)
		return sorted(red_prch_list, key = lambda x:x[0])
	def remove_zero_events(events):
		new_list = []
		for event in events:
			if event[1] > 0:
				new_list.append(event)
		return new_list
	raw_dat = {}; ccount = 0.0
	fh = open(data_file,'r')
	for line in fh:
		if ccount >= count_min and ccount <= count_max:
			rs_line = line.rstrip()
			spl_line = rs_line.split('\x01')
			if spl_line[0] in rkeys:
				spl_line_l2 = spl_line[1].split('\x02')
				alt_spl_line_l2 = parse_history(spl_line_l2)
				proc_hist = combine_same_day_prchs(alt_spl_line_l2)
				proc_hist_nz = remove_zero_events(proc_hist)
				if len(proc_hist_nz) >= 1:
					raw_dat[spl_line[0]] = proc_hist_nz
					ccount += 1.0;
		else:
			ccount += 1.0
	fh.close()
	return raw_dat

# Compose the day level Repurchase Counts to Yield Week Level Counts.
def compose_intervals(data_dict,interval=7):
	count = 0
	new_ls=[]; cv = []
	for key,value in data_dict.iteritems():
		if key != 0:
			if count < interval:
				cv.append(value)
				count += 1
			else:
				new_ls.append(sum(cv))
				cv = []; count = 1
				cv.append(value)
	return new_ls

# Function Generating/Specifying the Prior
def apply_multp_factor(ls_ip,asp=1,bs=0):
	def rand_increasing_func(ct,a=asp,b=bs):
		rv = a*ct+b
		return rv
	rv = []
	for index,item in enumerate(ls_ip):
		rv.append(rand_increasing_func(index+1)*item)
	return rv

#
def modify_joint_apply_multp_factor(count_store,shape_func=None):
	for idx1 in range(0,30):
		for idx2 in range(0,53):
			for idx3 in range(0,3):
				for idx4 in range(0,5):
					for idx5 in range(0,2):
						tmv = list(count_store[:,idx1,idx2,idx3,idx4,idx5])
						nmv = apply_multp_factor(tmv,1,0)
						for idx0 in range(0,53):
							count_store[idx0,idx1,idx2,idx3,idx4,idx5] = nmv[idx0]
	return None

#
def modify_count_store_ones(all_data,all_ts_data,com_keys,count_store):
	def get_le_elapse(ls_prchs,cdate,ivl=7):
		els_time = float((cdate - ls_prchs[-1][0]).days)
		tr_els_time = round(els_time/float(ivl))
		return tr_els_time
	def month_boundary(ls_prchs):
		rv = 0
		if ls_prchs[-1][0].day >= 25:
			rv = 1
		if ls_prchs[-1][0].day <=6:
			rv = 2
		return rv
	cdate = datetime.datetime(2016,7,1)
	for key in com_keys:
		value = all_data[key]
		if len(value) <= 30:
			value_2 = all_ts_data[key];val2_cdate = value_2[0][0]
			idx1 = get_le_elapse(value,val2_cdate)
			idx2 = len(value)
			idx3 = get_le_elapse(value,cdate)
			idx4 = month_boundary(value)
			idx5 = 0
			if idx1 <= 51:
				count_store[idx1,idx2-1,idx3,idx4,idx5] += 1
	return None

#
def modify_norm_count_store_ones(all_data,all_ts_data,com_keys,count_store):
	def get_le_elapse(ls_prchs,cdate,ivl=7):
		els_time = float((cdate - ls_prchs[-1][0]).days)
		tr_els_time = round(els_time/float(ivl))
		return tr_els_time
	def month_boundary(ls_prchs):
		rv = 0
		if ls_prchs[-1][0].day >= 25:
			rv = 1
		if ls_prchs[-1][0].day <=6:
			rv = 2
		return rv
	cdate = datetime.datetime(2016,7,1)
	for key in com_keys:
		value = all_data[key]
		if len(value) <= 30:
			idx1 = len(value)
			idx2 = get_le_elapse(value,cdate)
			idx3 = month_boundary(value)
			idx4 = 0
			count_store[idx1-1,idx2,idx3,idx4] += 1
	return None

# Generate Features for all customers
def build_ftrs_bayes_alt(all_data,rkeys):
	ls_var = [135,270,405,540,675]
	#ls_var = [20,40,60,80,100]
	def discretize_variance(ls_prchs,ls_var):
		diffs = []
		for it1,it2 in zip(ls_prchs,ls_prchs[1:]):
			diffs.append(round((it2[0]-it1[0]).days/7.0))
		var_diffs = np.var(diffs)
		dis_var = [abs(var_diffs-item) for item in ls_var]
		rv = min(enumerate(dis_var),key = lambda x:x[1])[0]
		return rv
	def month_boundary(ls_prchs):
		rv = 0
		if ls_prchs[-1][0].day > 25:
			rv = 1
		if ls_prchs[-1][0].day <=6:
			rv = 2
		return rv
	def last_year_prch(ls_prchs,pred_date):
		year_val = pred_date.year-1
		month_val = pred_date.month
		cfg = 0
		for item in ls_prchs:
			if item[0].year == year_val and (item[0].month == month_val+1 or item[0].month == month_val -1):
				cfg = 1; break
		return cfg
	def alternate_timescale(ls_prchs,pred_date):
		efg = 0
		els_time = (pred_date-ls_prchs[-1][0]).days
		rng = float((ls_prchs[-1][0]-ls_prchs[0][0]).days)
		avg_time = rng/len(ls_prchs)
		if els_time >= avg_time:
			efg = 1
		else:
			efg = 0
		return efg
	feature_set = {}
	pred_date = datetime.datetime(2016,7,1)
	for key in rkeys:
		feature_set[key] = []
	#for key in rkeys:
		#rng = float((all_data[key][-1][0]-all_data[key][0][0]).days)
		#feature_set[key].append(round(rng/len(all_data[key])))
	for key in rkeys:
		pre_lng = len(all_data[key])
		if pre_lng <= 29:
			feature_set[key].append(len(all_data[key]))
		else:
			feature_set[key].append(29)
	for key in rkeys:
		feature_set[key].append(round((pred_date-all_data[key][-1][0]).days/7.0))
	for key in rkeys:
		feature_set[key].append(month_boundary(all_data[key]))
	for key in rkeys:
		feature_set[key].append(discretize_variance(all_data[key],ls_var))
	for key in rkeys:
		feature_set[key].append(alternate_timescale(all_data[key],pred_date))
	#for key in rkeys:
		#feature_set[key].append(last_year_prch(all_data[key],pred_date))
	return feature_set

# Generate Labels for All customers
def build_labels_bayes(ts_data,rkeys):
	labels = {}
	for key in rkeys:
		labels[key] = []
	for key in rkeys:
		try:
			if ts_data[key][0][0].month >= 7 and ts_data[key][0][0].month <= 9:
				rl = 1
			else:
				rl = 0
			labels[key].append(rl)
		except KeyError:
			labels[key].append(0)
	return labels

# Transform data to x(feature),y(label) format
def build_xy(ftrs,lbls,rkeys):
	ftr_ls = []; lbl_ls = []
	for key in rkeys:
		ftr_ls.append(ftrs[key])
		lbl_ls.append(lbls[key])
	rv1 = np.asarray(ftr_ls); rv2 = np.asarray(lbl_ls)
	return rv1,rv2

# Function used by predict_for_cust
def bayes_mult(pr,lk):
	prob = []
	for el1,el2 in zip(pr,lk):
		prob.append(el1*el2)
	return prob

# Use the Prior and Likelihood to Get the Posterior for Customer
def predict_for_cust(pr_dist,lk_dist,norm_dist,f1,f2,f3,f4,f5,fg=1):
	ps_dist_pre = bayes_mult(pr_dist,lk_dist[:,f1,f2,f3,f4,f5])
	if fg == 1:
		if norm_dist[f1,f2,f3,f4,f5] != 0:
			ps_dist = [item/norm_dist[f1,f2,f3,f4,f5] for item in ps_dist_pre]
		else:
			ps_dist = ps_dist_pre
	else:
		ps_dist = ps_dist_pre
	return ps_dist

# Generate Predictions for all customers by using the Posterior
# Returns Maxima and the corresponding Week Number
def score_custs_all(pr_dist,lk_dist,norm_dist,ftrs):
	cust_scores = []
	for item in ftrs:
		cust_score_pre = predict_for_cust(pr_dist,lk_dist,norm_dist,int(item[0]),int(item[1]),int(item[2]),int(item[3]),int(item[4]),1)
		if int(item[1]) < 20:
			max_idx = 0
			max_score = cust_score_pre[int(max_idx)]
			for index,elem in enumerate(cust_score_pre):
				if elem > max_score:
					max_score = elem; max_idx = item[1]+index
			cust_scores.append((max_idx-item[1],max_score))
		else:
			cust_scores.append((51,cust_score_pre[51]))
	return cust_scores

# Customers whose max scores lies in 13 weeks(3months)
def preds_alt(cust_scores):
	rv = []
	for item in cust_scores:
		if item[0] <= 13:
			rv.append(1)
		else:
			rv.append(0)
	return rv

#
def sort_by_scores(cust_scores):
	s_scores = []
	for index,item in enumerate(cust_scores):
		s_scores.append((index,item))
	rv = sorted(s_scores,key = lambda x:x[1][1], reverse=True)
	return rv

#
def scores_from_top(cust_scores,top_k,tr_lbls):
	ac_lbls = []
	count = 0
	for item in cust_scores:
		if count <= top_k:
			count += 1
			ac_lbls.append(tr_lbls[item[0]])
	return ac_lbls

#
def preds_from_top(cust_scores,top_k,ac_preds):
	pr_lbls = []
	count = 0
	for item in cust_scores:
		if count <= top_k:
			count += 1
			pr_lbls.append(ac_preds[item[0]])
	return pr_lbls

#
def write_data_scored(scores,predictions,true_labels,lim=0):
	ccount = 0.0
	fh = open('scoring_file_1month.txt','w')
	fh.write('model_raw_score'+','+'model_prediction'+','+'actual_label'+'\n')
	for it1,it2,it3 in zip(scores,predictions,true_labels):
		if ccount > lim:
			fh.write(str(it1[1])+ ',' + str(it2) + ',' + str(it3[0])+'\n')
		else:
			ccount +=1
	fh.close()
	return None

data_file = '0000_all_o2'
ts_data_file = '0000_all'
count_store_file = 'count_store_nf.pkl'
norm_count_store_file = 'norm_count_store_nf.pkl'
prior_dist_file = 'prior_dist_nf.pkl'

min_val1 = 0
max_val1 = 1000000
min_val2 = 0
max_val2 = 1000000000

all_data = data_parser(data_file,min_val1,max_val1)
tr_base = all_data.keys(); s_tr_base = set(tr_base)
ts_data = data_parser_alt(ts_data_file,s_tr_base,min_val2,max_val2)
nw_tr_base = ts_data.keys(); s_nw_tr_base = set(nw_tr_base)

min_ftrs = build_ftrs_bayes_alt(all_data,tr_base)
min_lbls = build_labels_bayes(ts_data,tr_base)
ftrs,lbls = build_xy(min_ftrs,min_lbls,tr_base)

count_store = pickle.load(open(count_store_file,'r'))
norm_count_store = pickle.load(open(norm_count_store_file,'r'))
pop_dist = pickle.load(open(prior_dist_file,'r'))

com_pop_dist = compose_intervals(pop_dist,7)
tm_cor_com_pop_dist = apply_multp_factor(com_pop_dist,1,0)
modify_joint_apply_multp_factor(count_store)

cust_scores_ext = score_custs_all(tm_cor_com_pop_dist,count_store,norm_count_store,ftrs)
preds_smp = preds_alt(cust_scores_ext)
sorted_scores = sort_by_scores(cust_scores_ext)
s_cust_lbls = scores_from_top(sorted_scores,1000000,lbls)
s_cust_preds = preds_from_top(sorted_scores,1000000,preds_smp)
metrics.roc_auc_score(s_cust_lbls,s_cust_preds)
#write_data_scored(cust_scores_ext,s_cust_preds,s_cust_lbls)

#modify_count_store_ones(all_data,ts_data,nw_tr_base,count_store)
#modify_norm_count_store_ones(all_data,ts_data,nw_tr_base,norm_count_store)
#pickle.dump(count_store,open(count_store_file,'w'))
#pickle.dump(norm_count_store,open(norm_count_store_file,'w'))



