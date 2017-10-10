import time
import datetime

import numpy as np
import scipy as sp

import cPickle as pickle
import matplotlib.pyplot as plt

#
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
			if len(proc_hist_nz) > 1:
				raw_dat[spl_line[0]] = proc_hist_nz
				ccount += 1.0;
		else:
			ccount += 1.0
	fh.close()
	return raw_dat

#
def init_pop_delta(max_range=367):
	delta_counts = {}
	for itr in range(0,max_range):
		delta_counts[itr] = 0
	return delta_counts

# Population Level Occurence of Various Repurchase Times
def population_level_delta_counts(all_data,delta_counts,limit=0):
	for key,value in all_data.iteritems():
		if len(value) >= limit:
			for it1,it2 in zip(value,value[1:]):
				try:
					delta_counts[(it2[0]-it1[0]).days] += 1
				except KeyError:
					pass
	return None

# The Repurchase Counts for All Feature Combinations.
def joint_repurchase_ftr_counts_ext(all_data,count_store):
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
	cdate = datetime.datetime(2016,7,1)
	for key,value in all_data.iteritems():
		if len(value) <= 30:
			idx2 = len(value)
			idx3 = get_le_elapse(value,cdate)
			idx5 = discretize_variance(value,ls_var)
			idx4 = month_boundary(value)
			idx6 = alternate_timescale(value,cdate)
			for it1,it2 in zip(value,value[1:]):
				idx1 = round((it2[0]-it1[0]).days/7.0)
				count_store[idx1,idx2-1,idx3,idx4,idx5,idx6] += 1
	return None

# The joint Counts of Various Features.
def dist_normalizer_ext(all_data,count_store):
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
	cdate = datetime.datetime(2016,7,1)
	for key,value in all_data.iteritems():
		if len(value) <= 30:
			idx1 = len(value)
			idx2 = get_le_elapse(value,cdate)
			idx4 = discretize_variance(value,ls_var)
			idx3 = month_boundary(value)
			idx5 = alternate_timescale(value,cdate)
			count_store[idx1-1,idx2,idx3,idx4,idx5] += 1
	return None

data_file = '0000_all_o2'
ts_data_file = '0000_all'
count_store_file = 'count_store_nf.pkl'
norm_count_store_file = 'norm_count_store_nf.pkl'
prior_dist_file = 'prior_dist_nf.pkl'

min_val1 = 0000000
max_val1 = 2000000
min_val2 = 0
max_val2 = 1000000000

count_store = np.zeros((53,30,53,3,5,2))
norm_count_store = np.zeros((30,53,3,5,2))
pop_dist = init_pop_delta(367)

#count_store = pickle.load(open(count_store_file_nf,'r'))
#norm_count_store = pickle.load(open(norm_count_store_file_nf,'r'))
#pop_dist = pickle.load(open(prior_dist_file_nf,'r'))

all_data = data_parser(data_file,min_val1,max_val1)

population_level_delta_counts(all_data,pop_dist,1)
joint_repurchase_ftr_counts_ext(all_data,count_store)
dist_normalizer_ext(all_data,norm_count_store)

pickle.dump(count_store,open(count_store_file,'w'))
pickle.dump(norm_count_store,open(norm_count_store_file,'w'))
pickle.dump(pop_dist,open(prior_dist_file,'w'))

