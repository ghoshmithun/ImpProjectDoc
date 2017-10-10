import sys
import datetime
from collections import defaultdict
import random

import numpy as np
import scipy as sp
from scipy.sparse import csr_matrix
from scipy.sparse import vstack

import sklearn as sk
import sklearn.feature_extraction.text as txt
import sklearn.cluster as clstr
from sklearn.metrics import silhouette_samples, silhouette_score

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
			rv = pipeline1([spl_line_l1[1]])
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
			raw_dat_y.append(spl_line_l1[3])
		except:
			print 'Ecxeption handled'
			#pass
	fh.close()
	return raw_dat_x_p,raw_dat_x_t,raw_dat_y

# Remove items pertaining to customer services, short sessions and bounces
def filter_dataset_1(tr_dat_xp,tr_dat_xt,tr_dat_y):
	def trans_prod_pages(session):
		new_sess = []
		for click in session:
			if "br:browse" in click and "product" in click:
				new_sess.append('product')
			elif "br:browse:Hidden_Division_for_Promo_Exclusions" in click:
				new_sess.append('category')
			else:
				new_sess.append(click)
		return new_sess
	f_tr_dat_xp = []; f_tr_dat_xt = []; f_tr_dat_y = []
	for it1,it2,it3 in zip(tr_dat_xp,tr_dat_xt,tr_dat_y):
		if ('br: Store WiFi splash page with email entry' not in it1\
		and 'br:customerService:home' not in it1 and len(it1)> 2):
			tr_it1 = trans_prod_pages(it1)
			f_tr_dat_xp.append(tr_it1);f_tr_dat_xt.append(it2);f_tr_dat_y.append(it3)
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

#
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

def to_lof_strings_alt(dat):
	los = []
	for item in dat:
		ftr_item = [it2 for it2 in item]
		#los.append(' '.join(item))
		los.append(' '.join(ftr_item))
	return los

#
def basic_trans_dat(dat_xp,ftr_fg=0):
	def cust_token(sent):
		return sent.split(' ')
	vctrz = txt.CountVectorizer(ngram_range=(1,3),tokenizer=cust_token)
	#vctrz = txt.TfidfVectorizer(ngram_range=(1,2),tokenizer=cust_token)
	if ftr_fg == 0:
		dat_sent_form = to_lof_strings_alt(dat_xp)
	elif ftr_fg == 1:
		dat_sent_form = to_lof_strings_alt(dat_xp)
	#vctrz.fit(dat_sent_form[0:int(len(dat_sent_form)/2)])
	vctrz.fit(dat_sent_form)
	return vctrz

# Get a few sessions of length 20 and above
def get_few_sessions(all_sessions,req_count=10):
	def check_cond(sess):
		ck_fg = 0
		for click in sess:
			if ':' in click:
				ck_fg = 1
				break
		return ck_fg
	few_sess = []
	for sess in all_sessions:
		cfg = check_cond(sess)
		if len(sess) >=20 and cfg == 0:
			few_sess.append(sess)
		if len(few_sess) >= req_count:
			break
	return few_sess

# export sequence of clicks of raw sessions
def write_sess(all_sess):
	fh = open('sess_file.txt','w')
	for sess in all_sess:
		for click in sess:
			fh.write(click)
			fh.write(',')
		fh.write('\n')
	fh.close()
	return None

# export the vocabulary ordered by indexes
def write_vocab(vctrx):
	def invert_dict(vocab):
		new_dict = {}
		for key,value in vocab.iteritems():
			new_dict[value] = key
		return new_dict
	fh = open('vocab_file.txt','w')
	inv_vocab = invert_dict(vctrx.vocabulary_)
	for key in range(0,178):
		fh.write(inv_vocab[key].encode())
		fh.write(',')
		fh.write(str(key))
		fh.write('\n')
	fh.close()
	return None

# export the encoded sessions
def write_sessions(tr_sessx):
	fh = open('tr_sess_file.txt','w')
	for sess in tr_sess:
		tr1 = sess.toarray()
		tr2 = tr1.reshape(178)
		tr3 = map(str,list(tr2))
		fh.write(','.join(tr3))
		fh.write('\n')
	fh.close()
	return None

#
def length_dist_sess(all_sess):
	ln_set = list(set([len(item) for item in all_sess]))
	ln_dict = {}
	for item in ln_set:
		ln_dict[item] = 0
	for item in all_sess:
		ln_dict[len(item)] += 1
	return ln_dict

#
def grouping_function(max_len=500,thr=0.9):
	groups = []
	c_elem = 1; cgroup = [1]
	for itr in range(2,max_len):
		rat = c_elem/float(itr)
		if rat >= thr:
			cgroup.append(itr)
		else:
			c_elem = itr
			groups.append(cgroup); cgroup = [itr]
	return groups

#
def second_grouping_function(groups):
	con_groups = []
	len_elems = [len(item) for item in groups]
	set_len_elems = list(set(len_elems))
	for item in set_len_elems:
		con_list = []
		for item2 in groups:
			if len(item2) == item:
				con_list.append(item2)
		con_group = []
		for elem in con_list:
			con_group.extend(elem)
		con_groups.append(con_group)
	return con_groups

# group together sessions of a length group
def clustering_dataset_decomposition(sess_dat,len_groups):
	sess_lens = {}
	for itr in range(0,len(len_groups)):
		sess_lens[itr] = []
	for index,item in enumerate(sess_dat):
		sess_len = len(item)
		for index2,item2 in enumerate(len_groups):
			if sess_len in item2:
				sess_lens[index2].append(index)
	return sess_lens

# group together sessions based on landing page
def landing_page_dataset_decomposition(sess_dat):
	sess_lnd_pgs = {}
	for sess in sess_dat:
		sess_lnd_pgs[sess[0]] = []
	for index,item in enumerate(sess_dat):
		sess_lnd_pg = item[0]
		sess_lnd_pgs[sess_lnd_pg].append(index)
	return sess_lnd_pgs

#
def get_sess_length_range(all_sess,sess_gp):
	req_sess = []
	for item in sess_gp:
		req_sess.append(all_sess[item])
	return req_sess

#
def get_cluster_dat(raw_dat,cpreds,cno=0,smp_rate=1.0):
	ftr_dat = []
	for it1,it2 in zip(cpreds,raw_dat):
		if it1 == cno and random.random()< smp_rate:
			ftr_dat.append(it2)
	return ftr_dat

#
def get_cluster_dat_wl(raw_dat,raw_lbls,cpreds,cno=0,smp_rate=1.0):
	ftr_dat = []; ftr_lbl = []
	for it1,it2,it3 in zip(cpreds,raw_dat,raw_lbls):
		if it1 == cno and random.random()< smp_rate:
			ftr_dat.append(it2); ftr_lbl.append(it3)
	return ftr_dat,ftr_lbl

#
def get_cluster_stats(clr_pts,clr_lbls,smp_sz,o_smp_sz):
	conversion_rate = 0.0
	size_pct = 0.0
	o_size_pct = 0.0
	rvx = [1 for item in clr_lbls if item != '']
	conversion_rate = (len(rvx)/(1.0*len(clr_pts)))*100.0
	size_pct = (len(clr_pts)/(1.0*smp_sz))*100
	o_size_pct = (len(clr_pts)/(1.0*o_smp_sz))*100
	return conversion_rate,size_pct,o_size_pct

#
def get_cluster_silh_scores(all_sees,sess_grp,vctr):
	no_cls = [2,3,4,5,6,7,8,9,10]
	silh_scores = []
	sess_gps = clustering_dataset_decomposition(all_sees,sess_grp)
	rkeys_all = sess_gps.keys()
	rkeys = rkeys_all[2:14]
	for key in rkeys:
		req_sess = get_sess_length_range(all_sees,sess_gps[key])
		tr_req_sess = vctr.transform(to_lof_strings_alt(req_sess))
		lg_silh_scores = []
		for no_cl in no_cls:
			kmc = clstr.KMeans(n_clusters=no_cl,init='k-means++',max_iter=500)
			kmc.fit(tr_req_sess)
			cpreds = kmc.predict(tr_req_sess)
			silh_score = silhouette_score(tr_req_sess,cpreds)
			print 'silh_score ' + 'lng' + str(key) + str(no_cl)  +'-' + str(silh_score) + '\n'
			lg_silh_scores.append(silh_score)
		silh_scores.append(lg_silh_scores)
	return silh_scores

#
def get_cross_vec_silh_scores(all_sees,sess_grp,ls_vec):
	silh_scores = []
	for vec in ls_vec:
		silh_scores.append(get_cluster_silh_scores(all_sees,sess_grp,vec))
	return silh_scores

# For final Cluster Vzl
def clustering_ident_params(all_sess,sess_grp,vctr):
	sess_gps = clustering_dataset_decomposition(all_sees,sess_grp)
	cls_gps = range(2,14)
	no_cls = [3,4,4,4,4,2,2,2,3,3,3,4]
	for it1,it2 in zip(cls_gps,no_cls):
		req_sess = get_sess_length_range(all_sees,sess_gps[it1])
		tr_req_sess = vctr.transform(to_lof_strings_alt(req_sess))
		kmc = clstr.KMeans(n_clusters=it2,init='k-means++',max_iter=500)
		kmc.fit(tr_req_sess)
		cpreds = kmc.predict(tr_req_sess)
	return None

#
def ftr_clean_paths(raw_sess):
	cl_sess = []
	for sess in raw_sess:
		spl_sess = sess.split(' ')
		ftr_sess = [item for item in spl_sess if ':' not in item]
		cl_sess.append(' '.join(ftr_sess))
	return cl_sess

#
def get_all_ngram_counts(all_sess):
	vocab = set()
	for sess in all_sess:
		spl_sess = sess.split(' ')
		for word in spl_sess:
			vocab.add(word)
	rsz = len(list(vocab))
	count_store = np.zeros((rsz,rsz))
	vmap = {}
	for index,item in enumerate(list(vocab)):
		vmap[item] = index
	for sess in all_sess:
		spl_sess = sess.split(' ')
		for w1,w2 in zip(spl_sess,spl_sess[1:]):
			count_store[vmap[w1],vmap[w2]] += 1
	return count_store, vmap

#
def flatten_vmap(avmap):
	ord_list = []
	new_dict = {}
	for key,value in avmap.iteritems():
		new_dict[value] = key
	rsz = len(avmap.keys())
	for itr in range(0,rsz):
		ord_list.append(new_dict[itr])
	return ord_list

#
def save_trans_counts(filename,list_vertices,counts):
	hline = ','.join(list_vertices)
	fh = open(filename,'w')
	fh.write(hline+'\n')
	for idx in range(0,len(list_vertices)):
		hline = ','.join(map(str,counts[idx]))+'\n'
		fh.write(hline)
	fh.close()
	return None

#
def export_cluster_examples(all_sess,all_preds,filename,clr_gps=5,no_elems=20):
	fh = open(filename,'w')
	for itr in range(0,clr_gps):
		clsx = get_cluster_dat(req_sess,all_preds,itr,1)
		fh.write("Cluster "+str(itr+1)+'\n')
		for itr2 in range(0,no_elems):
			fh.write(','.join(clsx[itr2])+'\n')
		fh.write('\n')
	fh.close()
	return None

#
def get_count_prch(dat_subset2_2,sess_gps_0):
	lim_prch = []
	for item in sess_gps_0:
		if dat_subset2_2[item] != '':
			lim_prch.append(1.0)
	tot_prch = sum(lim_prch)
	den = len(sess_gps_0)
	conv_rate = (tot_prch/den)*100
	return conv_rate,tot_prch,den



