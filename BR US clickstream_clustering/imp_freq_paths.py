import freq_paths as fp
import path_vis as pv

###############################
##### Usage of freq_paths #####
###############################

save_count_file = 'count_store_all.txt'
train_dat = '000000_0'
p_tr_dat_xp, p_tr_dat_xt,p_tr_dat_y = fp.proc_split_raw_dat(train_dat)
dat_subset2 = fp.filter_dataset_1(p_tr_dat_xp,p_tr_dat_xt,p_tr_dat_y)

vctr = fp.basic_trans_dat(dat_subset2[0])
# Clustering of sessions of certain length range
grp1 = fp.grouping_function(500,0.90)
grp2 = fp.second_grouping_function(grp1)
#
#all_silh_scores = get_cluster_silh_scores(dat_subset2[0],grp2,vctr)
#clustering_ident_params(dat_subset2[0],grp2,vctr)
#
sess_gps = fp.clustering_dataset_decomposition(dat_subset2[0],grp2)
#sess_gps_lp = landing_page_dataset_decomposition(dat_subset2[0])

req_sess = fp.get_sess_length_range(dat_subset2[0],sess_gps[0])
req_lbls = fp.get_sess_length_range(dat_subset2[2],sess_gps[0])
tr_req_sess = vctr.transform(fp.to_lof_strings_alt(req_sess))

no_cls = 4
kmc = fp.clstr.KMeans(n_clusters=no_cls,init='k-means++',max_iter=500)
kmc.fit(tr_req_sess)
cpreds = kmc.predict(tr_req_sess)
#clsx = get_cluster_dat(req_sess,cpreds,0,1)
clsx,clsxl = fp.get_cluster_dat_wl(req_sess,req_lbls,cpreds,0,1)
# Processing for cluster vizualization
ftr_sess = fp.to_lof_strings_alt(clsx)
ftr2_sess = fp.ftr_clean_paths(ftr_sess)
count_store,vmap = fp.get_all_ngram_counts(ftr2_sess)
fvmap = fp.flatten_vmap(vmap)
fp.save_trans_counts(save_count_file,fvmap,count_store)


###############################
##### Usage of path_vis #####
###############################

input_data_file = 'count_store_all.txt'

raw_count_dat,vtx_names = pv.load_dat(input_data_file)
np_count_dat = pv.convert_np(raw_count_dat)

Gx = pv.convert_to_graph(raw_count_dat,vtx_names)
pv.draw_sorted_subgraph(Gx,10)
