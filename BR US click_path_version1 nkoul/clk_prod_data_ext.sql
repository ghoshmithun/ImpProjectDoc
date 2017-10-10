SET mapred.job.queue.name = cem;
--Select Required columns from the ClickStream Data and filter on br
--create table nkoul_clk.clk_sample_7d_br as
create table nkoul_clk.clk_sample_3m_br as
select t1.country,t1.cust_hit_time_gmt,t1.daily_visitor,t1.first_hit_pagename,t1.first_hit_page_url,t1.first_hit_referrer,
t1.first_hit_time_gmt,t1.geo_country,t1.geo_city,t1.hitid_high,t1.hitid_low,t1.hit_time_gmt,t1.hourly_visitor,t1.last_hit_time_gmt,
t1.last_purchase_num,t1.last_purchase_time_gmt,t1.monthly_visitor,t1.pagename,t1.page_type,t1.post_event_list,t1.post_page_event,
t1.post_visid_high,t1.post_visid_low,t1.post_visid_type,t1.prop1,t1.unk_shop_id,t1.session_id,t1.customer_key,t1.purchaseid,
t1.referrer,t1.search_engine,t1.visit_search_engine,t1.ref_type,t1.search_page_num,t1.transactionid, t1.visid_high,t1.visid_low,t1.visit_num,t1.visid_new,
t1.connection_type,t1.domain,t1.ref_domain,t1.duplicate_events,t1.visit_keywords,t1.visit_start_pagename,t1.visit_start_page_url,
t1.visit_start_time_gmt,t1.date_time,t1.geo_region,t1.geo_zip,t1.homepage,t1.prop33,t_time_info,click_action,click_action_type,click_context
from omniture.hit_data_t t1
where date_time between '2015-02-01' and '2015-05-30' and split(pagename, '[:]')[0]=='br';

--Select Required columns from the ClickStream Data without any brand filter
create table nkoul_clk.clk_sample_1m as
select t1.country,t1.cust_hit_time_gmt,t1.daily_visitor,t1.first_hit_pagename,t1.first_hit_page_url,t1.first_hit_referrer,
t1.first_hit_time_gmt,t1.geo_country,t1.geo_city,t1.hitid_high,t1.hitid_low,t1.hit_time_gmt,t1.hourly_visitor,t1.last_hit_time_gmt,
t1.last_purchase_num,t1.last_purchase_time_gmt,t1.monthly_visitor,t1.pagename,t1.page_type,t1.post_event_list,t1.post_page_event,
t1.post_visid_high,t1.post_visid_low,t1.post_visid_type,t1.prop1,t1.unk_shop_id,t1.session_id,t1.customer_key,t1.purchaseid,
t1.referrer,t1.search_engine,t1.visit_search_engine,t1.ref_type,t1.search_page_num,t1.transactionid, t1.visid_high,t1.visid_low,t1.visit_num,t1.visid_new,
t1.connection_type,t1.domain,t1.ref_domain,t1.duplicate_events,t1.visit_keywords,t1.visit_start_pagename,t1.visit_start_page_url,
t1.visit_start_time_gmt,t1.date_time,t1.geo_region,t1.geo_zip,t1.homepage,t1.prop33,t_time_info,click_action,click_action_type,click_context
from omniture.hit_data_t t1
where date_time between '2015-03-01' and '2015-03-30';

-- Selected columns from clickstream + mds customer key & masterkey
create table nkoul_clk.clk_cid_1 as
select t1.*, t2.customerkey, t2.masterkey, t2.emailkey, t2.unknownshopperid
from nkoul_clk.clk_sample_3m_br t1 left outer join (select distinct unknownshopperid, customerkey, masterkey, emailkey 
from crmanalytics.customerresolution) t2
on t1.unk_shop_id = t2.unknownshopperid;

-- Unfiltered by br Selected columns from clickstream + mds customer key & masterkey
create table nkoul_clk.clk_cid_1_nf as
select t1.*, t2.customerkey, t2.masterkey, t2.emailkey, t2.unknownshopperid
from nkoul_clk.clk_sample_1m t1 left outer join (select distinct unknownshopperid, customerkey, masterkey, emailkey 
from crmanalytics.customerresolution) t2
on t1.unk_shop_id = t2.unknownshopperid;

-- Subset of orderheader table Date range = Upper Range of clickstream table +3 days.
create table nkoul_clk.lim_ord_table as
select * from mds.ods_orderheader_t where ship_date between '2015-03-01' and '2015-04-4';

-- Associate transaction number, order status, order type with the clickstream table from the orderheader table
create table nkoul_clk.online_purchase as 
select t1.*, t2.transaction_num,t2.order_status,order_type,demand_date,ship_date,t2.order_num,t2.brand from nkoul_clk.clk_cid_1 t1 left outer join nkoul_clk.lim_ord_table t2
on (t1.purchaseid = t2.order_num and t1.customerkey = t2.customer_key);

-- Subset of orderline table Date range - same as order header table
create table nkoul_clk.lim_ord_line_table as
select * from mds.ods_orderline_t where transaction_date between '2015-03-01' and '2015-04-04';

-- Join the product ids from the orderline table to the clickstream data
create table nkoul_clk.online_purchase_pk as
select t1.* ,t2.product_key, t2.item_qty from nkoul_clk.online_purchase t1 left outer join nkoul_clk.lim_ord_line_table t2
on (t1.transaction_num = t2.transaction_num and t1.customerkey = t2.customer_key);

-- Product keys in the clickstream
create table nkoul_clk.prod_ids as
select distinct (product_key) from nkoul_clk.online_purchase_pk; 
-- Product details of the product keys in the clickstream
create table nkoul_clk.product_details as
select t1.product_key, t2.mdse_div_desc,t2.mdse_dept_desc,t2.mdse_class_desc,t2.style_cd, t2.season_desc,t2.season_cd,t2.prod_desc
from nkoul_clk.prod_ids t1 inner join mds.ods_product_t t2
on (t1.product_key = t2.product_key);

-- Append product detail information to clickstream data
create table nkoul_clk.online_purchase_pk_pd as
select t1.*, t2.mdse_div_desc,t2.mdse_dept_desc,t2.mdse_class_desc,t2.style_cd, t2.season_desc,t2.season_cd,t2.prod_desc
from nkoul_clk.online_purchase_pk t1 left outer join nkoul_clk.product_details t2
on (t1.product_key = t2.product_key);

---- Queries pertaining to pagename & prop33, eventid & session----

-- Limited session table
create table nkoul_clk.lim_sess_info as
select * from crmanalytics.clickstreamsession_v2
where datetime between '2015-03-01' and '2015-03-08';

-- Session info + clickstream info
create table nkoul_clk.sess_info_clk as
select t1.masterkey,t1.customerkey,t1.unknownshopperid,t2.purchaseids,t2.sessionduration
from nkoul_clk.online_purchase t1 inner join nkoul_clk.lim_sess_info t2 where array_contains(t2.unknownshopperids,t1.unk_shop_id);

-- Alternate link session info and clickstream info
create table nkoul_clk.link_sess_clk as
select t1.masterkey, t1.customerkey,t1.unknownshopperid,t2.purchaseids,t2.sessionduration
from nkoul_clk.online_purchase t1 inner join nkoul_clk.lim_sess_info t2 
on (t1.visid_high = t2.sessionidhigh and t1.visid_low = t2.sessionidlow and t1.visit_num = t2.visitnum);

-- Selected Session Information pertaining to purchases
create table nkoul_clk.sample_sess_info as
select masterkey,customerkey,product_key,item_qty,duplicate_events,prop33,pagename,post_event_list,session_id,
prod_desc,season_desc
from online_purchase_pk_pd where purchaseid is not null and item_qty > 0;

---- Session Features ----

-- Products viewed in a session
create table nkoul_clk.sess_prod_views as
select visid_low,visid_high,visit_num, count(prop1) a_ct, count(distinct(prop1)) d_ct from nkoul_clk.clk_sample_1m_br
group by visid_low, visid_high, visit_num;
--
create table nkoul_clk.sess_prod_views_dist as
select visid_low,visid_high,visit_num, count(prop1) a_ct, count(distinct(prop1)) d_ct from nkoul_clk.clk_sample_1m_br
group by visid_low, visid_high, visit_num order by a_ct desc;
-- Cross Distributions of product views and visit start page
create table nkoul_clk.prod_startpage_predist as
select visid_low,visid_high, visit_num, visit_start_pagename ,count(prop1) a_ct, count(distinct(prop1)) d_ct
from nkoul_clk.clk_sample_1m_br group by visid_low, visid_high, visit_num, visit_start_pagename;
--
create table nkoul_clk.prod_startpage_dist as
select visit_start_pagename, a_ct from nkoul_clk.prod_startpage_predist
group by visit_start_pagename,a_ct order by a_ct desc;
-- Distributions on last purchase number and products viewed
create table nkoul_clk.prcnum_cadd as
select visid_low, visid_high,visit_num,last_purchase_num,count(prop1) a_ct, count(distinct(prop1)) d_ct
from nkoul_clk.clk_sample_1m_br group by visid_low, visid_high, visit_num, last_purchase_num;
--
create table nkoul_clk.prcnum_cadd_d_dist as
select last_purchase_num, d_ct from nkoul_clk.prcnum_cadd
group by last_purchase_num, d_ct order by last_purchase_num desc;
--
create table nkoul_clk.prcnum_cadd_dist as
select last_purchase_num, a_ct from nkoul_clk.prcnum_cadd
group by last_purchase_num, a_ct order by last_purchase_num desc;

-- Cart removals related dists
create table nkoul_clk.sess_cart_action_remove as
select visid_low,visid_high,visit_num,prop33,pagename,click_action from nkoul_clk.clk_sample_1m_br 
where click_action_type = 3 and click_action = "remove item";
--
create table nkoul_clk.action_remove_dist as
select visid_low,visid_high,visit_num,count(click_action) count_ra from nkoul_clk.sess_cart_action_remove
group by visid_low, visid_high, visit_num order by count_ra desc;
-- select count_ra, count(count_ra) from nkoul_clk.action_remove_dist group by count_ra;
-- Cart additions related dists
create table nkoul_clk.sess_cart_action_add as
select visid_low, visid_high,visit_num,prop33,pagename,click_action from nkoul_clk.clk_sample_1m_br
where prop33 = 'inlineBagAdd';
--
create table nkoul_clk.action_add_dist as
select visid_low,visid_high,visit_num,count(prop33) count_aa from nkoul_clk.sess_cart_action_add
group by visid_low, visid_high, visit_num order by count_aa desc;
-- select count_aa,count(count_aa) from nkoul_clk.action_add_dist group by count_aa;

-- Distributions of cart actions and visit start page
-- Cart additions + visit start page
create table nkoul_clk.add_start_dd as
select t1.visid_low, t1.visid_high, t1.visit_num,t1.visit_start_pagename, count(t2.count_aa) ca
from nkoul_clk.action_add_dist t2 inner join nkoul_clk.clk_sample_1m_br t1
on t2.visid_low = t1.visid_low and t2.visid_high = t1.visid_high and t2.visit_num = t1.visit_num
group by t1.visid_low, t1.visid_high, t1.visit_num, t1.visit_start_pagename order by ca desc;
--
create table nkoul_clk.add_start_dist as
select visit_start_pagename, ca caa from nkoul_clk.add_start_dd
group by visit_start_pagename,ca order by  caa desc;

-- Cart removals + visit start page
create table nkoul_clk.remove_start_dd as
select t1.visid_low, t1.visid_high, t1.visit_num,t1.visit_start_pagename, count(t2.count_ra) ra
from nkoul_clk.action_remove_dist t2 inner join nkoul_clk.clk_sample_1m_br t1
on t2.visid_low = t1.visid_low and t2.visid_high = t1.visid_high and t2.visit_num = t1.visit_num
group by t1.visid_low, t1.visid_high, t1.visit_num, t1.visit_start_pagename order by ra desc;
--
create table nkoul_clk.remove_start_dist as
select visit_start_pagename, ra raa from nkoul_clk.remove_start_dd
group by visit_start_pagename,ra order by  raa desc;

-- Distributions of cart actions with last purchase number
-- Cart additions + last purchase number
create table nkoul_clk.add_prchnum_dd as
select t1.visid_low, t1.visid_high, t1.visit_num,t1.last_purchase_num, count(t2.count_aa) ca
from nkoul_clk.action_add_dist t2 inner join nkoul_clk.clk_sample_1m_br t1
on t2.visid_low = t1.visid_low and t2.visid_high = t1.visid_high and t2.visit_num = t1.visit_num
group by t1.visid_low, t1.visid_high, t1.visit_num, t1.last_purchase_num order by ca desc;
--
create table nkoul_clk.add_prchnum_dist as
select last_purchase_num, ca caa from nkoul_clk.add_prchnum_dd
group by last_purchase_num, ca order by  last_purchase_num desc;
-- Cart removals + last purchase number
create table nkoul_clk.remove_prchnum_dd as
select t1.visid_low, t1.visid_high, t1.visit_num,t1.last_purchase_num, count(t2.count_ra) ra
from nkoul_clk.action_remove_dist t2 inner join nkoul_clk.clk_sample_1m_br t1
on t2.visid_low = t1.visid_low and t2.visid_high = t1.visid_high and t2.visit_num = t1.visit_num
group by t1.visid_low, t1.visid_high, t1.visit_num, t1.last_purchase_num order by ra desc;
--
create table nkoul_clk.remove_prchnum_dist as
select last_purchase_num, ra raa from nkoul_clk.remove_prchnum_dd
group by last_purchase_num,ra order by  last_purchase_num desc;

-- Data cols for n-gram generation
create table nkoul_clk.ngram_prc as
select visid_low,visid_high,visit_num,prop33,pagename,click_context,t_time_info,purchaseid
from nkoul_clk.clk_sample_1m_br;
-- Aggregrate the clicks and times for a session
create table nkoul_clk.ngram_cmp as
select visid_low,visid_high,visit_num, collect_set(concat_ws('__',prop33,pagename,t_time_info)) cks
from nkoul_clk.ngram_prc group by visid_low,visid_high,visit_num;
-- Purchase ids for the data
create table nkoul_clk.ngram_lbl as
select visid_low,visid_high,visit_num, collect_set(purchaseid) prcid
from nkoul_clk.ngram_prc group by visid_low,visid_high,visit_num;
-- Combine the above two tables to create a labeled dataset
create table nkoul_clk.lab_ng_cmp as
select t1.visid_low, t1.visid_high, t1.visit_num, t1.cks, t2.prcid
from nkoul_clk.ngram_cmp t1 inner join nkoul_clk.ngram_lbl t2 on
t1.visid_low = t2.visid_low and t1.visid_high = t2.visid_high and t1.visit_num = t2.visit_num;

-- Transform the session data
create table nkoul_clk.trs_lbls2 as
select transform(cks) using 'python hive_col_trans.py' as c_cks from nkoul_clk.ngram_cmp;

-- Use collect set on multiple data points
create table nkoul_clk.conc_sess_dat as
select visid_low,visid_high,visit_num, collect_set(concat_ws('__',prop33,pagename,t_time_info)) cks, collect_set(purchaseid) prcid,
collect_set(customerkey) ckey, collect_set(masterkey) mkey, collect_set(last_purchase_num) last_pn
from nkoul_clk.clk_cid_1 group by visid_low,visid_high,visit_num;
-- Filter the entries without a masterkey
create table nkoul_clk.conc_sess_dat_ftr as
select * from nkoul_clk.conc_sess_dat where size(mkey) >= 1;
-- Create a version of dataset with mkey as identifer
create table nkoul_clk.conc_sess_mkidx as
select mkey[0] mkey,cks,last_pn,prcid from nkoul_clk.conc_sess_dat_ftr
order by mkey;
-- Export the data
--INSERT OVERWRITE LOCAL DIRECTORY '/home/xnkoul/dataset2' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' SELECT * FROM nkoul_clk.conc_sess_mkidx;
INSERT OVERWRITE LOCAL DIRECTORY '/home/xnkoul/dataset2' ROW FORMAT DELIMITED SELECT * FROM nkoul_clk.conc_sess_mkidx;
-- Count of sessions for each masterkey
create table nkoul_clk.repeat_visit_dist as
select count(mkey)dcount, mkey from nkoul_clk.conc_sess_mkidx group by mkey order by dcount desc;
-- Distribution of no of visits per 
create table nkoul_clk.repeat_count_dist as
select count(dcount) ddcount, dcount from nkoul_clk.repeat_visit_dist group by dcount;

--No of purchases for each masterkey
create table nkoul_clk.mkey_req_val as
select mkey from nkoul_clk.repeat_visit_dist where (dcount == 3);
-- Sessions for a specific value of count of the no of sessions
create table nkoul_clk.sess_req_val as
select t1.mkey,t2.cks,t2.last_pn,t2.prcid from
nkoul_clk.mkey_req_val t1 inner join nkoul_clk.conc_sess_mkidx t2 on
t1.mkey = t2.mkey;
INSERT OVERWRITE LOCAL DIRECTORY '/home/xnkoul/dataset2' ROW FORMAT DELIMITED SELECT * FROM nkoul_clk.sess_req_val;

--hive -e 'select * from some_table' > /home/yourfile.csv

-- Dists related to daily visitors
create table nkoul_clk.dly_trans_f as
select visid_low,visid_high,visit_num,prop1,unk_shop_id from nkoul_clk.clk_sample_1m where daily_visitor = 1;
-- Distribution of country
create table nkoul_clk.dist_ctry as
select country, count(unk_shop_id) from nkoul_clk.clk_sample_1m group by country;
-- Distribution of products viewed
create table nkoul_clk.dist_prod as
select prop1, count(unk_shop_id) from nkoul_clk.clk_sample_1m group by prop1;
-- Distribution of search engines
create table nkoul_clk.dist_srch as
select search_engine, count(unk_shop_id) from nkoul_clk.clk_sample_1m group by search_engine;
-- Distribution of domain
create table nkoul_clk.dist_domain as
select domain, count(unk_shop_id) from nkoul_clk.clk_sample_1m group by domain;
-- Homepage Distribution
create table nkoul_clk.dist_homepage as
select homepage, count(unk_shop_id) from nkoul_clk.clk_sample_1m group by homepage;
-- Last Purchase Number Distribution
create table nkoul_clk.dist_lpn as
select last_purchase_num, count(unk_shop_id) from nkoul_clk.clk_sample_1m group by last_purchase_num;
-- Visit Start Page Name Distribution
create table nkoul_clk.dist_vsp as
select visit_start_pagename, count(unk_shop_id) from nkoul_clk.clk_sample_1m group by visit_start_pagename;
-- Search Page Distribution
create table nkoul_clk.dist_srch_pg as
select search_page_num, count(unk_shop_id) from nkoul_clk.clk_sample_1m group by search_page_num;
-- Distribution of daily, monthly, hourly visitors
create table nkoul_clk.dist_monthly as
select monthly_visitor,count(unk_shop_id) from nkoul_clk.clk_sample_1m group by monthly_visitor;
--






