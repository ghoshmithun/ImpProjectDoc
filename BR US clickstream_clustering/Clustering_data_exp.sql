SET mapred.job.queue.name = cem;

-- One month Data Required for Clustering - BR
create table nkoul_clk.cls_dat_on_1m_dec as
select t1.country,t1.cust_hit_time_gmt,t1.daily_visitor,t1.first_hit_pagename,t1.first_hit_page_url,t1.first_hit_referrer,
t1.first_hit_time_gmt,t1.geo_country,t1.geo_city,t1.hitid_high,t1.hitid_low,t1.hit_time_gmt,t1.hourly_visitor,t1.last_hit_time_gmt,
t1.last_purchase_num,t1.last_purchase_time_gmt,t1.monthly_visitor,t1.pagename,t1.page_type,t1.post_event_list,t1.post_page_event,
t1.post_visid_high,t1.post_visid_low,t1.post_visid_type,t1.prop1,t1.unk_shop_id,t1.session_id,t1.customer_key,t1.purchaseid,
t1.referrer,t1.search_engine,t1.visit_search_engine,t1.ref_type,t1.search_page_num,t1.transactionid, t1.visid_high,t1.visid_low,t1.visit_num,t1.visid_new,
t1.connection_type,t1.domain,t1.ref_domain,t1.duplicate_events,t1.visit_keywords,t1.visit_start_pagename,t1.visit_start_page_url,
t1.visit_start_time_gmt,t1.date_time,t1.geo_region,t1.geo_zip,t1.homepage,t1.prop33,t_time_info,click_action,click_action_type,click_context
from omniture.hit_data_t t1
where date_time between '2016-12-01' and '2016-12-31' and (split(pagename, '[:]')[0]=='on' or split(pagename, '[:]')[1]=='on' or instr(pagename,'on:browse')>0 or instr(pagename,'mobile:on')>0);

-- Group by session identifiers and collect required fields
create table nkoul_clk.cls_dat_on_conc_sess_dat_dec as
select visid_low,visid_high,visit_num, collect_set(concat_ws('__',prop33,pagename,t_time_info)) cks, collect_set(purchaseid) prcid, 
collect_set(unk_shop_id) ukey, collect_set(last_purchase_num) last_pn
from nkoul_clk.cls_dat_on_1m_dec group by visid_low,visid_high,visit_num;
-- 9,845,163 - br_apr
-- 12,081,401 - br_dec
-- 25,460,451 - gp_dec
-- 19,278,794 - gp_apr
-- 32,353,254 - on_apr
-- 41,474,682 - on_dec

-- Filter the entries without a masterkey
create table nkoul_clk.cls_dat_on_conc_sess_dat_ftr_dec as
select * from nkoul_clk.cls_dat_on_conc_sess_dat_dec where size(ukey) >= 1;
-- 9,773,630 - br_apr
-- 11,959,099 - br_dec
-- 25,035,445 - gp_dec
-- 19,021,996 - gp_apr
-- 31,895,420 - on_apr
-- 40,430,354 - on_dec

-- Create a version of dataset with mkey as identifer
create table nkoul_clk.cls_dat_on_conc_sess_mkidx_dec as
select ukey[1] ukey,cks,last_pn,prcid from nkoul_clk.cls_dat_on_conc_sess_dat_ftr_dec
order by ukey;

-- Filter
create table nkoul_clk.cls_dat_on_conc_sess_mkidx_ftr_dec as
select * from nkoul_clk.cls_dat_on_conc_sess_mkidx_dec where ukey <> '' order by ukey;

-- Export the data
INSERT OVERWRITE LOCAL DIRECTORY '/home/xnkoul/dataset4' ROW FORMAT DELIMITED SELECT * FROM nkoul_clk.cls_dat_on_conc_sess_mkidx_ftr_dec;



