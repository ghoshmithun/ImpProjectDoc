----- File last updated - 5th July 2017

-- Set hive options
use ssoni;
set mapred.job.queue.name=bdload;  
set hive.cli.print.header=true;

--Set the variables. Run this everytime running hive. Also, change these parameters when initiating a new scoring script
set first_quarter_start_date = '2016-04-01';
set first_quarter_end_date = '2016-06-30';
set second_quarter_start_date = '2016-07-01';
set second_quarter_end_date = '2016-09-30';
set third_quarter_start_date = '2016-10-01';
set third_quarter_end_date = '2016-12-31';
set fourth_quarter_start_date = '2017-01-01';
set fourth_quarter_end_date = '2017-03-31';
set date_for_datediff = '2017-04-01'; --- purch_window_end_date + 1

--For browsing and cart additions use the fourth quarter dates only


--Get the data of crmanalytics stored at a place.
--drop table if exists ssoni.crmanalyticsdata;
create table ssoni.crmanalyticsdata stored as orc as 
select distinct masterkey,customerkey,unknownshopperid
from crmanalytics.customerresolution
where customerkey is not null;
--352,465,005 (142,159,419 null unknownshopperid)



--Check browsing history of last quarter same year
--drop table if exists ssoni.at_lastqrtr_clicks_score3;
create table ssoni.at_lastqrtr_clicks_score3 stored as orc as 
select distinct
unk_shop_id,
visit_num,
pagename,
purchaseid,
country,
t_time_info,
substr(prop1,1,6) as style_cd  
from omniture.hit_data_t 
where (instr(pagename,'at:browse')>0 or instr(pagename,'mobile:at')>0) and (date_time) between ${hiveconf:fourth_quarter_start_date} and ${hiveconf:fourth_quarter_end_date} and prop33 = 'product'; 
-- run 25,524,363



--drop table if exists ssoni.at_product_clicks_desc_activity_master_notnull_score3;
create table ssoni.at_product_clicks_desc_activity_master_notnull_score3 stored as orc as 
select d.masterkey,c.activity,a.*,b.mdse_corp_desc
from ssoni.at_lastqrtr_clicks_score3 a
inner join (select distinct style_cd,mdse_corp_desc from mds_new.ods_product_t where  mdse_corp_desc='ATH DIR') b 
on a.style_cd=b.style_cd
inner join ssoni.at_product_activity_list c
on b.style_cd = c.style_cd
left outer join (select distinct unknownshopperid,masterkey from ssoni.crmanalyticsdata ) d
on a.unk_shop_id=d.unknownshopperid
where d.masterkey is not null;
-- run 17,321,138

--drop table if exists ssoni.at_clicks_mk_activity_score3;
create table ssoni.at_clicks_mk_activity_score3 stored as orc as 
select masterkey,activity,count(distinct t_time_info) as items_browsed
from ssoni.at_product_clicks_desc_activity_master_notnull_score3
group by masterkey,activity;
-- run 2,306,038

-------------------------------Browsing history section ends--------------------------------


-------------------------------Abandon basket section starts--------------------------------

--drop table if exists ssoni.at_bagadd_score3;
create table ssoni.at_bagadd_score3 stored as orc as 
select distinct
unk_shop_id,
visit_num,
pagename,
purchaseid,
country,
t_time_info,
product_list,
substr(prop1,1,6) as style_cd  
from omniture.hit_data_t
where (instr(pagename,'at:browse')>0 or instr(pagename,'mobile:at')>0) and (date_time) between ${hiveconf:fourth_quarter_start_date} and ${hiveconf:fourth_quarter_end_date} and prop33='inlineBagAdd';
-- run 4,559,541






--drop table if exists ssoni.at_purchasenull_score3;
create table ssoni.at_purchasenull_score3 stored as orc as 
select distinct unk_shop_id,visit_num
from ssoni.at_lastqrtr_clicks_score3
where unk_shop_id != "" and (PurchaseID IS NULL or PurchaseID=='' or PurchaseID==' ');
-- run 6,566,156


--drop table if exists ssoni.at_abandon_basket_product_master_notnull_score3;
create table ssoni.at_abandon_basket_product_master_notnull_score3 stored as orc  as 
select  a.* , c.activity,d.masterkey
from ssoni.at_bagadd_score3 a 
inner join  ssoni.at_purchasenull_score3 b 
on a.unk_shop_id=b.unk_shop_id and a.visit_num=b.visit_num
inner join ssoni.at_product_activity_list c
on a.style_cd=c.style_cd
left outer join (select distinct unknownshopperid,masterkey from ssoni.crmanalyticsdata) d
on a.unk_shop_id=d.unknownshopperid
where d.masterkey is not null;
-- run 3,580,091



--drop table if exists ssoni.at_abandon_mk_activity_score3;
create table ssoni.at_abandon_mk_activity_score3 stored as orc as 
select masterkey,activity,count(distinct t_time_info) as items_abandoned
from ssoni.at_abandon_basket_product_master_notnull_score3
group by masterkey,activity;
-- 1,054,895

-------------------------------Abandon basket section ends--------------------------------



------------------------------Last year purchase history quarterwise section starts-----------------------------------


--drop table if exists ssoni.at_first_qrtr_purch_score3;
create table ssoni.at_first_qrtr_purch_score3 stored as orc as 
select customer_key,item_qty,product_key
from mds_new.ods_orderline_t 
where brand='AT' and country='US' 
and unix_timestamp(transaction_date,'yyyy-MM-dd') between ${hiveconf:first_quarter_start_date} and ${hiveconf:first_quarter_end_date}
and order_status in ('R','O') and item_qty>0 and sales_amt>0;
-- run 3,349,299

--drop table if exists ssoni.at_first_qrtr_master_notnull_score3;
create table ssoni.at_first_qrtr_master_notnull_score3 stored as orc as
select a.customer_key,a.item_qty,a.product_key,b.mdse_corp_desc,c.style_cd,c.activity,d.masterkey
from ssoni.at_first_qrtr_purch_score3 a 
inner join mds_new.ods_product_t b 
on a.product_key=b.product_key
inner join ssoni.at_product_activity_list c
on b.style_cd = c.style_cd
left outer join (select distinct customerkey,masterkey from ssoni.crmanalyticsdata) d
on a.customer_key=d.customerkey
where d.masterkey is not null and b.mdse_corp_desc = 'ATH DIR';
-- run 1,875,522



--drop table if exists ssoni.at_first_qrtr_mk_activity_score3;
create table ssoni.at_first_qrtr_mk_activity_score3 stored as orc as 
select masterkey,activity,sum(item_qty) as items_purch_first_qrtr
from ssoni.at_first_qrtr_master_notnull_score3 
group by masterkey,activity;
-- run 903,511

----------------Second quarter------------


--drop table if exists ssoni.at_second_qrtr_purch_score3;
create table ssoni.at_second_qrtr_purch_score3 stored as orc as 
select customer_key,item_qty,product_key
from mds_new.ods_orderline_t 
where brand='AT' and country='US' 
and unix_timestamp(transaction_date,'yyyy-MM-dd') between ${hiveconf:second_quarter_start_date} and ${hiveconf:second_quarter_end_date}
and order_status in ('R','O') and item_qty>0 and sales_amt>0;
-- run 3,158,135

--drop table if exists ssoni.at_second_qrtr_master_notnull_score3;
create table ssoni.at_second_qrtr_master_notnull_score3 stored as orc as
select a.customer_key,a.item_qty,a.product_key,b.mdse_corp_desc,c.style_cd,c.activity,d.masterkey
from ssoni.at_second_qrtr_purch_score3 a 
inner join mds_new.ods_product_t b 
on a.product_key=b.product_key
inner join ssoni.at_product_activity_list c
on b.style_cd = c.style_cd
left outer join (select distinct customerkey,masterkey from ssoni.crmanalyticsdata) d
on a.customer_key=d.customerkey
where d.masterkey is not null and b.mdse_corp_desc = 'ATH DIR';
-- run 2,800,172



--drop table if exists ssoni.at_second_qrtr_mk_activity_score3;
create table ssoni.at_second_qrtr_mk_activity_score3 stored as orc as 
select masterkey,activity,sum(item_qty) as items_purch_second_qrtr
from ssoni.at_second_qrtr_master_notnull_score3
group by masterkey,activity;
-- run 1,086,583


---------------------Third quarter details ---------------------

--drop table if exists ssoni.at_third_qrtr_purch_score3;
create table ssoni.at_third_qrtr_purch_score3 stored as orc as 
select customer_key,item_qty,product_key
from mds_new.ods_orderline_t 
where brand='AT' and country='US' 
and unix_timestamp(transaction_date,'yyyy-MM-dd') between ${hiveconf:third_quarter_start_date} and ${hiveconf:third_quarter_end_date}
and order_status in ('R','O') and item_qty>0 and sales_amt>0;
-- run 3,479,143


--drop table if exists ssoni.at_third_qrtr_master_notnull_score3;
create table ssoni.at_third_qrtr_master_notnull_score3 stored as orc as
select a.customer_key,a.item_qty,a.product_key,b.mdse_corp_desc,c.style_cd,c.activity,d.masterkey
from ssoni.at_third_qrtr_purch_score3 a 
inner join mds_new.ods_product_t b 
on a.product_key=b.product_key
inner join ssoni.at_product_activity_list c
on b.style_cd = c.style_cd
left outer join (select distinct customerkey,masterkey from ssoni.crmanalyticsdata) d
on a.customer_key=d.customerkey
where d.masterkey is not null and  b.mdse_corp_desc = 'ATH DIR' ;
-- run 3,279,152


--drop table if exists ssoni.at_third_qrtr_mk_activity_score3;
create table ssoni.at_third_qrtr_mk_activity_score3 stored as orc as 
select masterkey,activity,sum(item_qty) as items_purch_third_qrtr
from ssoni.at_third_qrtr_master_notnull_score3 
group by masterkey,activity;
-- run 1,246,702


---------------------Fourth quarter details -----------------

--drop table if exists ssoni.at_fourth_qrtr_purch_score3;
create table ssoni.at_fourth_qrtr_purch_score3 stored as orc as 
select customer_key,item_qty,product_key
from mds_new.ods_orderline_t 
where brand='AT' and country='US' 
and unix_timestamp(transaction_date,'yyyy-MM-dd') between ${hiveconf:fourth_quarter_start_date} and ${hiveconf:fourth_quarter_end_date}
and order_status in ('R','O') and item_qty>0 and sales_amt>0;
-- run 2,530,590

--drop table if exists ssoni.at_fourth_qrtr_master_notnull_score3;
create table ssoni.at_fourth_qrtr_master_notnull_score3 stored as orc as
select a.customer_key,a.item_qty,a.product_key,b.mdse_corp_desc,c.style_cd,c.activity,d.masterkey
from ssoni.at_fourth_qrtr_purch_score3 a 
inner join mds_new.ods_product_t b 
on a.product_key=b.product_key
inner join ssoni.at_product_activity_list c
on b.style_cd = c.style_cd
left outer join (select distinct customerkey,masterkey from ssoni.crmanalyticsdata) d
on a.customer_key=d.customerkey
where d.masterkey is not null and b.mdse_corp_desc = 'ATH DIR';
-- run 


--drop table if exists ssoni.at_fourth_qrtr_mk_activity_score3;
create table ssoni.at_fourth_qrtr_mk_activity_score3 stored as orc as 
select masterkey,activity,sum(item_qty) as items_purch_fourth_qrtr
from ssoni.at_fourth_qrtr_master_notnull_score3 
group by masterkey,activity;
-- run 


------------------------------Last year purchase history quarterwise section ends-----------------------------------


----------------------------- Recency of browse and abandon section starts-------------------------------------------

--drop table if exists ssoni.at_abandon_unixtime_score3;
create table ssoni.at_abandon_unixtime_score3 stored as orc as 
select a.*,b.date_time
from ssoni.at_abandon_basket_product_master_notnull_score3 a
inner join 
(select * from omniture.hit_data_t where date_time between ${hiveconf:fourth_quarter_start_date} and ${hiveconf:fourth_quarter_end_date}) b 
on a.unk_shop_id=b.unk_shop_id and             
a.visit_num=b.visit_num  and        
a.pagename=b.pagename and             
a.purchaseid=b.purchaseid and         
a.country=b.country  and               
a.t_time_info=b.t_time_info;
-- run 3,600,835


--drop table if exists ssoni.at_abandon_maxdt_score3;
create table ssoni.at_abandon_maxdt_score3 stored as orc as 
select masterkey,activity,round(datediff(${hiveconf:date_for_datediff},max(date_time))/90,2) as recency_abandon
from ssoni.at_abandon_unixtime_score3
group by masterkey,activity;
-- run 1,054,895

--drop table if exists ssoni.at_clicks_unixtime_score3;
create table ssoni.at_clicks_unixtime_score3 as 
select a.*,b.date_time
from ssoni.at_product_clicks_desc_activity_master_notnull_score3 a
inner join 
(select * from omniture.hit_data_t where date_time between ${hiveconf:fourth_quarter_start_date} and ${hiveconf:fourth_quarter_end_date}) b 
on a.unk_shop_id=b.unk_shop_id and             
a.visit_num=b.visit_num  and        
a.pagename=b.pagename and             
a.purchaseid=b.purchaseid and         
a.country=b.country  and               
a.t_time_info=b.t_time_info;
-- run 17,350,597

--drop table if exists ssoni.at_clicks_maxdt_score3;
create table ssoni.at_clicks_maxdt_score3 stored as orc as 
select masterkey,activity,round(datediff(${hiveconf:date_for_datediff},max(date_time))/90,2) as recency_click
from ssoni.at_clicks_unixtime_score3
group by masterkey,activity;
-- run 2,306,038


----------------------------- Recency of browse and abandon section ends-------------------------------------------



-- This table combines the masterkeys and activities from all the tables
--drop table if exists ssoni.at_all_mk_activity_score3;
create table ssoni.at_all_mk_activity_score3 stored as orc as 
select distinct combined.masterkey,combined.activity from 
(
select a.masterkey,a.activity from ssoni.at_clicks_mk_activity_score3 a
union all select b.masterkey,b.activity from ssoni.at_abandon_mk_activity_score3 b
union all select d.masterkey,d.activity from ssoni.at_first_qrtr_mk_activity_score3 d
union all select e.masterkey,e.activity from ssoni.at_second_qrtr_mk_activity_score3 e
union all select f.masterkey,f.activity from ssoni.at_third_qrtr_mk_activity_score3 f
union all select g.masterkey,g.activity from ssoni.at_fourth_qrtr_mk_activity_score3 g
) combined ;
-- run 4,235,612

-- This table adds the variables as columns to the above table
--drop table if exists ssoni.at_all_variables_score3
create table ssoni.at_all_variables_score3 stored as orc as 
select a.masterkey,a.activity, coalesce(b.items_browsed,0) as items_browsed,
coalesce(c.items_abandoned,0) as items_abandoned,
coalesce(e.items_purch_first_qrtr,0) as items_purch_first_qrtr,
coalesce(f.items_purch_second_qrtr,0) as items_purch_second_qrtr,
coalesce(g.items_purch_third_qrtr,0) as items_purch_third_qrtr,
coalesce(h.items_purch_fourth_qrtr,0) as items_purch_fourth_qrtr,
coalesce(i.recency_abandon,1) as recency_abandon,
coalesce(j.recency_click,1) as recency_click
from ssoni.at_all_mk_activity_score3 a
left outer join ssoni.at_clicks_mk_activity_score3 b on a.masterkey=b.masterkey and a.activity=b.activity
left outer join ssoni.at_abandon_mk_activity_score3 c on a.masterkey=c.masterkey and a.activity=c.activity
left outer join ssoni.at_first_qrtr_mk_activity_score3 e on a.masterkey=e.masterkey and a.activity=e.activity
left outer join ssoni.at_second_qrtr_mk_activity_score3 f on a.masterkey=f.masterkey and a.activity=f.activity
left outer join ssoni.at_third_qrtr_mk_activity_score3 g on a.masterkey=g.masterkey and a.activity=g.activity
left outer join ssoni.at_fourth_qrtr_mk_activity_score3 h on a.masterkey=h.masterkey and a.activity=h.activity
left outer join ssoni.at_abandon_maxdt_score3 i on a.masterkey=i.masterkey and a.activity=i.activity
left outer join ssoni.at_clicks_maxdt_score3 j on a.masterkey=j.masterkey and a.activity=j.activity;
-- run 4,235,612

--This table groups the variables by masterkey so as to normalize in the next table
--drop table if exists ssoni.at_all_variables_grouped_score3;
create table ssoni.at_all_variables_grouped_score3 stored as orc as
select masterkey,sum(items_abandoned) as sum_items_abandoned,
sum(items_browsed) as sum_items_browsed,
sum(items_purch_first_qrtr) as sum_items_purch_first_qrtr,
sum(items_purch_second_qrtr) as sum_items_purch_second_qrtr,
sum(items_purch_third_qrtr) as sum_items_purch_third_qrtr,
sum(items_purch_fourth_qrtr) as sum_items_purch_fourth_qrtr
from ssoni.at_all_variables_score3
group by masterkey;
-- run 2,136,390

--drop table if exists ssoni.at_final_table_score3;
create table ssoni.at_final_table_score3 stored as orc as 
select a.masterkey,a.activity,
(case when b.sum_items_abandoned >0 then a.items_abandoned/b.sum_items_abandoned else 0.0 end) as nrml_items_abandoned,
(case when b.sum_items_browsed >0 then a.items_browsed/b.sum_items_browsed else 0.0 end) as nrml_items_browsed,
(case when b.sum_items_purch_first_qrtr >0 then a.items_purch_first_qrtr/b.sum_items_purch_first_qrtr else 0.0 end) as nrml_items_purch_first_qrtr,
(case when b.sum_items_purch_second_qrtr >0 then a.items_purch_second_qrtr/b.sum_items_purch_second_qrtr else 0.0 end) as nrml_items_purch_second_qrtr,
(case when b.sum_items_purch_third_qrtr >0 then a.items_purch_third_qrtr/b.sum_items_purch_third_qrtr else 0.0 end) as nrml_items_purch_third_qrtr,
(case when b.sum_items_purch_fourth_qrtr >0 then a.items_purch_fourth_qrtr/b.sum_items_purch_fourth_qrtr else 0.0 end) as nrml_items_purch_fourth_qrtr,
a.recency_abandon,a.recency_click
from ssoni.at_all_variables_score3 a
left outer join at_all_variables_grouped_score3 b on a.masterkey = b.masterkey ;
-- run 4,235,612

------------------------------Final table for initial analysis above------------------------

--drop table if exists ssoni.at_final_table_score3_custkey;
create table ssoni.at_final_table_score3_custkey stored as orc as
select b.customerkey,a.*
from ssoni.at_final_table_score3 a
inner join (select distinct customerkey, masterkey from ssoni.crmanalyticsdata) b
 on a.masterkey = b.masterkey ;
 --run 8,995,408





