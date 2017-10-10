--- File last updated - 5th July 2017


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

--Get the data of crmanalytics stored at a place.
--drop table if exists ssoni.crmanalyticsdata;
create table ssoni.crmanalyticsdata stored as orc as 
select distinct masterkey,customerkey,unknownshopperid
from crmanalytics.customerresolution
where customerkey is not null;
--352,465,005 


--Check browsing history of last quarter same year
--drop table if exists ssoni.at_lastqrtr_clicks;
create table ssoni.at_lastqrtr_clicks as 
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
-- taking 2015 June to 2016 May for training and 2016 June to 2016 August for observation
--run  29,465,202



--drop table if exists ssoni.at_product_clicks_desc_activity_master_notnull;
create table ssoni.at_product_clicks_desc_activity_master_notnull as 
select d.masterkey,c.activity,a.*,b.mdse_corp_desc
from ssoni.at_lastqrtr_clicks a
inner join (select distinct style_cd,mdse_corp_desc from mds_new.ods_product_t where  mdse_corp_desc='ATH DIR') b 
on a.style_cd=b.style_cd
inner join ssoni.at_product_activity_list c
on b.style_cd = c.style_cd
left outer join (select distinct unknownshopperid,masterkey from crmanalytics.customerresolution ) d
on a.unk_shop_id=d.unknownshopperid
where d.masterkey is not null;
--run 10,700,544

--drop table if exists ssoni.at_clicks_mk_activity;
create table ssoni.at_clicks_mk_activity as 
select masterkey,activity,count(distinct t_time_info) as items_browsed
from ssoni.at_product_clicks_desc_activity_master_notnull
group by masterkey,activity;
--run 1,753,932

-------------------------------Browsing history section ends--------------------------------


-------------------------------Abandon basket section starts--------------------------------

--drop table if exists ssoni.at_bagadd;
create table ssoni.at_bagadd as 
select distinct
unk_shop_id,
visit_num,
pagename,
purchaseid,
country,
t_time_info,
substr(prop1,1,6) as style_cd  
from omniture.hit_data_t 
where (instr(pagename,'at:browse')>0 or instr(pagename,'mobile:at')>0) and (date_time) between ${hiveconf:fourth_quarter_start_date} and ${hiveconf:fourth_quarter_end_date} and prop33='inlineBagAdd';
--run 4,976,141


--drop table if exists ssoni.at_purchasenull;
create table ssoni.at_purchasenull as 
select distinct unk_shop_id,visit_num
from ssoni.at_lastqrtr_clicks
where unk_shop_id != "" and (PurchaseID IS NULL or PurchaseID=='' or PurchaseID==' ');
--run 106,974,000  (12,777,230 with distinct)


--drop table if exists ssoni.at_abandon_basket_product_master_notnull;
create table ssoni.at_abandon_basket_product_master_notnull  as 
select  a.* , c.*,d.masterkey
from ssoni.at_bagadd a 
inner join  ssoni.at_purchasenull b 
on a.unk_shop_id=b.unk_shop_id and a.visit_num=b.visit_num
inner join ssoni.at_product_activity_list c
on a.style_cd=c.style_cd
left outer join (select distinct unknownshopperid,masterkey from crmanalytics.customerresolution) d
on a.unk_shop_id=d.unknownshopperid
where d.masterkey is not null;
--run 136,332,431  (3,025,417 with distinct in at_purchasenull) 2,575,742



--drop table if exists ssoni.at_abandon_mk_activity;
create table ssoni.at_abandon_mk_activity as 
select masterkey,activity,count(distinct t_time_info) as items_abandoned
from ssoni.at_abandon_basket_product_master_notnull
group by masterkey,activity;
--run 819,159

-------------------------------Abandon basket section ends--------------------------------



------------------------------Target variable window section starts-----------------------------------

--drop table if exists ssoni.at_pred_window;
create table ssoni.at_pred_window as 
select customer_key,item_qty,product_key
from mds_new.ods_orderline_t 
where brand='AT' and country='US' 
and unix_timestamp(transaction_date,'yyyy-MM-dd') between date_add(${hiveconf:first_quarter_start_date},367) and date_add(${hiveconf:first_quarter_end_date},367)
and order_status in ('R','O') and item_qty>0 and sales_amt>0;
--run 3,441,907


--drop table if exists ssoni.at_pred_master_notnull;
create table ssoni.at_pred_master_notnull as
select a.customer_key,a.item_qty,a.product_key,b.mdse_corp_desc,c.style_cd,c.activity,d.masterkey
from ssoni.at_pred_window a 
inner join mds_new.ods_product_t b 
on a.product_key=b.product_key
inner join ssoni.at_product_activity_list c
on b.style_cd = c.style_cd
left outer join (select distinct customerkey,masterkey from crmanalytics.customerresolution) d
on a.customer_key=d.customerkey
where d.masterkey is not null and b.mdse_corp_desc = 'ATH DIR';
--run 3,242,840


--drop table if exists ssoni.at_pred_mk_activity;
create table ssoni.at_pred_mk_activity as 
select masterkey,activity,sum(item_qty) as items_purch_pred_window
from ssoni.at_pred_master_notnull 
group by masterkey,activity;
--run 1,234,167

------------------------------Target variable window section ends-----------------------------------


------------------------------Last year purchase history quarterwise section starts-----------------------------------


--drop table if exists ssoni.at_first_qrtr_purch;
create table ssoni.at_first_qrtr_purch as 
select customer_key,item_qty,product_key
from mds_new.ods_orderline_t 
where brand='AT' and country='US' 
and unix_timestamp(transaction_date,'yyyy-MM-dd') between ${hiveconf:first_quarter_start_date} and ${hiveconf:first_quarter_end_date}
and order_status in ('R','O') and item_qty>0 and sales_amt>0;
--run 3,342,392

--drop table if exists ssoni.at_first_qrtr_master_notnull;
create table ssoni.at_first_qrtr_master_notnull as
select a.customer_key,a.item_qty,a.product_key,b.mdse_corp_desc,c.style_cd,c.activity,d.masterkey
from ssoni.at_first_qrtr_purch a 
inner join mds_new.ods_product_t b 
on a.product_key=b.product_key
inner join ssoni.at_product_activity_list c
on b.style_cd = c.style_cd
left outer join (select distinct customerkey,masterkey from crmanalytics.customerresolution) d
on a.customer_key=d.customerkey
where d.masterkey is not null and b.mdse_corp_desc = 'ATH DIR';
--run 1,158,832



--drop table if exists ssoni.at_first_qrtr_mk_activity;
create table ssoni.at_first_qrtr_mk_activity as 
select masterkey,activity,sum(item_qty) as items_purch_first_qrtr
from ssoni.at_first_qrtr_master_notnull 
group by masterkey,activity;
--run 635,435

----------------Second quarter------------


--drop table if exists ssoni.at_second_qrtr_purch;
create table ssoni.at_second_qrtr_purch as 
select customer_key,item_qty,product_key
from mds_new.ods_orderline_t 
where brand='AT' and country='US' 
and unix_timestamp(transaction_date,'yyyy-MM-dd') between ${hiveconf:second_quarter_start_date} and ${hiveconf:second_quarter_end_date}
and order_status in ('R','O') and item_qty>0 and sales_amt>0;
--run 2,240,839

--drop table if exists ssoni.at_second_qrtr_master_notnull;
create table ssoni.at_second_qrtr_master_notnull as
select a.customer_key,a.item_qty,a.product_key,b.mdse_corp_desc,c.style_cd,c.activity,d.masterkey
from ssoni.at_second_qrtr_purch a 
inner join mds_new.ods_product_t b 
on a.product_key=b.product_key
inner join ssoni.at_product_activity_list c
on b.style_cd = c.style_cd
left outer join (select distinct customerkey,masterkey from crmanalytics.customerresolution) d
on a.customer_key=d.customerkey
where d.masterkey is not null and b.mdse_corp_desc = 'ATH DIR';
--run 934,593



--drop table if exists ssoni.at_second_qrtr_mk_activity;
create table ssoni.at_second_qrtr_mk_activity as 
select masterkey,activity,sum(item_qty) as items_purch_second_qrtr
from ssoni.at_second_qrtr_master_notnull 
group by masterkey,activity;
--run 524,525


---------------------Third quarter details ---------------------

--drop table if exists ssoni.at_third_qrtr_purch;
create table ssoni.at_third_qrtr_purch as 
select customer_key,item_qty,product_key
from mds_new.ods_orderline_t 
where brand='AT' and country='US' 
and unix_timestamp(transaction_date,'yyyy-MM-dd') between ${hiveconf:third_quarter_start_date} and ${hiveconf:third_quarter_end_date}
and order_status in ('R','O') and item_qty>0 and sales_amt>0;
--run 3,338,227


--drop table if exists ssoni.at_third_qrtr_master_notnull;
create table ssoni.at_third_qrtr_master_notnull as
select a.customer_key,a.item_qty,a.product_key,b.mdse_corp_desc,c.style_cd,c.activity,d.masterkey
from ssoni.at_third_qrtr_purch a 
inner join mds_new.ods_product_t b 
on a.product_key=b.product_key
inner join ssoni.at_product_activity_list c
on b.style_cd = c.style_cd
left outer join (select distinct customerkey,masterkey from crmanalytics.customerresolution) d
on a.customer_key=d.customerkey
where d.masterkey is not null and  b.mdse_corp_desc = 'ATH DIR' ;
--run 1,868,947


--drop table if exists ssoni.at_third_qrtr_mk_activity;
create table ssoni.at_third_qrtr_mk_activity as 
select masterkey,activity,sum(item_qty) as items_purch_third_qrtr
from ssoni.at_third_qrtr_master_notnull 
group by masterkey,activity;
--run 900,070


---------------------Fourth quarter details -----------------

--drop table if exists ssoni.at_fourth_qrtr_purch;
create table ssoni.at_fourth_qrtr_purch as 
select customer_key,item_qty,product_key
from mds_new.ods_orderline_t 
where brand='AT' and country='US' 
and unix_timestamp(transaction_date,'yyyy-MM-dd') between unix_timestamp('2016-03-01','yyyy-MM-dd') and unix_timestamp('2016-05-31','yyyy-MM-dd')
and order_status in ('R','O') and item_qty>0 and sales_amt>0;
--run 3,141,332

--drop table if exists ssoni.at_fourth_qrtr_master_notnull;
create table ssoni.at_fourth_qrtr_master_notnull as
select a.customer_key,a.item_qty,a.product_key,b.mdse_corp_desc,c.style_cd,c.activity,d.masterkey
from ssoni.at_fourth_qrtr_purch a 
inner join mds_new.ods_product_t b 
on a.product_key=b.product_key
inner join ssoni.at_product_activity_list c
on b.style_cd = c.style_cd
left outer join (select distinct customerkey,masterkey from crmanalytics.customerresolution) d
on a.customer_key=d.customerkey
where d.masterkey is not null and b.mdse_corp_desc = 'ATH DIR';
--run 2,784,339


--drop table if exists ssoni.at_fourth_qrtr_mk_activity;
create table ssoni.at_fourth_qrtr_mk_activity as 
select masterkey,activity,sum(item_qty) as items_purch_fourth_qrtr
from ssoni.at_fourth_qrtr_master_notnull 
group by masterkey,activity;
--run 1,080,279


------------------------------Last year purchase history quarterwise section ends-----------------------------------


----------------------------- Recency of browse and abandon section starts-------------------------------------------

--drop table if exists ssoni.at_abandon_unixtime;
create table ssoni.at_abandon_unixtime as 
select a.*,b.date_time
from ssoni.at_abandon_basket_product_master_notnull a
inner join 
(select * from omniture.hit_data_t where date_time between ${hiveconf:fourth_quarter_start_date} and ${hiveconf:fourth_quarter_end_date}) b 
on a.unk_shop_id=b.unk_shop_id and             
a.visit_num=b.visit_num  and        
a.pagename=b.pagename and             
a.hier1=b.hier1 and                   
a.prop1=b.prop1 and               
a.purchaseid=b.purchaseid and         
a.prop33=b.prop33 and                 
a.country=b.country  and               
a.t_time_info=b.t_time_info;
--run 


--drop table if exists ssoni.at_abandon_maxdt;
create table ssoni.at_abandon_maxdt as 
select masterkey,activity,round(datediff(${hiveconf:date_for_datediff},max(date_time))/90,2) as recency_abandon
from ssoni.at_abandon_unixtime
group by masterkey,activity;

--drop table if exists ssoni.at_clicks_unixtime;
create table ssoni.at_clicks_unixtime as 
select a.*,b.date_time
from ssoni.at_product_clicks_desc_activity_master_notnull a
inner join 
(select * from omniture.hit_data_t where date_time between ${hiveconf:fourth_quarter_start_date} and ${hiveconf:fourth_quarter_end_date}) b 
on a.unk_shop_id=b.unk_shop_id and             
a.visit_num=b.visit_num  and        
a.pagename=b.pagename and             
a.hier1=b.hier1 and                   
a.prop1=b.prop1 and               
a.purchaseid=b.purchaseid and         
a.prop33=b.prop33 and                 
a.country=b.country  and               
a.t_time_info=b.t_time_info;

--drop table if exists ssoni.at_clicks_maxdt;
create table ssoni.at_clicks_maxdt as 
select masterkey,activity,round(datediff(${hiveconf:date_for_datediff},max(date_time))/90,2) as recency_click
from ssoni.at_clicks_unixtime
group by masterkey,activity;
--run 1,753,932

----------------------------- Recency of browse and abandon section ends-------------------------------------------


-- This table combines the masterkeys and activities from all the tables
--drop table if exists ssoni.at_all_mk_activity;
create table ssoni.at_all_mk_activity as 
select distinct combined.masterkey,combined.activity from 
(
select a.masterkey,a.activity from at_clicks_mk_activity a
union all select b.masterkey,b.activity from at_abandon_mk_activity b
union all select c.masterkey,c.activity from at_pred_mk_activity c
union all select d.masterkey,d.activity from at_first_qrtr_mk_activity d
union all select e.masterkey,e.activity from at_second_qrtr_mk_activity e
union all select f.masterkey,f.activity from at_third_qrtr_mk_activity f
union all select g.masterkey,g.activity from at_fourth_qrtr_mk_activity g
) combined ;
--run 3,949,252

-- This table adds the variables as columns to the above table
--drop table if exists ssoni.at_all_variables
create table ssoni.at_all_variables as 
select a.masterkey,a.activity, coalesce(b.items_browsed,0) as items_browsed,
coalesce(c.items_abandoned,0) as items_abandoned,
coalesce(d.items_purch_pred_window,0) as items_purch_pred_window,
coalesce(e.items_purch_first_qrtr,0) as items_purch_first_qrtr,
coalesce(f.items_purch_second_qrtr,0) as items_purch_second_qrtr,
coalesce(g.items_purch_third_qrtr,0) as items_purch_third_qrtr,
coalesce(h.items_purch_fourth_qrtr,0) as items_purch_fourth_qrtr,
coalesce(i.recency_abandon,1) as recency_abandon,
coalesce(j.recency_click,1) as recency_click
from at_all_mk_activity a
left outer join at_clicks_mk_activity b on a.masterkey=b.masterkey and a.activity=b.activity
left outer join at_abandon_mk_activity c on a.masterkey=c.masterkey and a.activity=c.activity
left outer join at_pred_mk_activity d on a.masterkey=d.masterkey and a.activity=d.activity
left outer join at_first_qrtr_mk_activity e on a.masterkey=e.masterkey and a.activity=e.activity
left outer join at_second_qrtr_mk_activity f on a.masterkey=f.masterkey and a.activity=f.activity
left outer join at_third_qrtr_mk_activity g on a.masterkey=g.masterkey and a.activity=g.activity
left outer join at_fourth_qrtr_mk_activity h on a.masterkey=h.masterkey and a.activity=h.activity
left outer join at_abandon_maxdt i on a.masterkey=i.masterkey and a.activity=i.activity
left outer join at_clicks_maxdt j on a.masterkey=j.masterkey and a.activity=j.activity;
--run 3,949,252

--This table groups the variables by masterkey so as to normalize in the next table
--drop table if exists ssoni.at_all_variables_grouped;
create table ssoni.at_all_variables_grouped as
select masterkey,sum(items_abandoned) as sum_items_abandoned,
sum(items_browsed) as sum_items_browsed,
sum(items_purch_first_qrtr) as sum_items_purch_first_qrtr,
sum(items_purch_second_qrtr) as sum_items_purch_second_qrtr,
sum(items_purch_third_qrtr) as sum_items_purch_third_qrtr,
sum(items_purch_fourth_qrtr) as sum_items_purch_fourth_qrtr
from at_all_variables
group by masterkey;
--run 2,055,134

--drop table if exists ssoni.at_final_table;
create table ssoni.at_final_table as 
select a.masterkey,a.activity,
(case when b.sum_items_abandoned >0 then a.items_abandoned/b.sum_items_abandoned else 0.0 end) as nrml_items_abandoned,
(case when b.sum_items_browsed >0 then a.items_browsed/b.sum_items_browsed else 0.0 end) as nrml_items_browsed,
(case when b.sum_items_purch_first_qrtr >0 then a.items_purch_first_qrtr/b.sum_items_purch_first_qrtr else 0.0 end) as nrml_items_purch_first_qrtr,
(case when b.sum_items_purch_second_qrtr >0 then a.items_purch_second_qrtr/b.sum_items_purch_second_qrtr else 0.0 end) as nrml_items_purch_second_qrtr,
(case when b.sum_items_purch_third_qrtr >0 then a.items_purch_third_qrtr/b.sum_items_purch_third_qrtr else 0.0 end) as nrml_items_purch_third_qrtr,
(case when b.sum_items_purch_fourth_qrtr >0 then a.items_purch_fourth_qrtr/b.sum_items_purch_fourth_qrtr else 0.0 end) as nrml_items_purch_fourth_qrtr,
a.recency_abandon,a.recency_click,a.items_purch_pred_window
from at_all_variables a
left outer join at_all_variables_grouped b on a.masterkey = b.masterkey ;
--run 3,949,252

------------------------------Final table for initial analysis above------------------------





