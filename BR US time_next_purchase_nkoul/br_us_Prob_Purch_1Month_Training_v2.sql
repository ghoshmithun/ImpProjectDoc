-- set hive options
use nkoul_clk;
set mapred.job.queue.name=cem;  
set hive.cli.print.header=true;

--Set the variable dates. Change these dates for changing the time period of scoring
set purch_window_start_date = '2016-02-01';
set purch_window_end_date = '2017-01-31';

--Get the data of crmanalytics stored at a place.
--This table is already run. Refresh it if running if after few weeks.
create table nkoul_clk.crmanalyticsdata stored as orc as 
select distinct masterkey,customerkey,unknownshopperid
from crmanalytics.customerresolution
where masterkey is not null
order by masterkey,customerkey,unknownshopperid;


--- pulling this data for out of time validation. History of 2016 and target of 2017 Jan for 1 month  and Jan-Mar for 3 month
--initial table containing purchases

----Try the orderline version. Changing the dates to Jan 16 to Dec 16
--drop table if exists nkoul_clk.gp_lastyear_purchases_orderline_training;
create table nkoul_clk.gp_lastyear_purchases_orderline_training stored as orc as
select  customer_key,
transaction_date,
transaction_num,
line_num,
order_status,
item_qty,
sales_amt
from mds_new.ods_orderline_t
where country = 'US' and brand='BR' and order_status <> 'D' and transaction_date between ${hiveconf:purch_window_start_date} and ${hiveconf:purch_window_end_date}  and sales_amt > 0 and item_qty > 0;
--order by customer_key,transaction_date,line_num;
-- 109,635,161
-- 35,014,846
-- 32,728,258

--drop table if exists nkoul_clk.gp_lastyear_purchases_custkey_grouped_training;
create table nkoul_clk.gp_lastyear_purchases_custkey_grouped_training stored as orc  as 
select a.customer_key,a.transaction_date,a.transaction_num,b.first_purchase_brand_flag,sum(a.item_qty) as sum_items,sum(a.sales_amt) as sum_sales
from nkoul_clk.gp_lastyear_purchases_orderline_training a 
left outer join mds_new.ods_orderheader_t b 
on a.customer_key = b.customer_key and a.transaction_num = b.transaction_num
group by a.customer_key,a.transaction_date,a.transaction_num,b.first_purchase_brand_flag;
-- 33,190,451
-- 15,047,215
-- 14,416,264


--drop table if exists nkoul_clk.gp_lastyear_purch_master_notnull_training;
create table nkoul_clk.gp_lastyear_purch_master_notnull_training stored as orc as 
select b.masterkey,a.*
from nkoul_clk.gp_lastyear_purchases_custkey_grouped_training a 
left outer join (select distinct customerkey,masterkey from nkoul_clk.crmanalyticsdata) b
on a.customer_key=b.customerkey
where b.masterkey is not null;
--order by masterkey,transaction_date;
--  33,190,407 (8,487,598 distinct masterkeys)
-- 15,047,192
-- 14,402,355

create table nkoul_clk.gp_first_6months_purchases_training stored as orc as 
select masterkey,sum(sum_items) as first_6months_purchases
from nkoul_clk.gp_lastyear_purch_master_notnull_training
where transaction_date between ${hiveconf:purch_window_start_date} and  date_add(${hiveconf:purch_window_start_date},182)
group by masterkey;

create table nkoul_clk.gp_last_6months_purchases_training stored as orc as 
select masterkey,sum(sum_items) as last_6months_purchases
from nkoul_clk.gp_lastyear_purch_master_notnull_training
where transaction_date between date_add(${hiveconf:purch_window_start_date},183) and date_add(${hiveconf:purch_window_start_date},365)
group by masterkey;


--drop table if exists nkoul_clk.gp_ratio_purchases_training;
create table nkoul_clk.gp_ratio_purchases_training stored as orc as 
select a.masterkey,round(b.last_6months_purchases/a.first_6months_purchases,2)  as ratio_purchases
from nkoul_clk.gp_first_6months_purchases_training a 
inner join nkoul_clk.gp_last_6months_purchases_training b on a.masterkey = b.masterkey;

----Check browsing history
--drop table if exists nkoul_clk.gp_lastqrtr_browsing_training;
create table nkoul_clk.gp_lastqrtr_browsing_training stored as orc as 
select distinct masterkey,unk_shop_id,
visit_num,
pagename,
purchaseid,
prop33,
country,
t_time_info,
substr(prop1,1,6) as style_cd  
from omniture.hit_data_t a 
left outer join (select distinct unknownshopperid,masterkey from nkoul_clk.crmanalyticsdata) b  on a.unk_shop_id=b.unknownshopperid
where (instr(pagename,'br:browse')>0 or instr(pagename,'mobile:br')>0) and (date_time) between date_add(${hiveconf:purch_window_start_date},274) and date_add(${hiveconf:purch_window_start_date},365) and prop33 = 'product' and masterkey is not null; 


create table nkoul_clk.gp_lastqrtr_browsing_total_items_training stored as orc as 
select b.masterkey,count(distinct a.t_time_info) as items_browsed from nkoul_clk.gp_lastqrtr_browsing_training a 
inner join (select distinct masterkey from nkoul_clk.gp_lastyear_purch_master_notnull_training) b on a.masterkey = b.masterkey
group by b.masterkey;
-- 2,522,073
-- 1,504,719
-- 16,32,817

--Check the omniture table version here. For older data, old table to be used. Change it for newer data
--drop table if exists ssoni.gp_cart_add_training;
create table nkoul_clk.gp_cart_add_training as 
select distinct masterkey,unk_shop_id,
visit_num,
pagename,
purchaseid,
prop33,
country,
t_time_info,
substr(prop1,1,6) as style_cd  
from omniture.hit_data_t_history a 
left outer join (select distinct unknownshopperid,masterkey from nkoul_clk.crmanalyticsdata) b  on a.unk_shop_id=b.unknownshopperid
where (instr(pagename,'br:browse')>0 or instr(pagename,'mobile:br')>0) and (date_time) between date_add(${hiveconf:purch_window_start_date},274) and date_add(${hiveconf:purch_window_start_date},365) and prop33='inlineBagAdd';
--run 4,976,141
--run 8,869,791
--run 10,884,743

create table nkoul_clk.gp_lastqrtr_cart_add_total_items_training stored as orc as 
select b.masterkey,count(distinct a.t_time_info) as items_added from nkoul_clk.gp_cart_add_training a 
inner join (select distinct masterkey from nkoul_clk.gp_lastyear_purch_master_notnull_training) b on a.masterkey = b.masterkey
group by b.masterkey;



--drop table if exists ssoni.gp_lastyear_total_items_purch_training;
create table nkoul_clk.gp_lastyear_total_items_purch_training stored as orc  as
select masterkey,count(transaction_num) as total_orders_placed,sum(sum_sales) as total_sales_amt
from nkoul_clk.gp_lastyear_purch_master_notnull_training
group by masterkey
order by total_orders_placed desc,total_sales_amt desc;
--run 8,487,598
--run 4,663,059
--run 4,294,264

--drop table if exists ssoni.gp_first_purchase_flag_training;
create table nkoul_clk.gp_first_purchase_flag_training stored as orc as 
select masterkey,transaction_date,first_purchase_brand_flag from 
(select masterkey,transaction_date,first_purchase_brand_flag,row_number() over (partition by masterkey order by transaction_date,transaction_num) as first_click from nkoul_clk.gp_lastyear_purch_master_notnull_training) T
where first_click = 1;
-- 3,884,484
-- 4,663,059
-- 4,294,264

--drop table if exists ssoni.gp_active_period_training;
create table nkoul_clk.gp_active_period_training stored as orc as
select masterkey,transaction_date,first_purchase_brand_flag,
(case when first_purchase_brand_flag = 'N' then 12.0 else round(datediff(date_add(${hiveconf:purch_window_start_date},366),transaction_date)/30.4,1) end) as active_period
from nkoul_clk.gp_first_purchase_flag_training; 
-- 3,884,484
-- 4,663,059
-- 4,294,264

--drop table if exists ssoni.gp_last_purch_training;
create table nkoul_clk.gp_last_purch_training stored as orc as 
select masterkey,transaction_date from 
(select masterkey,transaction_date,row_number() over (partition by masterkey order by transaction_date desc,transaction_num desc) as last_purch from nkoul_clk.gp_lastyear_purch_master_notnull_training) T
where last_purch = 1;
-- 3,884,484
-- 4,663,059
-- 4,294,264

--drop table if exists ssoni.gp_recency_purch_training;
create table nkoul_clk.gp_recency_purch_training stored as orc as
select masterkey,transaction_date,round(datediff(date_add(${hiveconf:purch_window_start_date},366),transaction_date)/30.4,1) as recency_purch
from nkoul_clk.gp_last_purch_training; 
-- 3,884,484
-- 4,663,059

--drop table if exists ssoni.gp_all_variables_training;
create table nkoul_clk.gp_all_variables_training stored as orc as
select a.masterkey,a.total_orders_placed,b.active_period,c.recency_purch,round(a.total_orders_placed/b.active_period,3) as freq_purch,coalesce(d.ratio_purchases,0) as ratio_purchases,
coalesce(e.items_browsed,0) as items_browsed,coalesce(f.items_added,0) as items_added
from nkoul_clk.gp_lastyear_total_items_purch_training a
left outer join nkoul_clk.gp_active_period_training b
on a.masterkey = b.masterkey
left outer join nkoul_clk.gp_recency_purch_training c
on a.masterkey = c.masterkey
left outer join nkoul_clk.gp_ratio_purchases_training d
on a.masterkey = d.masterkey
left outer join nkoul_clk.gp_lastqrtr_browsing_total_items_training e
on a.masterkey = e.masterkey
left outer join nkoul_clk.gp_lastqrtr_cart_add_total_items_training f
on a.masterkey = f.masterkey;

-- 3,555,302
-- 4,294,264

----------------------------------------------------------target variable section--------------------------------------------------------

--drop table if exists ssoni.gp_next_month_purchases_training;
create table nkoul_clk.gp_next_month_purchases_training stored as orc as
select  customer_key,
transaction_date,
transaction_num,
line_num,
order_status,
item_qty,
sales_amt
from mds_new.ods_orderline_t
where country = 'US' and brand='BR' and order_status <> 'D' and transaction_date between date_add(${hiveconf:purch_window_start_date},366) and date_add(${hiveconf:purch_window_start_date},396)  and sales_amt > 0 and item_qty > 0;
--order by customer_key,transaction_date,line_num;
--

--drop table if exists ssoni.gp_next_month_custkey_grouped_training;
create table nkoul_clk.gp_next_month_custkey_grouped_training stored as orc as 
select customer_key,transaction_date,transaction_num,sum(item_qty) as sum_items,sum(sales_amt) as sum_sales
from nkoul_clk.gp_next_month_purchases_training
group by customer_key,transaction_date,transaction_num;
--

--drop table if exists ssoni.gp_next_month_purch_master_notnull_training;
create table nkoul_clk.gp_next_month_purch_master_notnull_training stored as orc as 
select b.masterkey,a.*,(case when a.sum_items > 0 then 1 else 0 end) as purchase_in_target_window
from nkoul_clk.gp_next_month_custkey_grouped_training a 
left outer join (select distinct customerkey,masterkey from nkoul_clk.crmanalyticsdata) b
on a.customer_key=b.customerkey
where b.masterkey is not null;
--order by masterkey,transaction_date;
-- 1,378,556
-- 1,159,874
-- 828,116

--drop table if exists ssoni.gp_purchase_in_target_window_training;
create table nkoul_clk.gp_purchase_in_target_window_training stored as orc as
select distinct masterkey,purchase_in_target_window 
from nkoul_clk.gp_next_month_purch_master_notnull_training;
-- 600,394
-- 721,225

-----------------------------target variable section ends--------------------------------------------------------



--drop table if exists ssoni.gp_lastyear_same_month_purchases_training;
create table nkoul_clk.gp_lastyear_same_month_purchases_training stored as orc as
select  customer_key,
transaction_date,
transaction_num,
line_num,
order_status,
item_qty,
sales_amt
from mds_new.ods_orderline_t
where country = 'US' and brand='BR' and order_status <> 'D' and transaction_date between ${hiveconf:purch_window_start_date} and date_add(${hiveconf:purch_window_start_date},30)  and sales_amt > 0 and item_qty > 0;
--order by customer_key,transaction_date,line_num;

--drop table if exists ssoni.gp_lastyear_same_month_custkey_grouped_training;
create table nkoul_clk.gp_lastyear_same_month_custkey_grouped_training stored as orc as 
select customer_key,transaction_date,transaction_num,sum(item_qty) as sum_items,sum(sales_amt) as sum_sales
from nkoul_clk.gp_lastyear_same_month_purchases_training
group by customer_key,transaction_date,transaction_num;


--drop table if exists ssoni.gp_lastyear_samemonth_purch_master_notnull_training;
create table nkoul_clk.gp_lastyear_samemonth_purch_master_notnull_training stored as orc as 
select b.masterkey,a.*,(case when a.sum_items > 0 then 1 else 0 end) as lastyear_samemonth_purchase
from nkoul_clk.gp_lastyear_same_month_custkey_grouped_training a 
left outer join (select distinct customerkey,masterkey from nkoul_clk.crmanalyticsdata) b
on a.customer_key=b.customerkey
where b.masterkey is not null
order by masterkey,transaction_date;
-- 1,210,527
-- 1,350,076

--drop table if exists ssoni.gp_lastyear_samemonth_purchase_training;
create table nkoul_clk.gp_lastyear_samemonth_purchase_training stored as orc as
select distinct masterkey,lastyear_samemonth_purchase 
from nkoul_clk.gp_lastyear_samemonth_purch_master_notnull_training;
-- 562,765
-- 873,524
-- 555,598

----------------------3 month variables section--------------------------------------------------------



--drop table if exists ssoni.gp_next_3months_purchases_training;
create table nkoul_clk.gp_next_3months_purchases_training stored as orc as
select  customer_key,
transaction_date,
transaction_num,
line_num,
order_status,
item_qty,
sales_amt
from mds_new.ods_orderline_t
where country = 'US' and brand='BR' and order_status <> 'D' and transaction_date between date_add(${hiveconf:purch_window_start_date},366) and date_add(${hiveconf:purch_window_start_date},457)  and sales_amt > 0 and item_qty > 0;
--order by customer_key,transaction_date,line_num;
--

--drop table if exists ssoni.gp_next_3months_custkey_grouped_training;
create table nkoul_clk.gp_next_3months_custkey_grouped_training stored as orc as 
select customer_key,transaction_date,transaction_num,sum(item_qty) as sum_items,sum(sales_amt) as sum_sales
from nkoul_clk.gp_next_3months_purchases_training 
group by customer_key,transaction_date,transaction_num;
--


--drop table if exists ssoni.gp_next_3months_purch_master_notnull_training;
create table nkoul_clk.gp_next_3months_purch_master_notnull_training stored as orc  as 
select b.masterkey,a.*,(case when a.sum_items > 0 then 1 else 0 end) as purchase_in_3month_target_window
from nkoul_clk.gp_next_3months_custkey_grouped_training a 
left outer join (select distinct customerkey,masterkey from nkoul_clk.crmanalyticsdata) b
on a.customer_key=b.customerkey
where b.masterkey is not null
order by masterkey,transaction_date;
-- 3,720,711
-- 3,223,906

--drop table if exists ssoni.gp_purchase_in_3month_target_window_training;
create table nkoul_clk.gp_purchase_in_3month_target_window_training stored as orc as
select distinct masterkey,purchase_in_3month_target_window 
from nkoul_clk.gp_next_3months_purch_master_notnull_training;
-- 1,451,972
-- 1,584,469
-- 1,419,834

-----------------------------3 month target variable section ends--------------------------------------------------------


--drop table if exists ssoni.gp_lastyear_same3months_purchases_training;
create table nkoul_clk.gp_lastyear_same3months_purchases_training stored as orc as
select  customer_key,
transaction_date,
transaction_num,
line_num,
order_status,
item_qty,
sales_amt
from mds_new.ods_orderline_t
where country = 'US' and brand='BR' and order_status <> 'D' and transaction_date between ${hiveconf:purch_window_start_date} and date_add(${hiveconf:purch_window_start_date},91)  and sales_amt > 0 and item_qty > 0;
--order by customer_key,transaction_date,line_num;
--

--drop table if exists ssoni.gp_lastyear_same3months_custkey_grouped_training;
create table nkoul_clk.gp_lastyear_same3months_custkey_grouped_training stored as orc as 
select customer_key,transaction_date,transaction_num,sum(item_qty) as sum_items,sum(sales_amt) as sum_sales
from nkoul_clk.gp_lastyear_same3months_purchases_training 
group by customer_key,transaction_date,transaction_num;
--


--drop table if exists ssoni.gp_lastyear_same3months_purch_master_notnull_training;
create table nkoul_clk.gp_lastyear_same3months_purch_master_notnull_training stored as orc  as 
select b.masterkey,a.*,(case when a.sum_items > 0 then 1 else 0 end) as lastyear_same3months_purchase
from nkoul_clk.gp_lastyear_same3months_custkey_grouped_training a 
left outer join (select distinct customerkey,masterkey from nkoul_clk.crmanalyticsdata) b
on a.customer_key=b.customerkey
where b.masterkey is not null
order by masterkey,transaction_date;
-- 3,819,997
-- 3,781,550
-- 3,117,620

--drop table if exists ssoni.gp_lastyear_same3months_purchase_training;
create table nkoul_clk.gp_lastyear_same3months_purchase_training stored as orc  as
select distinct masterkey,lastyear_same3months_purchase 
from nkoul_clk.gp_lastyear_same3months_purch_master_notnull_training;
-- 1,527,846
-- 1,922,694
-- 1,630,037

----------------------------3 month variable section ends--------------------------------------------------------


-- should be final table for analysis
--drop table if exists ssoni.gp_all_variables_targets_training;
create table nkoul_clk.gp_all_variables_targets_training stored as orc  as
select a.*,coalesce(c.lastyear_samemonth_purchase,0) as lastyear_samemonth_purchase,coalesce(b.purchase_in_target_window,0) as purchase_in_target_window,
coalesce(e.lastyear_same3months_purchase,0) as lastyear_same3months_purchase,coalesce(d.purchase_in_3month_target_window,0) as purchase_in_3month_target_window
from nkoul_clk.gp_all_variables_training a
left outer join nkoul_clk.gp_purchase_in_target_window_training b
on a.masterkey = b.masterkey
left outer join nkoul_clk.gp_lastyear_samemonth_purchase_training c
on a.masterkey = c.masterkey
left outer join nkoul_clk.gp_purchase_in_3month_target_window_training d
on a.masterkey = d.masterkey
left outer join nkoul_clk.gp_lastyear_same3months_purchase_training e
on a.masterkey = e.masterkey;
-- 3,555,302
-- 4,663,059
-- 4,294,264

----------------------------Data Export--------------------------------------------------------

create external table nkoul_clk.gp_prob_purch_1month_export
(masterkey string,
total_orders_placed int,
active_period double,
recency_purch double,
freq_purch double,
ratio_purchases double,
items_browsed int,
items_added int,
lastyear_samemonth_purchase int,
purchase_in_target_window int,
lastyear_same3months_purchase int,
purchase_in_3month_target_window int)              
row format delimited 
fields terminated by '|' 
lines terminated by '\n' 
location '/user/xnkoul/gp_prob_purch_1month_export'
TBLPROPERTIES('serialization.null.format'='');

insert overwrite table nkoul_clk.gp_prob_purch_1month_export
select * from nkoul_clk.gp_all_variables_targets_training;

hadoop fs -getmerge /user/xnkoul/gp_prob_purch_1month_export /home/xnkoul;



