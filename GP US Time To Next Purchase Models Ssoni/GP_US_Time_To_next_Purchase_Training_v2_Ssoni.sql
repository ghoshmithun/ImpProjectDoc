----------------------- set hive options
use ssoni;
set mapred.job.queue.name=cem;  
set hive.cli.print.header=true;

------------------------Set the variable dates. Change these dates for changing the time period of scoring

set purch_window_start_date = '2015-04-01';


------------------Get the data of crmanalytics stored at a place.
-------------------This table is already run. Refresh it if running if after few weeks.

--drop table if exists ssoni.crmanalyticsdata;
create table ssoni.crmanalyticsdata stored as orc as 
select distinct masterkey,customerkey,unknownshopperid
from crmanalytics.customerresolution
where customerkey is not null;


--- pulling this data for out of time validation. History of 2016 and target of 2017 Jan for 1 month  and Jan-Mar for 3 month


-----------------------initial table containing purchases

----Try the orderline version. Changing the dates to Jan 16 to Dec 16
--drop table if exists ssoni.gp_lastyear_purchases_orderline_training1;
create table ssoni.gp_lastyear_purchases_orderline_training1 stored as orc as
select  a.customer_key,
a.transaction_date,
a.transaction_num,
a.line_num,
a.order_status,
a.item_qty,
a.sales_amt,
a.product_key,
coalesce(b.discount_amt,0) as discount_amt,
coalesce(c.mdse_div_desc,'NA') as division
from mds_new.ods_orderline_t a 
left outer join mds_new.ods_orderline_discounts_t b on a.customer_key = b.customer_key and a.transaction_num = b.transaction_num and a.line_num = b.line_num
left outer join mds_new.ods_product_t c on a.product_key = c.product_key
where a.country = 'US' and a.brand='GP' and a.order_status <> 'D' and a.transaction_date between ${hiveconf:purch_window_start_date} and date_add(${hiveconf:purch_window_start_date},366)  and a.sales_amt > 0 and a.item_qty > 0;
--order by customer_key,transaction_date,line_num;
-- 109,635,161


--------------------- Group the transactions table by customer key, transaction date and transaction num

--drop table if exists ssoni.gp_lastyear_purchases_custkey_grouped_training1;
create table ssoni.gp_lastyear_purchases_custkey_grouped_training1 stored as orc  as 
select a.customer_key,a.transaction_date,a.transaction_num,b.first_purchase_brand_flag,sum(a.item_qty) as sum_items,sum(a.sales_amt) as sum_sales
from ssoni.gp_lastyear_purchases_orderline_training1 a 
left outer join mds_new.ods_orderheader_t b 
on a.customer_key = b.customer_key and a.transaction_num = b.transaction_num
group by a.customer_key,a.transaction_date,a.transaction_num,b.first_purchase_brand_flag;
-- 33,190,451 (10007115 distinct custkeys)


----------------------- Join the customer keys with masterkey

--drop table if exists ssoni.gp_lastyear_purch_master_notnull_training1;
create table ssoni.gp_lastyear_purch_master_notnull_training1 stored as orc as 
select b.masterkey,a.*
from ssoni.gp_lastyear_purchases_custkey_grouped_training1 a 
left outer join (select distinct customerkey,masterkey from ssoni.crmanalyticsdata) b
on a.customer_key=b.customerkey
where b.masterkey is not null;
--order by masterkey,transaction_date;
--  33,190,407 (9177392 distinct masterkeys) (10007080 distinct custkeys)


--------------------- Get the purchase data for first 6 months grouped by masterkey

create table ssoni.gp_first_6months_purchases_training1 stored as orc as 
select masterkey,sum(sum_items) as first_6months_purchases
from ssoni.gp_lastyear_purch_master_notnull_training1
where transaction_date between ${hiveconf:purch_window_start_date} and  date_add(${hiveconf:purch_window_start_date},182)
group by masterkey;


------------------- Get the purchase data for last 6 months grouped by masterkey

create table ssoni.gp_last_6months_purchases_training1 stored as orc as 
select masterkey,sum(sum_items) as last_6months_purchases
from ssoni.gp_lastyear_purch_master_notnull_training1
where transaction_date between date_add(${hiveconf:purch_window_start_date},183) and date_add(${hiveconf:purch_window_start_date},365)
group by masterkey;
--6,174,637

-------------------- Get the ratio of purchases in last 6 months to first 6 months. Ratio is of number of items purchased

--drop table if exists ssoni.gp_ratio_purchases_training1;
create table ssoni.gp_ratio_purchases_training1 stored as orc as 
select a.masterkey,round(b.last_6months_purchases/a.first_6months_purchases,2)  as ratio_purchases
from ssoni.gp_first_6months_purchases_training1 a 
inner join ssoni.gp_last_6months_purchases_training1 b on a.masterkey = b.masterkey;


-------------------------------------------------------- Browsing history section ---------------------------------------------------------------

----Check browsing history
--drop table if exists ssoni.gp_lastqrtr_browsing_training3;
--create table ssoni.gp_lastqrtr_browsing_training3 stored as orc as 
--select distinct unk_shop_id,
--visit_num,
--pagename,
--purchaseid,
--prop33,
--country,
--t_time_info,
--substr(prop1,1,6) as style_cd  
--from omniture.hit_data_t_history 
--where date_time = ('2016-01-01');
 --(instr(pagename,'gp:browse')>0 or instr(pagename,'mobile:gp')>0) and unix_timestamp(date_time) between unix_timestamp('2016-01-01') and unix_timestamp('2016-01-03') and prop33 = 'product' ; 

 
-----------------------------Try the browsing history from the hit_data_agg table in crm analytics

--drop table if exists ssoni.gp_lastqrtr_browsing_training1;
create table ssoni.gp_lastqrtr_browsing_training1 stored as orc as 
select distinct unk_shop_id,
visit_num,
brand_from_pagename,
gp_hit_ind_tot,
prop33,
market,
date_time,
bag_add_ind
from crmanalytics.hit_data_agg
where market = 'US' and date_time between date_add(${hiveconf:purch_window_start_date},274) and date_add(${hiveconf:purch_window_start_date},365) and brand_from_pagename = 'GAP' and prop33 = 'product';


----------------------- Get the total items browsed in GP

--drop table if exists ssoni.gp_lastqrtr_browsing_total_items_training1;
create table ssoni.gp_lastqrtr_browsing_total_items_training1 stored as orc as 
select b.masterkey,sum(a.gp_hit_ind_tot) as items_browsed 
from ssoni.gp_lastqrtr_browsing_training1 a 
inner join ssoni.crmanalyticsdata b on a.unk_shop_id = b.unknownshopperid
group by b.masterkey;



--Check the omniture table version here. For older data, old table to be used. Change it for newer data
--drop table if exists ssoni.gp_cart_add_training1;
--create table ssoni.gp_cart_add_training1 as 
--select distinct masterkey,unk_shop_id,
--visit_num,
--pagename,
--purchaseid,
--prop33,
--country,
--t_time_info,
--substr(prop1,1,6) as style_cd  
--from omniture.hit_data_t a 
--left outer join (select distinct unknownshopperid,masterkey from ssoni.crmanalyticsdata) b  on a.unk_shop_id=b.unknownshopperid
--where (instr(pagename,'gp:browse')>0 or instr(pagename,'mobile:gp')>0) and (date_time) between unix_timestamp(date_add(${hiveconf:purch_window_start_date},274)) and unix_timestamp(date_add(${hiveconf:purch_window_start_date},365)) and prop33='inlineBagAdd';
--run 4,976,141


------------------ Get the card addition data from hit_data_agg 

--drop table if exists ssoni.gp_cart_add_training1;
create table ssoni.gp_cart_add_training1 stored as orc as 
select distinct unk_shop_id,
visit_num,
brand_from_pagename,
gp_hit_ind_tot,
prop33,
market,
date_time,
bag_add_ind
from crmanalytics.hit_data_agg
where market = 'US' and date_time between date_add(${hiveconf:purch_window_start_date},274) and date_add(${hiveconf:purch_window_start_date},365) and brand_from_pagename = 'GAP' and prop33 = 'inlineBagAdd';


---------------- Get the total items added to cart for each masterkey

--drop table if exists ssoni.gp_lastqrtr_cart_add_total_items_training1;
create table ssoni.gp_lastqrtr_cart_add_total_items_training1 stored as orc as 
select b.masterkey,sum(a.gp_hit_ind_tot) as items_added
from ssoni.gp_cart_add_training1 a 
inner join ssoni.crmanalyticsdata b on a.unk_shop_id = b.unknownshopperid
group by b.masterkey;


-------------------- Get the total items purchased grouped at masterkey level

--drop table if exists ssoni.gp_lastyear_total_items_purch_training1;
create table ssoni.gp_lastyear_total_items_purch_training1 stored as orc  as
select masterkey,count(transaction_num) as total_orders_placed,sum(sum_sales) as total_sales_amt
from ssoni.gp_lastyear_purch_master_notnull_training1
group by masterkey
order by total_orders_placed desc,total_sales_amt desc;
--run 9,177,392 ()


--------------------- Get the flag status of first purchase

--drop table if exists ssoni.gp_first_purchase_flag_training1;
create table ssoni.gp_first_purchase_flag_training1 stored as orc as 
select masterkey,transaction_date,first_purchase_brand_flag from 
(select masterkey,transaction_date,first_purchase_brand_flag,row_number() over (partition by masterkey order by transaction_date,transaction_num) as first_click from ssoni.gp_lastyear_purch_master_notnull_training1) T
where first_click = 1;
-- 3,884,484


---------------------- Get the customer's active period in months. If first purchase brand flag is N for all transactions then it is 12, otherwise difference between first purchase and training window end date

--drop table if exists ssoni.gp_active_period_training1;
create table ssoni.gp_active_period_training1 stored as orc as
select masterkey,transaction_date,first_purchase_brand_flag,
(case when first_purchase_brand_flag = 'N' then 12.0 else round(datediff(date_add(${hiveconf:purch_window_start_date},366),transaction_date)/30.4,1) end) as active_period
from ssoni.gp_first_purchase_flag_training1; 
-- 3,884,484


--------------------- Get the transaction date for last purchase

--drop table if exists ssoni.gp_last_purch_training1;
create table ssoni.gp_last_purch_training1 stored as orc as 
select masterkey,transaction_date from 
(select masterkey,transaction_date,row_number() over (partition by masterkey order by transaction_date desc,transaction_num desc) as last_purch from ssoni.gp_lastyear_purch_master_notnull_training1) T
where last_purch = 1;
-- 3,884,484


---------------------- Get the recency of last purchase. Training window end date minus the last transaction date

--drop table if exists ssoni.gp_recency_purch_training1;
create table ssoni.gp_recency_purch_training1 stored as orc as
select masterkey,transaction_date,round(datediff(date_add(${hiveconf:purch_window_start_date},366),transaction_date)/30.4,1) as recency_purch
from ssoni.gp_last_purch_training1; 
-- 3,884,484


-------------------- Count the number of distinct divisions shopped from

--drop table if exists ssoni.gp_count_distinct_divisions_training1;
create table ssoni.gp_count_distinct_divisions_training1 stored as orc as 
select masterkey,count(distinct division) as count_distinct_divisions 
from ssoni.gp_lastyear_purchases_orderline_training1 a 
inner join ssoni.crmanalyticsdata b on  a.customer_key = b.customerkey
where division <> 'NA'
group by masterkey;


----------------- Get the average discount percentage, total purchases and total net sales for each masterkey

--drop table if exists ssoni.gp_avg_discount_percentage_training1;
create table ssoni.gp_avg_discount_percentage_training1 stored as orc as 
select masterkey,round(sum(sales_amt),2) as total_sales,round(sum(discount_amt),2) as total_discount, round(sum(discount_amt)*(-100.0)/(sum(sales_amt)),2) as discount_percentage,
count(distinct transaction_num) as total_purchases,round(sum(sales_amt) + sum(discount_amt),2) as total_net_sales
from ssoni.gp_lastyear_purchases_orderline_training1 a 
inner join ssoni.crmanalyticsdata b on a.customer_key = b.customerkey
where (item_qty > 0 and sales_amt > 0) and (sales_amt + discount_amt > 0)
group by masterkey;

-- 55866 with 0 total net sales


---------------------- Get the purchases of first month of training period

--drop table if exists ssoni.gp_lastyear_same_month_purchases_training1;
create table ssoni.gp_lastyear_same_month_purchases_training1 stored as orc as
select  customer_key,
transaction_date,
transaction_num,
line_num,
order_status,
item_qty,
sales_amt
from mds_new.ods_orderline_t
where country = 'US' and brand='GP' and order_status <> 'D' and transaction_date between ${hiveconf:purch_window_start_date} and date_add(${hiveconf:purch_window_start_date},30)  and sales_amt > 0 and item_qty > 0;
--order by customer_key,transaction_date,line_num;


------------------- Group the purchases of first month of training period by customerkey

--drop table if exists ssoni.gp_lastyear_same_month_custkey_grouped_training1;
create table ssoni.gp_lastyear_same_month_custkey_grouped_training1 stored as orc as 
select customer_key,transaction_date,transaction_num,sum(item_qty) as sum_items,sum(sales_amt) as sum_sales
from ssoni.gp_lastyear_same_month_purchases_training1
group by customer_key,transaction_date,transaction_num;


------------------ join with masterkey

--drop table if exists ssoni.gp_lastyear_samemonth_purch_master_notnull_training1;
create table ssoni.gp_lastyear_samemonth_purch_master_notnull_training1 stored as orc as 
select b.masterkey,a.*,(case when a.sum_items > 0 then 1 else 0 end) as lastyear_samemonth_purchase
from ssoni.gp_lastyear_same_month_custkey_grouped_training1 a 
left outer join (select distinct customerkey,masterkey from ssoni.crmanalyticsdata) b
on a.customer_key=b.customerkey
where b.masterkey is not null
order by masterkey,transaction_date;
-- 1,210,527


--------------------- Binary flag variable indicating whether made any purchase in first 1 month of training window 

--drop table if exists ssoni.gp_lastyear_samemonth_purchase_training1;
create table ssoni.gp_lastyear_samemonth_purchase_training1 stored as orc as
select distinct masterkey,lastyear_samemonth_purchase 
from ssoni.gp_lastyear_samemonth_purch_master_notnull_training1;
-- 562,765


------------------------- Repeating the same process for first 3 months of training window----------------------------

--drop table if exists ssoni.gp_lastyear_same3months_purchases_training1;
create table ssoni.gp_lastyear_same3months_purchases_training1 stored as orc as
select  customer_key,
transaction_date,
transaction_num,
line_num,
order_status,
item_qty,
sales_amt
from mds_new.ods_orderline_t
where country = 'US' and brand='GP' and order_status <> 'D' and transaction_date between ${hiveconf:purch_window_start_date} and date_add(${hiveconf:purch_window_start_date},91)  and sales_amt > 0 and item_qty > 0;
--order by customer_key,transaction_date,line_num;
--


----------------------- Group at customer key

--drop table if exists ssoni.gp_lastyear_same3months_custkey_grouped_training1;
create table ssoni.gp_lastyear_same3months_custkey_grouped_training1 stored as orc as 
select customer_key,transaction_date,transaction_num,sum(item_qty) as sum_items,sum(sales_amt) as sum_sales
from ssoni.gp_lastyear_same3months_purchases_training1 
group by customer_key,transaction_date,transaction_num;
--

----------------- Join with masterkey

--drop table if exists ssoni.gp_lastyear_same3months_purch_master_notnull_training1;
create table ssoni.gp_lastyear_same3months_purch_master_notnull_training1 stored as orc  as 
select b.masterkey,a.*,(case when a.sum_items > 0 then 1 else 0 end) as lastyear_same3months_purchase
from ssoni.gp_lastyear_same3months_custkey_grouped_training1 a 
left outer join (select distinct customerkey,masterkey from ssoni.crmanalyticsdata) b
on a.customer_key=b.customerkey
where b.masterkey is not null
order by masterkey,transaction_date;
-- 3,819,997


------------------ Binary flag variable indicating whether made any purchase in first 3 months of training window 

--drop table if exists ssoni.gp_lastyear_same3months_purchase_training1;
create table ssoni.gp_lastyear_same3months_purchase_training1 stored as orc  as
select distinct masterkey,lastyear_same3months_purchase 
from ssoni.gp_lastyear_same3months_purch_master_notnull_training1;
-- 1,527,846


--------------------------------- Combine all the variables into this consolidated table

--drop table if exists ssoni.gp_all_variables_training1;
create table ssoni.gp_all_variables_training1 stored as orc as
select a.masterkey,a.total_orders_placed,b.active_period,c.recency_purch,round(a.total_orders_placed/b.active_period,3) as freq_purch,coalesce(d.ratio_purchases,0) as ratio_purchases,
coalesce(e.items_browsed,0) as items_browsed,coalesce(f.items_added,0) as items_added,coalesce(g.count_distinct_divisions,0) as count_distinct_divisions,
coalesce(h.discount_percentage,0) as discount_percentage,coalesce(h.total_net_sales,0) as total_net_sales
from ssoni.gp_lastyear_total_items_purch_training1 a
left outer join ssoni.gp_active_period_training1 b
on a.masterkey = b.masterkey
left outer join ssoni.gp_recency_purch_training1 c
on a.masterkey = c.masterkey
left outer join ssoni.gp_ratio_purchases_training1 d
on a.masterkey = d.masterkey
left outer join ssoni.gp_lastqrtr_browsing_total_items_training1 e
on a.masterkey = e.masterkey
left outer join ssoni.gp_lastqrtr_cart_add_total_items_training1 f
on a.masterkey = f.masterkey
left outer join ssoni.gp_count_distinct_divisions_training1 g
on a.masterkey = g.masterkey
left outer join ssoni.gp_avg_discount_percentage_training1 h
on a.masterkey = h.masterkey
where total_net_sales > 0;

-- 9,176,287


----------------------------------------------------------target variable section--------------------------------------------------------


----------------- Get purchase data for next 1 month

--drop table if exists ssoni.gp_next_month_purchases_training1;
create table ssoni.gp_next_month_purchases_training1 stored as orc as
select  customer_key,
transaction_date,
transaction_num,
line_num,
order_status,
item_qty,
sales_amt
from mds_new.ods_orderline_t
where country = 'US' and brand='GP' and order_status <> 'D' and transaction_date between date_add(${hiveconf:purch_window_start_date},366) and date_add(${hiveconf:purch_window_start_date},396)  and sales_amt > 0 and item_qty > 0;
--order by customer_key,transaction_date,line_num;
--


-------------- Group the next 1 month purchases by customerkey

--drop table if exists ssoni.gp_next_month_custkey_grouped_training1;
create table ssoni.gp_next_month_custkey_grouped_training1 stored as orc as 
select customer_key,transaction_date,transaction_num,sum(item_qty) as sum_items,sum(sales_amt) as sum_sales
from ssoni.gp_next_month_purchases_training1
group by customer_key,transaction_date,transaction_num;
--


------------------ Join with masterkey

--drop table if exists ssoni.gp_next_month_purch_master_notnull_training1;
create table ssoni.gp_next_month_purch_master_notnull_training1 stored as orc as 
select b.masterkey,a.*,(case when a.sum_items > 0 then 1 else 0 end) as purchase_in_target_window
from ssoni.gp_next_month_custkey_grouped_training1 a 
left outer join (select distinct customerkey,masterkey from ssoni.crmanalyticsdata) b
on a.customer_key=b.customerkey
where b.masterkey is not null;
--order by masterkey,transaction_date;
-- 1,378,556


---------------- Get the total items purchased at masterkey level for next 1 month

--drop table if exists ssoni.gp_purchase_in_target_window_training1;
create table ssoni.gp_purchase_in_target_window_training1 stored as orc as
select distinct masterkey,purchase_in_target_window 
from ssoni.gp_next_month_purch_master_notnull_training1;
-- 600,394





----------------------3 month variables section---------------------

--drop table if exists ssoni.gp_next_3months_purchases_training1;
create table ssoni.gp_next_3months_purchases_training1 stored as orc as
select  customer_key,
transaction_date,
transaction_num,
line_num,
order_status,
item_qty,
sales_amt
from mds_new.ods_orderline_t
where country = 'US' and brand='GP' and order_status <> 'D' and transaction_date between date_add(${hiveconf:purch_window_start_date},366) and date_add(${hiveconf:purch_window_start_date},457)  and sales_amt > 0 and item_qty > 0;
--order by customer_key,transaction_date,line_num;
--

--drop table if exists ssoni.gp_next_3months_custkey_grouped_training1;
create table ssoni.gp_next_3months_custkey_grouped_training1 stored as orc as 
select customer_key,transaction_date,transaction_num,sum(item_qty) as sum_items,sum(sales_amt) as sum_sales
from ssoni.gp_next_3months_purchases_training1 
group by customer_key,transaction_date,transaction_num;
--


--drop table if exists ssoni.gp_next_3months_purch_master_notnull_training1;
create table ssoni.gp_next_3months_purch_master_notnull_training1 stored as orc  as 
select b.masterkey,a.*,(case when a.sum_items > 0 then 1 else 0 end) as purchase_in_3month_target_window
from ssoni.gp_next_3months_custkey_grouped_training1 a 
left outer join (select distinct customerkey,masterkey from ssoni.crmanalyticsdata) b
on a.customer_key=b.customerkey
where b.masterkey is not null
order by masterkey,transaction_date;
-- 3,720,711


--drop table if exists ssoni.gp_purchase_in_3month_target_window_training1;
create table ssoni.gp_purchase_in_3month_target_window_training1 stored as orc as
select distinct masterkey,purchase_in_3month_target_window 
from ssoni.gp_next_3months_purch_master_notnull_training1;
-- 1,451,972



-----------------------------6 month target variable section -------------------------


--drop table if exists ssoni.gp_next_6months_purchases_training1;
create table ssoni.gp_next_6months_purchases_training1 stored as orc as
select  customer_key,
transaction_date,
transaction_num,
line_num,
order_status,
item_qty,
sales_amt
from mds_new.ods_orderline_t
where country = 'US' and brand='GP' and order_status <> 'D' and transaction_date between date_add(${hiveconf:purch_window_start_date},366) and date_add(${hiveconf:purch_window_start_date},548)  and sales_amt > 0 and item_qty > 0;
--order by customer_key,transaction_date,line_num;
--

--drop table if exists ssoni.gp_next_6months_custkey_grouped_training1;
create table ssoni.gp_next_6months_custkey_grouped_training1 stored as orc as 
select customer_key,transaction_date,transaction_num,sum(item_qty) as sum_items,sum(sales_amt) as sum_sales
from ssoni.gp_next_6months_purchases_training1 
group by customer_key,transaction_date,transaction_num;
--


--drop table if exists ssoni.gp_next_6months_purch_master_notnull_training1;
create table ssoni.gp_next_6months_purch_master_notnull_training1 stored as orc  as 
select b.masterkey,a.*,(case when a.sum_items > 0 then 1 else 0 end) as purchase_in_6month_target_window
from ssoni.gp_next_6months_custkey_grouped_training1 a 
left outer join (select distinct customerkey,masterkey from ssoni.crmanalyticsdata) b
on a.customer_key=b.customerkey
where b.masterkey is not null
order by masterkey,transaction_date;
-- 3,720,711


--drop table if exists ssoni.gp_purchase_in_6month_target_window_training1;
create table ssoni.gp_purchase_in_6month_target_window_training1 stored as orc as
select distinct masterkey,purchase_in_6month_target_window 
from ssoni.gp_next_6months_purch_master_notnull_training1;
-- 5,071,654




----------------------------12 months target section --------------------------------------------------------


--drop table if exists ssoni.gp_next_12months_purchases_training1;
create table ssoni.gp_next_12months_purchases_training1 stored as orc as
select  customer_key,
transaction_date,
transaction_num,
line_num,
order_status,
item_qty,
sales_amt
from mds_new.ods_orderline_t
where country = 'US' and brand='GP' and order_status <> 'D' and transaction_date between date_add(${hiveconf:purch_window_start_date},366) and date_add(${hiveconf:purch_window_start_date},731)  and sales_amt > 0 and item_qty > 0;
--

--drop table if exists ssoni.gp_next_12months_custkey_grouped_training1;
create table ssoni.gp_next_12months_custkey_grouped_training1 stored as orc as 
select customer_key,transaction_date,transaction_num,sum(item_qty) as sum_items,sum(sales_amt) as sum_sales
from ssoni.gp_next_12months_purchases_training1 
group by customer_key,transaction_date,transaction_num;
--


--drop table if exists ssoni.gp_next_12months_purch_master_notnull_training1;
create table ssoni.gp_next_12months_purch_master_notnull_training1 stored as orc  as 
select b.masterkey,a.*,(case when a.sum_items > 0 then 1 else 0 end) as purchase_in_12month_target_window
from ssoni.gp_next_12months_custkey_grouped_training1 a 
left outer join (select distinct customerkey,masterkey from ssoni.crmanalyticsdata) b
on a.customer_key=b.customerkey
where b.masterkey is not null;
-- 30,358,461


--drop table if exists ssoni.gp_purchase_in_12month_target_window_training1;
create table ssoni.gp_purchase_in_12month_target_window_training1 stored as orc as
select distinct masterkey,purchase_in_12month_target_window 
from ssoni.gp_next_12months_purch_master_notnull_training1;
-- 7,744,811

----------------------------12 month target  section ends--------------------------------------------------------


-----------------------------------------------------------

-- should be final table for analysis
--drop table if exists ssoni.gp_all_variables_targets_training1;
create table ssoni.gp_all_variables_targets_training1 stored as orc  as
select a.*,coalesce(c.lastyear_samemonth_purchase,0) as lastyear_samemonth_purchase,coalesce(b.purchase_in_target_window,0) as purchase_in_target_window,
coalesce(e.lastyear_same3months_purchase,0) as lastyear_same3months_purchase,coalesce(d.purchase_in_3month_target_window,0) as purchase_in_3month_target_window,
coalesce(f.purchase_in_6month_target_window,0) as purchase_in_6month_target_window, coalesce(g.purchase_in_12month_target_window,0) as purchase_in_12month_target_window
from ssoni.gp_all_variables_training1 a
left outer join ssoni.gp_purchase_in_target_window_training1 b
on a.masterkey = b.masterkey
left outer join ssoni.gp_lastyear_samemonth_purchase_training1 c
on a.masterkey = c.masterkey
left outer join ssoni.gp_purchase_in_3month_target_window_training1 d
on a.masterkey = d.masterkey
left outer join ssoni.gp_lastyear_same3months_purchase_training1 e
on a.masterkey = e.masterkey
left outer join ssoni.gp_purchase_in_6month_target_window_training1 f
on a.masterkey = f.masterkey
left outer join ssoni.gp_purchase_in_12month_target_window_training1 g
on a.masterkey = g.masterkey;
-- 3,555,302


----------------------------------- Create external table and export

drop table if exists ssoni.gp_prob_purch_training1_export;

create external table ssoni.gp_prob_purch_training1_export
(masterkey string,
total_orders_placed int,
active_period double,
recency_purch double,
freq_purch double,
ratio_purchases double,
items_browsed int,
items_added int,
count_distinct_divisions int,
discount_percentage float,
total_net_sales float,
lastyear_samemonth_purchase int,
purchase_in_target_window int,
lastyear_same3months_purchase int,
purchase_in_3month_target_window int,
purchase_in_6month_target_window int,
purchase_in_12month_target_window int)              
row format delimited 
fields terminated by '|' 
lines terminated by '\n' 
location '/user/ssoni/gp_prob_purch_training1_export'
TBLPROPERTIES('serialization.null.format'='');

insert overwrite table ssoni.gp_prob_purch_training1_export
select * from ssoni.gp_all_variables_targets_training1;

hadoop fs -getmerge /user/ssoni/gp_prob_purch_training1_export /home/ssoni

