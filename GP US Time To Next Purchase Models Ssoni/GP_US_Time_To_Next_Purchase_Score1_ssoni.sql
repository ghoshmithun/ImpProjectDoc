----------------------- set hive options
use ssoni;
set mapred.job.queue.name=aa;  
set hive.cli.print.header=true;

------------------------Set the variable dates. Change these dates for changing the time period of scoring

set purch_window_start_date = '2016-07-22';


------------------Get the data of crmanalytics stored at a place.
-------------------This table is already run. Refresh it if running if after few weeks.

--drop table if exists ssoni.crmanalyticsdata;
create table ssoni.crmanalyticsdata stored as orc as 
select distinct customerkey,unknownshopperid
from crmanalytics.customerresolution
where customerkey is not null;



-----------------------initial table containing purchases

----Try the orderline version. Changing the dates to Jan 16 to Dec 16
--drop table if exists ssoni.gp_lastyear_purchases_orderline_score1;
create table ssoni.gp_lastyear_purchases_orderline_score1 stored as orc as
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

--drop table if exists ssoni.gp_lastyear_purchases_custkey_grouped_score1;
create table ssoni.gp_lastyear_purchases_custkey_grouped_score1 stored as orc  as 
select  a.customer_key,a.transaction_date,a.transaction_num,b.first_purchase_brand_flag,sum(a.item_qty) as sum_items,sum(a.sales_amt) as sum_sales
from ssoni.gp_lastyear_purchases_orderline_score1 a 
left outer join (select distinct customer_key,transaction_num,first_purchase_brand_flag from mds_new.ods_orderheader_t) b 
on a.customer_key = b.customer_key and a.transaction_num = b.transaction_num
group by a.customer_key,a.transaction_date,a.transaction_num,b.first_purchase_brand_flag;
-- 33,190,451 (10007115 distinct custkeys)



--------------------- Get the purchase data for first 6 months grouped by customer_key

create table ssoni.gp_first_6months_purchases_score1 stored as orc as 
select customer_key,sum(sum_items) as first_6months_purchases
from ssoni.gp_lastyear_purchases_custkey_grouped_score1
where transaction_date between ${hiveconf:purch_window_start_date} and  date_add(${hiveconf:purch_window_start_date},182)
group by customer_key;


------------------- Get the purchase data for last 6 months grouped by customer_key

create table ssoni.gp_last_6months_purchases_score1 stored as orc as 
select customer_key,sum(sum_items) as last_6months_purchases
from ssoni.gp_lastyear_purchases_custkey_grouped_score1
where transaction_date between date_add(${hiveconf:purch_window_start_date},183) and date_add(${hiveconf:purch_window_start_date},365)
group by customer_key;

	
-------------------- Get the ratio of purchases in last 6 months to first 6 months. Ratio is of number of items purchased

--drop table if exists ssoni.gp_ratio_purchases_score1;
create table ssoni.gp_ratio_purchases_score1 stored as orc as 
select a.customer_key,round(b.last_6months_purchases/a.first_6months_purchases,2)  as ratio_purchases
from ssoni.gp_first_6months_purchases_score1 a 
inner join ssoni.gp_last_6months_purchases_score1 b on a.customer_key = b.customer_key;


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

--drop table if exists ssoni.gp_lastqrtr_browsing_score1;
create table ssoni.gp_lastqrtr_browsing_score1 stored as orc as 
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

--drop table if exists ssoni.gp_lastqrtr_browsing_total_items_score1;
create table ssoni.gp_lastqrtr_browsing_total_items_score1 stored as orc as 
select b.customer_key,sum(a.gp_hit_ind_tot) as items_browsed 
from ssoni.gp_lastqrtr_browsing_score1 a 
inner join ssoni.crmanalyticsdata b on a.unk_shop_id = b.unknownshopperid
group by b.customer_key;



--Check the omniture table version here. For older data, old table to be used. Change it for newer data
--drop table if exists ssoni.gp_cart_add_score1;
--create table ssoni.gp_cart_add_score1 as 
--select distinct customer_key,unk_shop_id,
--visit_num,
--pagename,
--purchaseid,
--prop33,
--country,
--t_time_info,
--substr(prop1,1,6) as style_cd  
--from omniture.hit_data_t a 
--left outer join (select distinct unknownshopperid,customer_key from ssoni.crmanalyticsdata) b  on a.unk_shop_id=b.unknownshopperid
--where (instr(pagename,'gp:browse')>0 or instr(pagename,'mobile:gp')>0) and (date_time) between unix_timestamp(date_add(${hiveconf:purch_window_start_date},274)) and unix_timestamp(date_add(${hiveconf:purch_window_start_date},365)) and prop33='inlineBagAdd';
--run 4,976,141


------------------ Get the card addition data from hit_data_agg 

--drop table if exists ssoni.gp_cart_add_score1;
create table ssoni.gp_cart_add_score1 stored as orc as 
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


---------------- Get the total items added to cart for each customer_key

--drop table if exists ssoni.gp_lastqrtr_cart_add_total_items_score1;
create table ssoni.gp_lastqrtr_cart_add_total_items_score1 stored as orc as 
select b.customer_key,sum(a.gp_hit_ind_tot) as items_added
from ssoni.gp_cart_add_score1 a 
inner join ssoni.crmanalyticsdata b on a.unk_shop_id = b.unknownshopperid
group by b.customer_key;


-------------------- Get the total items purchased grouped at customer_key level

--drop table if exists ssoni.gp_lastyear_total_items_purch_score1;
create table ssoni.gp_lastyear_total_items_purch_score1 stored as orc  as
select customer_key,count(transaction_num) as total_orders_placed,sum(sum_sales) as total_sales_amt
from ssoni.gp_lastyear_purchases_custkey_grouped_score1
group by customer_key
order by total_orders_placed desc,total_sales_amt desc;
--run 9,177,392 ()


--------------------- Get the flag status of first purchase

--drop table if exists ssoni.gp_first_purchase_flag_score1;
create table ssoni.gp_first_purchase_flag_score1 stored as orc as 
select customer_key,transaction_date,first_purchase_brand_flag from 
(select customer_key,transaction_date,first_purchase_brand_flag,row_number() over (partition by customer_key order by transaction_date,transaction_num) as first_click from ssoni.gp_lastyear_purchases_custkey_grouped_score1) T
where first_click = 1;
-- 3,884,484


---------------------- Get the customer's active period in months. If first purchase brand flag is N for all transactions then it is 12, otherwise difference between first purchase and training window end date

--drop table if exists ssoni.gp_active_period_score1;
create table ssoni.gp_active_period_score1 stored as orc as
select customer_key,transaction_date,first_purchase_brand_flag,
(case when first_purchase_brand_flag = 'N' then 12.0 else round(datediff(date_add(${hiveconf:purch_window_start_date},366),transaction_date)/30.4,1) end) as active_period
from ssoni.gp_first_purchase_flag_score1; 
-- 3,884,484


--------------------- Get the transaction date for last purchase

--drop table if exists ssoni.gp_last_purch_score1;
create table ssoni.gp_last_purch_score1 stored as orc as 
select customer_key,transaction_date from 
(select customer_key,transaction_date,row_number() over (partition by customer_key order by transaction_date desc,transaction_num desc) as last_purch from ssoni.gp_lastyear_purchases_custkey_grouped_score1) T
where last_purch = 1;
-- 3,884,484


---------------------- Get the recency of last purchase. Training window end date minus the last transaction date

--drop table if exists ssoni.gp_recency_purch_score1;
create table ssoni.gp_recency_purch_score1 stored as orc as
select customer_key,transaction_date,round(datediff(date_add(${hiveconf:purch_window_start_date},366),transaction_date)/30.4,1) as recency_purch
from ssoni.gp_last_purch_score1; 
-- 3,884,484


-------------------- Count the number of distinct divisions shopped from

--drop table if exists ssoni.gp_count_distinct_divisions_score1;
create table ssoni.gp_count_distinct_divisions_score1 stored as orc as 
select customer_key,count(distinct division) as count_distinct_divisions 
from ssoni.gp_lastyear_purchases_orderline_score1 a 
inner join ssoni.crmanalyticsdata b on  a.customer_key = b.customerkey
where division <> 'NA'
group by customer_key;


----------------- Get the average discount percentage, total purchases and total net sales for each customer_key

--drop table if exists ssoni.gp_avg_discount_percentage_score1;
create table ssoni.gp_avg_discount_percentage_score1 stored as orc as 
select customer_key,round(sum(sales_amt),2) as total_sales,round(sum(discount_amt),2) as total_discount, round(sum(discount_amt)*(-100.0)/(sum(sales_amt)),2) as discount_percentage,
count(distinct transaction_num) as total_purchases,round(sum(sales_amt) + sum(discount_amt),2) as total_net_sales
from ssoni.gp_lastyear_purchases_orderline_score1 a 
inner join ssoni.crmanalyticsdata b on a.customer_key = b.customerkey
where (item_qty > 0 and sales_amt > 0) and (sales_amt + discount_amt > 0)
group by customer_key;

-- 55866 with 0 total net sales


---------------------- Get the purchases of first month of training period

--drop table if exists ssoni.gp_lastyear_same_month_purchases_score1;
create table ssoni.gp_lastyear_same_month_purchases_score1 stored as orc as
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

--drop table if exists ssoni.gp_lastyear_same_month_custkey_grouped_score1;
create table ssoni.gp_lastyear_same_month_custkey_grouped_score1 stored as orc as 
select customer_key,transaction_date,transaction_num,sum(item_qty) as sum_items,sum(sales_amt) as sum_sales
from ssoni.gp_lastyear_same_month_purchases_score1
group by customer_key,transaction_date,transaction_num;



--------------------- Binary flag variable indicating whether made any purchase in first 1 month of training window 

--drop table if exists ssoni.gp_lastyear_samemonth_purchase_score1;
create table ssoni.gp_lastyear_samemonth_purchase_score1 stored as orc as
select distinct customer_key,(case when sum_items > 0 then 1 else 0 end) as lastyear_samemonth_purchase 
from ssoni.gp_lastyear_same_month_custkey_grouped_score1;
-- 562,765


------------------------- Repeating the same process for first 3 months of training window----------------------------

--drop table if exists ssoni.gp_lastyear_same3months_purchases_score1;
create table ssoni.gp_lastyear_same3months_purchases_score1 stored as orc as
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

--drop table if exists ssoni.gp_lastyear_same3months_custkey_grouped_score1;
create table ssoni.gp_lastyear_same3months_custkey_grouped_score1 stored as orc as 
select customer_key,transaction_date,transaction_num,sum(item_qty) as sum_items,sum(sales_amt) as sum_sales
from ssoni.gp_lastyear_same3months_purchases_score1 
group by customer_key,transaction_date,transaction_num;
--


------------------ Binary flag variable indicating whether made any purchase in first 3 months of training window 

--drop table if exists ssoni.gp_lastyear_same3months_purchase_score1;
create table ssoni.gp_lastyear_same3months_purchase_score1 stored as orc  as
select distinct customer_key,(case when sum_items > 0 then 1 else 0 end) as lastyear_same3months_purchase 
from ssoni.gp_lastyear_same3months_custkey_grouped_score1;
-- 1,527,846


--------------------------------- Combine all the variables into this consolidated table

--drop table if exists ssoni.gp_all_variables_score1;
create table ssoni.gp_all_variables_score1 stored as orc as
select distinct a.customer_key,a.total_orders_placed,b.active_period,c.recency_purch,round(a.total_orders_placed/b.active_period,3) as freq_purch,coalesce(d.ratio_purchases,0) as ratio_purchases,
coalesce(e.items_browsed,0) as items_browsed,coalesce(f.items_added,0) as items_added,coalesce(g.count_distinct_divisions,0) as count_distinct_divisions,
coalesce(h.discount_percentage,0) as discount_percentage,coalesce(h.total_net_sales,0) as total_net_sales,
coalesce(i.lastyear_samemonth_purchase,0) as lastyear_samemonth_purchase, coalesce(j.lastyear_same3months_purchase,0) as lastyear_same3months_purchase
from ssoni.gp_lastyear_total_items_purch_score1 a
left outer join ssoni.gp_active_period_score1 b
on a.customer_key = b.customer_key
left outer join ssoni.gp_recency_purch_score1 c
on a.customer_key = c.customer_key
left outer join ssoni.gp_ratio_purchases_score1 d
on a.customer_key = d.customer_key
left outer join ssoni.gp_lastqrtr_browsing_total_items_score1 e
on a.customer_key = e.customer_key
left outer join ssoni.gp_lastqrtr_cart_add_total_items_score1 f
on a.customer_key = f.customer_key
left outer join ssoni.gp_count_distinct_divisions_score1 g
on a.customer_key = g.customer_key
left outer join ssoni.gp_avg_discount_percentage_score1 h
on a.customer_key = h.customer_key
left outer join ssoni.gp_lastyear_samemonth_purchase_score1 i 
on a.customer_key = i.customer_key
left outer join ssoni.gp_lastyear_same3months_purchase_score1 j
on a.customer_key = j.customer_key
where total_net_sales > 0;

-- 9,176,287



----------------------------------- Create external table and export

drop table if exists ssoni.gp_time_to_next_purchase_score1_export;

create external table ssoni.gp_time_to_next_purchase_score1_export
(customer_key string,
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
lastyear_same3months_purchase int)
row format delimited 
fields terminated by '|' 
lines terminated by '\n' 
location '/user/ssoni/gp_time_to_next_purchase_score1_export'
TBLPROPERTIES('serialization.null.format'='');

insert overwrite table ssoni.gp_time_to_next_purchase_score1_export
select * from ssoni.gp_all_variables_score1;

hadoop fs -getmerge /user/ssoni/gp_time_to_next_purchase_score1_export /home/ssoni

