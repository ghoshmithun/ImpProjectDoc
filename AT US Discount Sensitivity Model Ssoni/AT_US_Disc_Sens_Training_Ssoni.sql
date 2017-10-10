--------AT Discount Sensitivity Model
------- Author - Shrikant Soni
------- Last Updated - 30th June 2017



--------- Set the purchase window start date 

set purch_window_start_date = '2015-05-01';   ----  yyyy-mm-dd


-----------  Take the last year transactions for AT US 

--drop table if exists ssoni.AT_DS_last_year_purchases_training;
create table ssoni.AT_DS_last_year_purchases_training stored as orc as
select a.customer_key,
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
where a.country = 'US' and a.brand='AT' and a.order_status <> 'D' and a.transaction_date between ${hiveconf:purch_window_start_date} and date_add(${hiveconf:purch_window_start_date},366)  and a.sales_amt > 0 and a.item_qty > 0 ;
-- 13,942,105


------------- Group the last year purchases by customer key and transaction date and num

--drop table if exists ssoni.AT_DS_last_year_purchases_custkey_grouped_training;
create table ssoni.AT_DS_last_year_purchases_custkey_grouped_training stored as orc  as 
select a.customer_key,a.transaction_date,a.transaction_num,b.first_purchase_brand_flag,sum(a.item_qty) as sum_items,sum(a.sales_amt + a.discount_amt) as sum_sales
from ssoni.AT_DS_last_year_purchases_training a 
left outer join mds_new.ods_orderheader_t b 
on a.customer_key = b.customer_key and a.transaction_num = b.transaction_num
where (a.sales_amt + a.discount_amt >= 0) and (a.sales_amt >= a.discount_amt )
group by a.customer_key,a.transaction_date,a.transaction_num,b.first_purchase_brand_flag;
-- distinct custkey (1,918,932) total rows 5,431,222


------------ Get the avg discount percentage, total purchases and total net sales for last year transactions

--drop table if exists ssoni.AT_DS_avg_discount_percentage_training;
create table ssoni.AT_DS_avg_discount_percentage_training stored as orc as 
select customer_key,round(sum(sales_amt),2) as total_sales,round(sum(discount_amt),2) as total_discount, round(sum(discount_amt)*(-100.0)/(sum(sales_amt)),2) as discount_percentage,
count(distinct transaction_num) as total_purchases,round(sum(sales_amt) + sum(discount_amt),2) as total_net_sales
from ssoni.AT_DS_last_year_purchases_training a 
where item_qty > 0 and sales_amt > 0 and (sales_amt + discount_amt >= 0) and (sales_amt >= discount_amt )
group by customer_key;
-- 1,918,932

--------------- Map the customer base to employees

--drop table if exists ssoni.AT_DS_customer_base_training;
create table ssoni.AT_DS_customer_base_training stored as orc  as 
select a.customer_key,(case when b.flag_employee ='Y' then 'Y' else 'N' end) as flag_employee 
from (select distinct customer_key from ssoni.AT_DS_last_year_purchases_custkey_grouped_training) a 
left outer join (select distinct customer_key , flag_employee from mds_new.ods_corp_cust_t) b  on a.customer_key=b.customer_key;
-- 1,918,932


--------------- Get the Sister brands purchases for the customer base for last year

--drop table if exists ssoni.AT_DS_sister_brands_last_year_purchases_training;
create table ssoni.AT_DS_sister_brands_last_year_purchases_training stored as orc as
select distinct a.customer_key,
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
inner join ssoni.AT_DS_customer_base_training d on a.customer_key = d.customer_key 
where a.country = 'US' and a.brand in ('BR','GP','ON') and a.order_status <> 'D' and a.transaction_date between ${hiveconf:purch_window_start_date} and date_add(${hiveconf:purch_window_start_date},366)  and a.sales_amt > 0 and a.item_qty > 0;
-- 52,278,651


-------------- Group the above sister brand transactions by customer key and transaction date and num

--drop table if exists ssoni.AT_DS_last_year_sister_brand_purchases_custkey_grouped_training;
create table ssoni.AT_DS_last_year_sister_brand_purchases_custkey_grouped_training stored as orc  as 
select a.customer_key,a.transaction_date,a.transaction_num,sum(a.item_qty) as sum_items,sum(a.sales_amt + a.discount_amt) as sum_sales
from ssoni.AT_DS_sister_brands_last_year_purchases_training a
where (a.sales_amt + a.discount_amt >= 0) and (a.sales_amt >= a.discount_amt ) 
group by a.customer_key,a.transaction_date,a.transaction_num;
-- 11,818,654

--------------- Last purchase date of sister brands

--drop table if exists ssoni.AT_DS_sister_brands_last_purch_training;
create table ssoni.AT_DS_sister_brands_last_purch_training stored as orc as 
select customer_key,transaction_date from 
(select customer_key,transaction_date,row_number() over (partition by customer_key order by transaction_date desc,transaction_num desc) as last_purch from ssoni.AT_DS_last_year_sister_brand_purchases_custkey_grouped_training) T
where last_purch = 1;
-- 1,325,009


------------- Recency of purchase for sister brands in months

--drop table if exists ssoni.AT_DS_sister_brands_recency_purch_training;
create table ssoni.AT_DS_sister_brands_recency_purch_training stored as orc as
select customer_key,transaction_date,round(datediff(date_add(${hiveconf:purch_window_start_date},367),transaction_date)/30.4,1) as sister_brands_recency_purch
from ssoni.AT_DS_sister_brands_last_purch_training; 
-- 1,325,009


------------ Avg discount percentage, total net sales and total purchase of sister brands

--drop table if exists ssoni.AT_DS_sister_brands_avg_discount_percentage_training;
create table ssoni.AT_DS_sister_brands_avg_discount_percentage_training stored as orc as 
select customer_key,round(sum(sales_amt),2) as sister_brands_total_sales,round(sum(discount_amt),2) as sister_brands_total_discount, round(sum(discount_amt)*(-100.0)/(sum(sales_amt)),2) as sister_brands_discount_percentage,
count(distinct transaction_num) as sister_brands_total_purchases,round(sum(sales_amt) + sum(discount_amt),2) as sister_brands_total_net_sales
from ssoni.AT_DS_sister_brands_last_year_purchases_training a 
where item_qty > 0 and sales_amt > 0 and (sales_amt + discount_amt > 0) and (sales_amt >= discount_amt )
group by customer_key;
-- 1,324,973


------------ Purchase transactions data for the next year

--drop table if exists ssoni.AT_DS_next_year_purchases_training;
create table ssoni.AT_DS_next_year_purchases_training stored as orc as
select distinct a.customer_key,
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
inner join ssoni.AT_DS_customer_base_training d on a.customer_key = d.customer_key
where a.country = 'US' and a.brand='AT' and a.order_status <> 'D' and a.transaction_date between date_add(${hiveconf:purch_window_start_date},366) and date_add(${hiveconf:purch_window_start_date},732)  and a.sales_amt > 0 and a.item_qty > 0 ;
-- 10,410,334


----------------  Avg discount percentage, total net sales and total purchase for next year purchase data 

--drop table if exists ssoni.AT_DS_next_year_avg_discount_percentage_training;
create table ssoni.AT_DS_next_year_avg_discount_percentage_training stored as orc as 
select customer_key,round(sum(sales_amt),2) as next_year_total_sales,round(sum(discount_amt),2) as next_year_total_discount, round(sum(discount_amt)*(-100.0)/(sum(sales_amt)),2) as next_year_discount_percentage,
count(distinct transaction_num) as next_year_total_purchases,round(sum(sales_amt) + sum(discount_amt),2) as next_year_total_net_sales
from ssoni.AT_DS_next_year_purchases_training a 
where (item_qty > 0 and sales_amt > 0) and (sales_amt + discount_amt > 0) and (sales_amt >= discount_amt ) and discount_amt <= 0
group by customer_key;
-- 910,451


----------------1) DM Campaigns------------

--drop table if exists ssoni.AT_DS_DM_all_campaigns_training;
create table ssoni.AT_DS_DM_all_campaigns_training stored as orc as 
select a.customer_key, b.cell_key,substr(c.gap_cell_start_date,1,10) as campaign_start_date  ,substr(c.gap_cell_end_date,1,10) as campaign_end_date 
from ssoni.AT_DS_customer_base_training a 
inner join mds_new.ods_mailingmailhouse_t b on a.customer_key = b.customer_key 
inner join mds_new.ods_cell_t c on b.cell_key=c.cell_key 
where b.brand ='AT' and b.country='US' and b.event_type='M' and substr(c.gap_cell_start_date,1,10) between  ${hiveconf:purch_window_start_date} and  date_add(${hiveconf:purch_window_start_date}, 365)
and substr(c.gap_cell_end_date,1,10) between ${hiveconf:purch_window_start_date} and  date_add(${hiveconf:purch_window_start_date}, 365);
-- 449,764


---------------  Purchases during DM campaign

--drop table if exists ssoni.AT_DS_DM_Purchases_training;
create table ssoni.AT_DS_DM_Purchases_training stored as orc as 
select a.* , b.campaign_start_date , b.campaign_end_date , (case when a.transaction_date  between b.campaign_start_date and b.campaign_end_date then 1 else 0 end ) as promotion_period_purchase, 
(case when month(a.transaction_date) >11  then 1  when month(a.transaction_date) =11  and day(a.transaction_date) >16 then 1 else  0 end ) as holiday_season_purchase 
from ssoni.AT_DS_last_year_purchases_custkey_grouped_training a 
left outer join ssoni.AT_DS_DM_all_campaigns_training b on a.customer_key = b.customer_key;
-- 5,580,208


------------ Distinct values of the above table

--drop table if exists ssoni.AT_DS_DM_Purchases_Distinct_Training;
create table ssoni.AT_DS_DM_Purchases_Distinct_Training stored as orc as 
select distinct customer_key,transaction_date,transaction_num,sum_items,sum_sales,promotion_period_purchase,holiday_season_purchase
from AT_DS_DM_Purchases_training;
-- 5,441,468


-------------- Total sales during promotion periods

drop table if exists ssoni.AT_DS_Promotion_Period_Sales_Training;
create table ssoni.AT_DS_Promotion_Period_Sales_Training stored as orc as 
select customer_key,sum(sum_sales) as promotion_period_sales
from ssoni.AT_DS_DM_Purchases_Distinct_Training
where promotion_period_purchase = 1
group by customer_key;
-- 45,001

----------------- Total sales during holiday season (holiday season defifned as 15th Nov to 31st Dec)

drop table if exists ssoni.AT_DS_Holiday_Season_Sales_Training;
create table ssoni.AT_DS_Holiday_Season_Sales_Training stored as orc as 
select customer_key,sum(sum_sales) as holiday_season_sales
from ssoni.AT_DS_DM_Purchases_Distinct_Training
where holiday_season_purchase = 1
group by customer_key;
-- 547,945

----------------------  Ratio of sales in promotion period to total sales

drop table if exists ssoni.AT_DS_Promotion_Period_Sales_Ratio_Training;
create table ssoni.AT_DS_Promotion_Period_Sales_Ratio_Training stored as orc as 
select a.customer_key,a.promotion_period_sales,round(a.promotion_period_sales/b.total_net_sales,2) as promotion_period_sales_ratio
from ssoni.AT_DS_Promotion_Period_Sales_Training a 
inner join ssoni.AT_DS_avg_discount_percentage_training b on a.customer_key = b.customer_key
where b.total_net_sales > 0
group by a.customer_key,a.promotion_period_sales,b.total_net_sales;
-- 45,000

--------------------- Ratio of sales in holiday season to total sales

drop table if exists ssoni.AT_DS_Holiday_Season_Sales_Ratio_Training;
create table ssoni.AT_DS_Holiday_Season_Sales_Ratio_Training stored as orc as 
select a.customer_key,a.holiday_season_sales,round(a.holiday_season_sales/b.total_net_sales,2) as holiday_season_sales_ratio
from ssoni.AT_DS_Holiday_Season_Sales_Training a 
inner join ssoni.AT_DS_avg_discount_percentage_training b on a.customer_key = b.customer_key
where b.total_net_sales > 0
group by a.customer_key,a.holiday_season_sales,b.total_net_sales;
-- 547,924

-----------------------------------------------------Recency and frequency of purchases ---------------------------------------------


------------------ Total items purchased in first 6 months of the training window

drop table if exists ssoni.AT_DS_first_6months_purchases_training;
create table ssoni.AT_DS_first_6months_purchases_training stored as orc as 
select customer_key,sum(sum_items) as first_6months_purchases
from ssoni.AT_DS_last_year_purchases_custkey_grouped_training
where transaction_date between ${hiveconf:purch_window_start_date} and  date_add(${hiveconf:purch_window_start_date},182)
group by customer_key;
-- 1,135,442

------------------ Total items purchased in last 6 months of the training window

drop table if exists ssoni.AT_DS_last_6months_purchases_training;
create table ssoni.AT_DS_last_6months_purchases_training stored as orc as 
select customer_key,sum(sum_items) as last_6months_purchases
from ssoni.AT_DS_last_year_purchases_custkey_grouped_training
where transaction_date between date_add(${hiveconf:purch_window_start_date},183) and date_add(${hiveconf:purch_window_start_date},365)
group by customer_key;
-- 1,285,355

------------------ Ratio of Total items purchased in last 6 months to first 6 months of the training window

drop table if exists ssoni.AT_DS_ratio_purchases_training;
create table ssoni.AT_DS_ratio_purchases_training stored as orc as 
select a.customer_key,round(b.last_6months_purchases/a.first_6months_purchases,2)  as ratio_purchases
from ssoni.AT_DS_first_6months_purchases_training a 
inner join ssoni.AT_DS_last_6months_purchases_training b on a.customer_key = b.customer_key;
-- 504,009

----------------- Find the transaction date of the first purchase

drop table if exists ssoni.AT_DS_first_purchase_flag_training;
create table ssoni.AT_DS_first_purchase_flag_training stored as orc as 
select customer_key,transaction_date,first_purchase_brand_flag from 
(select customer_key,transaction_date,first_purchase_brand_flag,row_number() over (partition by customer_key order by transaction_date,transaction_num) as first_click from ssoni.AT_DS_last_year_purchases_custkey_grouped_training) T
where first_click = 1;
-- 1,918,932


---------------- Find the active period. If first purchase brand flag is N for all transactions, then customer is active for more than 12 months, else the time difference between first purchase and training period end date

drop table if exists ssoni.AT_DS_active_period_training;
create table ssoni.AT_DS_active_period_training stored as orc as
select customer_key,transaction_date,first_purchase_brand_flag,
(case when first_purchase_brand_flag = 'N' then 12.0 else round(datediff(date_add(${hiveconf:purch_window_start_date},367),transaction_date)/30.4,1) end) as active_period
from ssoni.AT_DS_first_purchase_flag_training; 
-- 1,918,932


----------------- Find the transaction date of last purchase

drop table if exists ssoni.AT_DS_last_purch_training;
create table ssoni.AT_DS_last_purch_training stored as orc as 
select customer_key,transaction_date from 
(select customer_key,transaction_date,row_number() over (partition by customer_key order by transaction_date desc,transaction_num desc) as last_purch from ssoni.AT_DS_last_year_purchases_custkey_grouped_training) T
where last_purch = 1;
-- 1,918,932


---------------- Find the recency of purchase, time difference between last purchase and training period end date

drop table if exists ssoni.AT_DS_recency_purch_training;
create table ssoni.AT_DS_recency_purch_training stored as orc as
select customer_key,transaction_date,round(datediff(date_add(${hiveconf:purch_window_start_date},367),transaction_date)/30.4,1) as recency_purch
from ssoni.AT_DS_last_purch_training; 
-- 1,918,932


------------------ Count the number of distinct divisions the customer has shopped from

drop table if exists ssoni.AT_DS_count_distinct_divisions_training;
create table ssoni.AT_DS_count_distinct_divisions_training stored as orc as 
select customer_key,count(distinct division) as count_distinct_divisions 
from ssoni.AT_DS_last_year_purchases_training a 
where division <> 'NA'
group by customer_key;
-- 1,883,710

--------------------------------------------------------------Customer card status----------------------------------------------


---------------- Get details of the customer brand loyalty card 

--drop table if exists ssoni.AT_DS_Cust_Card_Status_training;
create table ssoni.AT_DS_Cust_Card_Status_training stored as orc as 
select distinct a.customer_key,substr(b.open_date,1,10) as open_date,substr(b.reissue_date,1,10) as reissue_date,b.brand,b.plate_code,b.plcc_dropped,substr(b.est_closed_dt,1,10) as est_closed_dt,b.card_typ,substr(b.cbcc_actv_dt,1,10) as cbcc_actv_dt
from ssoni.AT_DS_customer_base_training a 
inner join mds_new.ods_plcc_cardholder_t b on a.customer_key = b.customer_key
where b.country = 'US' ;
-- 2,165,392 (929,326 distinct custkeys)


------------------- Get the highest tier of card held by the customer

create table ssoni.AT_DS_Cust_Max_Card_Status_training stored as orc as
select customer_key,max(plate_code) as card_highest_tier 
from ssoni.AT_DS_Cust_Card_Status_training
group by customer_key;
-- 929,326


--------------------------------------------------Browsing history section-----------------------------------------------


--------------- Get browsing details of customers from 4th quarter of training window

create table ssoni.AT_DS_lastqrtr_browsing_training stored as orc as 
select distinct unk_shop_id,
visit_num,
brand_from_pagename,
at_hit_ind_tot,
br_hit_ind_tot,
gp_hit_ind_tot,
on_hit_ind_tot,
prop33,
market,
date_time,
bag_add_ind
from crmanalytics.hit_data_agg
where market = 'US' and date_time between date_add(${hiveconf:purch_window_start_date},274) and date_add(${hiveconf:purch_window_start_date},365) and  prop33 = 'product';
-- 95,027,119

----------------- Get the total hits for all brands and products grouped at customerkey level

--drop table if exists ssoni.AT_DS_lastqrtr_browsing_total_items_training;
create table ssoni.AT_DS_lastqrtr_browsing_total_items_training stored as orc as 
select b.customerkey as customer_key,sum(a.at_hit_ind_tot) as at_items_browsed,sum(a.br_hit_ind_tot) as br_items_browsed,sum(a.gp_hit_ind_tot) as gp_items_browsed,sum(a.on_hit_ind_tot) as on_items_browsed
from ssoni.AT_DS_lastqrtr_browsing_training a 
inner join ssoni.crmanalyticsdata b on a.unk_shop_id = b.unknownshopperid
group by b.customerkey;
-- 12,459,129

---------------------------------------------Combine all variables------------------------------------

--drop table if exists ssoni.AT_DS_all_variables_training;
create table ssoni.AT_DS_all_variables_training stored as orc as 
select distinct a.customer_key,a.flag_employee,
b.active_period,
c.recency_purch,coalesce(round(d.total_purchases/b.active_period,3) ,0) as freq_purch,
coalesce(d.total_net_sales,0) as total_net_sales,coalesce(d.total_purchases,0) as total_purchases,coalesce(d.discount_percentage,0) as discount_percentage,
coalesce(e.count_distinct_divisions,0) as count_distinct_divisions,
coalesce(f.ratio_purchases,0) as ratio_purchases,
coalesce(g.at_items_browsed,0) as at_items_browsed,coalesce(g.br_items_browsed,0) as br_items_browsed,
coalesce(g.gp_items_browsed,0) as gp_items_browsed,coalesce(g.on_items_browsed,0) as on_items_browsed,
coalesce(h.sister_brands_recency_purch,12) as sister_brands_recency_purch,
coalesce(i.sister_brands_total_purchases,0) as sister_brands_total_purchases,
coalesce(i.sister_brands_total_net_sales,0) as sister_brands_total_net_sales,coalesce(i.sister_brands_discount_percentage,0) as sister_brands_discount_percentage,
coalesce(j.promotion_period_sales_ratio,0) as promotion_period_sales_ratio,
coalesce(k.holiday_season_sales_ratio,0) as holiday_season_sales_ratio,
coalesce(l.card_highest_tier,0) as card_highest_tier,
coalesce(m.next_year_discount_percentage,0) as next_year_discount_percentage,coalesce(m.next_year_total_purchases,0) as next_year_total_purchases
from ssoni.AT_DS_customer_base_training a 
left outer join ssoni.AT_DS_active_period_training b on a.customer_key = b.customer_key
left outer join ssoni.AT_DS_recency_purch_training c on a.customer_key = c.customer_key
left outer join ssoni.AT_DS_avg_discount_percentage_training d on a.customer_key = d.customer_key
left outer join ssoni.AT_DS_count_distinct_divisions_training e on a.customer_key = e.customer_key
left outer join ssoni.AT_DS_ratio_purchases_training f on a.customer_key = f.customer_key
left outer join ssoni.AT_DS_lastqrtr_browsing_total_items_training g on a.customer_key = g.customer_key
left outer join ssoni.AT_DS_sister_brands_recency_purch_training h on a.customer_key = h.customer_key
left outer join ssoni.AT_DS_sister_brands_avg_discount_percentage_training i on a.customer_key = i.customer_key
left outer join ssoni.AT_DS_promotion_period_sales_ratio_training j on a.customer_key = j.customer_key
left outer join ssoni.AT_DS_holiday_season_sales_ratio_training k on a.customer_key = k.customer_key
left outer join ssoni.AT_DS_cust_max_card_status_training l on a.customer_key = l.customer_key
left outer join ssoni.AT_DS_next_year_avg_discount_percentage_training m on a.customer_key = m.customer_key;
-- 1,918,932


-------------------Create external table and then transfer it to home directory

drop table if exists ssoni.AT_DS_all_variables_training_export;

create external table ssoni.AT_DS_all_variables_training_export
(customer_key string,
flag_employee string,
active_period double,
recency_purch double,
freq_purch double,
total_net_sales double,
total_purchases double,
discount_percentage double,
count_distinct_divisions int,
ratio_purchases double,
at_items_browsed int,
br_items_browsed int,
gp_items_browsed int,
on_items_browsed int,
sister_brands_recency_purch float,
sister_brands_total_purchases int,
sister_brands_total_net_sales float,
sister_brands_discount_percentage double,
promotion_period_sales_ratio double,
holiday_season_sales_ratio double,
card_highest_tier int,
next_year_discount_percentage double,
next_year_total_purchases int)              
row format delimited 
fields terminated by '|' 
lines terminated by '\n' 
location '/user/ssoni/AT_DS_all_variables_training_export'
TBLPROPERTIES('serialization.null.format'='');

insert overwrite table ssoni.AT_DS_all_variables_training_export
select * from ssoni.AT_DS_all_variables_training;

exit;
hadoop fs -getmerge /user/ssoni/AT_DS_all_variables_training_export /home/ssoni



