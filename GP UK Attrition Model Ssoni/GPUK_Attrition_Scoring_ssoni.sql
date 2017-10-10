---------- File last updated - 28/08/2017

set hive.cli.print.header=true;
set mapred.job.queue.name=aa;
set mapred.job.priority = HIGH;
use ssoni;

------------- Set the date from where the purchase window starts;

set purch_window_start_date = '2016-09-07';


------------------Get the data of crmanalytics stored at a place.;
-------------------This table is already run. Refresh it if running if after few weeks.;

drop table if exists ssoni.crmanalyticsdata;
create table ssoni.crmanalyticsdata stored as orc as 
select distinct customerkey,unknownshopperid,emailkey
from crmanalytics.customerresolution
where customerkey is not null and emailkey is not null;

select count(*) from ssoni.crmanalyticsdata;
-- 346,142,127

------------ Get the transaction data for GP and BR from orderline table for UK

--drop table if exists ssoni.GPUK_Attrition_Txn_2016_Orderline_Score5;
create table ssoni.GPUK_Attrition_Txn_2016_Orderline_Score5 stored as orc as
select a.customer_key, a.brand, a.transaction_date, a.transaction_num, a.line_num, a.item_qty, a.sales_amt,coalesce(b.discount_amt,0) as discount_amt, a.order_status, a.product_key, a.country
from mds_new.ods_orderline_t a
left outer join mds_new.ods_orderline_discounts_t b 
on a.customer_key = b.customer_key and a.transaction_num = b.transaction_num and a.line_num = b.line_num
where a.transaction_date between ${hiveconf:purch_window_start_date} and date_add(${hiveconf:purch_window_start_date},364) and a.brand in ('GP') and a.country= 'GB' and a.sales_amt is NOT NULL and a.order_status <> 'D'
order by a.customer_key,a.brand,a.transaction_date,a.line_num;

select count(*) from ssoni.GPUK_Attrition_Txn_2016_Orderline_Score5;
-- run 19,255,904


------------Check the recency of purchase of GP Transactions

--drop table if exists ssoni.GPUK_Attrition_Recency_Purch_Score5;
create table ssoni.GPUK_Attrition_Recency_Purch_Score5 stored as orc as
select customer_key,round(datediff(date_add(${hiveconf:purch_window_start_date},365),max(transaction_date))/7,1) as weeks_since_last_purch
from ssoni.GPUK_Attrition_Txn_2016_Orderline_Score5
where brand = 'GP' and (item_qty > 0 and sales_amt > 0) and (sales_amt + discount_amt > 0) and discount_amt <= sales_amt
group by customer_key;

select count(*) from ssoni.GPUK_Attrition_Recency_Purch_Score5;
--run 1,627,867


----------- Get the average discount percentage, total purchases and total net sales of GP customers

--drop table if exists ssoni.GPUK_Attrition_Discount_Percentage_Score5;
create table ssoni.GPUK_Attrition_Discount_Percentage_Score5 stored as orc as
select customer_key,round(sum(sales_amt),2) as total_sales,round(sum(discount_amt),2) as total_discount, round(sum(discount_amt)*(-100.0)/(sum(sales_amt)),2) as discount_percentage,
count(distinct transaction_num) as total_purchases,round(sum(sales_amt) + sum(discount_amt),2) as total_net_sales
from ssoni.GPUK_Attrition_Txn_2016_Orderline_Score5
where brand = 'GP' and (item_qty > 0 and sales_amt > 0) and (sales_amt + discount_amt > 0) and discount_amt <= sales_amt
group by customer_key;

select count(*) from ssoni.GPUK_Attrition_Discount_Percentage_Score5;
-- there are people with negative total sales, confirm about them
--run 1,627,867


------------- Create a customer base for GP

--drop table if exists ssoni.GPUK_Attrition_Distinct_Customers_Score5;
create table ssoni.GPUK_Attrition_Distinct_Customers_Score5 stored as orc as
select distinct customer_key from GPUK_Attrition_Txn_2016_Orderline_Score5
where brand = 'GP' and (item_qty > 0 and sales_amt > 0) and (sales_amt + discount_amt > 0) and discount_amt <= sales_amt
order by customer_key;

select count(*) from ssoni.GPUK_Attrition_Distinct_Customers_Score5;
--run 1,627,867


---------------- Get the items return ratio for customers. It computes total items returned to total items purchased in the base year

--drop table if exists ssoni.GPUK_Attrition_Items_Return_Ratio_Score5;
create table ssoni.GPUK_Attrition_Items_Return_Ratio_Score5 stored as orc as 
select a.customer_key, coalesce(b.total_items_returned,0) as total_items_returned, coalesce(c.total_items_purchased,1) as total_items_purchased, round(coalesce(b.total_items_returned,0)/coalesce(c.total_items_purchased,1),2) as items_return_ratio
from ssoni.GPUK_Attrition_Distinct_Customers_Score5 a 
left outer join (select customer_key,sum(item_qty)*(-1) as total_items_returned from ssoni.GPUK_Attrition_Txn_2016_Orderline_Score5 where brand = 'GP' and (item_qty < 0 and sales_amt < 0) and (sales_amt + discount_amt < 0) group by customer_key) b on a.customer_key = b.customer_key
left outer join (select customer_key,sum(item_qty) as total_items_purchased from ssoni.GPUK_Attrition_Txn_2016_Orderline_Score5 where brand = 'GP' and (item_qty > 0 and sales_amt > 0) and (sales_amt + discount_amt > 0) group by customer_key) c on a.customer_key = c.customer_key;

select count(*) from ssoni.GPUK_Attrition_Items_Return_Ratio_Score5;
-- run  1,627,867


--------------- Join with product_t to get the division for each product shopped for

--drop table if exists ssoni.GPUK_Attrition_Product_Division_Score5;
create table ssoni.GPUK_Attrition_Product_Division_Score5 stored as orc as 
select a.customer_key,a.transaction_num,a.line_num,a.transaction_date,a.product_key,coalesce(b.div_desc,'NA') as div_desc,coalesce(b.div_id,-1) as div_id
from ssoni.GPUK_Attrition_Txn_2016_Orderline_Score5 a 
left outer join mds_new.ods_product_unvl_t b on a.product_key = b.product_key 
where a.brand='GP' and (a.item_qty > 0 and a.sales_amt > 0) and (sales_amt + discount_amt > 0);

select count(*) from ssoni.GPUK_Attrition_Product_Division_Score5;
-- 17,392,565


------------- Count the number of distinct divisions the customer has shopped from

--drop table if exists ssoni.GPUK_Attrition_Count_Distinct_Divisions_Score5;
create table ssoni.GPUK_Attrition_Count_Distinct_Divisions_Score5 stored as orc as 
select customer_key,count(distinct div_desc) as count_distinct_divisions 
from ssoni.GPUK_Attrition_Product_Division_Score5
where div_desc <> 'NA'
group by customer_key;

select count(*) from ssoni.GPUK_Attrition_Count_Distinct_Divisions_Score5;
-- 1,623,412


--------------- This is the final table that contains all variables
----- Remove the product key later once cluster is stabilized

--drop table if exists ssoni.GPUK_Attrition_All_Variables_Score5;
create table ssoni.GPUK_Attrition_All_Variables_Score5 stored as orc as 
select distinct a.customer_key,
coalesce(c.weeks_since_last_purch,52) as weeks_since_last_purch,
coalesce(g.total_purchases,0) as total_purchases,
coalesce(g.total_net_sales,0) as total_net_sales,
coalesce(g.discount_percentage,0) as discount_percentage,
coalesce(i.items_return_ratio,0) as items_return_ratio,
coalesce(j.count_distinct_divisions,0) as count_distinct_divisions
from ssoni.GPUK_Attrition_Distinct_Customers_Score5 a
left outer join ssoni.GPUK_Attrition_Recency_Purch_Score5 c on a.customer_key = c.customer_key 
left outer join ssoni.GPUK_Attrition_Discount_Percentage_Score5 g on a.customer_key = g.customer_key
left outer join ssoni.GPUK_Attrition_Items_Return_Ratio_Score5 i on a.customer_key = i.customer_key
left outer join ssoni.GPUK_Attrition_Count_Distinct_Divisions_Score5 j   on a.customer_key = j.customer_key;

select count(*) from ssoni.GPUK_Attrition_All_Variables_Score5;
--run 1,627,867


---------------This table contains the emailkeys, export this table from hive 

--drop table if exists ssoni.GPUK_Attrition_Emailkey_Score5;
create table ssoni.GPUK_Attrition_Emailkey_Score5 stored as orc as 
select distinct b.emailkey,a.customer_key,a.weeks_since_last_purch,a.total_purchases,a.total_net_sales,a.discount_percentage,a.items_return_ratio,a.count_distinct_divisions
from ssoni.GPUK_Attrition_All_Variables_Score5 a 
inner join (select distinct customerkey,emailkey from ssoni.crmanalyticsdata where emailkey is not null and customerkey is not null) b on a.customer_key = b.customerkey;

select count(*) from ssoni.GPUK_Attrition_Emailkey_Score5;
-- 1,606,836 emailkeys




------------- Create the external table 

drop table if exists ssoni.GPUK_Attrition_Emailkey_Export_Score5;

create external table ssoni.GPUK_Attrition_Emailkey_Export_Score5
(emailkey string,
customer_key string,
weeks_since_last_purch double,
total_purchases int,
total_net_sales double,
discount_percentage double,
items_return_ratio double,
count_distinct_divisions int)              
row format delimited 
fields terminated by '|' 
lines terminated by '\n' 
location '/user/ssoni/GPUK_Attrition_Emailkey_Export_Score5'
TBLPROPERTIES('serialization.null.format'='');

insert overwrite table ssoni.GPUK_Attrition_Emailkey_Export_Score5
select * from ssoni.GPUK_Attrition_Emailkey_Score5;
exit;
hadoop fs -getmerge /user/ssoni/GPUK_Attrition_Emailkey_Export_Score5 /home/ssoni

----------------------------------------- External tables created due to hive problem

drop table if exists ssoni.customerresolutionemailkey;
create external table ssoni.customerresolutionemailkey
(customerkey int,
emailkey int)
row format delimited 
fields terminated by '|' 
lines terminated by '\n' 
location '/user/ssoni/customerresolutionemailkey'
TBLPROPERTIES('serialization.null.format'='');

insert overwrite table ssoni.customerresolutionemailkey
select distinct customerkey,emailkey from crmanalytics.customerresolution where emailkey is not null and customerkey is not null;
select count(*) from ssoni.customerresolutionemailkey ;
--- 30063711 null emailkeys, 184719953 -- updated 154656242
exit;
hadoop fs -getmerge /user/ssoni/customerresolutionemailkey /home/ssoni

drop table if exists ssoni.GPUK_Attrition_Emailkey_Score5;
create table ssoni.GPUK_Attrition_Emailkey_Score5 stored as orc as 
select b.emailkey,a.*
from ssoni.GPUK_Attrition_All_Variables_Score5 a 
inner join ssoni.customerresolutionemailkey b  on a.customer_key = b.customerkey;


create external table ssoni.product_t_local
(product_key bigint,
div_desc string)
row format delimited 
fields terminated by '|' 
lines terminated by '\n' 
location '/user/ssoni/product_t_local'
TBLPROPERTIES('serialization.null.format'='');

insert overwrite table ssoni.product_t_local
select distinct product_key,div_desc from mds_new.ods_product_t;

exit;
hadoop fs -getmerge /user/ssoni/product_t_local /home/ssoni


drop table if exists ssoni.GPUK_Attrition_Product_Division_Score5;
create table ssoni.GPUK_Attrition_Product_Division_Score5 stored as orc as 
select a.customer_key,a.transaction_num,a.line_num,a.transaction_date,a.product_key,coalesce(b.div_desc,'NA') as div_desc
from ssoni.GPUK_Attrition_Txn_2016_Orderline_Score5 a 
left outer join ssoni.product_t_local b on a.product_key = b.product_key 
where a.brand='GP' and (a.item_qty > 0 and a.sales_amt > 0) and (a.sales_amt + a.discount_amt > 0)
order by a.customer_key,a.transaction_date,a.line_num;

select distinct div_desc from ssoni.GPUK_Attrition_Product_Division_Score5 limit 50;


drop table if exists ssoni.GPUK_Attrition_Emailkey_Export_Score5;

create external table ssoni.GPUK_Attrition_Emailkey_Export_Score5
(customer_key bigint,
weeks_since_last_purch double,
total_purchases int,
total_net_sales double,
discount_percentage double,
items_return_ratio double,
count_distinct_divisions int)              
row format delimited 
fields terminated by '|' 
lines terminated by '\n' 
location '/user/ssoni/GPUK_Attrition_Emailkey_Export_Score5'
TBLPROPERTIES('serialization.null.format'='');

insert overwrite table ssoni.GPUK_Attrition_Emailkey_Export_Score5
select * from ssoni.GPUK_Attrition_All_Variables_Score5;
exit;
hadoop fs -getmerge /user/ssoni/GPUK_Attrition_Emailkey_Export_Score5 /home/ssoni
