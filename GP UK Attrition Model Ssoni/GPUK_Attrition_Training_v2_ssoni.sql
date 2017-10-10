

------------- Set the date from where the purchase window starts

set purch_window_start_date = '2015-01-01';


------------ Get the transaction data for GP and BR from orderline table for UK

--drop table if exists ssoni.GPUK_Attrition_Txn_2015_Orderline;
create table ssoni.GPUK_Attrition_Txn_2015_Orderline as
select a.customer_key, a.brand, a.transaction_date, a.transaction_num, a.line_num, a.item_qty, a.sales_amt, a.on_sale_flag,coalesce(b.discount_amt,0) as discount_amt, a.order_status, a.product_key, a.country
from mds_new.ods_orderline_t a
left outer join mds_new.ods_orderline_discounts_t b 
on a.customer_key = b.customer_key and a.transaction_num = b.transaction_num and a.line_num = b.line_num
where a.transaction_date between ${hiveconf:purch_window_start_date} and date_add(${hiveconf:purch_window_start_date},366) and a.brand in ('GP','BR') and a.country= 'GB' and a.sales_amt is NOT NULL and a.order_status <> 'D' 
order by a.customer_key,a.brand,a.transaction_date,a.line_num;
--run 15,949,541 (distinct customers 1,235,409 for GP, 170,004 for BR)



----------- Check for BR purchases of customer keys in the transaction table  (Not a significant variable)

--drop table if exists ssoni.GPUK_Attrition_BR_Purch;
create table ssoni.GPUK_Attrition_BR_Purch as 
select distinct customer_key,1 as BR_Purch 
from ssoni.GPUK_Attrition_Txn_2015_Orderline
where brand = 'BR' and (item_qty > 0 and sales_amt > 0) and (sales_amt + discount_amt > 0)
order by customer_key;
--run 166,545


------------Check the recency of purchase of GP Transactions

--drop table if exists ssoni.GPUK_Attrition_Recency_Purch;
create table ssoni.GPUK_Attrition_Recency_Purch as
select customer_key,round(datediff(date_add(${hiveconf:purch_window_start_date},367),max(transaction_date))/7,1) as weeks_since_last_purch
from ssoni.GPUK_Attrition_Txn_2015_Orderline
where brand = 'GP' and (item_qty > 0 and sales_amt > 0) and (sales_amt + discount_amt > 0)
group by customer_key;
--run 1,228,469


------------Check the recency of purchase of BR Transactions  (Not a significant variable)

--drop table if exists ssoni.GPUK_Attrition_Recency_Purch_BR;
create table ssoni.GPUK_Attrition_Recency_Purch_BR as
select customer_key,round(datediff(date_add(${hiveconf:purch_window_start_date},367),max(transaction_date))/7,1) as weeks_since_last_purch_br
from ssoni.GPUK_Attrition_Txn_2015_Orderline
where brand = 'BR' and (item_qty > 0 and sales_amt > 0) and (sales_amt + discount_amt > 0)
group by customer_key;
--run 166,545


----------- Get the average discount percentage, total purchases and total net sales of GP customers

--drop table if exists ssoni.GPUK_Attrition_Discount_Percentage;
create table ssoni.GPUK_Attrition_Discount_Percentage as
select customer_key,round(sum(sales_amt),2) as total_sales,round(sum(discount_amt),2) as total_discount, round(sum(discount_amt)*(-100.0)/(sum(sales_amt)),2) as discount_percentage,
count(distinct transaction_num) as total_purchases,round(sum(sales_amt) + sum(discount_amt),2) as total_net_sales
from ssoni.GPUK_Attrition_Txn_2015_Orderline
where brand = 'GP' and (item_qty > 0 and sales_amt > 0) and (sales_amt + discount_amt > 0)
group by customer_key
order by customer_key;
-- there are people with negative total sales, confirm about them
--run 1,228,301


----------- Get the average ticket size of GP customers  (Not a significant variable)

--drop table if exists ssoni.GPUK_Attrition_Avg_Ticket_Size;
create table ssoni.GPUK_Attrition_Avg_Ticket_Size as
select customer_key,round(sum(sales_amt),2) as total_sales,round(sum(discount_amt),2) as total_discount, 
count(distinct transaction_num) as total_purchases,round(((sum(sales_amt) + sum(discount_amt))/count(distinct transaction_num)),2) as avg_ticket_size
from ssoni.GPUK_Attrition_Txn_2015_Orderline
where brand = 'GP' and (item_qty > 0 and sales_amt > 0)and (sales_amt + discount_amt > 0)
group by customer_key
order by customer_key;
--run 1,228,301


------------- Get the average ticket size for BR customers   (Not a significant variable)

--drop table if exists ssoni.GPUK_Attrition_BR_Avg_Ticket_Size;
create table ssoni.GPUK_Attrition_BR_Avg_Ticket_Size as
select customer_key,round(sum(sales_amt),2) as br_total_sales,round(sum(discount_amt),2) as br_total_discount, 
count(distinct transaction_num) as br_total_purchases,round(((sum(sales_amt) + sum(discount_amt))/count(distinct transaction_num)),2) as br_avg_ticket_size
from ssoni.GPUK_Attrition_Txn_2015_Orderline
where brand = 'BR' and (item_qty > 0 and sales_amt > 0) and (sales_amt + discount_amt > 0)
group by customer_key
order by customer_key;
--run 166,545


------------- Get the ratio of sales in last transaction to the first transaction  (Not a significant variable)

--drop table if exists ssoni.GPUK_Attrition_Net_Sales_Ratio;
create table ssoni.GPUK_Attrition_Net_Sales_Ratio as 
select a.customer_key,a.first_txn_net_sales,b.last_txn_net_sales,round(b.last_txn_net_sales/a.first_txn_net_sales,2) as last_to_first_net_sales_ratio
from (select customer_key,transaction_num,transaction_date,sum(sales_amt + discount_amt) as first_txn_net_sales , row_number() over (partition by customer_key order by transaction_date) as p from ssoni.GPUK_Attrition_Txn_2015_Orderline where brand='GP' and (item_qty > 0 and sales_amt > 0) and (sales_amt + discount_amt > 0) group by customer_key,transaction_num,transaction_date) a
inner join (select customer_key,transaction_num,transaction_date,sum(sales_amt + discount_amt) as last_txn_net_sales , row_number() over (partition by customer_key order by transaction_date desc) as q from ssoni.GPUK_Attrition_Txn_2015_Orderline where brand='GP' and (item_qty > 0 and sales_amt > 0) and (sales_amt + discount_amt > 0) group by customer_key,transaction_num,transaction_date) b
on a.customer_key = b.customer_key
where a.p = 1 and b.q = 1
order by customer_key;
--run 1,228,301


------------- Create a customer base for GP

--drop table if exists ssoni.GPUK_Attrition_Distinct_Customers;
create table ssoni.GPUK_Attrition_Distinct_Customers as
select distinct customer_key from GPUK_Attrition_Txn_2015_Orderline
where brand = 'GP' and (item_qty > 0 and sales_amt > 0) and (sales_amt + discount_amt > 0)
order by customer_key;
--run 1,228,301


------------ Get the transactions for the above customer basse for the next year to check for attrition

--drop table if exists ssoni.GPUK_Attrition_Txn_2016;
create table ssoni.GPUK_Attrition_Txn_2016 as
select c.customer_key,  a.brand, a.transaction_date, a.transaction_num, a.line_num, a.item_qty, a.sales_amt, a.on_sale_flag,a.discount_amt as orderline_discount_amt,coalesce(b.discount_amt,0) as discount_amt, a.order_status, a.product_key, a.country
from ssoni.GPUK_Attrition_Distinct_Customers c 
left outer join mds_new.ods_orderline_t a
on c.customer_key = a.customer_key
left outer join mds_new.ods_orderline_discounts_t b 
on c.customer_key = b.customer_key and a.transaction_num = b.transaction_num and a.line_num = b.line_num
where a.transaction_date between date_add(${hiveconf:purch_window_start_date},367) and date_add(${hiveconf:purch_window_start_date},733) and a.brand in ('GP','BR') and a.country= 'GB' and a.sales_amt is NOT NULL and a.order_status <> 'D'
order by c.customer_key,a.brand,a.transaction_date,a.line_num;
-- run 10,637,370


---------------- Get the items return ratio for customers. It computes total items returned to total items purchased in the base year

--drop table if exists ssoni.GPUK_Attrition_Items_Return_Ratio;
create table ssoni.GPUK_Attrition_Items_Return_Ratio as 
select a.customer_key, coalesce(b.total_items_returned,0) as total_items_returned, coalesce(c.total_items_purchased,1) as total_items_purchased, round(coalesce(b.total_items_returned,0)/coalesce(c.total_items_purchased,1),2) as items_return_ratio
from ssoni.GPUK_Attrition_Distinct_Customers a 
left outer join (select customer_key,sum(item_qty)*(-1) as total_items_returned from ssoni.GPUK_Attrition_Txn_2015_Orderline where brand = 'GP' and (item_qty < 0 and sales_amt < 0) and (sales_amt + discount_amt < 0) group by customer_key) b on a.customer_key = b.customer_key
left outer join (select customer_key,sum(item_qty) as total_items_purchased from ssoni.GPUK_Attrition_Txn_2015_Orderline where brand = 'GP' and (item_qty > 0 and sales_amt > 0) and (sales_amt + discount_amt > 0) group by customer_key) c on a.customer_key = c.customer_key;
--run 1,228,301


--------------- Join with product_t to get the division for each product shopped for

--drop table if exists ssoni.GPUK_Attrition_Product_Division;
create table ssoni.GPUK_Attrition_Product_Division as 
select a.customer_key,a.transaction_num,a.line_num,a.transaction_date,a.product_key,coalesce(b.mdse_div_desc,'NA') as mdse_div_desc,coalesce(b.mdse_div_id,-1) as mdse_div_id
from ssoni.GPUK_Attrition_Txn_2015_Orderline a 
left outer join mds_new.ods_product_t b 
on a.product_key = b.product_key
where a.brand='GP' and (a.item_qty > 0 and a.sales_amt > 0) and (sales_amt + discount_amt > 0)
order by a.customer_key,a.transaction_date,a.line_num;
--run 13,307,906


------------- Count the number of distinct divisions the customer has shopped from

--drop table if exists ssoni.GPUK_Attrition_Count_Distinct_Divisions;
create table ssoni.GPUK_Attrition_Count_Distinct_Divisions as 
select customer_key,count(distinct mdse_div_desc) as count_distinct_divisions 
from ssoni.GPUK_Attrition_Product_Division
where mdse_div_desc <> 'NA'
group by customer_key;
--run 685,088


------------- Set the target variable. It is 0 if the customer has not made a purchase. It is coalesced to 1 in the final table where all variables are joined, 1 indicating attrited customers

--drop table if exists ssoni.GPUK_Attrition_Attrited;
create table ssoni.GPUK_Attrition_Attrited as
select distinct customer_key,0 as Attrited
from ssoni.GPUK_Attrition_Txn_2016
where brand = 'GP' and (item_qty >0 and sales_amt > 0) and (sales_amt + discount_amt > 0)
order by customer_key;
--run 565,644


--------------- This is the final table that contains all variables

--drop table if exists ssoni.GPUK_Attrition_All_Variables;
create table ssoni.GPUK_Attrition_All_Variables as 
select a.customer_key,
coalesce(c.weeks_since_last_purch,52) as weeks_since_last_purch,
coalesce(g.total_purchases,0) as total_purchases,
coalesce(g.total_net_sales,0) as total_net_sales,
coalesce(g.discount_percentage,0) as discount_percentage,
coalesce(i.items_return_ratio,0) as items_return_ratio,
coalesce(j.count_distinct_divisions,0) as count_distinct_divisions,
coalesce(k.attrited,1) as attrited
from ssoni.GPUK_Attrition_Distinct_Customers a
left outer join ssoni.GPUK_Attrition_Recency_Purch c on a.customer_key = c.customer_key 
left outer join ssoni.GPUK_Attrition_Discount_Percentage g on a.customer_key = g.customer_key
left outer join ssoni.GPUK_Attrition_Items_Return_Ratio i on a.customer_key = i.customer_key
left outer join ssoni.GPUK_Attrition_Count_Distinct_Divisions j   on a.customer_key = j.customer_key
left outer join ssoni.GPUK_Attrition_Attrited k on a.customer_key = k.customer_key
order by customer_key;
--run 1,228,301



---------------this table contains the emailkeys, export this table from hive 

--drop table if exists ssoni.GPUK_Attrition_Emailkey;
create table ssoni.GPUK_Attrition_Emailkey stored as orc as 
select distinct b.emailkey,a.customer_key,a.weeks_since_last_purch,a.total_purchases,a.total_net_sales,a.discount_percentage,a.items_return_ratio,a.count_distinct_divisions
from ssoni.GPUK_Attrition_All_Variables a 
inner join ssoni.customerresolutiondata b  on a.customer_key = b.customerkey
where b.emailkey is not null;
-- 1,413,205 (1,233,730 distinct customer_keys)



------------- Create the external table 

drop table if exists ssoni.GPUK_Attrition_Emailkey_Export;

create external table ssoni.GPUK_Attrition_Emailkey_Export
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
location '/user/ssoni/GPUK_Attrition_Emailkey_Export'
TBLPROPERTIES('serialization.null.format'='');

insert overwrite table ssoni.GPUK_Attrition_Emailkey_Export
select * from ssoni.GPUK_Attrition_Emailkey;

hadoop fs -getmerge /user/ssoni/GPUK_Attrition_Emailkey_Export /home/ssoni






