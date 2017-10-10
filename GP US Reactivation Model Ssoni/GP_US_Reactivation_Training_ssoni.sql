-------------------------- Gap US Reactivated Customers, Purchase Categories
------------------------- Get customers who purchased in 1st year, didn't purchase anything in 2nd year and then purchased again in 3rd year, alongwith the categories from which they purchased
---------- Set the start date of purchase window
set purch_window_start_date = '2014-08-01';


set hive.cli.print.header=true;
set mapred.job.queue.name=aa;
set mapred.job.priority = HIGH;
use ssoni;

-------------------- Get crmanalyltics data 

--drop table if exists ssoni.crmanalyticsdata;
create table ssoni.crmanalyticsdata stored as orc as 
select distinct customerkey,unknownshopperid,emailkey
from crmanalytics.customerresolution
where customerkey is not null and emailkey is not null;

---------------- Get the data for first year customer purchases
create table ssoni.GP_Reactivation_Firstyear_Customers stored as orc as 
select distinct a.customer_key,
a.transaction_date,
a.transaction_num,
a.line_num,
a.order_status,
a.item_qty,
a.sales_amt,
a.product_key,
coalesce(b.discount_amt,0) as discount_amt,
coalesce(c.mdse_div_desc,'NA') as product_division,
coalesce(c.mdse_dept_desc,'NA') as product_department,
coalesce(c.mdse_class_desc,'NA') as product_class
from mds_new.ods_orderline_t a 
left outer join (select distinct customer_key,transaction_num,line_num,discount_amt from mds_new.ods_orderline_discounts_t) b on a.customer_key = b.customer_key and a.transaction_num = b.transaction_num and a.line_num = b.line_num
left outer join (select distinct product_key,mdse_div_desc,mdse_dept_desc,mdse_class_desc from mds_new.ods_product_t) c on a.product_key = c.product_key
where a.country = 'US' and a.brand='GP' and a.order_status <> 'D' and a.transaction_date between ${hiveconf:purch_window_start_date} and date_add(${hiveconf:purch_window_start_date},364)  and a.sales_amt > 0 and a.item_qty > 0;
-- 133,258,099 (7,855,728 distinct customer_key)

----------------- Set a flag variable for customers who made a purchase in the second year
drop table if exists ssoni.GP_Reactivation_Secondyear_Purchase;
create table ssoni.GP_Reactivation_Secondyear_Purchase stored as orc as 
select distinct a.customer_key,(case when b.sum_items > 0 then 1 else 0 end) as second_year_purchase
from ssoni.GP_Reactivation_Firstyear_Customers a 
left outer join (select  customer_key,sum(item_qty) as sum_items from mds_new.ods_orderline_t where transaction_date between date_add(${hiveconf:purch_window_start_date},365) and date_add(${hiveconf:purch_window_start_date},729) and
country = 'US' and brand='GP' and order_status <> 'D' and sales_amt > 0 and item_qty > 0 group by customer_key) b on a.customer_key = b.customer_key;
-- 7,855,728 (2,974,363 zeroes)

------------------  Get the custbase, customers who lapsed in the second year

create table ssoni.GP_Reactivation_Secondyear_Lapsed_Custbase stored as orc as 
select distinct customer_key
from ssoni.GP_Reactivation_Secondyear_Purchase
where second_year_purchase = 0;
-- 2,974,363

----------- Get the average discount percentage, total purchases and total net sales of GP customers


create table ssoni.GP_Reactivation_Avg_Discount_Percentage stored as orc as
select a.customer_key,round(sum(sales_amt),2) as total_sales,round(sum(discount_amt),2) as total_discount, round(sum(discount_amt)*(-100.0)/(sum(sales_amt)),2) as discount_percentage,
count(distinct transaction_num) as total_purchases,round(sum(sales_amt) + sum(discount_amt),2) as total_net_sales,count(distinct product_division) as count_distinct_divisions,
count(distinct product_department) as count_distinct_department
from ssoni.GP_Reactivation_Firstyear_Customers a 
inner join ssoni.GP_Reactivation_Secondyear_Lapsed_Custbase b on a.customer_key = b.customer_key
where (sales_amt + discount_amt > 0) and discount_amt <= sales_amt
group by a.customer_key;
-- 2,974,047




drop table if exists ssoni.GP_Reactivation_Dept_Net_Sales;
create table ssoni.GP_Reactivation_Dept_Net_Sales stored as orc as 
select a.customer_key,product_department,round(sum(sales_amt + discount_amt),2) as net_sales
from ssoni.GP_Reactivation_Firstyear_Customers a 
inner join ssoni.GP_Reactivation_Secondyear_Lapsed_Custbase b on a.customer_key = b.customer_key
where (sales_amt + discount_amt > 0) and discount_amt <= sales_amt
group by a.customer_key,product_department
order by customer_key,net_sales desc;
-- 8,676,396

drop table if exists ssoni.GP_Reactivation_Dept_Net_Sales_Rownum;
create table ssoni.GP_Reactivation_Dept_Net_Sales_Rownum stored as orc as 
select *,row_number() over (partition by customer_key order by net_sales desc  ) as row_num
from ssoni.GP_Reactivation_Dept_Net_Sales;
-- 8,676,396

create table ssoni.GP_Reactivation_Top3Dept_Net_Sales_Ratio stored as orc as 
select a.customer_key,a.top3dept_net_sales,b.total_net_sales,round(a.top3dept_net_sales/b.total_net_sales,2) as top3dept_net_sales_ratio
from (select customer_key,sum(net_sales) as top3dept_net_sales from ssoni.GP_Reactivation_Dept_Net_Sales_Rownum where row_num <= 3 group by customer_key) a 
inner join (select customer_key,sum(net_sales) as total_net_sales from ssoni.GP_Reactivation_Dept_Net_Sales_Rownum group by customer_key) b 
on a.customer_key = b.customer_key;
-- 2,974,047

------------------------- get the sister brands purchases

--drop table if exists ssoni.GP_Reactivation_Sister_Brands_Purchases;
create table ssoni.GP_Reactivation_Sister_Brands_Purchases stored as orc as 
select distinct d.customer_key,
a.transaction_date,
a.transaction_num,
a.brand,
a.line_num,
a.order_status,
a.item_qty,
a.sales_amt,
a.product_key,
coalesce(b.discount_amt,0) as discount_amt,
coalesce(c.mdse_div_desc,'NA') as product_division,
coalesce(c.mdse_dept_desc,'NA') as product_department,
coalesce(c.mdse_class_desc,'NA') as product_class
from ssoni.GP_Reactivation_Secondyear_Lapsed_Custbase d 
left outer join mds_new.ods_orderline_t a on d.customer_key = a.customer_key
left outer join (select distinct customer_key,transaction_num,line_num,discount_amt from mds_new.ods_orderline_discounts_t) b on a.customer_key = b.customer_key and a.transaction_num = b.transaction_num and a.line_num = b.line_num
left outer join (select distinct product_key,mdse_div_desc,mdse_dept_desc,mdse_class_desc from mds_new.ods_product_t) c on a.product_key = c.product_key
where a.country = 'US' and a.brand <>'GP' and a.order_status <> 'D' and a.transaction_date between ${hiveconf:purch_window_start_date} and date_add(${hiveconf:purch_window_start_date},364)  and a.sales_amt > 0 and a.item_qty > 0;
-- 49,516,214



create table ssoni.GP_Reactivation_Sister_Brands_Avg_Discount_Percentage stored as orc as
select customer_key,round(sum(sales_amt),2) as sister_brands_total_sales,round(sum(discount_amt),2) as sister_brands_total_discount, round(sum(discount_amt)*(-100.0)/(sum(sales_amt)),2) as sister_brands_discount_percentage,
count(distinct transaction_num) as sister_brands_total_purchases,round(sum(sales_amt) + sum(discount_amt),2) as sister_brands_total_net_sales,count(distinct product_division) as sister_brands_count_distinct_divisions,
count(distinct product_department) as sister_brands_count_distinct_department
from ssoni.GP_Reactivation_Sister_Brands_Purchases 
where (sales_amt + discount_amt > 0) and discount_amt <= sales_amt
group by customer_key;
-- 1,787,585


--drop table if exists ssoni.GP_Reactivation_Sister_Brands_Purchases_Secondyear;
create table ssoni.GP_Reactivation_Sister_Brands_Purchases_Secondyear stored as orc as 
select distinct d.customer_key,
a.transaction_date,
a.transaction_num,
a.brand,
a.line_num,
a.order_status,
a.item_qty,
a.sales_amt,
a.product_key,
coalesce(b.discount_amt,0) as discount_amt,
coalesce(c.mdse_div_desc,'NA') as product_division,
coalesce(c.mdse_dept_desc,'NA') as product_department,
coalesce(c.mdse_class_desc,'NA') as product_class
from ssoni.GP_Reactivation_Secondyear_Lapsed_Custbase d 
left outer join mds_new.ods_orderline_t a on d.customer_key = a.customer_key
left outer join (select distinct customer_key,transaction_num,line_num,discount_amt from mds_new.ods_orderline_discounts_t) b on a.customer_key = b.customer_key and a.transaction_num = b.transaction_num and a.line_num = b.line_num
left outer join (select distinct product_key,mdse_div_desc,mdse_dept_desc,mdse_class_desc from mds_new.ods_product_t) c on a.product_key = c.product_key
where a.country = 'US' and a.brand <>'GP' and a.order_status <> 'D' and a.transaction_date between date_add(${hiveconf:purch_window_start_date},365) and date_add(${hiveconf:purch_window_start_date},729)  and a.sales_amt > 0 and a.item_qty > 0;
-- 41,611,710



create table ssoni.GP_Reactivation_Sister_Brands_Secondyear_Avg_Discount_Percentage stored as orc as
select customer_key,round(sum(sales_amt),2) as sister_brands_secondyear_total_sales,round(sum(discount_amt),2) as sister_brands_secondyear_total_discount, round(sum(discount_amt)*(-100.0)/(sum(sales_amt)),2) as sister_brands_secondyear_discount_percentage,
count(distinct transaction_num) as sister_brands_secondyear_total_purchases,round(sum(sales_amt) + sum(discount_amt),2) as sister_brands_secondyear_total_net_sales,count(distinct product_division) as sister_brands_secondyear_count_distinct_divisions,
count(distinct product_department) as sister_brands_secondyear_count_distinct_department
from ssoni.GP_Reactivation_Sister_Brands_Purchases_Secondyear 
where (sales_amt + discount_amt > 0) and discount_amt <= sales_amt
group by customer_key;



-----------------------------Try the browsing history from the hit_data_agg table in crm analytics

--drop table if exists ssoni.GP_Reactivation_Secondyear_Browsing_Fourthqrtr;
create table ssoni.GP_Reactivation_Secondyear_Browsing_Fourthqrtr stored as orc as 
select distinct unk_shop_id,
visit_num,
brand_from_pagename,
gp_hit_ind_tot,
prop33,
market,
date_time,
bag_add_ind
from crmanalytics.hit_data_agg
where market = 'US' and date_time between date_add(${hiveconf:purch_window_start_date},639) and date_add(${hiveconf:purch_window_start_date},730) and brand_from_pagename = 'GAP' and prop33 = 'product';



----------------------- Get the total items browsed in GP

--drop table if exists ssoni.GP_Reactivation_Secondyear_Browsing_Fourthqrtr_Items;
create table ssoni.GP_Reactivation_Secondyear_Browsing_Fourthqrtr_Items stored as orc as 
select b.customerkey,sum(a.gp_hit_ind_tot) as items_browsed 
from ssoni.GP_Reactivation_Secondyear_Browsing_Fourthqrtr a 
inner join ssoni.crmanalyticsdata b on a.unk_shop_id = b.unknownshopperid
group by b.customerkey;


-------------------------------Final table for analysis

create table ssoni.GP_Reactivation_All_Variables stored as orc as 
select distinct a.customer_key,coalesce(b.total_purchases,0) as total_purchases, coalesce(b.total_net_sales,0) as total_net_sales,coalesce(b.discount_percentage,0) as discount_percentage,
coalesce(b.count_distinct_divisions,0) as count_distinct_divisions,coalesce(b.count_distinct_department,0) as count_distinct_department,
coalesce(c.top3dept_net_sales,0) as top3dept_net_sales, coalesce(c.top3dept_net_sales_ratio,0) as top3dept_net_sales_ratio,
coalesce(d.sister_brands_total_purchases,0) as sister_brands_total_purchases, coalesce(d.sister_brands_total_net_sales,0) as sister_brands_total_net_sales,coalesce(d.sister_brands_discount_percentage,0) as sister_brands_discount_percentage,
coalesce(d.sister_brands_count_distinct_divisions,0) as sister_brands_count_distinct_divisions,coalesce(d.sister_brands_count_distinct_department,0) as sister_brands_count_distinct_department,
coalesce(e.sister_brands_secondyear_total_purchases,0) as sister_brands_secondyear_total_purchases, coalesce(e.sister_brands_secondyear_total_net_sales,0) as sister_brands_secondyear_total_net_sales,coalesce(e.sister_brands_secondyear_discount_percentage,0) as sister_brands_secondyear_discount_percentage,
coalesce(e.sister_brands_secondyear_count_distinct_divisions,0) as sister_brands_secondyear_count_distinct_divisions,coalesce(e.sister_brands_secondyear_count_distinct_department,0) as sister_brands_secondyear_count_distinct_department,
coalesce(f.items_browsed,0) as items_browsed,
coalesce(g.thirdyear_purchase,0) as thirdyear_purchase
from ssoni.GP_Reactivation_Secondyear_Lapsed_Custbase a 
left outer join ssoni.GP_Reactivation_Avg_Discount_Percentage b on a.customer_key = b.customer_key
left outer join ssoni.GP_Reactivation_Top3Dept_Net_Sales_Ratio c on a.customer_key = c.customer_key
left outer join ssoni.GP_Reactivation_Sister_Brands_Avg_Discount_Percentage d on a.customer_key = d.customer_key
left outer join ssoni.GP_Reactivation_Sister_Brands_Secondyear_Avg_Discount_Percentage e on a.customer_key = e.customer_key
left outer join ssoni.GP_Reactivation_Secondyear_Browsing_Fourthqrtr_Items f on a.customer_key = f.customerkey
left outer join ssoni.GP_Reactivation_Thirdyear_Purchase g on a.customer_key = g.customer_key;

select count(*) from ssoni.GP_Reactivation_All_Variables;
-- 2,974,363



----------------------------- Create external table for export


create external table ssoni.GP_Reactivation_All_Variables_Export 
(customer_key bigint,
total_purchases bigint,
total_net_sales double,
discount_percentage double,
count_distinct_divisions bigint,
count_distinct_department bigint,
top3dept_net_sales double,
top3dept_net_sales_ratio double,
sister_brands_total_purchases bigint,
sister_brands_total_net_sales double,
sister_brands_discount_percentage double,
sister_brands_count_distinct_divisions bigint,
sister_brands_count_distinct_department bigint,
sister_brands_secondyear_total_purchases bigint,
sister_brands_secondyear_total_net_sales double,
sister_brands_secondyear_discount_percentage double,
sister_brands_secondyear_count_distinct_divisions bigint,
sister_brands_secondyear_count_distinct_department bigint,
items_browsed bigint,
thirdyear_purchase int

)              
row format delimited 
fields terminated by ',' 
lines terminated by '\n' 
location '/user/ssoni/GP_Reactivation_All_Variables_Export'
TBLPROPERTIES('serialization.null.format'='');

insert overwrite table ssoni.GP_Reactivation_All_Variables_Export
select * from ssoni.GP_Reactivation_All_Variables;
exit;
hadoop fs -getmerge /user/ssoni/GP_Reactivation_All_Variables_Export /home/ssoni


























create table ssoni.GP_Reactivation_Thirdyear_Purchases stored as orc as 
select distinct d.customer_key,
a.transaction_date,
a.transaction_num,
a.line_num,
a.order_status,
a.item_qty,
a.sales_amt,
a.product_key,
coalesce(b.discount_amt,0) as discount_amt,
coalesce(c.mdse_div_desc,'NA') as product_division,
coalesce(c.mdse_dept_desc,'NA') as product_department,
coalesce(c.mdse_class_desc,'NA') as product_class
from ssoni.GP_Reactivation_Secondyear_Purchase d 
left outer join mds_new.ods_orderline_t a on a.customer_key = d.customer_key
left outer join (select distinct customer_key,transaction_num,line_num,discount_amt from mds_new.ods_orderline_discounts_t) b on d.customer_key = b.customer_key and a.transaction_num = b.transaction_num and a.line_num = b.line_num
left outer join (select distinct product_key,mdse_div_desc,mdse_dept_desc,mdse_class_desc from mds_new.ods_product_t) c on a.product_key = c.product_key
where a.country = 'US' and a.brand='GP' and a.order_status <> 'D' and a.transaction_date between date_add(${hiveconf:purch_window_start_date},730) and date_add(${hiveconf:purch_window_start_date},1095)  and a.sales_amt > 0 and a.item_qty > 0 and d.second_year_purchase = 0;
-- 5,755,958 (672,207 distinct customer_key)


create table ssoni.GP_Reactivation_Thirdyear_Purchase stored as orc as 
select customer_key,(case when sum(item_qty) > 0 then 1 else 0 end) as thirdyear_purchase
from ssoni.GP_Reactivation_Thirdyear_Purchases
group by customer_key;


create table ssoni.GP_Reactivation_Div_Desc_Group stored as orc as 
select product_division,product_department,count(distinct customer_key) as num_customers,sum(item_qty) as total_items
from ssoni.GP_Reactivation_Firstyear_Customers
group by product_division,product_department
order by num_customers desc,total_items desc;
-- 76


create table ssoni.GP_Reactivation_Firstyear_Reactivated_Denim stored as orc as 
select distinct a.customer_key,b.product_department,b.sum_items
from (select distinct customer_key from ssoni.GP_Reactivation_Thirdyear_Purchases) a 
inner join (select  customer_key,product_department,sum(item_qty) as sum_items from ssoni.GP_Reactivation_Firstyear_Customers group by customer_key,product_department) b on a.customer_key = b.customer_key
where b.product_department like '%DENIM%';
-- 186,921

drop table if exists ssoni.GP_Reactivation_Firstyear_Reactivated_Denim_Thirdyear_Department;
create table ssoni.GP_Reactivation_Firstyear_Reactivated_Denim_Thirdyear_Department stored as orc as 
select distinct a.customer_key,a.product_department as firstyear_category,b.product_department as thirdyear_category,b.sum_items ,b.num_orders,b.sum_gross_sales,b.sum_discount,b.sum_net_sales
from (select distinct customer_key,product_department from ssoni.GP_Reactivation_Firstyear_Reactivated_Denim) a 
inner join (select customer_key,product_department,sum(item_qty) as sum_items,count(distinct transaction_num) as num_orders,sum(sales_amt ) as sum_gross_sales,sum(discount_amt) as sum_discount, sum(sales_amt + discount_amt) as sum_net_sales from ssoni.GP_Reactivation_Thirdyear_Purchases group by customer_key,product_department) b on a.customer_key = b.customer_key;
-- 565,587 (176,325 distinct customer_key)

--------------------- Create grouped table of above table for pivot analysis
drop table if exists ssoni.GP_Reactivation_Pivot_Analysis;
create table ssoni.GP_Reactivation_Pivot_Analysis stored as orc as 
select firstyear_category,thirdyear_category,count(distinct customer_key) as total_customers,sum(sum_items) as total_items,sum(num_orders) as total_orders,round(sum(sum_gross_sales),2) as total_gross_sales,round(sum(sum_discount),2) as total_discount,round(sum(sum_net_sales),2) as total_net_sales
from GP_Reactivation_Firstyear_Reactivated_Denim_Thirdyear_Department
group by firstyear_category,thirdyear_category
order by firstyear_category,total_items desc;
-- 99

---------------------------- Create the external table for export

create external table ssoni.GP_Reactivation_Pivot_Analysis_Export 
(firstyear_category string,
thirdyear_category string,
total_customers int,
total_items int,
total_orders int,
total_gross_sales double,
total_discount double,
total_net_sales double)              
row format delimited 
fields terminated by ',' 
lines terminated by '\n' 
location '/user/ssoni/GP_Reactivation_Pivot_Analysis_Export'
TBLPROPERTIES('serialization.null.format'='');

insert overwrite table ssoni.GP_Reactivation_Pivot_Analysis_Export
select * from ssoni.GP_Reactivation_Pivot_Analysis;
exit;
hadoop fs -getmerge /user/ssoni/GP_Reactivation_Pivot_Analysis_Export /home/ssoni


drop table if exists ssoni.GP_Reactivation_Secondyear_Lapsed_Sister_Brands_Active;
create table ssoni.GP_Reactivation_Secondyear_Lapsed_Sister_Brands_Active stored as orc as 
select distinct a.customer_key,b.brand
from (select distinct customer_key from GP_Reactivation_Secondyear_Purchase where second_year_purchase = 0) a
inner join (select distinct customer_key,brand  from mds_new.ods_orderline_t where item_qty > 0 and sales_amt > 0 and brand <> 'GP' and country = 'US' and order_status <> 'D' and transaction_date between date_add(${hiveconf:purch_window_start_date},365) and date_add(${hiveconf:purch_window_start_date},729) ) b  on a.customer_key = b.customer_key;
-- 1,551,917 distinct customer_key

create external table ssoni.GP_Reactivation_Secondyear_Lapsed_Sister_Brands_Active_Export 
(customer_key bigint,
brand string)              
row format delimited 
fields terminated by ',' 
lines terminated by '\n' 
location '/user/ssoni/GP_Reactivation_Secondyear_Lapsed_Sister_Brands_Active_Export'
TBLPROPERTIES('serialization.null.format'='');

insert overwrite table ssoni.GP_Reactivation_Secondyear_Lapsed_Sister_Brands_Active_Export
select * from ssoni.GP_Reactivation_Secondyear_Lapsed_Sister_Brands_Active;
exit;
hadoop fs -getmerge /user/ssoni/GP_Reactivation_Secondyear_Lapsed_Sister_Brands_Active_Export /home/ssoni







--AT      145611
--BF      238429
--BR      374260
--GO      405472
--ON      1094949


-------------------------------------------------------------------------------------------------------------------------------------------------------------
select distinct product_division,product_department,product_class
from ssoni.GP_Reactivation_Firstyear_Customers
order by product_division,product_department,product_class;

case
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens woven tops' and lower(t1.mdse_cls_desc) = 'mens red woven tops' then 'mens_tops_woven'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens woven tops' and lower(t1.mdse_cls_desc) = 's/s shirts' then 'mens_tops_woven'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens woven tops' and lower(t1.mdse_cls_desc) = 'l/s fashion' then 'mens_tops_woven'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens woven tops' and lower(t1.mdse_cls_desc) = 'basic haberdashery' then 'mens_tops_woven'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens knits' and lower(t1.mdse_cls_desc) = 'basic tees' then 'mens_tops_knits'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens knits' and lower(t1.mdse_cls_desc) = 'l/s knits' then 'mens_tops_knits'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens knits' and lower(t1.mdse_cls_desc) = 'mens red knits' then 'mens_tops_knits'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens knits' and lower(t1.mdse_cls_desc) = 's/s knits' then 'mens_tops_knits'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens knits' and lower(t1.mdse_cls_desc) = 'polo' then 'mens_tops_knits'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens knits' and lower(t1.mdse_cls_desc) = 'novety tees' then 'mens_tops_knits'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens knits' and lower(t1.mdse_cls_desc) = 'fleece' then 'mens_tops_knits'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens fit' and lower(t1.mdse_cls_desc) = 'l/s knits' then 'mens_tops_knits'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens fit' and lower(t1.mdse_cls_desc) = 's/s knits' then 'mens_tops_knits'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens fit' and lower(t1.mdse_cls_desc) = 'tanks' then 'mens_tops_knits'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens outerwear' and lower(t1.mdse_cls_desc) = 'mens red outerwear' then 'mens_outerwear_jacket_like'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens outerwear' and lower(t1.mdse_cls_desc) = 'wool jackets' then 'mens_outerwear_jacket_like'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens outerwear' and lower(t1.mdse_cls_desc) = 'vests' then 'mens_outerwear_jacket_like'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens outerwear' and lower(t1.mdse_cls_desc) = 'leather' then 'mens_outerwear_jacket_like'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens outerwear' and lower(t1.mdse_cls_desc) = 'jackets' then 'mens_outerwear_jacket_like'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens outerwear' and lower(t1.mdse_cls_desc) = 'blazers' then 'mens_outerwear_jacket_like'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens fit' and lower(t1.mdse_cls_desc) = 'outerwear' then 'mens_outerwear_jacket_like'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens sweaters' and lower(t1.mdse_cls_desc) = 'wool/wool blend' then 'mens_outerwear_sweater_like'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens sweaters' and lower(t1.mdse_cls_desc) = 'mens red sweaters' then 'mens_outerwear_sweater_like'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens sweaters' and lower(t1.mdse_cls_desc) = 'vests' then 'mens_outerwear_sweater_like'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens sweaters' and lower(t1.mdse_cls_desc) = 'cotton' then 'mens_outerwear_sweater_like'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens sweaters' and lower(t1.mdse_cls_desc) = 'other' then 'mens_outerwear_sweater_like'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens outerwear' and lower(t1.mdse_cls_desc) = 'fleece' then 'mens_outerwear_sweater_like'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens fit' and lower(t1.mdse_cls_desc) = 'fleece tops' then 'mens_outerwear_sweater_like'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens denim' and lower(t1.mdse_cls_desc) = '5 pocket' then 'mens_denim'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens denim' and lower(t1.mdse_cls_desc) = 'fashion denim' then 'mens_denim'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens denim' and lower(t1.mdse_cls_desc) = 'mens non-indigo' then 'mens_denim'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens denim' and lower(t1.mdse_cls_desc) = 'shirts' then 'mens_denim'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens denim' and lower(t1.mdse_cls_desc) = 'shorts' then 'mens_denim'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens denim' and lower(t1.mdse_cls_desc) = 'denim jackets' then 'mens_denim'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens denim' and lower(t1.mdse_cls_desc) = 'mens 1969' then 'mens_denim'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens bottoms' and lower(t1.mdse_cls_desc) = 'fashion bottoms' then 'mens_bottom_woven'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens bottoms' and lower(t1.mdse_cls_desc) = 'active bottoms' then 'mens_bottom_woven'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens bottoms' and lower(t1.mdse_cls_desc) = 'basics' then 'mens_bottom_woven'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens fit' and lower(t1.mdse_cls_desc) = 'shorts' then 'mens_bottom_half'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens bottoms' and lower(t1.mdse_cls_desc) = 'shorts' then 'mens_bottom_half'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'mens accessories' and lower(t1.mdse_cls_desc) = 'mens socks' then 'mens_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'mens accessories' and lower(t1.mdse_cls_desc) = 'shoes' then 'mens_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'mens accessories' and lower(t1.mdse_cls_desc) = 'mens red accessories' then 'mens_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'mens accessories' and lower(t1.mdse_cls_desc) = 'mens belts' then 'mens_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'mens accessories' and lower(t1.mdse_cls_desc) = 'mens hats' then 'mens_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'mens accessories' and lower(t1.mdse_cls_desc) = 'keds' then 'mens_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'mens body' and lower(t1.mdse_cls_desc) = 'boxer brief' then 'mens_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'mens body' and lower(t1.mdse_cls_desc) = 'lounge' then 'mens_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'mens body' and lower(t1.mdse_cls_desc) = 'boxers' then 'mens_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'mens body' and lower(t1.mdse_cls_desc) = 'packaged underwear' then 'mens_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'sunglasses' and lower(t1.mdse_cls_desc) = 'mens sunglasses' then 'mens_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'mens accessories' and lower(t1.mdse_cls_desc) = 'mens bags' then 'mens_accessories_other'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'mens accessories' and lower(t1.mdse_cls_desc) = 'miscellaneous' then 'mens_accessories_other'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'mens accessories' and lower(t1.mdse_cls_desc) = 'seasonal accessories' then 'mens_accessories_other'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens woven tops' and lower(t1.mdse_cls_desc) = 's/s tops' then 'womens_tops_woven'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens woven tops' and lower(t1.mdse_cls_desc) = 'sleeveless' then 'womens_tops_woven'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens woven tops' and lower(t1.mdse_cls_desc) = 'l/s tops' then 'womens_tops_woven'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens knits' and lower(t1.mdse_cls_desc) = 'l/s knits' then 'womens_tops_knits'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens knits' and lower(t1.mdse_cls_desc) = 'sleeveless' then 'womens_tops_knits'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens knits' and lower(t1.mdse_cls_desc) = 'womens red knits/activ' then 'womens_tops_knits'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens knits' and lower(t1.mdse_cls_desc) = 'active tops' then 'womens_tops_knits'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens knits' and lower(t1.mdse_cls_desc) = 's/s knits' then 'womens_tops_knits'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens outerwear/items' and lower(t1.mdse_cls_desc) = 'denim' then 'womens_outerwear_jacket_like'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens outerwear/items' and lower(t1.mdse_cls_desc) = 'jackets' then 'womens_outerwear_jacket_like'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens outerwear/items' and lower(t1.mdse_cls_desc) = 'vests' then 'womens_outerwear_jacket_like'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens outerwear/items' and lower(t1.mdse_cls_desc) = 'womens red outr/items' then 'womens_outerwear_jacket_like'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens sweaters' and lower(t1.mdse_cls_desc) = 'items' then 'womens_outerwear_sweater_like'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens sweaters' and lower(t1.mdse_cls_desc) = 'long sleeve' then 'womens_outerwear_sweater_like'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens sweaters' and lower(t1.mdse_cls_desc) = 'vests' then 'womens_outerwear_sweater_like'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens sweaters' and lower(t1.mdse_cls_desc) = 'sleeveless' then 'womens_outerwear_sweater_like'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens sweaters' and lower(t1.mdse_cls_desc) = 'short sleeve' then 'womens_outerwear_sweater_like'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens sweaters' and lower(t1.mdse_cls_desc) = 'womens red sweaters' then 'womens_outerwear_sweater_like'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens denim' and lower(t1.mdse_cls_desc) = '1969' then 'womens_denim'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens denim' and lower(t1.mdse_cls_desc) = 'basic 5 pocket' then 'womens_denim'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens denim' and lower(t1.mdse_cls_desc) = 'dress' then 'womens_denim'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens denim' and lower(t1.mdse_cls_desc) = 'fashion denim' then 'womens_denim'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens denim' and lower(t1.mdse_cls_desc) = 'overalls/shortalls' then 'womens_denim'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens denim' and lower(t1.mdse_cls_desc) = 'shorts' then 'womens_denim'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens denim' and lower(t1.mdse_cls_desc) = 'womens red denim' then 'womens_denim'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens denim' and lower(t1.mdse_cls_desc) = 'capris/cropped' then 'womens_denim'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens denim' and lower(t1.mdse_cls_desc) = 'denim jackets' then 'womens_denim'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens denim' and lower(t1.mdse_cls_desc) = 'denim woven tops' then 'womens_denim'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens denim' and lower(t1.mdse_cls_desc) = 'skirts' then 'womens_denim'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens bottoms' and lower(t1.mdse_cls_desc) = 'capris/cropped' then 'womens_bottoms_pant_short_like'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens bottoms' and lower(t1.mdse_cls_desc) = 'leather/suede' then 'womens_bottoms_pant_short_like'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens bottoms' and lower(t1.mdse_cls_desc) = 'long pants' then 'womens_bottoms_pant_short_like'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens bottoms' and lower(t1.mdse_cls_desc) = 'shorts' then 'womens_bottoms_pant_short_like'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens knits' and lower(t1.mdse_cls_desc) = 'active bottoms' then 'womens_bottoms_pant_short_like'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens bottoms' and lower(t1.mdse_cls_desc) = 'skirts' then 'womens_bottoms_skirt_like'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens bottoms' and lower(t1.mdse_cls_desc) = 'dresses' then 'womens_dresses'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens knits' and lower(t1.mdse_cls_desc) = 'knit dressing' then 'womens_dresses'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens outerwear/items' and lower(t1.mdse_cls_desc) = 'dresses' then 'womens_dresses'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'swim wear' and lower(t1.mdse_cls_desc) = 'swim other' then 'womens_swimwear'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'swim wear' and lower(t1.mdse_cls_desc) = 'swim tops' then 'womens_swimwear'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'swim wear' and lower(t1.mdse_cls_desc) = 'swim 1pc' then 'womens_swimwear'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'swim wear' and lower(t1.mdse_cls_desc) = 'swim bottoms' then 'womens_swimwear'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'sunglasses' and lower(t1.mdse_cls_desc) = 'womens sunglasses' then 'womens_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'womens accessories' and lower(t1.mdse_cls_desc) = 'miscellaneous' then 'womens_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'womens accessories' and lower(t1.mdse_cls_desc) = 'seasonal accessories' then 'womens_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'womens accessories' and lower(t1.mdse_cls_desc) = 'shoes' then 'womens_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'womens accessories' and lower(t1.mdse_cls_desc) = 'women s keds' then 'womens_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'womens accessories' and lower(t1.mdse_cls_desc) = 'womens scarves' then 'womens_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'womens accessories' and lower(t1.mdse_cls_desc) = 'womens socks' then 'womens_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'womens accessories' and lower(t1.mdse_cls_desc) = 'womens tights' then 'womens_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'womens accessories' and lower(t1.mdse_cls_desc) = 'womens belts' then 'womens_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'womens accessories' and lower(t1.mdse_cls_desc) = 'womens hair' then 'womens_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'womens accessories' and lower(t1.mdse_cls_desc) = 'womens hats' then 'womens_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'womens accessories' and lower(t1.mdse_cls_desc) = 'jewelry' then 'womens_accessories_jewelry_bags'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'womens accessories' and lower(t1.mdse_cls_desc) = 'womens bags' then 'womens_accessories_jewelry_bags'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'jewelry' and lower(t1.mdse_cls_desc) = 'hair' then 'womens_accessories_jewelry_bags'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'jewelry' and lower(t1.mdse_cls_desc) = 'bracelets' then 'womens_accessories_jewelry_bags'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'jewelry' and lower(t1.mdse_cls_desc) = 'necklaces' then 'womens_accessories_jewelry_bags'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby boy' and lower(t1.mdse_cls_desc) = 'denim bottoms' then 'baby_boy_bottoms'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby boy' and lower(t1.mdse_cls_desc) = 'woven bottoms' then 'baby_boy_bottoms'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby boy' and lower(t1.mdse_cls_desc) = 'knit bottoms' then 'baby_boy_bottoms'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby boy' and lower(t1.mdse_cls_desc) = 'tops' then 'baby_boy_tops'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby boy' and lower(t1.mdse_cls_desc) = 'active/layering tops' then 'baby_boy_tops'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby boy' and lower(t1.mdse_cls_desc) = 'sweaters' then 'baby_boy_outerwear'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby boy' and lower(t1.mdse_cls_desc) = 'outerwear' then 'baby_boy_outerwear'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby boy' and lower(t1.mdse_cls_desc) = 'swim' then 'baby_boy_swim'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby boy' and lower(t1.mdse_cls_desc) = 'bodysuits' then 'baby_boy_onepiece_like'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby boy' and lower(t1.mdse_cls_desc) = 'overalls' then 'baby_boy_onepiece_like'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby boy' and lower(t1.mdse_cls_desc) = 'one pieces' then 'baby_boy_onepiece_like'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby boy' and lower(t1.mdse_cls_desc) = 'socks' then 'baby_boy_other_wearables'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby boy' and lower(t1.mdse_cls_desc) = 'shoes' then 'baby_boy_other_wearables'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby boy' and lower(t1.mdse_cls_desc) = 'hats' then 'baby_boy_other_wearables'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby boy' and lower(t1.mdse_cls_desc) = 'other' then 'baby_boy_other_wearables'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler accessories' and lower(t1.mdse_cls_desc) = 'infant boy hats' then 'baby_boy_other_wearables'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler accessories' and lower(t1.mdse_cls_desc) = 'infant boy other' then 'baby_boy_other_wearables'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler accessories' and lower(t1.mdse_cls_desc) = 'infant boy shoes' then 'baby_boy_other_wearables'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler accessories' and lower(t1.mdse_cls_desc) = 'infant boy socks' then 'baby_boy_other_wearables'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby girl' and lower(t1.mdse_cls_desc) = 'dresses' then 'baby_girl_dresses'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby girl' and lower(t1.mdse_cls_desc) = 'tops' then 'baby_girl_tops'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby girl' and lower(t1.mdse_cls_desc) = 'active/layering tops' then 'baby_girl_tops'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby girl' and lower(t1.mdse_cls_desc) = 'denim bottoms' then 'baby_girl_bottoms'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby girl' and lower(t1.mdse_cls_desc) = 'woven bottoms' then 'baby_girl_bottoms'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby girl' and lower(t1.mdse_cls_desc) = 'knit bottoms' then 'baby_girl_bottoms'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby girl' and lower(t1.mdse_cls_desc) = 'swim' then 'baby_girl_swim'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby girl' and lower(t1.mdse_cls_desc) = 'sweaters' then 'baby_girl_outerwear'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby girl' and lower(t1.mdse_cls_desc) = 'outerwear' then 'baby_girl_outerwear'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby girl' and lower(t1.mdse_cls_desc) = 'bodysuits' then 'baby_girl_onepiece_like'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby girl' and lower(t1.mdse_cls_desc) = 'one pieces' then 'baby_girl_onepiece_like'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby girl' and lower(t1.mdse_cls_desc) = 'socks' then 'baby_girl_other_wearables'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby girl' and lower(t1.mdse_cls_desc) = 'shoes' then 'baby_girl_other_wearables'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby girl' and lower(t1.mdse_cls_desc) = 'hats' then 'baby_girl_other_wearables'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby girl' and lower(t1.mdse_cls_desc) = 'other' then 'baby_girl_other_wearables'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler accessories' and lower(t1.mdse_cls_desc) = 'infant girl hats' then 'baby_girl_other_wearables'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler accessories' and lower(t1.mdse_cls_desc) = 'infant girl other' then 'baby_girl_other_wearables'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler accessories' and lower(t1.mdse_cls_desc) = 'infant girl shoes' then 'baby_girl_other_wearables'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler accessories' and lower(t1.mdse_cls_desc) = 'infant girl socks' then 'baby_girl_other_wearables'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler boy knits' and lower(t1.mdse_cls_desc) = 'tops' then 'toddler_boy_tops'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler boy knits' and lower(t1.mdse_cls_desc) = 'active/layering tops' then 'toddler_boy_tops'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler boy knits' and lower(t1.mdse_cls_desc) = 'graphics' then 'toddler_boy_tops'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler boy wovens' and lower(t1.mdse_cls_desc) = 'tops' then 'toddler_boy_tops'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler boy knits' and lower(t1.mdse_cls_desc) = 'sweaters' then 'toddler_boy_outerwear'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler boy knits' and lower(t1.mdse_cls_desc) = 'outerwear' then 'toddler_boy_outerwear'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler boy wovens' and lower(t1.mdse_cls_desc) = 'outerwear' then 'toddler_boy_outerwear'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler boy wovens' and lower(t1.mdse_cls_desc) = 'overalls' then 'toddler_boy_onepiece_like'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler boy wovens' and lower(t1.mdse_cls_desc) = 'shortalls' then 'toddler_boy_onepiece_like'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler boy knits' and lower(t1.mdse_cls_desc) = 'pants' then 'toddler_boy_bottoms'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler boy knits' and lower(t1.mdse_cls_desc) = 'shorts' then 'toddler_boy_bottoms'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler boy wovens' and lower(t1.mdse_cls_desc) = 'pants' then 'toddler_boy_bottoms'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler boy wovens' and lower(t1.mdse_cls_desc) = 'shorts' then 'toddler_boy_bottoms'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler boy wovens' and lower(t1.mdse_cls_desc) = 'denim' then 'toddler_boy_denim'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler boy knits' and lower(t1.mdse_cls_desc) = 'swim' then 'toddler_boy_swim'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler boy wovens' and lower(t1.mdse_cls_desc) = 'swim' then 'toddler_boy_swim'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler boy knits' and lower(t1.mdse_cls_desc) = 'other' then 'toddler_boy_other'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler boy wovens' and lower(t1.mdse_cls_desc) = 'other' then 'toddler_boy_other'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler accessories' and lower(t1.mdse_cls_desc) = 'toddler boy socks' then 'toddler_boy_accessories_wearable'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler accessories' and lower(t1.mdse_cls_desc) = 'toddler boy hats' then 'toddler_boy_accessories_wearable'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler accessories' and lower(t1.mdse_cls_desc) = 'toddler boy other' then 'toddler_boy_accessories_wearable'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler accessories' and lower(t1.mdse_cls_desc) = 'toddler boy shoes' then 'toddler_boy_accessories_wearable'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler girl knits' and lower(t1.mdse_cls_desc) = 'tops' then 'toddler_girl_tops'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler girl knits' and lower(t1.mdse_cls_desc) = 'active/layering tops' then 'toddler_girl_tops'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler girl knits' and lower(t1.mdse_cls_desc) = 'graphic/embroidered' then 'toddler_girl_tops'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler girl wovens' and lower(t1.mdse_cls_desc) = 'tops' then 'toddler_girl_tops'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler girl knits' and lower(t1.mdse_cls_desc) = 'sweaters' then 'toddler_girl_outerwear'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler girl knits' and lower(t1.mdse_cls_desc) = 'outerwear' then 'toddler_girl_outerwear'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler girl wovens' and lower(t1.mdse_cls_desc) = 'outerwear' then 'toddler_girl_outerwear'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler girl wovens' and lower(t1.mdse_cls_desc) = 'overalls' then 'toddler_girl_onepiece_like'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler girl wovens' and lower(t1.mdse_cls_desc) = 'shortalls' then 'toddler_girl_onepiece_like'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler girl knits' and lower(t1.mdse_cls_desc) = 'pants' then 'toddler_girl_bottoms'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler girl knits' and lower(t1.mdse_cls_desc) = 'knit short' then 'toddler_girl_bottoms'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler girl wovens' and lower(t1.mdse_cls_desc) = 'pants' then 'toddler_girl_bottoms'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler girl wovens' and lower(t1.mdse_cls_desc) = 'shorts' then 'toddler_girl_bottoms'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler girl knits' and lower(t1.mdse_cls_desc) = 'dresses' then 'toddler_girl_dresses_skirts'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler girl knits' and lower(t1.mdse_cls_desc) = 'skirts/skorts' then 'toddler_girl_dresses_skirts'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler girl wovens' and lower(t1.mdse_cls_desc) = 'dresses' then 'toddler_girl_dresses_skirts'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler girl wovens' and lower(t1.mdse_cls_desc) = 'skirts/skorts' then 'toddler_girl_dresses_skirts'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler girl wovens' and lower(t1.mdse_cls_desc) = 'denim' then 'toddler_girl_denim'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler girl knits' and lower(t1.mdse_cls_desc) = 'swim' then 'toddler_girl_swim'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler accessories' and lower(t1.mdse_cls_desc) = 'toddler girl socks' then 'toddler_girl_accessories_wearable'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler accessories' and lower(t1.mdse_cls_desc) = 'infant girl hats' then 'toddler_girl_accessories_wearable'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler accessories' and lower(t1.mdse_cls_desc) = 'infant girl other' then 'toddler_girl_accessories_wearable'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler accessories' and lower(t1.mdse_cls_desc) = 'infant girl shoes' then 'toddler_girl_accessories_wearable'
when lower(t1.mdse_div_desc) = 'boys' and lower(t1.mdse_dept_desc) = 'boys active knits' and lower(t1.mdse_cls_desc) = 'vests' then 'boys_tops'
when lower(t1.mdse_div_desc) = 'boys' and lower(t1.mdse_dept_desc) = 'boys active knits' and lower(t1.mdse_cls_desc) = 'active tops' then 'boys_tops'
when lower(t1.mdse_div_desc) = 'boys' and lower(t1.mdse_dept_desc) = 'boys knts/sweaters' and lower(t1.mdse_cls_desc) = 'l/s knits' then 'boys_tops'
when lower(t1.mdse_div_desc) = 'boys' and lower(t1.mdse_dept_desc) = 'boys knts/sweaters' and lower(t1.mdse_cls_desc) = 'screen tees' then 'boys_tops'
when lower(t1.mdse_div_desc) = 'boys' and lower(t1.mdse_dept_desc) = 'boys knts/sweaters' and lower(t1.mdse_cls_desc) = 'tanks' then 'boys_tops'
when lower(t1.mdse_div_desc) = 'boys' and lower(t1.mdse_dept_desc) = 'boys knts/sweaters' and lower(t1.mdse_cls_desc) = 'vests' then 'boys_tops'
when lower(t1.mdse_div_desc) = 'boys' and lower(t1.mdse_dept_desc) = 'boys knts/sweaters' and lower(t1.mdse_cls_desc) = 's/s knits' then 'boys_tops'
when lower(t1.mdse_div_desc) = 'boys' and lower(t1.mdse_dept_desc) = 'boys woven items' and lower(t1.mdse_cls_desc) = 'long sleeve' then 'boys_tops'
when lower(t1.mdse_div_desc) = 'boys' and lower(t1.mdse_dept_desc) = 'boys woven items' and lower(t1.mdse_cls_desc) = 'short sleeve' then 'boys_tops'
when lower(t1.mdse_div_desc) = 'boys' and lower(t1.mdse_dept_desc) = 'boys active knits' and lower(t1.mdse_cls_desc) = 'bottoms' then 'boys_bottoms'
when lower(t1.mdse_div_desc) = 'boys' and lower(t1.mdse_dept_desc) = 'boys active knits' and lower(t1.mdse_cls_desc) = 'short' then 'boys_bottoms'
when lower(t1.mdse_div_desc) = 'boys' and lower(t1.mdse_dept_desc) = 'boys woven bottoms' and lower(t1.mdse_cls_desc) = 'basic woven pants' then 'boys_bottoms'
when lower(t1.mdse_div_desc) = 'boys' and lower(t1.mdse_dept_desc) = 'boys woven bottoms' and lower(t1.mdse_cls_desc) = 'fashion woven pants' then 'boys_bottoms'
when lower(t1.mdse_div_desc) = 'boys' and lower(t1.mdse_dept_desc) = 'boys woven bottoms' and lower(t1.mdse_cls_desc) = 'basic woven shorts' then 'boys_bottoms'
when lower(t1.mdse_div_desc) = 'boys' and lower(t1.mdse_dept_desc) = 'boys woven bottoms' and lower(t1.mdse_cls_desc) = 'fashion woven shorts' then 'boys_bottoms'
when lower(t1.mdse_div_desc) = 'boys' and lower(t1.mdse_dept_desc) = 'boys woven bottoms' and lower(t1.mdse_cls_desc) = 'fashion jeans' then 'boys_denim'
when lower(t1.mdse_div_desc) = 'boys' and lower(t1.mdse_dept_desc) = 'boys woven bottoms' and lower(t1.mdse_cls_desc) = 'basic denim shorts' then 'boys_denim'
when lower(t1.mdse_div_desc) = 'boys' and lower(t1.mdse_dept_desc) = 'boys woven bottoms' and lower(t1.mdse_cls_desc) = 'basic jean' then 'boys_denim'
when lower(t1.mdse_div_desc) = 'boys' and lower(t1.mdse_dept_desc) = 'boys woven bottoms' and lower(t1.mdse_cls_desc) = 'fashion denim shorts' then 'boys_denim'
when lower(t1.mdse_div_desc) = 'boys' and lower(t1.mdse_dept_desc) = 'boys knts/sweaters' and lower(t1.mdse_cls_desc) = 'sweaters' then 'boys_outerwear'
when lower(t1.mdse_div_desc) = 'boys' and lower(t1.mdse_dept_desc) = 'boys woven bottoms' and lower(t1.mdse_cls_desc) = 'swim' then 'boys_outerwear'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'boys accessories' and lower(t1.mdse_cls_desc) = 'bags' then 'boys_accessories_other'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'boys accessories' and lower(t1.mdse_cls_desc) = 'non-apparel items' then 'boys_accessories_other'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'boys accessories' and lower(t1.mdse_cls_desc) = 'other' then 'boys_accessories_other'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'boys accessories' and lower(t1.mdse_cls_desc) = 'apparel items' then 'boys_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'boys accessories' and lower(t1.mdse_cls_desc) = 'socks' then 'boys_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'boys accessories' and lower(t1.mdse_cls_desc) = 'belts' then 'boys_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'boys accessories' and lower(t1.mdse_cls_desc) = 'shoes' then 'boys_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'boys accessories' and lower(t1.mdse_cls_desc) = 'sunglasses' then 'boys_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'boys accessories' and lower(t1.mdse_cls_desc) = 'cold weather' then 'boys_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'boys accessories' and lower(t1.mdse_cls_desc) = 'hats' then 'boys_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'boys underwear' and lower(t1.mdse_cls_desc) = 'long johns' then 'boys_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'boys underwear' and lower(t1.mdse_cls_desc) = 'other' then 'boys_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'boys underwear' and lower(t1.mdse_cls_desc) = 'sleepwear' then 'boys_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'boys underwear' and lower(t1.mdse_cls_desc) = 'slippers' then 'boys_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'boys underwear' and lower(t1.mdse_cls_desc) = 'boxers' then 'boys_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'boys underwear' and lower(t1.mdse_cls_desc) = 'underwear' then 'boys_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'boys underwear' and lower(t1.mdse_cls_desc) = 'underwear tops' then 'boys_accessories_wearables'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'girls active knits' and lower(t1.mdse_cls_desc) = 'tops' then 'girls_tops'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'girls knits/sweaters' and lower(t1.mdse_cls_desc) = 'sleeveless' then 'girls_tops'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'girls knits/sweaters' and lower(t1.mdse_cls_desc) = 'long sleeve tops' then 'girls_tops'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'girls knits/sweaters' and lower(t1.mdse_cls_desc) = 'short sleeve tops' then 'girls_tops'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'girls active knits' and lower(t1.mdse_cls_desc) = 'bottoms' then 'girls_tops'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'girls active knits' and lower(t1.mdse_cls_desc) = 'shorts' then 'girls_tops'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'girls woven bottoms' and lower(t1.mdse_cls_desc) = 'basic woven pant' then 'girls_bottoms'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'girls woven bottoms' and lower(t1.mdse_cls_desc) = 'fashion woven capri' then 'girls_bottoms'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'girls woven bottoms' and lower(t1.mdse_cls_desc) = 'fashion woven pant' then 'girls_bottoms'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'girls woven bottoms' and lower(t1.mdse_cls_desc) = 'woven shorts' then 'girls_bottoms'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'girls active knits' and lower(t1.mdse_cls_desc) = 'dresses' then 'girls_dresses_skirts'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'girls active knits' and lower(t1.mdse_cls_desc) = 'skirts' then 'girls_dresses_skirts'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'girls woven bottoms' and lower(t1.mdse_cls_desc) = 'basic jean' then 'girls_denim'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'girls woven bottoms' and lower(t1.mdse_cls_desc) = 'denim capri' then 'girls_denim'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'girls woven bottoms' and lower(t1.mdse_cls_desc) = 'denim shorts' then 'girls_denim'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'girls woven bottoms' and lower(t1.mdse_cls_desc) = 'fashion jean' then 'girls_denim'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'girls knits/sweaters' and lower(t1.mdse_cls_desc) = 'sweaters' then 'girls_outerwear'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'girls woven bottoms' and lower(t1.mdse_cls_desc) = 'shortalls/overalls' then 'girls_outerwear'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'girls active knits' and lower(t1.mdse_cls_desc) = 'swimwear' then 'girls_swim'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'kids/bby/matrnty red' and lower(t1.mdse_cls_desc) = 'boys red' then 'girls_maternity'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'kids/bby/matrnty red' and lower(t1.mdse_cls_desc) = 'girls red' then 'girls_maternity'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'kids/bby/matrnty red' and lower(t1.mdse_cls_desc) = 'kids red accessories' then 'girls_maternity'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'kids/bby/matrnty red' and lower(t1.mdse_cls_desc) = 'maternity red' then 'girls_maternity'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'kids/bby/matrnty red' and lower(t1.mdse_cls_desc) = 'baby red' then 'girls_maternity'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'girls accessories' and lower(t1.mdse_cls_desc) = 'bags' then 'girls_accessories_other'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'girls accessories' and lower(t1.mdse_cls_desc) = 'non-apparel items' then 'girls_accessories_other'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'girls accessories' and lower(t1.mdse_cls_desc) = 'apparel items' then 'girls_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'girls accessories' and lower(t1.mdse_cls_desc) = ' socks' then 'girls_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'girls accessories' and lower(t1.mdse_cls_desc) = 'tights' then 'girls_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'girls accessories' and lower(t1.mdse_cls_desc) = 'belts' then 'girls_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'girls accessories' and lower(t1.mdse_cls_desc) = 'cold weather' then 'girls_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'girls accessories' and lower(t1.mdse_cls_desc) = 'hats' then 'girls_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'girls accessories' and lower(t1.mdse_cls_desc) = 'shoes' then 'girls_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'girls accessories' and lower(t1.mdse_cls_desc) = 'sunglasses' then 'girls_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'girls underwear' and lower(t1.mdse_cls_desc) = 'longjohns' then 'girls_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'girls underwear' and lower(t1.mdse_cls_desc) = 'sleepwear' then 'girls_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'girls underwear' and lower(t1.mdse_cls_desc) = 'slippers' then 'girls_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'girls underwear' and lower(t1.mdse_cls_desc) = 'boxers' then 'girls_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'girls underwear' and lower(t1.mdse_cls_desc) = 'underwear' then 'girls_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'girls underwear' and lower(t1.mdse_cls_desc) = 'underwear tops' then 'girls_accessories_wearables'
when lower(t1.mdse_div_desc) = 'maternity' and lower(t1.mdse_dept_desc) = 'tops' and lower(t1.mdse_cls_desc) = 'wovens' then 'maternity_tops'
when lower(t1.mdse_div_desc) = 'maternity' and lower(t1.mdse_dept_desc) = 'tops' and lower(t1.mdse_cls_desc) = 'active tops' then 'maternity_tops'
when lower(t1.mdse_div_desc) = 'maternity' and lower(t1.mdse_dept_desc) = 'tops' and lower(t1.mdse_cls_desc) = 'knits' then 'maternity_tops'
when lower(t1.mdse_div_desc) = 'maternity' and lower(t1.mdse_dept_desc) = 'active' and lower(t1.mdse_cls_desc) = 'tops' then 'maternity_tops'
when lower(t1.mdse_div_desc) = 'maternity' and lower(t1.mdse_dept_desc) = 'bottoms' and lower(t1.mdse_cls_desc) = 'shorts' then 'maternity_bottoms'
when lower(t1.mdse_div_desc) = 'maternity' and lower(t1.mdse_dept_desc) = 'bottoms' and lower(t1.mdse_cls_desc) = 'denim' then 'maternity_bottoms'
when lower(t1.mdse_div_desc) = 'maternity' and lower(t1.mdse_dept_desc) = 'bottoms' and lower(t1.mdse_cls_desc) = 'pants' then 'maternity_bottoms'
when lower(t1.mdse_div_desc) = 'maternity' and lower(t1.mdse_dept_desc) = 'bottoms' and lower(t1.mdse_cls_desc) = 'overalls' then 'maternity_bottoms'
when lower(t1.mdse_div_desc) = 'maternity' and lower(t1.mdse_dept_desc) = 'bottoms' and lower(t1.mdse_cls_desc) = 'capris' then 'maternity_bottoms'
when lower(t1.mdse_div_desc) = 'maternity' and lower(t1.mdse_dept_desc) = 'bottoms' and lower(t1.mdse_cls_desc) = 'active bottoms' then 'maternity_bottoms'
when lower(t1.mdse_div_desc) = 'maternity' and lower(t1.mdse_dept_desc) = 'active' and lower(t1.mdse_cls_desc) = 'bottoms' then 'maternity_bottoms'
when lower(t1.mdse_div_desc) = 'maternity' and lower(t1.mdse_dept_desc) = 'tops' and lower(t1.mdse_cls_desc) = 'sweaters' then 'maternity_outerwear'
when lower(t1.mdse_div_desc) = 'maternity' and lower(t1.mdse_dept_desc) = 'tops' and lower(t1.mdse_cls_desc) = 'outerwear' then 'maternity_outerwear'
when lower(t1.mdse_div_desc) = 'maternity' and lower(t1.mdse_dept_desc) = 'items' and lower(t1.mdse_cls_desc) = 'outerwear' then 'maternity_outerwear'
when lower(t1.mdse_div_desc) = 'maternity' and lower(t1.mdse_dept_desc) = 'tops' and lower(t1.mdse_cls_desc) = 'dresses' then 'maternity_dresses_skirts'
when lower(t1.mdse_div_desc) = 'maternity' and lower(t1.mdse_dept_desc) = 'items' and lower(t1.mdse_cls_desc) = 'dresses' then 'maternity_dresses_skirts'
when lower(t1.mdse_div_desc) = 'maternity' and lower(t1.mdse_dept_desc) = 'bottoms' and lower(t1.mdse_cls_desc) = 'skirts' then 'maternity_dresses_skirts'
when lower(t1.mdse_div_desc) = 'maternity' and lower(t1.mdse_dept_desc) = 'tops' and lower(t1.mdse_cls_desc) = 'swimwear' then 'maternity_swim'
when lower(t1.mdse_div_desc) = 'maternity' and lower(t1.mdse_dept_desc) = 'active' and lower(t1.mdse_cls_desc) = 'swimwear' then 'maternity_swim'
when lower(t1.mdse_div_desc) = 'maternity' and lower(t1.mdse_dept_desc) = 'body' and lower(t1.mdse_cls_desc) = 'underwear' then 'maternity_body'
when lower(t1.mdse_div_desc) = 'maternity' and lower(t1.mdse_dept_desc) = 'body' and lower(t1.mdse_cls_desc) = 'sleepwear' then 'maternity_body'
when lower(t1.mdse_div_desc) = 'maternity' and lower(t1.mdse_dept_desc) = 'body' and lower(t1.mdse_cls_desc) = 'foundations' then 'maternity_body'
else 'NA'
end as ctg

