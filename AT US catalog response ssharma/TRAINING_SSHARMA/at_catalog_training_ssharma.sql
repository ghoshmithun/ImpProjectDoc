drop table if exists ssharma_db.fall_1_2016;
create table ssharma_db.fall_1_2016
(id string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/home/xsa2d2mf/FALL_1_2016.txt' INTO TABLE ssharma_db.fall_1_2016;

drop table if exists ssharma_db.spring_1_2016;
create table ssharma_db.spring_1_2016
(id string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/home/xsa2d2mf/SPRING_1_2016.txt' INTO TABLE ssharma_db.spring_1_2016;

drop table if exists ssharma_db.summer_1_2016;
create table ssharma_db.summer_1_2016
(id string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/home/xsa2d2mf/SUMMER_1_2016.txt' INTO TABLE ssharma_db.summer_1_2016;

drop table if exists ssharma_db.winter_4_2015;
create table ssharma_db.winter_4_2015
(id string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/home/xsa2d2mf/WINTER_4_2015.txt' INTO TABLE ssharma_db.winter_4_2015;

drop table  if exists ssharma_db.fall_cust;
create table ssharma_db.fall_cust as 
select distinct  cast(substr(id,11) as float ) as customer_key from ssharma_db.fall_1_2016;

drop table  if exists ssharma_db.winter_cust;
create table ssharma_db.winter_cust as 
select distinct  cast(substr(id,11) as float ) as customer_key from ssharma_db.winter_4_2015;

drop table  if exists ssharma_db.spring_cust;
create table ssharma_db.spring_cust as 
select distinct  cast(substr(id,11) as float ) as customer_key from ssharma_db.spring_1_2016;

drop table  if exists ssharma_db.summer_cust;
create table ssharma_db.summer_cust as 
select distinct  cast(substr(id,11) as float ) as customer_key from ssharma_db.summer_1_2016;

drop table if exists ssharma_db.fall_customer;
create table ssharma_db.fall_customer as 
select cast(customer_key as bigint) as customer_key from ssharma_db.fall_cust;

drop table if exists ssharma_db.winter_customer;
create table ssharma_db.winter_customer as 
select cast(customer_key as bigint) as customer_key from ssharma_db.winter_cust;

drop table if exists ssharma_db.spring_customer;
create table ssharma_db.spring_customer as 
select cast(customer_key as bigint) as customer_key from ssharma_db.spring_cust;

drop table if exists ssharma_db.summer_customer;
create table ssharma_db.summer_customer as 
select cast(customer_key as bigint) as customer_key from ssharma_db.summer_cust;

drop table if exists ssharma_db.fall_lastyr;
create table ssharma_db.fall_lastyr as 
select distinct a.customer_key,a.transaction_num,a.line_num,a.sales_amt,a.item_qty,a.product_key,a.transaction_date,a.on_sale_flag
from
(select distinct customer_key,transaction_num,line_num,sales_amt,discount_amt,item_qty,product_key,transaction_date,on_sale_flag from  mds_new.ods_orderline_t 
where brand='AT' and country='US' and item_qty > 0 and sales_amt>0 and transaction_date between '2015-08-02' and '2016-08-01' and order_status in ('R','O')) a 
inner join 
ssharma_db.fall_customer b 
on a.customer_key=b.customer_key; 

drop table if exists ssharma_db.winter_lastyr;
create table ssharma_db.winter_lastyr as 
select distinct a.customer_key,a.transaction_num,a.line_num,a.sales_amt,a.item_qty,a.product_key,a.transaction_date,a.on_sale_flag
from
(select distinct customer_key,transaction_num,line_num,sales_amt,discount_amt,item_qty,product_key,transaction_date,on_sale_flag from  mds_new.ods_orderline_t 
where brand='AT' and country='US' and item_qty > 0 and sales_amt>0 and transaction_date between '2015-01-05' and '2016-01-04' and order_status in ('R','O')) a 
inner join 
ssharma_db.winter_customer b 
on a.customer_key=b.customer_key;

drop table if exists ssharma_db.spring_lastyr;
create table ssharma_db.spring_lastyr as 
select distinct a.customer_key,a.transaction_num,a.line_num,a.sales_amt,a.item_qty,a.product_key,a.transaction_date,a.on_sale_flag
from
(select distinct customer_key,transaction_num,line_num,sales_amt,discount_amt,item_qty,product_key,transaction_date,on_sale_flag from  mds_new.ods_orderline_t 
where brand='AT' and country='US' and item_qty > 0 and sales_amt>0 and transaction_date between '2015-01-26' and '2016-01-25' and order_status in ('R','O')) a 
inner join 
ssharma_db.spring_customer b 
on a.customer_key=b.customer_key;

drop table if exists ssharma_db.summer_lastyr;
create table ssharma_db.summer_lastyr as 
select distinct a.customer_key,a.transaction_num,a.line_num,a.sales_amt,a.item_qty,a.product_key,a.transaction_date,a.on_sale_flag
from
(select distinct customer_key,transaction_num,line_num,sales_amt,discount_amt,item_qty,product_key,transaction_date,on_sale_flag from  mds_new.ods_orderline_t 
where brand='AT' and country='US' and item_qty > 0 and sales_amt>0 and transaction_date between '2015-04-26' and '2016-04-25' and order_status in ('R','O')) a 
inner join 
ssharma_db.summer_customer b 
on a.customer_key=b.customer_key;

drop table if exists ssharma_db.fall_lastyr_disc;
create table ssharma_db.fall_lastyr_disc as
select a.customer_key, 
a.transaction_num,
a.line_num,
sum(b.discount_amt) as discount_amt
from (select * from ssharma_db.fall_lastyr where transaction_date is NOT NULL) a left outer join mds_new.ods_orderline_discounts_t b
on (a.transaction_num=b.transaction_num
and a.line_num=b.line_num)
group by a.customer_key, a.transaction_num, a.line_num;

drop table if exists ssharma_db.winter_lastyr_disc;
create table ssharma_db.winter_lastyr_disc as
select a.customer_key, 
a.transaction_num,
a.line_num,
sum(b.discount_amt) as discount_amt
from (select * from ssharma_db.winter_lastyr where transaction_date is NOT NULL) a left outer join mds_new.ods_orderline_discounts_t b
on (a.transaction_num=b.transaction_num
and a.line_num=b.line_num)
group by a.customer_key, a.transaction_num, a.line_num;

drop table if exists ssharma_db.spring_lastyr_disc;
create table ssharma_db.spring_lastyr_disc as
select a.customer_key, 
a.transaction_num,
a.line_num,
sum(b.discount_amt) as discount_amt
from (select * from ssharma_db.spring_lastyr where transaction_date is NOT NULL) a left outer join mds_new.ods_orderline_discounts_t b
on (a.transaction_num=b.transaction_num
and a.line_num=b.line_num)
group by a.customer_key, a.transaction_num, a.line_num;

drop table if exists ssharma_db.summer_lastyr_disc;
create table ssharma_db.summer_lastyr_disc as
select a.customer_key, 
a.transaction_num,
a.line_num,
sum(b.discount_amt) as discount_amt
from (select * from ssharma_db.summer_lastyr where transaction_date is NOT NULL) a left outer join mds_new.ods_orderline_discounts_t b
on (a.transaction_num=b.transaction_num
and a.line_num=b.line_num)
group by a.customer_key, a.transaction_num, a.line_num;

drop table if exists ssharma_db.fall_lastyr_data;
create table ssharma_db.fall_lastyr_data as
select distinct a.customer_key,a.transaction_num,a.line_num,a.sales_amt,a.item_qty,a.product_key,a.transaction_date,a.on_sale_flag,
coalesce(b.discount_amt,cast('0.0' as double)) as discount_amt
from (select * from ssharma_db.fall_lastyr where transaction_date is NOT NULL) a left outer join ssharma_db.fall_lastyr_disc b
on (a.transaction_num=b.transaction_num
and a.line_num=b.line_num
and a.customer_key=b.customer_key);

drop table if exists ssharma_db.winter_lastyr_data;
create table ssharma_db.winter_lastyr_data as
select distinct a.customer_key,a.transaction_num,a.line_num,a.sales_amt,a.item_qty,a.product_key,a.transaction_date,a.on_sale_flag,
coalesce(b.discount_amt,cast('0.0' as double)) as discount_amt
from (select * from ssharma_db.winter_lastyr where transaction_date is NOT NULL) a left outer join ssharma_db.winter_lastyr_disc b
on (a.transaction_num=b.transaction_num
and a.line_num=b.line_num
and a.customer_key=b.customer_key);

drop table if exists ssharma_db.spring_lastyr_data;
create table ssharma_db.spring_lastyr_data as
select distinct a.customer_key,a.transaction_num,a.line_num,a.sales_amt,a.item_qty,a.product_key,a.transaction_date,a.on_sale_flag,
coalesce(b.discount_amt,cast('0.0' as double)) as discount_amt
from (select * from ssharma_db.spring_lastyr where transaction_date is NOT NULL) a left outer join ssharma_db.spring_lastyr_disc b
on (a.transaction_num=b.transaction_num
and a.line_num=b.line_num
and a.customer_key=b.customer_key);

drop table if exists ssharma_db.summer_lastyr_data;
create table ssharma_db.summer_lastyr_data as
select distinct a.customer_key,a.transaction_num,a.line_num,a.sales_amt,a.item_qty,a.product_key,a.transaction_date,a.on_sale_flag,
coalesce(b.discount_amt,cast('0.0' as double)) as discount_amt
from (select * from ssharma_db.summer_lastyr where transaction_date is NOT NULL) a left outer join ssharma_db.summer_lastyr_disc b
on (a.transaction_num=b.transaction_num
and a.line_num=b.line_num
and a.customer_key=b.customer_key);

drop table if exists ssharma_db.fall_agg;
create table ssharma_db.fall_agg as 
select customer_key,sum(sales_amt+discount_amt) as net_sales,sum(abs(coalesce(discount_amt,0)))/sum(sales_amt) as disc_p,
sum(item_qty) as items_purch,
count(distinct transaction_num) as num_txn,
sum(sales_amt+discount_amt)/count(distinct transaction_num) as avg_ord_sz,
sum(sales_amt+discount_amt)/sum(item_qty) as avg_unt_rtl,
sum(item_qty)/count(distinct transaction_num) as unt_per_txn,
sum(item_qty*(case when on_sale_flag='Y' then 1 else 0 end)) as on_sale_items,
DATEDIFF('2016-08-02',max(transaction_date)) as recency
from ssharma_db.fall_lastyr_data
group by customer_key;

drop table if exists ssharma_db.winter_agg;
create table ssharma_db.winter_agg as 
select customer_key,sum(sales_amt+discount_amt) as net_sales,sum(abs(coalesce(discount_amt,0)))/sum(sales_amt) as disc_p,
sum(item_qty) as items_purch,
count(distinct transaction_num) as num_txn,
sum(sales_amt+discount_amt)/count(distinct transaction_num) as avg_ord_sz,
sum(sales_amt+discount_amt)/sum(item_qty) as avg_unt_rtl,
sum(item_qty)/count(distinct transaction_num) as unt_per_txn,
sum(item_qty*(case when on_sale_flag='Y' then 1 else 0 end)) as on_sale_items,
DATEDIFF('2016-01-05',max(transaction_date)) as recency
from ssharma_db.winter_lastyr_data
group by customer_key;

drop table if exists ssharma_db.spring_agg;
create table ssharma_db.spring_agg as 
select customer_key,sum(sales_amt+discount_amt) as net_sales,sum(abs(coalesce(discount_amt,0)))/sum(sales_amt) as disc_p,
sum(item_qty) as items_purch,
count(distinct transaction_num) as num_txn,
sum(sales_amt+discount_amt)/count(distinct transaction_num) as avg_ord_sz,
sum(sales_amt+discount_amt)/sum(item_qty) as avg_unt_rtl,
sum(item_qty)/count(distinct transaction_num) as unt_per_txn,
sum(item_qty*(case when on_sale_flag='Y' then 1 else 0 end)) as on_sale_items,
DATEDIFF('2016-01-26',max(transaction_date)) as recency
from ssharma_db.spring_lastyr_data
group by customer_key;

drop table if exists ssharma_db.summer_agg;
create table ssharma_db.summer_agg as 
select customer_key,sum(sales_amt+discount_amt) as net_sales,sum(abs(coalesce(discount_amt,0)))/sum(sales_amt) as disc_p,
sum(item_qty) as items_purch,
count(distinct transaction_num) as num_txns,
sum(sales_amt+discount_amt)/count(distinct transaction_num) as avg_ord_sz,
sum(sales_amt+discount_amt)/sum(item_qty) as avg_unt_rtl,
sum(item_qty)/count(distinct transaction_num) as unt_per_txn,
sum(item_qty*(case when on_sale_flag='Y' then 1 else 0 end)) as on_sale_items,
DATEDIFF('2016-04-26',max(transaction_date)) as recency
from ssharma_db.summer_lastyr_data
group by customer_key;

drop table if exists ssharma_db.fall_prod;
create table ssharma_db.fall_prod as 
select distinct a.customer_key,a.transaction_num,a.line_num,a.sales_amt,a.item_qty,a.product_key,a.transaction_date,a.on_sale_flag,b.season_desc,
b.mdse_div_desc,b.mdse_dept_desc,b.mdse_class_desc,
(case when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='CAPRIS' then 'GIRLS CAPRIS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='PANTS' then 'GIRLS PANTS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='SHORTS' then 'GIRLS SHORTS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='SKIRTS/SKORTS' then 'GIRLS SKIRTS/SKORTS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='GIRLS ACCESSORY' then 'GIRLS ACCESSORY'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='GIRLS INTIMATES' then 'GIRLS INTIMATES'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='GIRLS SWIMWEAR' then 'GIRLS SWIMWEAR'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='OUTERWEAR' then 'GIRLS OUTERWEAR'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='TOPS/SUPPORT/SWEATER' and b.mdse_class_desc<>'SWEATSHIRTS' then 'GIRLS TOPS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='TOPS/SUPPORT/SWEATER' and b.mdse_class_desc='SWEATSHIRTS' then 'GIRLS SWEATSHIRTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='ACCESSORY' then 'WOMENS ACCESSORY' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'CAPRI')>0 then 'WOMENS CAPRIS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and b.mdse_class_desc='DRESS' then 'WOMENS DRESSES'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and (b.mdse_class_desc='JACKET' or b.mdse_class_desc='OUTERWEAR' or b.mdse_class_desc='VEST') then 'WOMENS OUTERWEAR'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'PANT')>0 then 'WOMENS PANTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'SHORT')>0 then 'WOMENS SHORTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'SKIRT')>0 then 'WOMENS SKIRTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'SKORT')>0 then 'WOMENS SKORTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and b.mdse_class_desc='SUPPORT TOPS' then 'WOMENS SUPPORT TOPS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and b.mdse_class_desc='SWEATER' then 'WOMENS SWEATERS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and (instr(b.mdse_class_desc,'COVER UP')>0 or instr(b.mdse_class_desc,'TOP')>0)  then 'WOMENS TOPS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='FOOTWEAR' then 'WOMENS FOOTWEAR' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='INTIMATES' and b.mdse_class_desc<>'BRAS' then 'WOMENS INTIMATES'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='INTIMATES' and b.mdse_class_desc='BRAS' then 'WOMENS BRAS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='BIKINI TOP' then 'WOMENS SWIM BIKINI'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='SWIM BOTTOMS' then 'WOMENS SWIM BOTTOMS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='ONE PIECE' then 'WOMENS SWIM ONE PIECE'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='RASHGUARDS' then 'WOMENS SWIM RASHGUARDS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='TANKINI' then 'WOMENS SWIM TANKINI'
else 'N/A' end) as category
from 
ssharma_db.fall_lastyr a 
inner join 
mds_new.ods_product_t b 
on a.product_key=b.product_key;

drop table if exists ssharma_db.winter_prod;
create table ssharma_db.winter_prod as 
select distinct a.customer_key,a.transaction_num,a.line_num,a.sales_amt,a.item_qty,a.product_key,a.transaction_date,a.on_sale_flag,b.season_desc,b.mdse_div_desc,b.mdse_dept_desc,b.mdse_class_desc,
(case when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='CAPRIS' then 'GIRLS CAPRIS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='PANTS' then 'GIRLS PANTS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='SHORTS' then 'GIRLS SHORTS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='SKIRTS/SKORTS' then 'GIRLS SKIRTS/SKORTS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='GIRLS ACCESSORY' then 'GIRLS ACCESSORY'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='GIRLS INTIMATES' then 'GIRLS INTIMATES'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='GIRLS SWIMWEAR' then 'GIRLS SWIMWEAR'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='OUTERWEAR' then 'GIRLS OUTERWEAR'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='TOPS/SUPPORT/SWEATER' and b.mdse_class_desc<>'SWEATSHIRTS' then 'GIRLS TOPS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='TOPS/SUPPORT/SWEATER' and b.mdse_class_desc='SWEATSHIRTS' then 'GIRLS SWEATSHIRTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='ACCESSORY' then 'WOMENS ACCESSORY' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'CAPRI')>0 then 'WOMENS CAPRIS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and b.mdse_class_desc='DRESS' then 'WOMENS DRESSES'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and (b.mdse_class_desc='JACKET' or b.mdse_class_desc='OUTERWEAR' or b.mdse_class_desc='VEST') then 'WOMENS OUTERWEAR'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'PANT')>0 then 'WOMENS PANTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'SHORT')>0 then 'WOMENS SHORTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'SKIRT')>0 then 'WOMENS SKIRTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'SKORT')>0 then 'WOMENS SKORTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and b.mdse_class_desc='SUPPORT TOPS' then 'WOMENS SUPPORT TOPS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and b.mdse_class_desc='SWEATER' then 'WOMENS SWEATERS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and (instr(b.mdse_class_desc,'COVER UP')>0 or instr(b.mdse_class_desc,'TOP')>0)  then 'WOMENS TOPS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='FOOTWEAR' then 'WOMENS FOOTWEAR' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='INTIMATES' and b.mdse_class_desc<>'BRAS' then 'WOMENS INTIMATES'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='INTIMATES' and b.mdse_class_desc='BRAS' then 'WOMENS BRAS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='BIKINI TOP' then 'WOMENS SWIM BIKINI'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='SWIM BOTTOMS' then 'WOMENS SWIM BOTTOMS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='ONE PIECE' then 'WOMENS SWIM ONE PIECE'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='RASHGUARDS' then 'WOMENS SWIM RASHGUARDS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='TANKINI' then 'WOMENS SWIM TANKINI'
else 'N/A' end) as category
from 
ssharma_db.winter_lastyr a 
inner join 
mds_new.ods_product_t b 
on a.product_key=b.product_key;

drop table if exists ssharma_db.spring_prod;
create table ssharma_db.spring_prod as 
select distinct a.customer_key,a.transaction_num,a.line_num,a.sales_amt,a.item_qty,a.product_key,a.transaction_date,a.on_sale_flag,b.season_desc,b.mdse_div_desc,b.mdse_dept_desc,b.mdse_class_desc,
(case when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='CAPRIS' then 'GIRLS CAPRIS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='PANTS' then 'GIRLS PANTS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='SHORTS' then 'GIRLS SHORTS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='SKIRTS/SKORTS' then 'GIRLS SKIRTS/SKORTS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='GIRLS ACCESSORY' then 'GIRLS ACCESSORY'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='GIRLS INTIMATES' then 'GIRLS INTIMATES'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='GIRLS SWIMWEAR' then 'GIRLS SWIMWEAR'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='OUTERWEAR' then 'GIRLS OUTERWEAR'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='TOPS/SUPPORT/SWEATER' and b.mdse_class_desc<>'SWEATSHIRTS' then 'GIRLS TOPS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='TOPS/SUPPORT/SWEATER' and b.mdse_class_desc='SWEATSHIRTS' then 'GIRLS SWEATSHIRTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='ACCESSORY' then 'WOMENS ACCESSORY' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'CAPRI')>0 then 'WOMENS CAPRIS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and b.mdse_class_desc='DRESS' then 'WOMENS DRESSES'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and (b.mdse_class_desc='JACKET' or b.mdse_class_desc='OUTERWEAR' or b.mdse_class_desc='VEST') then 'WOMENS OUTERWEAR'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'PANT')>0 then 'WOMENS PANTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'SHORT')>0 then 'WOMENS SHORTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'SKIRT')>0 then 'WOMENS SKIRTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'SKORT')>0 then 'WOMENS SKORTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and b.mdse_class_desc='SUPPORT TOPS' then 'WOMENS SUPPORT TOPS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and b.mdse_class_desc='SWEATER' then 'WOMENS SWEATERS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and (instr(b.mdse_class_desc,'COVER UP')>0 or instr(b.mdse_class_desc,'TOP')>0)  then 'WOMENS TOPS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='FOOTWEAR' then 'WOMENS FOOTWEAR' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='INTIMATES' and b.mdse_class_desc<>'BRAS' then 'WOMENS INTIMATES'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='INTIMATES' and b.mdse_class_desc='BRAS' then 'WOMENS BRAS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='BIKINI TOP' then 'WOMENS SWIM BIKINI'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='SWIM BOTTOMS' then 'WOMENS SWIM BOTTOMS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='ONE PIECE' then 'WOMENS SWIM ONE PIECE'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='RASHGUARDS' then 'WOMENS SWIM RASHGUARDS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='TANKINI' then 'WOMENS SWIM TANKINI'
else 'N/A' end) as category
from 
ssharma_db.spring_lastyr a 
inner join 
mds_new.ods_product_t b 
on a.product_key=b.product_key;

drop table if exists ssharma_db.summer_prod;
create table ssharma_db.summer_prod as 
select distinct a.customer_key,a.transaction_num,a.line_num,a.sales_amt,a.item_qty,a.product_key,a.transaction_date,a.on_sale_flag,b.season_desc,b.mdse_div_desc,b.mdse_dept_desc,b.mdse_class_desc,
(case when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='CAPRIS' then 'GIRLS CAPRIS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='PANTS' then 'GIRLS PANTS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='SHORTS' then 'GIRLS SHORTS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='SKIRTS/SKORTS' then 'GIRLS SKIRTS/SKORTS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='GIRLS ACCESSORY' then 'GIRLS ACCESSORY'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='GIRLS INTIMATES' then 'GIRLS INTIMATES'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='GIRLS SWIMWEAR' then 'GIRLS SWIMWEAR'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='OUTERWEAR' then 'GIRLS OUTERWEAR'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='TOPS/SUPPORT/SWEATER' and b.mdse_class_desc<>'SWEATSHIRTS' then 'GIRLS TOPS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='TOPS/SUPPORT/SWEATER' and b.mdse_class_desc='SWEATSHIRTS' then 'GIRLS SWEATSHIRTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='ACCESSORY' then 'WOMENS ACCESSORY' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'CAPRI')>0 then 'WOMENS CAPRIS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and b.mdse_class_desc='DRESS' then 'WOMENS DRESSES'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and (b.mdse_class_desc='JACKET' or b.mdse_class_desc='OUTERWEAR' or b.mdse_class_desc='VEST') then 'WOMENS OUTERWEAR'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'PANT')>0 then 'WOMENS PANTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'SHORT')>0 then 'WOMENS SHORTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'SKIRT')>0 then 'WOMENS SKIRTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'SKORT')>0 then 'WOMENS SKORTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and b.mdse_class_desc='SUPPORT TOPS' then 'WOMENS SUPPORT TOPS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and b.mdse_class_desc='SWEATER' then 'WOMENS SWEATERS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and (instr(b.mdse_class_desc,'COVER UP')>0 or instr(b.mdse_class_desc,'TOP')>0)  then 'WOMENS TOPS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='FOOTWEAR' then 'WOMENS FOOTWEAR' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='INTIMATES' and b.mdse_class_desc<>'BRAS' then 'WOMENS INTIMATES'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='INTIMATES' and b.mdse_class_desc='BRAS' then 'WOMENS BRAS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='BIKINI TOP' then 'WOMENS SWIM BIKINI'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='SWIM BOTTOMS' then 'WOMENS SWIM BOTTOMS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='ONE PIECE' then 'WOMENS SWIM ONE PIECE'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='RASHGUARDS' then 'WOMENS SWIM RASHGUARDS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='TANKINI' then 'WOMENS SWIM TANKINI'
else 'N/A' end) as category
from 
ssharma_db.summer_lastyr a 
inner join 
mds_new.ods_product_t b 
on a.product_key=b.product_key;

--drop table if exists ssharma_db.fall_agg_season;
--create table ssharma_db.fall_agg_season as 
--select customer_key,sum(sales_amt+discount_amt) as net_sales_season,sum(abs(coalesce(discount_amt,0)))/sum(sales_amt) as disc_p_season,
--sum(item_qty) as items_purch_season,
--count(distinct transaction_num) as num_txn_season,
--sum(sales_amt+discount_amt)/count(distinct transaction_num) as avg_ord_sz_season,
--sum(sales_amt+discount_amt)/sum(item_qty) as avg_unt_rtl_season,
--sum(item_qty)/count(distinct transaction_num) as unt_per_txn_season,
--sum(item_qty*(case when on_sale_flag='Y' then 1 else 0 end)) as on_sale_items_season,
--count(distinct category) as num_cat_season,
--DATEDIFF('2015-08-04',max(transaction_date)) as recency_season 
--from ssharma_db.fall_prod
--where instr(lower(season_desc),'fa')>0
--group by customer_key;

--drop table if exists ssharma_db.winter_agg_season;
--create table ssharma_db.winter_agg_season as 
--select customer_key,sum(sales_amt+discount_amt) as net_sales_season,sum(abs(coalesce(discount_amt,0)))/sum(sales_amt) as disc_p_season,
--sum(item_qty) as items_purch_season,
--count(distinct transaction_num) as num_txn_season,
--sum(sales_amt+discount_amt)/count(distinct transaction_num) as avg_ord_sz_season,
--sum(sales_amt+discount_amt)/sum(item_qty) as avg_unt_rtl_season,
--sum(item_qty)/count(distinct transaction_num) as unt_per_txn_season,
--sum(item_qty*(case when on_sale_flag='Y' then 1 else 0 end)) as on_sale_items_season,
--count(distinct category) as num_cat_season,
--DATEDIFF('2015-11-10',max(transaction_date)) as recency_season 
--from ssharma_db.winter_prod
--where instr(lower(season_desc),'ho')>0
--group by customer_key;

--drop table if exists ssharma_db.spring_agg_season;
--create table ssharma_db.spring_agg_season as 
--select customer_key,sum(sales_amt+discount_amt) as net_sales_season,sum(abs(coalesce(discount_amt,0)))/sum(sales_amt) as disc_p_season,
--sum(item_qty) as items_purch_season,
--count(distinct transaction_num) as num_txn_season,
--sum(sales_amt+discount_amt)/count(distinct transaction_num) as avg_ord_sz_season,
--sum(sales_amt+discount_amt)/sum(item_qty) as avg_unt_rtl_season,
--sum(item_qty)/count(distinct transaction_num) as unt_per_txn_season,
--sum(item_qty*(case when on_sale_flag='Y' then 1 else 0 end)) as on_sale_items_season,
--count(distinct category) as num_cat_season,
--DATEDIFF('2016-01-26',max(transaction_date)) as recency_season 
--from ssharma_db.spring_prod
--where instr(lower(season_desc),'sp')>0
--group by customer_key;

--drop table if exists ssharma_db.summer_agg_season;
--create table ssharma_db.summer_agg_season as 
--select customer_key,sum(sales_amt+discount_amt) as net_sales_season,sum(abs(coalesce(discount_amt,0)))/sum(sales_amt) as disc_p_season,
--sum(item_qty) as items_purch_season,
--count(distinct transaction_num) as num_txn_season,
--sum(sales_amt+discount_amt)/count(distinct transaction_num) as avg_ord_sz_season,
--sum(sales_amt+discount_amt)/sum(item_qty) as avg_unt_rtl_season,
--sum(item_qty)/count(distinct transaction_num) as unt_per_txn_season,
--sum(item_qty*(case when on_sale_flag='Y' then 1 else 0 end)) as on_sale_items_season,
--count(distinct category) as num_cat_season,
--DATEDIFF('2016-04-26',max(transaction_date)) as recency_season 
--from ssharma_db.summer_prod
--where instr(lower(season_desc),'su')>0
--group by customer_key;

drop table if exists ssharma_db.fall_cat;
create table ssharma_db.fall_cat as 
select customer_key,count(distinct category) as num_cat 
from ssharma_db.fall_prod 
group by customer_key;

drop table if exists ssharma_db.winter_cat;
create table ssharma_db.winter_cat as 
select customer_key,count(distinct category) as num_cat 
from ssharma_db.winter_prod 
group by customer_key;

drop table if exists ssharma_db.spring_cat;
create table ssharma_db.spring_cat as 
select customer_key,count(distinct category) as num_cat 
from ssharma_db.spring_prod 
group by customer_key;

drop table if exists ssharma_db.summer_cat;
create table ssharma_db.summer_cat as 
select customer_key,count(distinct category) as num_cat 
from ssharma_db.summer_prod 
group by customer_key;

--drop table if exists ssharma_db.fall_cat_season;
--create table ssharma_db.fall_cat_season as 
--select customer_key,count(distinct category) as num_cat_season 
--from ssharma_db.fall_prod 
--where instr(lower(season_desc),'fa')>0
--group by customer_key;

--drop table if exists ssharma_db.winter_cat_season;
--create table ssharma_db.winter_cat_season as 
--select customer_key,count(distinct category) as num_cat_season  
--from ssharma_db.winter_prod 
--where instr(lower(season_desc),'ho')>0
--group by customer_key;

--drop table if exists ssharma_db.spring_cat_season;
--create table ssharma_db.spring_cat_season as 
--select customer_key,count(distinct category) as num_cat_season  
--from ssharma_db.spring_prod 
--where instr(lower(season_desc),'sp')>0
--group by customer_key;

--drop table if exists ssharma_db.summer_cat_season;
--create table ssharma_db.summer_cat_season as 
--select customer_key,count(distinct category) as num_cat_season  
--from ssharma_db.summer_prod 
--where instr(lower(season_desc),'su')>0
--group by customer_key;

--DROP TABLE IF EXISTS ssharma_db.fall_emailclicks;
--CREATE TABLE ssharma_db.fall_emailclicks
--AS SELECT T1.CUSTOMER_KEY, COUNT(DISTINCT T3.URL_KEY) AS EMAILS_CLICKED
--FROM 
--ssharma_db.fall_customer T1
--INNER JOIN
--(
--SELECT DISTINCT CUSTOMER_KEY, CELL_KEY, URL_KEY, CLICK_DATE, COUNTRY, BRAND
--FROM
--MDS_NEW.ODS_EMAILCLICKS_T
--) T3
--ON T1.CUSTOMER_KEY = T3.CUSTOMER_KEY
--INNER JOIN MDS_NEW.ODS_CELL_T T4
--ON T3.CELL_KEY = T4.CELL_KEY
--WHERE
--UNIX_TIMESTAMP(TO_DATE(T3.CLICK_DATE),'yyyy-MM-dd')
--BETWEEN
--UNIX_TIMESTAMP('2015-08-02', 'yyyy-MM-dd') AND UNIX_TIMESTAMP('2016-08-01', 'yyyy-MM-dd')
--AND T3.COUNTRY='US' AND T3.BRAND='AT' AND
--T4.CAMPAIGN_SUBJECT != 'T'
--GROUP BY T1.CUSTOMER_KEY;

--DROP TABLE IF EXISTS ssharma_db.winter_emailclicks;
--CREATE TABLE ssharma_db.winter_emailclicks
--AS SELECT T1.CUSTOMER_KEY, COUNT(DISTINCT T3.URL_KEY) AS EMAILS_CLICKED
--FROM 
--ssharma_db.winter_customer T1
--INNER JOIN
--(
--SELECT DISTINCT CUSTOMER_KEY, CELL_KEY, URL_KEY, CLICK_DATE, COUNTRY, BRAND
--FROM
--MDS_NEW.ODS_EMAILCLICKS_T
--) T3
--ON T1.CUSTOMER_KEY = T3.CUSTOMER_KEY
--INNER JOIN MDS_NEW.ODS_CELL_T T4
--ON T3.CELL_KEY = T4.CELL_KEY
--WHERE
--UNIX_TIMESTAMP(TO_DATE(T3.CLICK_DATE),'yyyy-MM-dd')
--BETWEEN
--UNIX_TIMESTAMP('2014-11-10', 'yyyy-MM-dd') AND UNIX_TIMESTAMP('2015-11-09', 'yyyy-MM-dd')
--AND T3.COUNTRY='US' AND T3.BRAND='AT' AND
--T4.CAMPAIGN_SUBJECT != 'T'
--GROUP BY T1.CUSTOMER_KEY;

--DROP TABLE IF EXISTS ssharma_db.spring_emailclicks;
--CREATE TABLE ssharma_db.spring_emailclicks
--AS SELECT T1.CUSTOMER_KEY, COUNT(DISTINCT T3.URL_KEY) AS EMAILS_CLICKED
--FROM 
--ssharma_db.spring_customer T1
--INNER JOIN
--(
--SELECT DISTINCT CUSTOMER_KEY, CELL_KEY, URL_KEY, CLICK_DATE, COUNTRY, BRAND
--FROM
--MDS_NEW.ODS_EMAILCLICKS_T
--) T3
--ON T1.CUSTOMER_KEY = T3.CUSTOMER_KEY
--INNER JOIN MDS_NEW.ODS_CELL_T T4
--ON T3.CELL_KEY = T4.CELL_KEY
--WHERE
--UNIX_TIMESTAMP(TO_DATE(T3.CLICK_DATE),'yyyy-MM-dd')
--BETWEEN
--UNIX_TIMESTAMP('2015-01-26', 'yyyy-MM-dd') AND UNIX_TIMESTAMP('2016-01-25', 'yyyy-MM-dd')
--AND T3.COUNTRY='US' AND T3.BRAND='AT' AND
--T4.CAMPAIGN_SUBJECT != 'T'
--GROUP BY T1.CUSTOMER_KEY;
 
--DROP TABLE IF EXISTS ssharma_db.summer_emailclicks;
--CREATE TABLE ssharma_db.summer_emailclicks
--AS SELECT T1.CUSTOMER_KEY, COUNT(DISTINCT T3.URL_KEY) AS EMAILS_CLICKED
--FROM 
--ssharma_db.summer_customer T1
--INNER JOIN
--(
--SELECT DISTINCT CUSTOMER_KEY, CELL_KEY, URL_KEY, CLICK_DATE, COUNTRY, BRAND
--FROM
--MDS_NEW.ODS_EMAILCLICKS_T
--) T3
--ON T1.CUSTOMER_KEY = T3.CUSTOMER_KEY
--INNER JOIN MDS_NEW.ODS_CELL_T T4
--ON T3.CELL_KEY = T4.CELL_KEY
--WHERE
--UNIX_TIMESTAMP(TO_DATE(T3.CLICK_DATE),'yyyy-MM-dd')
--BETWEEN
--UNIX_TIMESTAMP('2015-04-26', 'yyyy-MM-dd') AND UNIX_TIMESTAMP('2016-04-25', 'yyyy-MM-dd')
--AND T3.COUNTRY='US' AND T3.BRAND='AT' AND
--T4.CAMPAIGN_SUBJECT != 'T'
--GROUP BY T1.CUSTOMER_KEY;
 
--DROP TABLE IF EXISTS ssharma_db.fall_emailviews;
--CREATE TABLE ssharma_db.fall_emailviews
--AS SELECT T1.CUSTOMER_KEY, COUNT(DISTINCT T3.CELL_KEY) AS EMAILS_VIEWED
--FROM 
--ssharma_db.fall_customer T1
--INNER JOIN
--(
--SELECT DISTINCT CUSTOMER_KEY, CELL_KEY, EVENT_TYPE, EVENT_DATE, COUNTRY, BRAND
--FROM
--MDS_NEW.ODS_EMAILOPENS_T
--) T3
--ON T1.CUSTOMER_KEY = T3.CUSTOMER_KEY
--INNER JOIN
--MDS_NEW.ODS_CELL_T T4
--ON T3.CELL_KEY = T4.CELL_KEY
--WHERE
--UNIX_TIMESTAMP(TO_DATE(T3.EVENT_DATE),'yyyy-MM-dd')
--BETWEEN
--UNIX_TIMESTAMP('2015-08-02', 'yyyy-MM-dd') AND UNIX_TIMESTAMP('2016-08-01', 'yyyy-MM-dd')
--AND T3.COUNTRY='US' AND T3.BRAND='AT' AND T3.EVENT_TYPE='V' AND
--T4.CAMPAIGN_SUBJECT != 'T'
--GROUP BY T1.CUSTOMER_KEY; 

--DROP TABLE IF EXISTS ssharma_db.winter_emailviews;
--CREATE TABLE ssharma_db.winter_emailviews
--AS SELECT T1.CUSTOMER_KEY, COUNT(DISTINCT T3.CELL_KEY) AS EMAILS_VIEWED
--FROM 
--ssharma_db.winter_customer T1
--INNER JOIN
--(
--SELECT DISTINCT CUSTOMER_KEY, CELL_KEY, EVENT_TYPE, EVENT_DATE, COUNTRY, BRAND
--FROM
--MDS_NEW.ODS_EMAILOPENS_T
--) T3
--ON T1.CUSTOMER_KEY = T3.CUSTOMER_KEY
--INNER JOIN
--MDS_NEW.ODS_CELL_T T4
--ON T3.CELL_KEY = T4.CELL_KEY
--WHERE
--UNIX_TIMESTAMP(TO_DATE(T3.EVENT_DATE),'yyyy-MM-dd')
--BETWEEN
--UNIX_TIMESTAMP('2014-11-10', 'yyyy-MM-dd') AND UNIX_TIMESTAMP('2015-11-09', 'yyyy-MM-dd')
--AND T3.COUNTRY='US' AND T3.BRAND='AT' AND T3.EVENT_TYPE='V' AND
--T4.CAMPAIGN_SUBJECT != 'T'
--GROUP BY T1.CUSTOMER_KEY; 

--DROP TABLE IF EXISTS ssharma_db.spring_emailviews;
--CREATE TABLE ssharma_db.spring_emailviews
--AS SELECT T1.CUSTOMER_KEY, COUNT(DISTINCT T3.CELL_KEY) AS EMAILS_VIEWED
--FROM 
--ssharma_db.spring_customer T1
--INNER JOIN
--(
--SELECT DISTINCT CUSTOMER_KEY, CELL_KEY, EVENT_TYPE, EVENT_DATE, COUNTRY, BRAND
--FROM
--MDS_NEW.ODS_EMAILOPENS_T
--) T3
--ON T1.CUSTOMER_KEY = T3.CUSTOMER_KEY
--INNER JOIN
--MDS_NEW.ODS_CELL_T T4
--ON T3.CELL_KEY = T4.CELL_KEY
--WHERE
--UNIX_TIMESTAMP(TO_DATE(T3.EVENT_DATE),'yyyy-MM-dd')
--BETWEEN
--UNIX_TIMESTAMP('2015-01-26', 'yyyy-MM-dd') AND UNIX_TIMESTAMP('2016-01-25', 'yyyy-MM-dd')
--AND T3.COUNTRY='US' AND T3.BRAND='AT' AND T3.EVENT_TYPE='V' AND
--T4.CAMPAIGN_SUBJECT != 'T'
--GROUP BY T1.CUSTOMER_KEY; 

--DROP TABLE IF EXISTS ssharma_db.summer_emailviews;
--CREATE TABLE ssharma_db.summer_emailviews
--AS SELECT T1.CUSTOMER_KEY, COUNT(DISTINCT T3.CELL_KEY) AS EMAILS_VIEWED
--FROM 
--ssharma_db.summer_customer T1
--INNER JOIN
--(
--SELECT DISTINCT CUSTOMER_KEY, CELL_KEY, EVENT_TYPE, EVENT_DATE, COUNTRY, BRAND
--FROM
--MDS_NEW.ODS_EMAILOPENS_T
--) T3
--ON T1.CUSTOMER_KEY = T3.CUSTOMER_KEY
--INNER JOIN
--MDS_NEW.ODS_CELL_T T4
--ON T3.CELL_KEY = T4.CELL_KEY
--WHERE
--UNIX_TIMESTAMP(TO_DATE(T3.EVENT_DATE),'yyyy-MM-dd')
--BETWEEN
--UNIX_TIMESTAMP('2015-04-26', 'yyyy-MM-dd') AND UNIX_TIMESTAMP('2016-04-25', 'yyyy-MM-dd')
--AND T3.COUNTRY='US' AND T3.BRAND='AT' AND T3.EVENT_TYPE='V' AND
--T4.CAMPAIGN_SUBJECT != 'T'
--GROUP BY T1.CUSTOMER_KEY; 

drop table if exists ssharma_db.fall_browse;
create table ssharma_db.fall_browse as 
select distinct
unk_shop_id,
visit_num,
visid_high,
visid_low,
pagename,
hier1,
prop1,
purchaseid,
prop33,
country,
t_time_info,
product_list,
date_time 
from omniture.hit_data_t 
where (instr(pagename,'at:browse')>0 or instr(pagename,'mobile:at')>0) and 
UNIX_TIMESTAMP(date_time,'yyyy-MM-dd') BETWEEN
UNIX_TIMESTAMP('2016-05-02', 'yyyy-MM-dd') AND 
UNIX_TIMESTAMP('2016-08-01', 'yyyy-MM-dd');

drop table if exists ssharma_db.winter_browse_1;
create table ssharma_db.winter_browse_1 as 
select distinct
unk_shop_id,
visit_num,
visid_high,
visid_low,
pagename,
hier1,
prop1,
purchaseid,
prop33,
country,
t_time_info,
product_list,
date_time 
from omniture.hit_data_t_history 
where (instr(pagename,'at:browse')>0 or instr(pagename,'mobile:at')>0) and 
UNIX_TIMESTAMP(date_time,'yyyy-MM-dd') BETWEEN
UNIX_TIMESTAMP('2015-10-05', 'yyyy-MM-dd') AND 
UNIX_TIMESTAMP('2015-12-31', 'yyyy-MM-dd');

drop table if exists ssharma_db.winter_browse_2;
create table ssharma_db.winter_browse_2 as 
select distinct
unk_shop_id,
visit_num,
visid_high,
visid_low,
pagename,
hier1,
prop1,
purchaseid,
prop33,
country,
t_time_info,
product_list,
date_time 
from omniture.hit_data_t 
where (instr(pagename,'at:browse')>0 or instr(pagename,'mobile:at')>0) and 
UNIX_TIMESTAMP(date_time,'yyyy-MM-dd') BETWEEN
UNIX_TIMESTAMP('2016-01-01', 'yyyy-MM-dd') AND 
UNIX_TIMESTAMP('2016-01-04', 'yyyy-MM-dd');

drop table if exists ssharma_db.winter_browse;
create table ssharma_db.winter_browse as 
select distinct
a.unk_shop_id,
a.visit_num,
a.visid_high,
a.visid_low,
a.pagename,
a.hier1,
a.prop1,
a.purchaseid,
a.prop33,
a.country,
a.t_time_info,
a.product_list,
a.date_time 
from 
(select distinct
unk_shop_id,
visit_num,
visid_high,
visid_low,
pagename,
hier1,
prop1,
purchaseid,
prop33,
country,
t_time_info,
product_list,
date_time 
from ssharma_db.winter_browse_1
union all
select distinct
unk_shop_id,
visit_num,
visid_high,
visid_low,
pagename,
hier1,
prop1,
purchaseid,
prop33,
country,
t_time_info,
product_list,
date_time 
from ssharma_db.winter_browse_2) a;

drop table if exists ssharma_db.spring_browse_1;
create table ssharma_db.spring_browse_1 as 
select distinct
unk_shop_id,
visit_num,
visid_high,
visid_low,
pagename,
hier1,
prop1,
purchaseid,
prop33,
country,
t_time_info,
product_list,
date_time 
from omniture.hit_data_t_history 
where (instr(pagename,'at:browse')>0 or instr(pagename,'mobile:at')>0) and 
UNIX_TIMESTAMP(date_time,'yyyy-MM-dd') BETWEEN
UNIX_TIMESTAMP('2015-10-26', 'yyyy-MM-dd') AND 
UNIX_TIMESTAMP('2015-12-31', 'yyyy-MM-dd');

drop table if exists ssharma_db.spring_browse_2;
create table ssharma_db.spring_browse_2 as 
select distinct
unk_shop_id,
visit_num,
visid_high,
visid_low,
pagename,
hier1,
prop1,
purchaseid,
prop33,
country,
t_time_info,
product_list,
date_time 
from omniture.hit_data_t 
where (instr(pagename,'at:browse')>0 or instr(pagename,'mobile:at')>0) and 
UNIX_TIMESTAMP(date_time,'yyyy-MM-dd') BETWEEN
UNIX_TIMESTAMP('2016-01-01', 'yyyy-MM-dd') AND 
UNIX_TIMESTAMP('2016-01-25', 'yyyy-MM-dd');

drop table if exists ssharma_db.spring_browse;
create table ssharma_db.spring_browse as 
select distinct
a.unk_shop_id,
a.visit_num,
a.visid_high,
a.visid_low,
a.pagename,
a.hier1,
a.prop1,
a.purchaseid,
a.prop33,
a.country,
a.t_time_info,
a.product_list,
a.date_time 
from 
(select distinct
unk_shop_id,
visit_num,
visid_high,
visid_low,
pagename,
hier1,
prop1,
purchaseid,
prop33,
country,
t_time_info,
product_list,
date_time 
from ssharma_db.spring_browse_1
union all
select distinct
unk_shop_id,
visit_num,
visid_high,
visid_low,
pagename,
hier1,
prop1,
purchaseid,
prop33,
country,
t_time_info,
product_list,
date_time 
from ssharma_db.spring_browse_2) a;


drop table if exists ssharma_db.summer_browse;
create table ssharma_db.summer_browse as 
select distinct
unk_shop_id,
visit_num,
visid_high,
visid_low,
pagename,
hier1,
prop1,
purchaseid,
prop33,
country,
t_time_info,
product_list,
date_time 
from omniture.hit_data_t 
where (instr(pagename,'at:browse')>0 or instr(pagename,'mobile:at')>0) and 
UNIX_TIMESTAMP(date_time,'yyyy-MM-dd') BETWEEN
UNIX_TIMESTAMP('2016-01-26', 'yyyy-MM-dd') AND 
UNIX_TIMESTAMP('2016-04-25', 'yyyy-MM-dd');

drop table if exists ssharma_db.fall_clicks;
create table ssharma_db.fall_clicks as 
select *,substr(prop1,1,6) as style_cd from ssharma_db.fall_browse
where prop33='product';

--drop table if exists ssharma_db.fall_clicks_desc;
--create table ssharma_db.fall_clicks_desc as 
--select a.*,b.season_desc
--from ssharma_db.fall_clicks a 
--inner join (select distinct style_cd,season_desc from mds_new.ods_product_t where instr(lower(season_desc),'fa')>0  and mdse_corp_desc='ATH DIR') b 
--on a.style_cd=b.style_cd
--;

drop table if exists ssharma_db.fall_clicks_desc_total;
create table ssharma_db.fall_clicks_desc_total as 
select a.*,b.season_desc
from ssharma_db.fall_clicks a 
inner join (select distinct style_cd,season_desc from mds_new.ods_product_t where  mdse_corp_desc='ATH DIR') b 
on a.style_cd=b.style_cd
;

drop table if exists ssharma_db.winter_clicks;
create table ssharma_db.winter_clicks as 
select *,substr(prop1,1,6) as style_cd from ssharma_db.winter_browse
where prop33='product';

--drop table if exists ssharma_db.winter_clicks_desc;
--create table ssharma_db.winter_clicks_desc as 
--select a.*,b.season_desc
--from ssharma_db.winter_clicks a 
--inner join (select distinct style_cd,season_desc from mds_new.ods_product_t where instr(lower(season_desc),'ho')>0  and mdse_corp_desc='ATH DIR') b 
--on a.style_cd=b.style_cd
--;

drop table if exists ssharma_db.winter_clicks_desc_total;
create table ssharma_db.winter_clicks_desc_total as 
select a.*,b.season_desc
from ssharma_db.winter_clicks a 
inner join (select distinct style_cd,season_desc from mds_new.ods_product_t where  mdse_corp_desc='ATH DIR') b 
on a.style_cd=b.style_cd
;

drop table if exists ssharma_db.spring_clicks;
create table ssharma_db.spring_clicks as 
select *,substr(prop1,1,6) as style_cd from ssharma_db.spring_browse
where prop33='product';

--drop table if exists ssharma_db.spring_clicks_desc;
--create table ssharma_db.spring_clicks_desc as 
--select a.*,b.season_desc
--from ssharma_db.spring_clicks a 
--inner join (select distinct style_cd,season_desc from mds_new.ods_product_t where instr(lower(season_desc),'sp')>0  and mdse_corp_desc='ATH DIR') b 
--on a.style_cd=b.style_cd
--;
drop table if exists ssharma_db.spring_clicks_desc_total;
create table ssharma_db.spring_clicks_desc_total as 
select a.*,b.season_desc
from ssharma_db.spring_clicks a 
inner join (select distinct style_cd,season_desc from mds_new.ods_product_t where  mdse_corp_desc='ATH DIR') b 
on a.style_cd=b.style_cd
;



drop table if exists ssharma_db.summer_clicks;
create table ssharma_db.summer_clicks as 
select *,substr(prop1,1,6) as style_cd from ssharma_db.summer_browse
where prop33='product';

--drop table if exists ssharma_db.summer_clicks_desc;
--create table ssharma_db.summer_clicks_desc as 
--select a.*,b.season_desc
--from ssharma_db.summer_clicks a 
--inner join (select distinct style_cd,season_desc from mds_new.ods_product_t where instr(lower(season_desc),'su')>0  and mdse_corp_desc='ATH DIR') b 
--on a.style_cd=b.style_cd
--;

drop table if exists ssharma_db.summer_clicks_desc_total;
create table ssharma_db.summer_clicks_desc_total as 
select a.*,b.season_desc
from ssharma_db.summer_clicks a 
inner join (select distinct style_cd,season_desc from mds_new.ods_product_t where mdse_corp_desc='ATH DIR') b 
on a.style_cd=b.style_cd
;

drop table if exists ssharma_db.fall_bagadd;
create table ssharma_db.fall_bagadd as 
select distinct
unk_shop_id,
visit_num,
visid_high,
visid_low,
pagename,
hier1,
prop1,
purchaseid,
prop33,
country,
t_time_info,
product_list 
from ssharma_db.fall_browse
where  prop33='inlineBagAdd';

drop table if exists ssharma_db.fall_purchasenull;
create table ssharma_db.fall_purchasenull as 
select unk_shop_id,visit_num
from ssharma_db.fall_browse
where unk_shop_id != "" and (PurchaseID IS NULL or PurchaseID=='' or PurchaseID==' ');

drop table if exists ssharma_db.fall_abandon_basket;
create table ssharma_db.fall_abandon_basket  as 
select a.* 
from ssharma_db.fall_bagadd a 
inner join  ssharma_db.fall_purchasenull b 
on a.unk_shop_id=b.unk_shop_id and a.visit_num=b.visit_num;

drop table if exists ssharma_db.winter_bagadd;
create table ssharma_db.winter_bagadd as 
select distinct
unk_shop_id,
visit_num,
visid_high,
visid_low,
pagename,
hier1,
prop1,
purchaseid,
prop33,
country,
t_time_info,
product_list 
from ssharma_db.winter_browse
where  prop33='inlineBagAdd';

drop table if exists ssharma_db.winter_purchasenull;
create table ssharma_db.winter_purchasenull as 
select unk_shop_id,visit_num
from ssharma_db.winter_browse
where unk_shop_id != "" and (PurchaseID IS NULL or PurchaseID=='' or PurchaseID==' ');

drop table if exists ssharma_db.winter_abandon_basket;
create table ssharma_db.winter_abandon_basket  as 
select a.* 
from ssharma_db.winter_bagadd a 
inner join  ssharma_db.winter_purchasenull b 
on a.unk_shop_id=b.unk_shop_id and a.visit_num=b.visit_num;

drop table if exists ssharma_db.spring_bagadd;
create table ssharma_db.spring_bagadd as 
select distinct
unk_shop_id,
visit_num,
visid_high,
visid_low,
pagename,
hier1,
prop1,
purchaseid,
prop33,
country,
t_time_info,
product_list 
from ssharma_db.spring_browse
where  prop33='inlineBagAdd';

drop table if exists ssharma_db.spring_purchasenull;
create table ssharma_db.spring_purchasenull as 
select unk_shop_id,visit_num
from ssharma_db.spring_browse
where unk_shop_id != "" and (PurchaseID IS NULL or PurchaseID=='' or PurchaseID==' ');

drop table if exists ssharma_db.spring_abandon_basket;
create table ssharma_db.spring_abandon_basket  as 
select a.* 
from ssharma_db.spring_bagadd a 
inner join  ssharma_db.spring_purchasenull b 
on a.unk_shop_id=b.unk_shop_id and a.visit_num=b.visit_num;

drop table if exists ssharma_db.summer_bagadd;
create table ssharma_db.summer_bagadd as 
select distinct
unk_shop_id,
visit_num,
visid_high,
visid_low,
pagename,
hier1,
prop1,
purchaseid,
prop33,
country,
t_time_info,
product_list 
from ssharma_db.summer_browse
where  prop33='inlineBagAdd';

drop table if exists ssharma_db.summer_purchasenull;
create table ssharma_db.summer_purchasenull as 
select unk_shop_id,visit_num
from ssharma_db.summer_browse
where unk_shop_id != "" and (PurchaseID IS NULL or PurchaseID=='' or PurchaseID==' ');

drop table if exists ssharma_db.summer_abandon_basket;
create table ssharma_db.summer_abandon_basket  as 
select a.* 
from ssharma_db.summer_bagadd a 
inner join  ssharma_db.summer_purchasenull b 
on a.unk_shop_id=b.unk_shop_id and a.visit_num=b.visit_num;

--drop table if exists ssharma_db.fall_abandon_basket_prod;
--create table ssharma_db.fall_abandon_basket_prod as 
--select a.*,b.season_desc
--from ssharma_db.fall_abandon_basket a 
--inner join (select distinct style_cd,season_desc from  mds_new.ods_product_t where instr(lower(season_desc),'fa')>0 and mdse_corp_desc='ATH DIR') b 
--on substr(a.prop1,1,6)=b.style_cd;

--drop table if exists ssharma_db.winter_abandon_basket_prod;
--create table ssharma_db.winter_abandon_basket_prod as 
--select a.*,b.season_desc
--from ssharma_db.winter_abandon_basket a 
--inner join (select distinct style_cd,season_desc from  mds_new.ods_product_t where instr(lower(season_desc),'ho')>0 and mdse_corp_desc='ATH DIR') b 
--on substr(a.prop1,1,6)=b.style_cd;

--drop table if exists ssharma_db.spring_abandon_basket_prod;
--create table ssharma_db.spring_abandon_basket_prod as 
--select a.*,b.season_desc
--from ssharma_db.spring_abandon_basket a 
--inner join (select distinct style_cd,season_desc from  mds_new.ods_product_t where instr(lower(season_desc),'sp')>0 and mdse_corp_desc='ATH DIR') b 
--on substr(a.prop1,1,6)=b.style_cd;

--drop table if exists ssharma_db.summer_abandon_basket_prod;
--create table ssharma_db.summer_abandon_basket_prod as 
--select a.*,b.season_desc
--from ssharma_db.summer_abandon_basket a 
--inner join (select distinct style_cd,season_desc from  mds_new.ods_product_t where instr(lower(season_desc),'su')>0 and mdse_corp_desc='ATH DIR') b 
--on substr(a.prop1,1,6)=b.style_cd;

drop table if exists ssharma_db.fall_abandon_basket_prod_total;
create table ssharma_db.fall_abandon_basket_prod_total as 
select a.*,b.season_desc
from ssharma_db.fall_abandon_basket a 
inner join (select distinct style_cd,season_desc from  mds_new.ods_product_t where mdse_corp_desc='ATH DIR') b 
on substr(a.prop1,1,6)=b.style_cd;

drop table if exists ssharma_db.winter_abandon_basket_prod_total;
create table ssharma_db.winter_abandon_basket_prod_total as 
select a.*,b.season_desc
from ssharma_db.winter_abandon_basket a 
inner join (select distinct style_cd,season_desc from  mds_new.ods_product_t where  mdse_corp_desc='ATH DIR') b 
on substr(a.prop1,1,6)=b.style_cd;

drop table if exists ssharma_db.spring_abandon_basket_prod_total;
create table ssharma_db.spring_abandon_basket_prod_total as 
select a.*,b.season_desc
from ssharma_db.spring_abandon_basket a 
inner join (select distinct style_cd,season_desc from  mds_new.ods_product_t where   mdse_corp_desc='ATH DIR') b 
on substr(a.prop1,1,6)=b.style_cd;

drop table if exists ssharma_db.summer_abandon_basket_prod_total;
create table ssharma_db.summer_abandon_basket_prod_total as 
select a.*,b.season_desc
from ssharma_db.summer_abandon_basket a 
inner join (select distinct style_cd,season_desc from  mds_new.ods_product_t where  mdse_corp_desc='ATH DIR') b 
on substr(a.prop1,1,6)=b.style_cd;

--drop table if exists ssharma_db.fall_clicks_cat_master;
--create table ssharma_db.fall_clicks_cat_master as 
--select a.*,b.customerkey
--from ssharma_db.fall_clicks_desc a 
--left outer join 
--(select distinct unknownshopperid,customerkey from crmanalytics.customerresolution) b 
--on a.unk_shop_id=b.unknownshopperid;

--drop table if exists ssharma_db.fall_abandon_basket_prod_master;
--create table ssharma_db.fall_abandon_basket_prod_master as 
--select a.*,b.customerkey
--from  ssharma_db.fall_abandon_basket_prod a 
--left outer join 
--(select distinct unknownshopperid,customerkey from crmanalytics.customerresolution) b 
--on a.unk_shop_id=b.unknownshopperid;

--drop table if exists ssharma_db.winter_clicks_cat_master;
--create table ssharma_db.winter_clicks_cat_master as 
--select a.*,b.customerkey
--from ssharma_db.winter_clicks_desc a 
--left outer join 
--(select distinct unknownshopperid,customerkey from crmanalytics.customerresolution) b 
--on a.unk_shop_id=b.unknownshopperid;

--drop table if exists ssharma_db.winter_abandon_basket_prod_master;
--create table ssharma_db.winter_abandon_basket_prod_master as 
--select a.*,b.customerkey
--from  ssharma_db.winter_abandon_basket_prod a 
--left outer join 
--(select distinct unknownshopperid,customerkey from crmanalytics.customerresolution) b 
--on a.unk_shop_id=b.unknownshopperid;

--drop table if exists ssharma_db.spring_clicks_cat_master;
--create table ssharma_db.spring_clicks_cat_master as 
--select a.*,b.customerkey
--from ssharma_db.spring_clicks_desc a 
--left outer join 
--(select distinct unknownshopperid,customerkey from crmanalytics.customerresolution) b 
--on a.unk_shop_id=b.unknownshopperid;

--drop table if exists ssharma_db.spring_abandon_basket_prod_master;
--create table ssharma_db.spring_abandon_basket_prod_master as 
--select a.*,b.customerkey
--from  ssharma_db.spring_abandon_basket_prod a 
--left outer join 
--(select distinct unknownshopperid,customerkey from crmanalytics.customerresolution) b 
--on a.unk_shop_id=b.unknownshopperid;

--drop table if exists ssharma_db.summer_clicks_cat_master;
--create table ssharma_db.summer_clicks_cat_master as 
--select a.*,b.customerkey
--from ssharma_db.summer_clicks_desc  a 
--left outer join 
--(select distinct unknownshopperid,customerkey from crmanalytics.customerresolution) b 
--on a.unk_shop_id=b.unknownshopperid;

--drop table if exists ssharma_db.summer_abandon_basket_prod_master;
--create table ssharma_db.summer_abandon_basket_prod_master as 
--select a.*,b.customerkey
--from  ssharma_db.summer_abandon_basket_prod a 
--left outer join 
--(select distinct unknownshopperid,customerkey from crmanalytics.customerresolution) b 
--on a.unk_shop_id=b.unknownshopperid;


drop table if exists ssharma_db.fall_clicks_cat_master_total;
create table ssharma_db.fall_clicks_cat_master_total as 
select a.*,b.customerkey
from ssharma_db.fall_clicks_desc_total a 
left outer join 
(select distinct unknownshopperid,customerkey from crmanalytics.customerresolution) b 
on a.unk_shop_id=b.unknownshopperid;

drop table if exists ssharma_db.fall_abandon_basket_prod_master_total;
create table ssharma_db.fall_abandon_basket_prod_master_total as 
select a.*,b.customerkey
from  ssharma_db.fall_abandon_basket_prod_total a 
left outer join 
(select distinct unknownshopperid,customerkey from crmanalytics.customerresolution) b 
on a.unk_shop_id=b.unknownshopperid;

drop table if exists ssharma_db.winter_clicks_cat_master_total;
create table ssharma_db.winter_clicks_cat_master_total as 
select a.*,b.customerkey
from ssharma_db.winter_clicks_desc_total a 
left outer join 
(select distinct unknownshopperid,customerkey from crmanalytics.customerresolution) b 
on a.unk_shop_id=b.unknownshopperid;

drop table if exists ssharma_db.winter_abandon_basket_prod_master_total;
create table ssharma_db.winter_abandon_basket_prod_master_total as 
select a.*,b.customerkey
from  ssharma_db.winter_abandon_basket_prod_total a 
left outer join 
(select distinct unknownshopperid,customerkey from crmanalytics.customerresolution) b 
on a.unk_shop_id=b.unknownshopperid;

drop table if exists ssharma_db.spring_clicks_cat_master_total;
create table ssharma_db.spring_clicks_cat_master_total as 
select a.*,b.customerkey
from ssharma_db.spring_clicks_desc_total a 
left outer join 
(select distinct unknownshopperid,customerkey from crmanalytics.customerresolution) b 
on a.unk_shop_id=b.unknownshopperid;

drop table if exists ssharma_db.spring_abandon_basket_prod_master_total;
create table ssharma_db.spring_abandon_basket_prod_master_total as 
select a.*,b.customerkey
from  ssharma_db.spring_abandon_basket_prod_total a 
left outer join 
(select distinct unknownshopperid,customerkey from crmanalytics.customerresolution) b 
on a.unk_shop_id=b.unknownshopperid;

drop table if exists ssharma_db.summer_clicks_cat_master_total;
create table ssharma_db.summer_clicks_cat_master_total as 
select a.*,b.customerkey
from ssharma_db.summer_clicks_desc_total  a 
left outer join 
(select distinct unknownshopperid,customerkey from crmanalytics.customerresolution) b 
on a.unk_shop_id=b.unknownshopperid;

drop table if exists ssharma_db.summer_abandon_basket_prod_master_total;
create table ssharma_db.summer_abandon_basket_prod_master_total as 
select a.*,b.customerkey
from  ssharma_db.summer_abandon_basket_prod_total a 
left outer join 
(select distinct unknownshopperid,customerkey from crmanalytics.customerresolution) b 
on a.unk_shop_id=b.unknownshopperid;


--drop table if exists ssharma_db.fall_abandon_basket_prod_master_notnull;
--create table ssharma_db.fall_abandon_basket_prod_master_notnull as
--select * from  ssharma_db.fall_abandon_basket_prod_master where customerkey is not null and instr(lower(season_desc),'fa')>0;

--drop table if exists ssharma_db.fall_clicks_cat_master_notnull;
--create table ssharma_db.fall_clicks_cat_master_notnull as
--select * from  ssharma_db.fall_clicks_cat_master where customerkey is not null and instr(lower(season_desc),'fa')>0;

--drop table if exists ssharma_db.winter_abandon_basket_prod_master_notnull;
--create table ssharma_db.winter_abandon_basket_prod_master_notnull as
--select * from  ssharma_db.winter_abandon_basket_prod_master where customerkey is not null and instr(lower(season_desc),'ho')>0;

--drop table if exists ssharma_db.winter_clicks_cat_master_notnull;
--create table ssharma_db.winter_clicks_cat_master_notnull as
--select * from  ssharma_db.winter_clicks_cat_master where customerkey is not null and instr(lower(season_desc),'ho')>0;

--drop table if exists ssharma_db.spring_abandon_basket_prod_master_notnull;
--create table ssharma_db.spring_abandon_basket_prod_master_notnull as
--select * from  ssharma_db.spring_abandon_basket_prod_master where customerkey is not null and instr(lower(season_desc),'sp')>0;

--drop table if exists ssharma_db.spring_clicks_cat_master_notnull;
--create table ssharma_db.spring_clicks_cat_master_notnull as
--select * from  ssharma_db.spring_clicks_cat_master where customerkey is not null and instr(lower(season_desc),'sp')>0;

--drop table if exists ssharma_db.summer_abandon_basket_prod_master_notnull;
--create table ssharma_db.summer_abandon_basket_prod_master_notnull as
--select * from  ssharma_db.summer_abandon_basket_prod_master where customerkey is not null and instr(lower(season_desc),'su')>0;

--drop table if exists ssharma_db.summer_clicks_cat_master_notnull;
--create table ssharma_db.summer_clicks_cat_master_notnull as
--select * from  ssharma_db.summer_clicks_cat_master where customerkey is not null and instr(lower(season_desc),'su')>0;

drop table if exists ssharma_db.fall_abandon_basket_prod_master_notnull_total;
create table ssharma_db.fall_abandon_basket_prod_master_notnull_total as
select * from  ssharma_db.fall_abandon_basket_prod_master_total where customerkey is not null ;

drop table if exists ssharma_db.fall_clicks_cat_master_notnull_total;
create table ssharma_db.fall_clicks_cat_master_notnull_total as
select * from  ssharma_db.fall_clicks_cat_master_total where customerkey is not null ;

drop table if exists ssharma_db.winter_abandon_basket_prod_master_notnull_total;
create table ssharma_db.winter_abandon_basket_prod_master_notnull_total as
select * from  ssharma_db.winter_abandon_basket_prod_master_total where customerkey is not null ;

drop table if exists ssharma_db.winter_clicks_cat_master_notnull_total;
create table ssharma_db.winter_clicks_cat_master_notnull_total as
select * from  ssharma_db.winter_clicks_cat_master_total where customerkey is not null ;

drop table if exists ssharma_db.spring_abandon_basket_prod_master_notnull_total;
create table ssharma_db.spring_abandon_basket_prod_master_notnull_total as
select * from  ssharma_db.spring_abandon_basket_prod_master_total where customerkey is not null ;

drop table if exists ssharma_db.spring_clicks_cat_master_notnull_total;
create table ssharma_db.spring_clicks_cat_master_notnull_total as
select * from  ssharma_db.spring_clicks_cat_master_total where customerkey is not null ;

drop table if exists ssharma_db.summer_abandon_basket_prod_master_notnull_total;
create table ssharma_db.summer_abandon_basket_prod_master_notnull_total as
select * from  ssharma_db.summer_abandon_basket_prod_master_total where customerkey is not null ;

drop table if exists ssharma_db.summer_clicks_cat_master_notnull_total;
create table ssharma_db.summer_clicks_cat_master_notnull_total as
select * from  ssharma_db.summer_clicks_cat_master_total where customerkey is not null ;

--drop table if exists ssharma_db.fall_abandon_mk_cat;
--create table ssharma_db.fall_abandon_mk_cat as 
--select customerkey,count(distinct t_time_info) as items_abandoned
--from ssharma_db.fall_abandon_basket_prod_master_notnull
--group by customerkey;

--drop table if exists ssharma_db.fall_clicks_mk_cat;
--create table ssharma_db.fall_clicks_mk_cat as 
--select customerkey,count(distinct t_time_info) as items_browsed
--from ssharma_db.fall_clicks_cat_master_notnull
--group by customerkey;

--drop table if exists ssharma_db.winter_abandon_mk_cat;
--create table ssharma_db.winter_abandon_mk_cat as 
--select customerkey,count(distinct t_time_info) as items_abandoned
--from ssharma_db.winter_abandon_basket_prod_master_notnull
--group by customerkey;

--drop table if exists ssharma_db.winter_clicks_mk_cat;
--create table ssharma_db.winter_clicks_mk_cat as 
--select customerkey,count(distinct t_time_info) as items_browsed
--from ssharma_db.winter_clicks_cat_master_notnull
--group by customerkey;

--drop table if exists ssharma_db.spring_abandon_mk_cat;
--create table ssharma_db.spring_abandon_mk_cat as 
--select customerkey,count(distinct t_time_info) as items_abandoned
--from ssharma_db.spring_abandon_basket_prod_master_notnull
--group by customerkey;

--drop table if exists ssharma_db.spring_clicks_mk_cat;
--create table ssharma_db.spring_clicks_mk_cat as 
--select customerkey,count(distinct t_time_info) as items_browsed
--from ssharma_db.spring_clicks_cat_master_notnull
--group by customerkey;

--drop table if exists ssharma_db.summer_abandon_mk_cat;
--create table ssharma_db.summer_abandon_mk_cat as 
--select customerkey,count(distinct t_time_info) as items_abandoned
--from ssharma_db.summer_abandon_basket_prod_master_notnull
--group by customerkey;

--drop table if exists ssharma_db.summer_clicks_mk_cat;
--create table ssharma_db.summer_clicks_mk_cat as 
--select customerkey,count(distinct t_time_info) as items_browsed
--from ssharma_db.summer_clicks_cat_master_notnull
--group by customerkey;

drop table if exists ssharma_db.fall_abandon_mk_cat_total;
create table ssharma_db.fall_abandon_mk_cat_total as 
select customerkey,count(distinct t_time_info) as items_abandoned
from ssharma_db.fall_abandon_basket_prod_master_notnull_total
group by customerkey;

drop table if exists ssharma_db.fall_clicks_mk_cat_total;
create table ssharma_db.fall_clicks_mk_cat_total as 
select customerkey,count(distinct t_time_info) as items_browsed
from ssharma_db.fall_clicks_cat_master_notnull_total
group by customerkey;

drop table if exists ssharma_db.winter_abandon_mk_cat_total;
create table ssharma_db.winter_abandon_mk_cat_total as 
select customerkey,count(distinct t_time_info) as items_abandoned
from ssharma_db.winter_abandon_basket_prod_master_notnull_total
group by customerkey;

drop table if exists ssharma_db.winter_clicks_mk_cat_total;
create table ssharma_db.winter_clicks_mk_cat_total as 
select customerkey,count(distinct t_time_info) as items_browsed
from ssharma_db.winter_clicks_cat_master_notnull_total
group by customerkey;

drop table if exists ssharma_db.spring_abandon_mk_cat_total;
create table ssharma_db.spring_abandon_mk_cat_total as 
select customerkey,count(distinct t_time_info) as items_abandoned
from ssharma_db.spring_abandon_basket_prod_master_notnull_total
group by customerkey;

drop table if exists ssharma_db.spring_clicks_mk_cat_total;
create table ssharma_db.spring_clicks_mk_cat_total as 
select customerkey,count(distinct t_time_info) as items_browsed
from ssharma_db.spring_clicks_cat_master_notnull_total
group by customerkey;

drop table if exists ssharma_db.summer_abandon_mk_ca_total;
create table ssharma_db.summer_abandon_mk_cat_total as 
select customerkey,count(distinct t_time_info) as items_abandoned
from ssharma_db.summer_abandon_basket_prod_master_notnull_total
group by customerkey;

drop table if exists ssharma_db.summer_clicks_mk_cat_total;
create table ssharma_db.summer_clicks_mk_cat_total as 
select customerkey,count(distinct t_time_info) as items_browsed
from ssharma_db.summer_clicks_cat_master_notnull_total
group by customerkey;

drop table if exists ssharma_db.purch_fall_1_2016;
create table ssharma_db.purch_fall_1_2016 as 
select distinct a.customer_key,a.transaction_num,a.line_num,a.sales_amt,a.item_qty,a.product_key,a.transaction_date,a.on_sale_flag
from
(select distinct customer_key,transaction_num,line_num,sales_amt,discount_amt,item_qty,product_key,transaction_date,on_sale_flag from  mds_new.ods_orderline_t 
where brand='AT' and country='US' and item_qty > 0 and sales_amt>0 and transaction_date between '2016-08-02' and '2016-11-22') a 
inner join 
ssharma_db.fall_customer b 
on a.customer_key=b.customer_key;



drop table if exists ssharma_db.purch_spring_1_2016;
create table ssharma_db.purch_spring_1_2016 as 
select distinct a.customer_key,a.transaction_num,a.line_num,a.sales_amt,a.item_qty,a.product_key,a.transaction_date,a.on_sale_flag
from
(select distinct customer_key,transaction_num,line_num,sales_amt,discount_amt,item_qty,product_key,transaction_date,on_sale_flag from  mds_new.ods_orderline_t 
where brand='AT' and country='US' and item_qty > 0 and sales_amt>0 and transaction_date between '2016-01-26' and '2016-05-17') a 
inner join 
ssharma_db.spring_customer b 
on a.customer_key=b.customer_key;

drop table if exists ssharma_db.purch_summer_1_2016;
create table ssharma_db.purch_summer_1_2016 as 
select distinct a.customer_key,a.transaction_num,a.line_num,a.sales_amt,a.item_qty,a.product_key,a.transaction_date,a.on_sale_flag
from
(select distinct customer_key,transaction_num,line_num,sales_amt,discount_amt,item_qty,product_key,transaction_date,on_sale_flag from  mds_new.ods_orderline_t 
where brand='AT' and country='US' and item_qty > 0 and sales_amt>0 and transaction_date between '2016-04-26' and '2016-08-16') a 
inner join 
ssharma_db.summer_customer b 
on a.customer_key=b.customer_key;



drop table if exists ssharma_db.purch_winter_4_2015;
create table ssharma_db.purch_winter_4_2015 as 
select distinct a.customer_key,a.transaction_num,a.line_num,a.sales_amt,a.item_qty,a.product_key,a.transaction_date,a.on_sale_flag
from
(select distinct customer_key,transaction_num,line_num,sales_amt,discount_amt,item_qty,product_key,transaction_date,on_sale_flag from  mds_new.ods_orderline_t 
where brand='AT' and country='US' and item_qty > 0 and sales_amt>0 and transaction_date between '2016-01-05' and '2016-04-26') a 
inner join 
ssharma_db.winter_customer b 
on a.customer_key=b.customer_key;

drop table if exists ssharma_db.fall_resp;
create table ssharma_db.fall_resp as 
select distinct customer_key,
1 as purch
from ssharma_db.purch_fall_1_2016 ;

drop table if exists ssharma_db.winter_resp;
create table ssharma_db.winter_resp as 
select distinct customer_key,
1 as purch
from ssharma_db.purch_winter_4_2015 ;

drop table if exists ssharma_db.spring_resp;
create table ssharma_db.spring_resp as 
select distinct customer_key,
1 as purch
from ssharma_db.purch_spring_1_2016 a ;

drop table if exists ssharma_db.summer_resp;
create table ssharma_db.summer_resp as 
select distinct customer_key,1 as purch
from ssharma_db.purch_summer_1_2016;

drop table if exists ssharma_db.fall_sameprd;
create table ssharma_db.fall_sameprd as 
select distinct a.customer_key,a.transaction_num,a.line_num,a.sales_amt,a.item_qty,a.product_key,a.transaction_date,a.on_sale_flag
from
(select distinct customer_key,transaction_num,line_num,sales_amt,discount_amt,item_qty,product_key,transaction_date,on_sale_flag from  mds_new.ods_orderline_t 
where brand='AT' and country='US' and item_qty > 0 and sales_amt>0 and transaction_date between '2015-08-02' and '2015-11-01' and order_status in ('R','O')) a 
inner join 
ssharma_db.fall_customer b 
on a.customer_key=b.customer_key; 

drop table if exists ssharma_db.winter_sameprd;
create table ssharma_db.winter_sameprd as 
select distinct a.customer_key,a.transaction_num,a.line_num,a.sales_amt,a.item_qty,a.product_key,a.transaction_date,a.on_sale_flag
from
(select distinct customer_key,transaction_num,line_num,sales_amt,discount_amt,item_qty,product_key,transaction_date,on_sale_flag from  mds_new.ods_orderline_t 
where brand='AT' and country='US' and item_qty > 0 and sales_amt>0 and transaction_date between '2015-01-05' and '2015-04-05' and order_status in ('R','O')) a 
inner join 
ssharma_db.winter_customer b 
on a.customer_key=b.customer_key;

drop table if exists ssharma_db.spring_sameprd;
create table ssharma_db.spring_sameprd as 
select distinct a.customer_key,a.transaction_num,a.line_num,a.sales_amt,a.item_qty,a.product_key,a.transaction_date,a.on_sale_flag
from
(select distinct customer_key,transaction_num,line_num,sales_amt,discount_amt,item_qty,product_key,transaction_date,on_sale_flag from  mds_new.ods_orderline_t 
where brand='AT' and country='US' and item_qty > 0 and sales_amt>0 and transaction_date between '2015-01-26' and '2015-04-25' and order_status in ('R','O')) a 
inner join 
ssharma_db.spring_customer b 
on a.customer_key=b.customer_key;

drop table if exists ssharma_db.summer_sameprd;
create table ssharma_db.summer_sameprd as 
select distinct a.customer_key,a.transaction_num,a.line_num,a.sales_amt,a.item_qty,a.product_key,a.transaction_date,a.on_sale_flag
from
(select distinct customer_key,transaction_num,line_num,sales_amt,discount_amt,item_qty,product_key,transaction_date,on_sale_flag from  mds_new.ods_orderline_t 
where brand='AT' and country='US' and item_qty > 0 and sales_amt>0 and transaction_date between '2015-04-26' and '2015-07-25' and order_status in ('R','O')) a 
inner join 
ssharma_db.summer_customer b 
on a.customer_key=b.customer_key;

drop table if exists ssharma_db.fall_sameprd_disc;
create table ssharma_db.fall_sameprd_disc as 
select a.customer_key, 
a.transaction_num,
a.line_num,
sum(b.discount_amt) as discount_amt
from (select * from ssharma_db.fall_sameprd where transaction_date is NOT NULL) a left outer join mds_new.ods_orderline_discounts_t b
on (a.transaction_num=b.transaction_num
and a.line_num=b.line_num)
group by a.customer_key, a.transaction_num, a.line_num;

drop table if exists ssharma_db.winter_sameprd_disc;
create table ssharma_db.winter_sameprd_disc as 
select a.customer_key, 
a.transaction_num,
a.line_num,
sum(b.discount_amt) as discount_amt
from (select * from ssharma_db.winter_sameprd where transaction_date is NOT NULL) a left outer join mds_new.ods_orderline_discounts_t b
on (a.transaction_num=b.transaction_num
and a.line_num=b.line_num)
group by a.customer_key, a.transaction_num, a.line_num;

drop table if exists ssharma_db.spring_sameprd_disc;
create table ssharma_db.spring_sameprd_disc as 
select a.customer_key, 
a.transaction_num,
a.line_num,
sum(b.discount_amt) as discount_amt
from (select * from ssharma_db.spring_sameprd where transaction_date is NOT NULL) a left outer join mds_new.ods_orderline_discounts_t b
on (a.transaction_num=b.transaction_num
and a.line_num=b.line_num)
group by a.customer_key, a.transaction_num, a.line_num;

drop table if exists ssharma_db.summer_sameprd_disc;
create table ssharma_db.summer_sameprd_disc as 
select a.customer_key, 
a.transaction_num,
a.line_num,
sum(b.discount_amt) as discount_amt
from (select * from ssharma_db.summer_sameprd where transaction_date is NOT NULL) a left outer join mds_new.ods_orderline_discounts_t b
on (a.transaction_num=b.transaction_num
and a.line_num=b.line_num)
group by a.customer_key, a.transaction_num, a.line_num;

drop table if exists ssharma_db.fall_sameprd_data;
create table ssharma_db.fall_sameprd_data as
select distinct a.customer_key,a.transaction_num,a.line_num,a.sales_amt,a.item_qty,a.product_key,a.transaction_date,a.on_sale_flag,
coalesce(b.discount_amt,cast('0.0' as double)) as discount_amt
from (select * from ssharma_db.fall_sameprd where transaction_date is NOT NULL) a left outer join ssharma_db.fall_sameprd_disc b
on (a.transaction_num=b.transaction_num
and a.line_num=b.line_num
and a.customer_key=b.customer_key);

drop table if exists ssharma_db.winter_sameprd_data;
create table ssharma_db.winter_sameprd_data as
select distinct a.customer_key,a.transaction_num,a.line_num,a.sales_amt,a.item_qty,a.product_key,a.transaction_date,a.on_sale_flag,
coalesce(b.discount_amt,cast('0.0' as double)) as discount_amt
from (select * from ssharma_db.winter_sameprd where transaction_date is NOT NULL) a left outer join ssharma_db.winter_sameprd_disc b
on (a.transaction_num=b.transaction_num
and a.line_num=b.line_num
and a.customer_key=b.customer_key);


drop table if exists ssharma_db.spring_sameprd_data;
create table ssharma_db.spring_sameprd_data as
select distinct a.customer_key,a.transaction_num,a.line_num,a.sales_amt,a.item_qty,a.product_key,a.transaction_date,a.on_sale_flag,
coalesce(b.discount_amt,cast('0.0' as double)) as discount_amt
from (select * from ssharma_db.spring_sameprd where transaction_date is NOT NULL) a left outer join ssharma_db.spring_sameprd_disc b
on (a.transaction_num=b.transaction_num
and a.line_num=b.line_num
and a.customer_key=b.customer_key);

drop table if exists ssharma_db.summer_sameprd_data;
create table ssharma_db.summer_sameprd_data as
select distinct a.customer_key,a.transaction_num,a.line_num,a.sales_amt,a.item_qty,a.product_key,a.transaction_date,a.on_sale_flag,
coalesce(b.discount_amt,cast('0.0' as double)) as discount_amt
from (select * from ssharma_db.summer_sameprd where transaction_date is NOT NULL) a left outer join ssharma_db.summer_sameprd_disc b
on (a.transaction_num=b.transaction_num
and a.line_num=b.line_num
and a.customer_key=b.customer_key);



drop table if exists ssharma_db.fall_agg_sameprd;
create table ssharma_db.fall_agg_sameprd as 
select customer_key,sum(sales_amt+discount_amt) as net_sales,sum(abs(coalesce(discount_amt,0)))/sum(sales_amt) as disc_p,
sum(item_qty) as items_purch,
count(distinct transaction_num) as num_txn,
sum(sales_amt+discount_amt)/count(distinct transaction_num) as avg_ord_sz,
sum(sales_amt+discount_amt)/sum(item_qty) as avg_unt_rtl,
sum(item_qty)/count(distinct transaction_num) as unt_per_txn,
sum(item_qty*(case when on_sale_flag='Y' then 1 else 0 end)) as on_sale_items
from ssharma_db.fall_sameprd_data
group by customer_key;

drop table if exists ssharma_db.winter_agg_sameprd;
create table ssharma_db.winter_agg_sameprd as 
select customer_key,sum(sales_amt+discount_amt) as net_sales,sum(abs(coalesce(discount_amt,0)))/sum(sales_amt) as disc_p,
sum(item_qty) as items_purch,
count(distinct transaction_num) as num_txn,
sum(sales_amt+discount_amt)/count(distinct transaction_num) as avg_ord_sz,
sum(sales_amt+discount_amt)/sum(item_qty) as avg_unt_rtl,
sum(item_qty)/count(distinct transaction_num) as unt_per_txn,
sum(item_qty*(case when on_sale_flag='Y' then 1 else 0 end)) as on_sale_items
from ssharma_db.winter_sameprd_data
group by customer_key;

drop table if exists ssharma_db.spring_agg_sameprd;
create table ssharma_db.spring_agg_sameprd as 
select customer_key,sum(sales_amt+discount_amt) as net_sales,sum(abs(coalesce(discount_amt,0)))/sum(sales_amt) as disc_p,
sum(item_qty) as items_purch,
count(distinct transaction_num) as num_txn,
sum(sales_amt+discount_amt)/count(distinct transaction_num) as avg_ord_sz,
sum(sales_amt+discount_amt)/sum(item_qty) as avg_unt_rtl,
sum(item_qty)/count(distinct transaction_num) as unt_per_txn,
sum(item_qty*(case when on_sale_flag='Y' then 1 else 0 end)) as on_sale_items
from ssharma_db.spring_sameprd_data
group by customer_key;

drop table if exists ssharma_db.summer_agg_sameprd;
create table ssharma_db.summer_agg_sameprd as 
select customer_key,sum(sales_amt+discount_amt) as net_sales,sum(abs(coalesce(discount_amt,0)))/sum(sales_amt) as disc_p,
sum(item_qty) as items_purch,
count(distinct transaction_num) as num_txns,
sum(sales_amt+discount_amt)/count(distinct transaction_num) as avg_ord_sz,
sum(sales_amt+discount_amt)/sum(item_qty) as avg_unt_rtl,
sum(item_qty)/count(distinct transaction_num) as unt_per_txn,
sum(item_qty*(case when on_sale_flag='Y' then 1 else 0 end)) as on_sale_items
from ssharma_db.summer_sameprd_data
group by customer_key;

drop table if exists ssharma_db.fall_prod_sameprd;
create table ssharma_db.fall_prod_sameprd as 
select distinct a.customer_key,a.transaction_num,a.line_num,a.sales_amt,a.item_qty,a.product_key,a.transaction_date,a.on_sale_flag,b.season_desc,
b.mdse_div_desc,b.mdse_dept_desc,b.mdse_class_desc,
(case when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='CAPRIS' then 'GIRLS CAPRIS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='PANTS' then 'GIRLS PANTS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='SHORTS' then 'GIRLS SHORTS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='SKIRTS/SKORTS' then 'GIRLS SKIRTS/SKORTS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='GIRLS ACCESSORY' then 'GIRLS ACCESSORY'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='GIRLS INTIMATES' then 'GIRLS INTIMATES'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='GIRLS SWIMWEAR' then 'GIRLS SWIMWEAR'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='OUTERWEAR' then 'GIRLS OUTERWEAR'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='TOPS/SUPPORT/SWEATER' and b.mdse_class_desc<>'SWEATSHIRTS' then 'GIRLS TOPS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='TOPS/SUPPORT/SWEATER' and b.mdse_class_desc='SWEATSHIRTS' then 'GIRLS SWEATSHIRTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='ACCESSORY' then 'WOMENS ACCESSORY' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'CAPRI')>0 then 'WOMENS CAPRIS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and b.mdse_class_desc='DRESS' then 'WOMENS DRESSES'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and (b.mdse_class_desc='JACKET' or b.mdse_class_desc='OUTERWEAR' or b.mdse_class_desc='VEST') then 'WOMENS OUTERWEAR'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'PANT')>0 then 'WOMENS PANTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'SHORT')>0 then 'WOMENS SHORTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'SKIRT')>0 then 'WOMENS SKIRTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'SKORT')>0 then 'WOMENS SKORTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and b.mdse_class_desc='SUPPORT TOPS' then 'WOMENS SUPPORT TOPS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and b.mdse_class_desc='SWEATER' then 'WOMENS SWEATERS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and (instr(b.mdse_class_desc,'COVER UP')>0 or instr(b.mdse_class_desc,'TOP')>0)  then 'WOMENS TOPS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='FOOTWEAR' then 'WOMENS FOOTWEAR' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='INTIMATES' and b.mdse_class_desc<>'BRAS' then 'WOMENS INTIMATES'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='INTIMATES' and b.mdse_class_desc='BRAS' then 'WOMENS BRAS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='BIKINI TOP' then 'WOMENS SWIM BIKINI'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='SWIM BOTTOMS' then 'WOMENS SWIM BOTTOMS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='ONE PIECE' then 'WOMENS SWIM ONE PIECE'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='RASHGUARDS' then 'WOMENS SWIM RASHGUARDS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='TANKINI' then 'WOMENS SWIM TANKINI'
else 'N/A' end) as category
from 
ssharma_db.fall_sameprd a 
inner join 
mds_new.ods_product_t b 
on a.product_key=b.product_key;

drop table if exists ssharma_db.winter_prod_sameprd;
create table ssharma_db.winter_prod_sameprd as 
select distinct a.customer_key,a.transaction_num,a.line_num,a.sales_amt,a.item_qty,a.product_key,a.transaction_date,a.on_sale_flag,b.season_desc,b.mdse_div_desc,b.mdse_dept_desc,b.mdse_class_desc,
(case when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='CAPRIS' then 'GIRLS CAPRIS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='PANTS' then 'GIRLS PANTS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='SHORTS' then 'GIRLS SHORTS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='SKIRTS/SKORTS' then 'GIRLS SKIRTS/SKORTS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='GIRLS ACCESSORY' then 'GIRLS ACCESSORY'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='GIRLS INTIMATES' then 'GIRLS INTIMATES'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='GIRLS SWIMWEAR' then 'GIRLS SWIMWEAR'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='OUTERWEAR' then 'GIRLS OUTERWEAR'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='TOPS/SUPPORT/SWEATER' and b.mdse_class_desc<>'SWEATSHIRTS' then 'GIRLS TOPS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='TOPS/SUPPORT/SWEATER' and b.mdse_class_desc='SWEATSHIRTS' then 'GIRLS SWEATSHIRTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='ACCESSORY' then 'WOMENS ACCESSORY' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'CAPRI')>0 then 'WOMENS CAPRIS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and b.mdse_class_desc='DRESS' then 'WOMENS DRESSES'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and (b.mdse_class_desc='JACKET' or b.mdse_class_desc='OUTERWEAR' or b.mdse_class_desc='VEST') then 'WOMENS OUTERWEAR'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'PANT')>0 then 'WOMENS PANTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'SHORT')>0 then 'WOMENS SHORTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'SKIRT')>0 then 'WOMENS SKIRTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'SKORT')>0 then 'WOMENS SKORTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and b.mdse_class_desc='SUPPORT TOPS' then 'WOMENS SUPPORT TOPS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and b.mdse_class_desc='SWEATER' then 'WOMENS SWEATERS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and (instr(b.mdse_class_desc,'COVER UP')>0 or instr(b.mdse_class_desc,'TOP')>0)  then 'WOMENS TOPS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='FOOTWEAR' then 'WOMENS FOOTWEAR' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='INTIMATES' and b.mdse_class_desc<>'BRAS' then 'WOMENS INTIMATES'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='INTIMATES' and b.mdse_class_desc='BRAS' then 'WOMENS BRAS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='BIKINI TOP' then 'WOMENS SWIM BIKINI'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='SWIM BOTTOMS' then 'WOMENS SWIM BOTTOMS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='ONE PIECE' then 'WOMENS SWIM ONE PIECE'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='RASHGUARDS' then 'WOMENS SWIM RASHGUARDS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='TANKINI' then 'WOMENS SWIM TANKINI'
else 'N/A' end) as category
from 
ssharma_db.winter_sameprd a 
inner join 
mds_new.ods_product_t b 
on a.product_key=b.product_key;

drop table if exists ssharma_db.spring_prod_sameprd;
create table ssharma_db.spring_prod_sameprd as 
select distinct a.customer_key,a.transaction_num,a.line_num,a.sales_amt,a.item_qty,a.product_key,a.transaction_date,a.on_sale_flag,b.season_desc,b.mdse_div_desc,b.mdse_dept_desc,b.mdse_class_desc,
(case when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='CAPRIS' then 'GIRLS CAPRIS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='PANTS' then 'GIRLS PANTS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='SHORTS' then 'GIRLS SHORTS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='SKIRTS/SKORTS' then 'GIRLS SKIRTS/SKORTS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='GIRLS ACCESSORY' then 'GIRLS ACCESSORY'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='GIRLS INTIMATES' then 'GIRLS INTIMATES'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='GIRLS SWIMWEAR' then 'GIRLS SWIMWEAR'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='OUTERWEAR' then 'GIRLS OUTERWEAR'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='TOPS/SUPPORT/SWEATER' and b.mdse_class_desc<>'SWEATSHIRTS' then 'GIRLS TOPS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='TOPS/SUPPORT/SWEATER' and b.mdse_class_desc='SWEATSHIRTS' then 'GIRLS SWEATSHIRTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='ACCESSORY' then 'WOMENS ACCESSORY' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'CAPRI')>0 then 'WOMENS CAPRIS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and b.mdse_class_desc='DRESS' then 'WOMENS DRESSES'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and (b.mdse_class_desc='JACKET' or b.mdse_class_desc='OUTERWEAR' or b.mdse_class_desc='VEST') then 'WOMENS OUTERWEAR'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'PANT')>0 then 'WOMENS PANTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'SHORT')>0 then 'WOMENS SHORTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'SKIRT')>0 then 'WOMENS SKIRTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'SKORT')>0 then 'WOMENS SKORTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and b.mdse_class_desc='SUPPORT TOPS' then 'WOMENS SUPPORT TOPS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and b.mdse_class_desc='SWEATER' then 'WOMENS SWEATERS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and (instr(b.mdse_class_desc,'COVER UP')>0 or instr(b.mdse_class_desc,'TOP')>0)  then 'WOMENS TOPS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='FOOTWEAR' then 'WOMENS FOOTWEAR' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='INTIMATES' and b.mdse_class_desc<>'BRAS' then 'WOMENS INTIMATES'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='INTIMATES' and b.mdse_class_desc='BRAS' then 'WOMENS BRAS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='BIKINI TOP' then 'WOMENS SWIM BIKINI'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='SWIM BOTTOMS' then 'WOMENS SWIM BOTTOMS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='ONE PIECE' then 'WOMENS SWIM ONE PIECE'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='RASHGUARDS' then 'WOMENS SWIM RASHGUARDS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='TANKINI' then 'WOMENS SWIM TANKINI'
else 'N/A' end) as category
from 
ssharma_db.spring_sameprd a 
inner join 
mds_new.ods_product_t b 
on a.product_key=b.product_key;

drop table if exists ssharma_db.summer_prod_sameprd;
create table ssharma_db.summer_prod_sameprd as 
select distinct a.customer_key,a.transaction_num,a.line_num,a.sales_amt,a.item_qty,a.product_key,a.transaction_date,a.on_sale_flag,b.season_desc,b.mdse_div_desc,b.mdse_dept_desc,b.mdse_class_desc,
(case when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='CAPRIS' then 'GIRLS CAPRIS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='PANTS' then 'GIRLS PANTS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='SHORTS' then 'GIRLS SHORTS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='SKIRTS/SKORTS' then 'GIRLS SKIRTS/SKORTS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='GIRLS ACCESSORY' then 'GIRLS ACCESSORY'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='GIRLS INTIMATES' then 'GIRLS INTIMATES'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='GIRLS SWIMWEAR' then 'GIRLS SWIMWEAR'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='OUTERWEAR' then 'GIRLS OUTERWEAR'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='TOPS/SUPPORT/SWEATER' and b.mdse_class_desc<>'SWEATSHIRTS' then 'GIRLS TOPS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='TOPS/SUPPORT/SWEATER' and b.mdse_class_desc='SWEATSHIRTS' then 'GIRLS SWEATSHIRTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='ACCESSORY' then 'WOMENS ACCESSORY' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'CAPRI')>0 then 'WOMENS CAPRIS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and b.mdse_class_desc='DRESS' then 'WOMENS DRESSES'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and (b.mdse_class_desc='JACKET' or b.mdse_class_desc='OUTERWEAR' or b.mdse_class_desc='VEST') then 'WOMENS OUTERWEAR'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'PANT')>0 then 'WOMENS PANTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'SHORT')>0 then 'WOMENS SHORTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'SKIRT')>0 then 'WOMENS SKIRTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'SKORT')>0 then 'WOMENS SKORTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and b.mdse_class_desc='SUPPORT TOPS' then 'WOMENS SUPPORT TOPS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and b.mdse_class_desc='SWEATER' then 'WOMENS SWEATERS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and (instr(b.mdse_class_desc,'COVER UP')>0 or instr(b.mdse_class_desc,'TOP')>0)  then 'WOMENS TOPS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='FOOTWEAR' then 'WOMENS FOOTWEAR' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='INTIMATES' and b.mdse_class_desc<>'BRAS' then 'WOMENS INTIMATES'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='INTIMATES' and b.mdse_class_desc='BRAS' then 'WOMENS BRAS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='BIKINI TOP' then 'WOMENS SWIM BIKINI'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='SWIM BOTTOMS' then 'WOMENS SWIM BOTTOMS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='ONE PIECE' then 'WOMENS SWIM ONE PIECE'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='RASHGUARDS' then 'WOMENS SWIM RASHGUARDS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='TANKINI' then 'WOMENS SWIM TANKINI'
else 'N/A' end) as category
from 
ssharma_db.summer_sameprd a 
inner join 
mds_new.ods_product_t b 
on a.product_key=b.product_key;

drop table if exists ssharma_db.fall_cat_sameprd;
create table ssharma_db.fall_cat_sameprd as 
select customer_key,count(distinct category) as num_cat 
from ssharma_db.fall_prod_sameprd 
group by customer_key;

drop table if exists ssharma_db.winter_cat_sameprd;
create table ssharma_db.winter_cat_sameprd as 
select customer_key,count(distinct category) as num_cat 
from ssharma_db.winter_prod_sameprd 
group by customer_key;

drop table if exists ssharma_db.spring_cat_sameprd;
create table ssharma_db.spring_cat_sameprd as 
select customer_key,count(distinct category) as num_cat 
from ssharma_db.spring_prod_sameprd 
group by customer_key;

drop table if exists ssharma_db.summer_cat_sameprd;
create table ssharma_db.summer_cat_sameprd as 
select customer_key,count(distinct category) as num_cat 
from ssharma_db.summer_prod_sameprd 
group by customer_key;

drop table if exists ssharma_db.fall_summary;
create table ssharma_db.fall_summary as 
select a.customer_key,coalesce(b.purch,0) as bought,
coalesce(c.net_sales,0) as net_sales,coalesce(c.disc_p,0) as disc_p,
coalesce(c.items_purch,0) as items_purch,
coalesce(c.num_txn,0) as num_txn,
coalesce(c.avg_ord_sz,0) as avg_ord_sz,
coalesce(c.avg_unt_rtl,0) as avg_unt_rtl,
coalesce(c.unt_per_txn,0) as unt_per_txn,
coalesce(c.on_sale_items,0) as on_sale_items,
coalesce(c.recency,365) as recency,
coalesce(f.items_browsed,0) as items_browsed,
coalesce(h.items_abandoned,0) as items_abandoned,
coalesce(l.num_cat,0) as num_cat,
coalesce(e.net_sales,0) as net_sales_sameprd,coalesce(e.disc_p,0) as disc_p_sameprd,
coalesce(e.items_purch,0) as items_purch_sameprd,
coalesce(e.num_txn,0) as num_txn_sameprd,
coalesce(e.avg_ord_sz,0) as avg_ord_sz_sameprd,
coalesce(e.avg_unt_rtl,0) as avg_unt_rtl_sameprd,
coalesce(e.unt_per_txn,0) as unt_per_txn_sameprd,
coalesce(e.on_sale_items,0) as on_sale_items_sameprd,
coalesce(d.num_cat,0) as num_cat_sameprd,
'fall' as season
from ssharma_db.fall_customer a 
left outer join 
ssharma_db.fall_resp b 
on a.customer_key=b.customer_key
left outer join 
ssharma_db.fall_agg c 
on a.customer_key=c.customer_key  
left outer join 
ssharma_db.fall_clicks_mk_cat_total f 
on a.customer_key=f.customerkey 
left outer join 
ssharma_db.fall_abandon_mk_cat_total h 
on a.customer_key=h.customerkey
left outer join 
ssharma_db.fall_cat l
on a.customer_key=l.customer_key
left outer join 
ssharma_db.fall_agg_sameprd e 
on a.customer_key=e.customer_key
left outer join 
ssharma_db.fall_cat_sameprd d 
on a.customer_key=d.customer_key;

drop table if exists ssharma_db.spring_summary;
create table ssharma_db.spring_summary as 
select a.customer_key,coalesce(b.purch,0) as bought,
coalesce(c.net_sales,0) as net_sales,coalesce(c.disc_p,0) as disc_p,
coalesce(c.items_purch,0) as items_purch,
coalesce(c.num_txn,0) as num_txn,
coalesce(c.avg_ord_sz,0) as avg_ord_sz,
coalesce(c.avg_unt_rtl,0) as avg_unt_rtl,
coalesce(c.unt_per_txn,0) as unt_per_txn,
coalesce(c.on_sale_items,0) as on_sale_items,
coalesce(c.recency,365) as recency,
coalesce(f.items_browsed,0) as items_browsed,
coalesce(h.items_abandoned,0) as items_abandoned,
coalesce(l.num_cat,0) as num_cat,
coalesce(e.net_sales,0) as net_sales_sameprd,coalesce(e.disc_p,0) as disc_p_sameprd,
coalesce(e.items_purch,0) as items_purch_sameprd,
coalesce(e.num_txn,0) as num_txn_sameprd,
coalesce(e.avg_ord_sz,0) as avg_ord_sz_sameprd,
coalesce(e.avg_unt_rtl,0) as avg_unt_rtl_sameprd,
coalesce(e.unt_per_txn,0) as unt_per_txn_sameprd,
coalesce(e.on_sale_items,0) as on_sale_items_sameprd,
coalesce(d.num_cat,0) as num_cat_sameprd,
'spring' as season
from ssharma_db.spring_customer a 
left outer join 
ssharma_db.spring_resp b 
on a.customer_key=b.customer_key
left outer join 
ssharma_db.spring_agg c 
on a.customer_key=c.customer_key  
left outer join 
ssharma_db.spring_clicks_mk_cat_total f 
on a.customer_key=f.customerkey 
left outer join 
ssharma_db.spring_abandon_mk_cat_total h 
on a.customer_key=h.customerkey
left outer join 
ssharma_db.spring_cat l
on a.customer_key=l.customer_key
left outer join 
ssharma_db.spring_agg_sameprd e 
on a.customer_key=e.customer_key
left outer join 
ssharma_db.spring_cat_sameprd d 
on a.customer_key=d.customer_key;

drop table if exists ssharma_db.summer_summary;
create table ssharma_db.summer_summary as 
select a.customer_key,coalesce(b.purch,0) as bought,
coalesce(c.net_sales,0) as net_sales,coalesce(c.disc_p,0) as disc_p,
coalesce(c.items_purch,0) as items_purch,
coalesce(c.num_txns,0) as num_txn,
coalesce(c.avg_ord_sz,0) as avg_ord_sz,
coalesce(c.avg_unt_rtl,0) as avg_unt_rtl,
coalesce(c.unt_per_txn,0) as unt_per_txn,
coalesce(c.on_sale_items,0) as on_sale_items,
coalesce(c.recency,365) as recency,
coalesce(f.items_browsed,0) as items_browsed,
coalesce(h.items_abandoned,0) as items_abandoned,
coalesce(l.num_cat,0) as num_cat,
coalesce(e.net_sales,0) as net_sales_sameprd,coalesce(e.disc_p,0) as disc_p_sameprd,
coalesce(e.items_purch,0) as items_purch_sameprd,
coalesce(e.num_txns,0) as num_txn_sameprd,
coalesce(e.avg_ord_sz,0) as avg_ord_sz_sameprd,
coalesce(e.avg_unt_rtl,0) as avg_unt_rtl_sameprd,
coalesce(e.unt_per_txn,0) as unt_per_txn_sameprd,
coalesce(e.on_sale_items,0) as on_sale_items_sameprd,
coalesce(d.num_cat,0) as num_cat_sameprd,
'summer' as season
from ssharma_db.summer_customer a 
left outer join 
ssharma_db.summer_resp b 
on a.customer_key=b.customer_key
left outer join 
ssharma_db.summer_agg c 
on a.customer_key=c.customer_key  
left outer join 
ssharma_db.summer_clicks_mk_cat_total f 
on a.customer_key=f.customerkey 
left outer join 
ssharma_db.summer_abandon_mk_cat_total h 
on a.customer_key=h.customerkey
left outer join 
ssharma_db.summer_cat l
on a.customer_key=l.customer_key
left outer join 
ssharma_db.summer_agg_sameprd e 
on a.customer_key=e.customer_key
left outer join 
ssharma_db.summer_cat_sameprd d 
on a.customer_key=d.customer_key;

drop table if exists ssharma_db.winter_summary;
create table ssharma_db.winter_summary as 
select a.customer_key,coalesce(b.purch,0) as bought,
coalesce(c.net_sales,0) as net_sales,coalesce(c.disc_p,0) as disc_p,
coalesce(c.items_purch,0) as items_purch,
coalesce(c.num_txn,0) as num_txn,
coalesce(c.avg_ord_sz,0) as avg_ord_sz,
coalesce(c.avg_unt_rtl,0) as avg_unt_rtl,
coalesce(c.unt_per_txn,0) as unt_per_txn,
coalesce(c.on_sale_items,0) as on_sale_items,
coalesce(c.recency,365) as recency,
coalesce(f.items_browsed,0) as items_browsed,
coalesce(h.items_abandoned,0) as items_abandoned,
coalesce(l.num_cat,0) as num_cat,
coalesce(e.net_sales,0) as net_sales_sameprd,coalesce(e.disc_p,0) as disc_p_sameprd,
coalesce(e.items_purch,0) as items_purch_sameprd,
coalesce(e.num_txn,0) as num_txn_sameprd,
coalesce(e.avg_ord_sz,0) as avg_ord_sz_sameprd,
coalesce(e.avg_unt_rtl,0) as avg_unt_rtl_sameprd,
coalesce(e.unt_per_txn,0) as unt_per_txn_sameprd,
coalesce(e.on_sale_items,0) as on_sale_items_sameprd,
coalesce(d.num_cat,0) as num_cat_sameprd,
'winter' as season
from ssharma_db.winter_customer a 
left outer join 
ssharma_db.winter_resp b 
on a.customer_key=b.customer_key
left outer join 
ssharma_db.winter_agg c 
on a.customer_key=c.customer_key  
left outer join 
ssharma_db.winter_clicks_mk_cat_total f 
on a.customer_key=f.customerkey 
left outer join 
ssharma_db.winter_abandon_mk_cat_total h 
on a.customer_key=h.customerkey
left outer join 
ssharma_db.winter_cat l
on a.customer_key=l.customer_key
left outer join 
ssharma_db.winter_agg_sameprd e 
on a.customer_key=e.customer_key
left outer join 
ssharma_db.winter_cat_sameprd d 
on a.customer_key=d.customer_key;



drop table if exists ssharma_db.allseason_summary;
create table ssharma_db.allseason_summary as
select x.customer_key,
x.bought,
x.net_sales,
x.disc_p,
x.items_purch,
x.num_txn,
x.avg_ord_sz,
x.avg_unt_rtl,
x.unt_per_txn,
x.on_sale_items,
x.recency,
x.items_browsed,
x.items_abandoned,
x.num_cat,
x.net_sales_sameprd,
x.disc_p_sameprd,
x.items_purch_sameprd,
x.num_txn_sameprd,
x.avg_ord_sz_sameprd,
x.avg_unt_rtl_sameprd,
x.unt_per_txn_sameprd,
x.on_sale_items_sameprd,
x.num_cat_sameprd,
x.season
from 
(select a.customer_key,
a.bought,
a.net_sales,
a.disc_p,
a.items_purch,
a.num_txn,
a.avg_ord_sz,
a.avg_unt_rtl,
a.unt_per_txn,
a.on_sale_items,
a.recency,
a.items_browsed,
a.items_abandoned,
a.num_cat,
a.net_sales_sameprd,
a.disc_p_sameprd,
a.items_purch_sameprd,
a.num_txn_sameprd,
a.avg_ord_sz_sameprd,
a.avg_unt_rtl_sameprd,
a.unt_per_txn_sameprd,
a.on_sale_items_sameprd,
a.num_cat_sameprd,
a.season  from ssharma_db.fall_summary a
union all 
select b.customer_key,
b.bought,
b.net_sales,
b.disc_p,
b.items_purch,
b.num_txn,
b.avg_ord_sz,
b.avg_unt_rtl,
b.unt_per_txn,
b.on_sale_items,
b.recency,
b.items_browsed,
b.items_abandoned,
b.num_cat,
b.net_sales_sameprd,
b.disc_p_sameprd,
b.items_purch_sameprd,
b.num_txn_sameprd,
b.avg_ord_sz_sameprd,
b.avg_unt_rtl_sameprd,
b.unt_per_txn_sameprd,
b.on_sale_items_sameprd,
b.num_cat_sameprd,
b.season from ssharma_db.winter_summary b 
union all
select c.customer_key,
c.bought,
c.net_sales,
c.disc_p,
c.items_purch,
c.num_txn,
c.avg_ord_sz,
c.avg_unt_rtl,
c.unt_per_txn,
c.on_sale_items,
c.recency,
c.items_browsed,
c.items_abandoned,
c.num_cat,
c.net_sales_sameprd,
c.disc_p_sameprd,
c.items_purch_sameprd,
c.num_txn_sameprd,
c.avg_ord_sz_sameprd,
c.avg_unt_rtl_sameprd,
c.unt_per_txn_sameprd,
c.on_sale_items_sameprd,
c.num_cat_sameprd,
c.season from ssharma_db.spring_summary c 
union all
select d.customer_key,
d.bought,
d.net_sales,
d.disc_p,
d.items_purch,
d.num_txn,
d.avg_ord_sz,
d.avg_unt_rtl,
d.unt_per_txn,
d.on_sale_items,
d.recency,
d.items_browsed,
d.items_abandoned,
d.num_cat,
d.net_sales_sameprd,
d.disc_p_sameprd,
d.items_purch_sameprd,
d.num_txn_sameprd,
d.avg_ord_sz_sameprd,
d.avg_unt_rtl_sameprd,
d.unt_per_txn_sameprd,
d.on_sale_items_sameprd,
d.num_cat_sameprd,
d.season from ssharma_db.summer_summary d ) x;