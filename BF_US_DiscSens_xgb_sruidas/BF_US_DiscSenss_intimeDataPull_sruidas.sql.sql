/* set database=mghosh_ds_br_jan15;
DROP DATABASE IF EXISTS sruidas CASCADE;
CREATE DATABASE sruidas  LOCATION '/apps/hive/warehouse/aa_cem/mghosh/sruidas';  */

SET DATE_NOW=2016-03-01;
set mapred.job.queue.name=default;
set mapred.job.priority=VERY_HIGH;
SET hive.cli.print.header=true;
SET DATEINDEX = 20160301;
set BRAND = BF;
set hive.variable.substitute=true;
set hive.exec.reducers.max=200;

----------Data Pulling for Discount Sensitivity model------------------------

------------------Retail Variables---------------------------


drop table if exists sruidas.${hiveconf:BRAND}_customer_txn_last_year;
create table sruidas.${hiveconf:BRAND}_customer_txn_last_year as
select b.customer_key,   b.order_status,  COALESCE(b.gross_sales_amt, 0L) as gross_sales_amt,   COALESCE(b.discount_amt, 0L) as discount_amt, 
             COALESCE(b.item_qty, 0L) as item_qty, b.order_num, b.transaction_num,
             (CASE WHEN (b.order_status='O') THEN b.order_num
                   WHEN (b.order_status='R') THEN cast(b.transaction_num as STRING) 
               END) AS txn_num ,
to_date(b.demand_date) as demand_date, to_date(b.ship_date) as ship_date,
(case when order_status='O' and demand_date is NOT NULL then to_date(b.demand_date) else to_date(b.ship_date) end) as transaction_date
from mds_new.ods_orderheader_t b
where  
(
(order_status='R' and unix_timestamp(to_date(ship_date),'yyyy-MM-dd') between UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}', - 366), 'yyyy-MM-dd') AND 
 UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}', - 1), 'yyyy-MM-dd'))
or 
(order_status='O' and unix_timestamp(to_date(demand_date),'yyyy-MM-dd') between UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}', - 366), 'yyyy-MM-dd') AND 
 UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}', - 1), 'yyyy-MM-dd'))
) 
and brand='${hiveconf:BRAND}' and country= 'US' and gross_sales_amt is NOT NULL  
and  ((transaction_type_cd = 'S' and item_qty>0) or transaction_type_cd='M') ;
--count(*) 15,768,486  count(distinct key) 5206883

--Mapping the Gap Employee
drop table if exists sruidas.${hiveconf:BRAND}_customer_base ;
create table sruidas.${hiveconf:BRAND}_customer_base as
select a.customer_key ,(case when b.flag_employee ='Y' then 'Y' else 'N' end) as flag_employee from
(select distinct customer_key from sruidas.${hiveconf:BRAND}_customer_txn_last_year) a 
left outer join 
(select distinct customer_key , flag_employee from mds_new.ods_corp_cust_t) b 
on a.customer_key=b.customer_key;

--select count(1) from sruidas.${hiveconf:BRAND}_customer_base;
---5206883

--Next One year Transaction
drop table if exists sruidas.${hiveconf:BRAND}* ;
create table sruidas.${hiveconf:BRAND}_customer_txn_resp_period as
select distinct a.customer_key, 
             a.order_status, 
             COALESCE(a.gross_sales_amt, 0L) as gross_sales_amt, 
             COALESCE(a.discount_amt, 0L) as discount_amt, 
             COALESCE(a.item_qty, 0L) as item_qty, 
             a.order_num, 
             a.transaction_num,
             (CASE WHEN (a.order_status='O') THEN a.order_num
                   WHEN (a.order_status='R') THEN cast(a.transaction_num as STRING) 
               END) AS txn_num
from mds_new.ods_orderheader_t a
inner join sruidas.${hiveconf:BRAND}_customer_base b
on a.customer_key=b.customer_key
where (
(a.order_status='R' and unix_timestamp(to_date(a.ship_date),'yyyy-MM-dd') between UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}', 0 ), 'yyyy-MM-dd') AND 
 UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}', 365 ), 'yyyy-MM-dd'))
or 
(a.order_status='O' and unix_timestamp(to_date(a.demand_date),'yyyy-MM-dd') between UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}', 0 ), 'yyyy-MM-dd') AND 
 UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}', 365 ), 'yyyy-MM-dd'))
) 
and a.brand='${hiveconf:BRAND}' and a.country= 'US' and a.gross_sales_amt is NOT NULL  
and  ((a.transaction_type_cd = 'S' and a.item_qty>0) or a.transaction_type_cd='M');

--count(*) 9986184 count(distinct customer_key) 2315711

 
 ----Transaction done during campaign
 
 --DM Campaign
  drop table if exists sruidas.${hiveconf:BRAND}_DM_Campaign_Txn_tmp ;
  create table sruidas.${hiveconf:BRAND}_DM_Campaign_Txn_tmp  as
  select T1.customer_key, T2.cell_key, T2.Camp_Start_Date , T2.Camp_end_date
  from sruidas.${hiveconf:BRAND}_customer_base  T1 left outer join ( select a.customer_key, a.cell_key,b.GAP_CELL_START_DATE as Camp_Start_Date ,b.gap_cell_end_date as Camp_end_date from mds_new.ods_mailingmailhouse_t a inner join mds_new.ods_cell_t b on a.cell_key=b.cell_key where a.brand ='${hiveconf:BRAND}' and a.country='US' and a.event_type='M' and 
  UNIX_TIMESTAMP(b.GAP_CELL_START_DATE, 'yyyy-MM-dd_HH-mm-ss_Z') BETWEEN  UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}', - 366), 'yyyy-MM-dd') AND  UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}', - 1), 'yyyy-MM-dd') 
  or UNIX_TIMESTAMP(b.gap_cell_end_date, 'yyyy-MM-dd_HH-mm-ss_Z') BETWEEN  UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}', - 366), 'yyyy-MM-dd')
  AND  UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}', - 1), 'yyyy-MM-dd')  ) T2 on T1.customer_key=T2.customer_key  ;
  
 drop table if exists sruidas.${hiveconf:BRAND}_DM_Campaign_Txn2_tmp;
create table sruidas.${hiveconf:BRAND}_DM_Campaign_Txn2_tmp as
  select a.*, b.Camp_Start_Date , b.Camp_end_date , (case when a.transaction_date  between b.Camp_Start_Date and b.Camp_end_date then 1 else 0 end ) as Promotion_purchase, 
  (case when month(a.transaction_date) >11  then 1  when month(a.transaction_date) =11  and day(a.transaction_date) >16 then 1 else  0 end ) as Holiday_season_purchase ,
  a.gross_sales_amt + a.discount_amt as net_sales_amt
  from sruidas.${hiveconf:BRAND}_customer_txn_last_year a left outer join sruidas.${hiveconf:BRAND}_DM_Campaign_Txn_tmp b on a.customer_key=b.customer_key;
   
--Count(*) 40512986 count(distinct customer_key) 5206883
  
  
-- 1	Recency	Time since last retail & Online purchase	

drop table if exists sruidas.${hiveconf:BRAND}_disc_sens_model1_tmp ;
create table sruidas.${hiveconf:BRAND}_disc_sens_model1_tmp as 
select a.* ,b.Offline_Last_txn_date ,  d.Online_Last_txn_date ,c.Offline_disc_Last_txn_date , e.Total_PLCC_Cards ,  e.acquisition_date,
datediff(from_unixtime(UNIX_TIMESTAMP('${hiveconf:DATE_NOW}','yyyy-MM-dd')),from_unixtime(UNIX_TIMESTAMP(to_date(b.Offline_Last_txn_date),'yyyy-MM-dd'))) as Time_Since_last_Retail_purchase, 
datediff(from_unixtime(UNIX_TIMESTAMP('${hiveconf:DATE_NOW}','yyyy-MM-dd')),from_unixtime(UNIX_TIMESTAMP(to_date(d.Online_Last_txn_date),'yyyy-MM-dd'))) as Time_Since_last_Online_purchase,
datediff(from_unixtime(UNIX_TIMESTAMP('${hiveconf:DATE_NOW}','yyyy-MM-dd')),from_unixtime(UNIX_TIMESTAMP(to_date(c.Offline_disc_Last_txn_date),'yyyy-MM-dd'))) as Time_Since_last_disc_purchase, 
datediff(from_unixtime(UNIX_TIMESTAMP('${hiveconf:DATE_NOW}','yyyy-MM-dd')),from_unixtime(UNIX_TIMESTAMP(to_date(e.acquisition_date),'yyyy-MM-dd'))) as Num_days_on_books  
from sruidas.${hiveconf:BRAND}_customer_base a 
left outer join 
(select customer_key,  max(transaction_date) as Offline_Last_txn_date from sruidas.${hiveconf:BRAND}_customer_txn_last_year where order_status='R' 
group by customer_key) b
on a.customer_key = b.customer_key
left outer join
(select customer_key,  max(transaction_date) as Offline_disc_Last_txn_date from sruidas.${hiveconf:BRAND}_customer_txn_last_year where order_status='R' 
and discount_amt < 0 group by customer_key) c
on a.customer_key = c.customer_key
left outer join
(select customer_key,  max(transaction_date) as Online_Last_txn_date from sruidas.${hiveconf:BRAND}_customer_txn_last_year where order_status='O' 
group by customer_key) d
on a.customer_key = d.customer_key
left outer join
(select cust_key, min(from_unixtime(UNIX_TIMESTAMP(to_date(acquisition_date),'yyyy-MM-dd'))) as acquisition_date , sum(tot_plc_cards) as Total_PLCC_Cards from mds_new.ods_brand_customer_t  where  brand = '${hiveconf:BRAND}' 
and country='US' group by cust_key ) e
on a.customer_key=e.cust_key ;


--select count(*) ,count(distinct customer_key) from sruidas.${hiveconf:BRAND}_disc_sens_model1_tmp;
----  5206883, 5206883

-- 2	Monetary	Average order size retail & Online together	6 months and 12 months

drop table if exists sruidas.${hiveconf:BRAND}_disc_sens_model2_tmp  ;
create table sruidas.${hiveconf:BRAND}_disc_sens_model2_tmp  as
select t1.*,t2.Avg_order_amt_last_12_mth , t1.Avg_order_amt_last_6_mth/t2.Avg_order_amt_last_12_mth as Ratio_order_6_12_mth ,
t2.Num_Order_num_last_12_mth , t1.Num_Order_num_last_6_mth/t2.Num_Order_num_last_12_mth as Ratio_order_units_6_12_mth from
(select a.*,b.Avg_order_amt_last_6_mth, Num_Order_num_last_6_mth
from sruidas.${hiveconf:BRAND}_disc_sens_model1_tmp a left outer join
(select customer_key, avg(gross_sales_amt + discount_amt) as Avg_order_amt_last_6_mth, 
sum(case when gross_sales_amt>0 then 1 else 0 end) as Num_Order_num_last_6_mth
from sruidas.${hiveconf:BRAND}_customer_txn_last_year
where transaction_date between
DATE_ADD('${hiveconf:DATE_NOW}', - 183) AND
DATE_ADD('${hiveconf:DATE_NOW}', - 1)
group by customer_key) b
on a.customer_key=b.customer_key ) t1 left outer join
(select customer_key, avg(gross_sales_amt + discount_amt) as Avg_order_amt_last_12_mth, sum(case when gross_sales_amt>0 then 1 else 0 end) 
as Num_Order_num_last_12_mth from sruidas.${hiveconf:BRAND}_customer_txn_last_year
where transaction_date between DATE_ADD('${hiveconf:DATE_NOW}', - 366) AND DATE_ADD('${hiveconf:DATE_NOW}', - 1) group by customer_key) t2
on t1.customer_key=t2.customer_key ;
--5206883

-- 1	Monetary	% of discounted  item purchase in dollars	6 months and 12 months

 drop table if exists sruidas.${hiveconf:BRAND}_discounted_item_tmp ;
 create table sruidas.${hiveconf:BRAND}_discounted_item_tmp as 
select t2.customer_key, t1.Percent_disc_last_6_mth, t2.Percent_disc_last_12_mth from 
 (select customer_key, (cast(1 as float)-coalesce(sum(gross_sales_amt + discount_amt)/sum(gross_sales_amt),cast(0 as float))) as Percent_disc_last_6_mth 
 from sruidas.${hiveconf:BRAND}_customer_txn_last_year  where transaction_date between DATE_ADD('${hiveconf:DATE_NOW}', - 183) 
 AND DATE_ADD('${hiveconf:DATE_NOW}', - 1) group by customer_key ) t1 right outer join 
 (select customer_key, (cast(1 as float)-coalesce(sum(gross_sales_amt + discount_amt)/sum(gross_sales_amt),cast(0 as float))) as Percent_disc_last_12_mth 
 from sruidas.${hiveconf:BRAND}_customer_txn_last_year where transaction_date between DATE_ADD('${hiveconf:DATE_NOW}', -  366) AND
 DATE_ADD('${hiveconf:DATE_NOW}', - 1) group by customer_key) t2
 on t1.customer_key=t2.customer_key ;



-- 2	Monetary	% of discount communication customer responded 	12 months only Discount reason code 117 521 646 893
-- Non PLCC MPC promotion Codes
 drop table if exists sruidas.${hiveconf:BRAND}_Monetary_disc_comm_resp_temp ;
 create table sruidas.${hiveconf:BRAND}_Monetary_disc_comm_resp_temp as   
select  customer_key ,  count(transaction_num) as Num_Disc_Comm_responded from mds_new.ods_orderline_discounts_t where brand='${hiveconf:BRAND}'  and country= 'US' 
and discount_reason_cd in ('117', '521', '646', '893') and  transaction_date between DATE_ADD('${hiveconf:DATE_NOW}', - 366) AND DATE_ADD('${hiveconf:DATE_NOW}', - 1) group by customer_key ;
 
 
 -- Purchase from Sister Brands
-- 3	Monetary	% ratio of product purchased revenue from ON (Old Navy) in last 12 months
drop table if exists sruidas.on_txn_last_12mth_tmp;
create table sruidas.on_txn_last_12mth_tmp as 
select b.customer_key, b.transaction_num,b.order_num, b.order_status,b.gross_sales_amt,c.discount_amt,(coalesce(b.gross_sales_amt,cast(0 as float)) + coalesce(c.discount_amt,cast(0 as float)))  as ON_Net_Sales_Amt_12mth ,
(
case when b.order_status='O' and b.demand_date is NOT NULL then to_date(b.demand_date) else b.ship_date end
) as transaction_date
from mds_new.ods_orderheader_t b  inner join 
 mds_new.ods_orderline_discounts_t  c 
 on b.transaction_num=c.transaction_num
where  
(
(b.order_status='R' and unix_timestamp(b.ship_date,'yyyy-MM-dd') between UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}', - 366), 'yyyy-MM-dd') AND 
 UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}', - 1), 'yyyy-MM-dd'))
or 
(b.order_status='O' and unix_timestamp(to_date(b.demand_date),'yyyy-MM-dd') between UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}', - 366), 'yyyy-MM-dd') AND 
 UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}', - 1), 'yyyy-MM-dd'))) 

and b.brand='ON' and b.country= 'US' and b.gross_sales_amt is NOT NULL  
and  ((b.transaction_type_cd = 'S' and b.item_qty>0) or b.transaction_type_cd='M') ;

---USING THE BELOW TABLE FROM MGHOSH_DS database used for pulling while pulling for GP brand
  
-- 4	Monetary	% revenue came from Banana Republic factory outlet purchase	12 months only br_txn_last_12mth_tmp, go_txn_last_12mth_tmp
drop table if exists sruidas.br_txn_last_12mth_tmp;
create table sruidas.br_txn_last_12mth_tmp as 
select b.customer_key, b.transaction_num,b.order_num, b.order_status,b.gross_sales_amt,c.discount_amt,(coalesce(b.gross_sales_amt,cast(0 as float)) + coalesce(c.discount_amt,cast(0 as float)))  as br_Net_Sales_Amt_12mth ,
(
case when b.order_status='O' and b.demand_date is NOT NULL then to_date(b.demand_date) else b.ship_date end
) as transaction_date
from mds_new.ods_orderheader_t b  inner join 
 mds_new.ods_orderline_discounts_t  c 
 on b.transaction_num=c.transaction_num
where  
(
(b.order_status='R' and unix_timestamp(b.ship_date,'yyyy-MM-dd') between UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}', - 366), 'yyyy-MM-dd') AND 
 UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}', - 1), 'yyyy-MM-dd'))
or 
(b.order_status='O' and unix_timestamp(to_date(b.demand_date),'yyyy-MM-dd') between UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}', - 366), 'yyyy-MM-dd') AND 
 UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}', - 1), 'yyyy-MM-dd'))) 

and b.brand='BR' and b.country= 'US' and b.gross_sales_amt is NOT NULL  
and  ((b.transaction_type_cd = 'S' and b.item_qty>0) or b.transaction_type_cd='M') ;

-- 11	Monetary	% revenue came from Gap factory outlet purchase	12 months only
drop table if exists sruidas.GO_txn_last_12mth_tmp;
create table sruidas.GO_txn_last_12mth_tmp as 
select b.customer_key, (coalesce(b.gross_sales_amt,cast(0 as float)) + coalesce(c.discount_amt,cast(0 as float)))  as GO_Net_Sales_Amt_12mth 
from mds_new.ods_orderheader_t b  inner join 
 mds_new.ods_orderline_discounts_t  c 
 on b.transaction_num=c.transaction_num
where  
(
(b.order_status='R' and unix_timestamp(b.ship_date,'yyyy-MM-dd') between UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}', - 366), 'yyyy-MM-dd') AND 
 UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}', - 1), 'yyyy-MM-dd'))
or 
(b.order_status='O' and unix_timestamp(to_date(b.demand_date),'yyyy-MM-dd') between UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}', - 366), 'yyyy-MM-dd') AND 
 UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}', - 1), 'yyyy-MM-dd'))) 

and b.brand='GO' and b.country= 'US' and b.gross_sales_amt is NOT NULL  
and  ((b.transaction_type_cd = 'S' and b.item_qty>0) or b.transaction_type_cd='M') ;


drop table if exists sruidas.GP_txn_last_12mth_tmp;
create table sruidas.GP_txn_last_12mth_tmp as 
select b.customer_key, (coalesce(b.gross_sales_amt,cast(0 as float)) + coalesce(c.discount_amt,cast(0 as float)))  as GP_net_sales_amt 
from mds_new.ods_orderheader_t b  inner join 
 mds_new.ods_orderline_discounts_t  c 
 on b.transaction_num=c.transaction_num
where  
(
(b.order_status='R' and unix_timestamp(b.ship_date,'yyyy-MM-dd') between UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}', - 366), 'yyyy-MM-dd') AND 
 UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}', - 1), 'yyyy-MM-dd'))
or 
(b.order_status='O' and unix_timestamp(to_date(b.demand_date),'yyyy-MM-dd') between UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}', - 366), 'yyyy-MM-dd') AND 
 UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}', - 1), 'yyyy-MM-dd'))) 

and b.brand='GP' and b.country= 'US' and b.gross_sales_amt is NOT NULL  
and  ((b.transaction_type_cd = 'S' and b.item_qty>0) or b.transaction_type_cd='M') ;
 

---Merging to base table
drop table if exists sruidas.${hiveconf:BRAND}_disc_sens_model3_tmp  ;
create table sruidas.${hiveconf:BRAND}_disc_sens_model3_tmp as   
select a.*, h.bf_net_sales_amt_12_mth, h.bf_net_sales_amt_12_mth/b.ON_Net_Sales_Amt_12mth as bf_on_sales_ratio, c.Num_Disc_Comm_responded , d.percent_disc_last_6_mth, d.percent_disc_last_12_mth ,
h.bf_net_sales_amt_12_mth/e.GO_Net_Sales_Amt_12mth as bf_go_net_sales_ratio , h.bf_net_sales_amt_12_mth/f.BR_Net_Sales_Amt_12mth as bf_br_net_sales_ratio ,
h.bf_net_sales_amt_12_mth/g.GP_Net_Sales_Amt_12mth as bf_gp_net_sales_ratio
from  sruidas.${hiveconf:BRAND}_disc_sens_model2_tmp a 
left outer join 
(select customer_key,sum(ON_Net_Sales_Amt_12mth)  as ON_Net_Sales_Amt_12mth from sruidas.on_txn_last_12mth_tmp group by customer_key) b
on a.customer_key=b.customer_key
left outer join   sruidas.${hiveconf:BRAND}_Monetary_disc_comm_resp_temp  c 
on a.customer_key=c.customer_key
left outer join
sruidas.${hiveconf:BRAND}_discounted_item_tmp  d 
on a.customer_key=d.customer_key 
left outer join 
(select customer_key,sum(GO_Net_Sales_Amt_12mth)  as GO_Net_Sales_Amt_12mth from sruidas.GO_txn_last_12mth_tmp group by customer_key) e 
on a.customer_key=e.customer_key
left outer join 
(select customer_key,sum(BR_Net_Sales_Amt_12mth)  as BR_Net_Sales_Amt_12mth from sruidas.BR_txn_last_12mth_tmp group by customer_key) f 
on a.customer_key=f.customer_key
left outer join 
(select customer_key, sum(gp_net_sales_amt) as GP_Net_Sales_Amt_12mth from sruidas.GP_txn_last_12mth_tmp group by customer_key) g 
on a.customer_key=g.customer_key 
left outer join
( select customer_key , sum(gross_sales_amt + discount_amt) as bf_net_sales_amt_12_mth from sruidas.${hiveconf:BRAND}_customer_txn_last_year group by customer_key) h 
on a.customer_key=h.customer_key;
--5206883


-- 	Monetary	On sale item quantity (price ends in .99)	12 months  % on sale items to total items	Yes

drop table if exists sruidas.On_sale_qty_12_mth_tmp ;
create table sruidas.On_sale_qty_12_mth_tmp as  
 select x.customer_key, sum(x.net_sales_amt*x.on_sale) as On_sales_item_rev_12mth, sum(x.net_sales_amt*x.on_sale)/sum(x.net_sales_amt) as On_sales_rev_ratio_12mth, count(x.transaction_num) as on_sale_item_qty_12mth from 
(select a.customer_key,a.demand_date,a.gross_sales_amt,a.order_num,a.order_status,a.ship_date,a.transaction_date,a.transaction_num,b.discount_amt , (coalesce(a.gross_sales_amt,cast(0 as float)) + coalesce(b.discount_amt,cast(0 as float))) as net_sales_amt , b.on_sale
from sruidas.${hiveconf:BRAND}_customer_txn_last_year a left outer join (select transaction_num , (case when ( on_sale_flag="Y" or sales_amt%1 = 0.99 ) then 1 else 0 end) as on_sale, sum(discount_amt) as discount_amt from mds_new.ods_orderline_discounts_t where brand='${hiveconf:BRAND}'  and country= 'US'    group by transaction_num,(case when ( on_sale_flag="Y" or sales_amt%1 = 0.99 ) then 1 else 0 end) ) b
on a.transaction_num =b.transaction_num) x group by x.customer_key ;

 

-- Monetary	Average discount percent retail without rewards	12 months
drop table if exists sruidas.Avg_disc_percent_wo_rewards ;
create table sruidas.Avg_disc_percent_wo_rewards as  
select x.customer_key, (1.00-sum(x.net_sales_amt)/sum(x.gross_sales_amt)) as  ratio_rev_wo_rewd_12mth from
(select a.customer_key, a.gross_sales_amt,a.ship_date,a.transaction_date,a.transaction_num,b.discount_amt , (coalesce(a.gross_sales_amt,cast(0 as float)) + coalesce(b.discount_amt,cast(0 as float)))  as net_sales_amt
from sruidas.${hiveconf:BRAND}_customer_txn_last_year a left outer join (select transaction_num ,  sum(discount_amt) as discount_amt from mds_new.ods_orderline_discounts_t where brand='${hiveconf:BRAND}'  and country= 'US' and discount_reason_cd not in ('505','513','562','638','661')  group by transaction_num ) b
on a.transaction_num =b.transaction_num) x group by x.customer_key ;


-- 17	Monetary	Average discount percent retail with rewards 12 months
drop table if exists sruidas.Avg_disc_percent_with_rewards ;
create table sruidas.Avg_disc_percent_with_rewards as  
select x.customer_key, (1.00-sum(x.net_sales_amt)/sum(x.gross_sales_amt)) as  ratio_rev_rewd_12mth from
(select a.customer_key, a.gross_sales_amt,a.ship_date,a.transaction_date,a.transaction_num,b.discount_amt , (coalesce(a.gross_sales_amt,cast(0 as float)) + coalesce(b.discount_amt,cast(0 as float)))  as net_sales_amt
from sruidas.${hiveconf:BRAND}_customer_txn_last_year a left outer join (select transaction_num ,  sum(discount_amt) as discount_amt from mds_new.ods_orderline_discounts_t where brand='${hiveconf:BRAND}'  and country= 'US' and discount_reason_cd  in ('505','513','562','638','661')  group by transaction_num ) b
on a.transaction_num =b.transaction_num) x group by x.customer_key ;


-- 20	Ratio of communication sent through electronic medium	12 months and EM plus DM

/*   drop table if exists sruidas.Total_DM_EM_12mth;
   create table sruidas.Total_DM_EM_12mth as
   select t1.*,t2.Num_EM_Campaign  , t2.Num_EM_Campaign/(coalesce(t1.Num_DM_Campaign,0)+coalesce(t2.Num_EM_Campaign)) as Per_Elec_Comm
   from 
   (select a.customer_key, b.Num_DM_Campaign   
   from    sruidas.${hiveconf:BRAND}_customer_base  a left outer join (select  customer_key,count(1) as Num_DM_Campaign from mds_new.ods_mailingmailhouse_t group by customer_key ) b on a.customer_key=b.customer_key) t1 left outer join
   (select customer_key, count(1) as Num_EM_Campaign from mds_new.ods_mailingemail_t group by customer_key) t2
   on t1.customer_key=t2.customer_key; */
   

drop table if exists sruidas.${hiveconf:BRAND}_order_txn_size ;
create table sruidas.${hiveconf:BRAND}_order_txn_size as 
select a.customer_key,a.Num_units_6mth,a.Num_txn_6mth,b.Num_units_12mth,b.Num_txn_12mth from
 (select x.customer_key, count(x.line_num) as Num_units_6mth, count(distinct x.transaction_num) as Num_txn_6mth from mds_new.Ods_orderline_t x where  x.transaction_date between DATE_ADD('${hiveconf:DATE_NOW}', - 183) AND DATE_ADD('${hiveconf:DATE_NOW}', - 1) group by x.customer_key ) a
 full outer  join 
 (select y.customer_key, count(y.line_num) as Num_units_12mth, count(distinct y.transaction_num) as Num_txn_12mth from mds_new.Ods_orderline_t y where y.transaction_date between DATE_ADD('${hiveconf:DATE_NOW}', - 366) AND DATE_ADD('${hiveconf:DATE_NOW}', - 1) group by y.customer_key ) b
on a.customer_key=b.customer_key;


-- 26	Other	# of distinct product category the customer is buying (baby/woman/man/boy/girl etc)	No
drop table if exists sruidas.Product_category_12mth ;
create table sruidas.Product_category_12mth as
select t1.customer_key,t1.transaction_num,t1.line_num, t1.product_key,t2.mdse_div_desc,
(case
 when mdse_div_desc='ACCESSORIES' or mdse_div_desc='ACESS/SHOES/NONAPPRL' then 'acc'
 when mdse_div_desc='FUN ZONE' then 'funz'
 when mdse_div_desc='MENS' then 'mens'
 when mdse_div_desc='GIRLS DIVISION' then 'girls'
 when mdse_div_desc='BABY&TODDLER DIVISION' then 'baby'
 when mdse_div_desc='MATERNITY' then 'mat'
 when mdse_div_desc='KIDS UNDETERMINABLES' then 'kids'
 when mdse_div_desc='PLUS SIZES' then 'plus'
 when mdse_div_desc='WOMENS' then 'womens'
 when mdse_div_desc='BOYS DIVISION' then 'boys'
 else 'misc'
 end) as division
from
(select a.customer_key,a.transaction_num,b.line_num, b.product_key from sruidas.${hiveconf:BRAND}_customer_txn_last_year a inner join mds_new.ods_orderline_t b on a.transaction_num=b.transaction_num ) t1 left outer join  mds_new.ods_product_t t2
on t1.product_key=t2.product_key;

drop table if exists sruidas.Num_Dist_Catg_purchased ;
create table sruidas.Num_Dist_Catg_purchased as
select customer_key, count(distinct mdse_div_desc) as Num_Dist_Catg_purchased from sruidas.Product_category_12mth group by customer_key;

-- 28	Monetary	Avg ticket size Ratio of non discounted  items and discounted items 
drop table if exists sruidas.ticket_size_ratio ;
create table sruidas.ticket_size_ratio as  
 select x.customer_key, a.disc_ATS,b.non_disc_ATS , a.disc_ATS/b.non_disc_ATS as Ratio_disc_non_disc_ATS from
 (select customer_key from sruidas.${hiveconf:BRAND}_customer_base ) x
 left outer join
(select customer_key, sum(coalesce(gross_sales_amt,cast(0 as float)) + coalesce(discount_amt,cast(0 as float)))/count(transaction_num) as disc_ATS from sruidas.${hiveconf:BRAND}_customer_txn_last_year  where discount_amt < 0 
group by customer_key ) a 
on x.customer_key=a.customer_key
left outer join 
(select customer_key, sum(coalesce(gross_sales_amt,cast(0 as float)) + coalesce(discount_amt,cast(0 as float)))/count(transaction_num) as non_disc_ATS from sruidas.${hiveconf:BRAND}_customer_txn_last_year where discount_amt = 0 
group by customer_key ) b 
on x.customer_key=b.customer_key;




DROP TABLE IF EXISTS sruidas.${hiveconf:BRAND}_${hiveconf:DATEINDEX}_CUST_CARD_STATUS_1;
CREATE TABLE sruidas.${hiveconf:BRAND}_${hiveconf:DATEINDEX}_CUST_CARD_STATUS_1 AS SELECT
T1.CUSTOMER_KEY, 
(
CASE
WHEN T1.CUSTOMER_KEY > 0 THEN DATE_ADD('${hiveconf:DATE_NOW}', - 3 - 365 + 1)
ELSE NULL
END
) AS START_DT,
(
CASE
WHEN T1.CUSTOMER_KEY > 0 THEN DATE_ADD('${hiveconf:DATE_NOW}', - 3)
ELSE NULL
END
) AS END_DT,
T2.OPEN_DATE, 
T2.REISSUE_DATE, T2.BRAND, T2.PLATE_CODE, 
T2.PLCC_DROPPED, T2.EST_CLOSED_DT, T2.CARD_TYP
FROM sruidas.${hiveconf:BRAND}_customer_base T1
LEFT OUTER JOIN 
mds_new.ODS_PLCC_CARDHOLDER_T T2
ON T1.CUSTOMER_KEY = T2.CUSTOMER_KEY
WHERE 
TO_DATE(T2.OPEN_DATE) <= DATE_ADD('${hiveconf:DATE_NOW}', - 3) AND 
(
TO_DATE(T2.EST_CLOSED_DT) IS NULL OR 
TO_DATE(T2.EST_CLOSED_DT) > DATE_ADD('${hiveconf:DATE_NOW}', - 3)
);


DROP TABLE IF EXISTS sruidas.${hiveconf:BRAND}_${hiveconf:DATEINDEX}_CUST_CARD_STATUS_2;
CREATE TABLE sruidas.${hiveconf:BRAND}_${hiveconf:DATEINDEX}_CUST_CARD_STATUS_2 AS SELECT
T1.CUSTOMER_KEY, T1.START_DT, T1.END_DT, T1.BRAND, T1.PLATE_CODE, T1.CARD_TYP,
MAX(T1.OPEN_DATE) AS OPEN_DATE, MAX(T1.REISSUE_DATE) AS REISSUE_DATE, MAX(T1.EST_CLOSED_DT) AS EST_CLOSED_DT
FROM sruidas.${hiveconf:BRAND}_${hiveconf:DATEINDEX}_CUST_CARD_STATUS_1 T1 
GROUP BY T1.CUSTOMER_KEY, T1.START_DT, T1.END_DT, T1.BRAND, T1.PLATE_CODE, T1.CARD_TYP;

DROP TABLE IF EXISTS sruidas.${hiveconf:BRAND}_${hiveconf:DATEINDEX}_CUST_CARD_STATUS_1;



DROP TABLE IF EXISTS sruidas.${hiveconf:BRAND}_${hiveconf:DATEINDEX}_CUST_CARD_STATUS_3;
CREATE TABLE sruidas.${hiveconf:BRAND}_${hiveconf:DATEINDEX}_CUST_CARD_STATUS_3 AS SELECT
T1.CUSTOMER_KEY, T1.START_DT, T1.END_DT, T1.BRAND, T1.PLATE_CODE, T1.CARD_TYP,  
T1.OPEN_DATE, T1.REISSUE_DATE,  T1.EST_CLOSED_DT,
(CASE WHEN BRAND='${hiveconf:BRAND}' AND PLATE_CODE<4 THEN 1 ELSE 0 END) BASIC_FLAG,
(CASE WHEN BRAND='${hiveconf:BRAND}' AND PLATE_CODE=4 THEN 1 ELSE 0 END) SILVER_FLAG,
(CASE WHEN BRAND<>'${hiveconf:BRAND}' THEN 1 ELSE 0 END) SISTER_FLAG
FROM sruidas.${hiveconf:BRAND}_${hiveconf:DATEINDEX}_CUST_CARD_STATUS_2 T1;

DROP TABLE IF EXISTS sruidas.${hiveconf:BRAND}_${hiveconf:DATEINDEX}_CUST_CARD_STATUS_2;


DROP TABLE IF EXISTS sruidas.${hiveconf:BRAND}_${hiveconf:DATEINDEX}_CUST_CARD_STATUS_4;             
CREATE TABLE sruidas.${hiveconf:BRAND}_${hiveconf:DATEINDEX}_CUST_CARD_STATUS_4 AS SELECT
CUSTOMER_KEY, 
MAX(BASIC_FLAG) AS BASIC_FLAG,
MAX(SISTER_FLAG) AS SISTER_FLAG,
MAX(SILVER_FLAG) AS SILVER_FLAG
FROM sruidas.${hiveconf:BRAND}_${hiveconf:DATEINDEX}_CUST_CARD_STATUS_3
GROUP BY CUSTOMER_KEY;

DROP TABLE IF EXISTS sruidas.${hiveconf:BRAND}_${hiveconf:DATEINDEX}_CUST_CARD_STATUS_3; 


DROP TABLE IF EXISTS sruidas.${hiveconf:BRAND}_${hiveconf:DATEINDEX}_CUST_CARD_STATUS_5; 
CREATE TABLE sruidas.${hiveconf:BRAND}_${hiveconf:DATEINDEX}_CUST_CARD_STATUS_5 AS SELECT
T1.CUSTOMER_KEY, T2.BASIC_FLAG, T2.SILVER_FLAG, T2.SISTER_FLAG
FROM sruidas.${hiveconf:BRAND}_customer_base T1
LEFT OUTER JOIN sruidas.${hiveconf:BRAND}_${hiveconf:DATEINDEX}_CUST_CARD_STATUS_4 T2
ON T1.CUSTOMER_KEY=T2.CUSTOMER_KEY;

DROP TABLE IF EXISTS sruidas.${hiveconf:BRAND}_${hiveconf:DATEINDEX}_CUST_CARD_STATUS_4;                    


DROP TABLE IF EXISTS sruidas.${hiveconf:BRAND}_${hiveconf:DATEINDEX}_CUST_CARD_STATUS;  
CREATE TABLE sruidas.${hiveconf:BRAND}_${hiveconf:DATEINDEX}_CUST_CARD_STATUS AS SELECT
CUSTOMER_KEY, BASIC_FLAG, SILVER_FLAG, SISTER_FLAG,
(
CASE 
WHEN SILVER_FLAG = 1 THEN 1
WHEN BASIC_FLAG = 1 AND SILVER_FLAG <> 1 THEN 2
WHEN SISTER_FLAG = 1 AND SILVER_FLAG <> 1 AND BASIC_FLAG <> 1 THEN 3 
ELSE 4 
END
) AS CARD_STATUS
FROM sruidas.${hiveconf:BRAND}_${hiveconf:DATEINDEX}_CUST_CARD_STATUS_5;

DROP TABLE IF EXISTS sruidas.${hiveconf:BRAND}_${hiveconf:DATEINDEX}_CUST_CARD_STATUS_5; 



drop table if exists sruidas.${hiveconf:BRAND}_disc_sens_model4_tmp ;
create table sruidas.${hiveconf:BRAND}_disc_sens_model4_tmp as 
select a.*,b.card_status , c.disc_ATS, c.non_disc_ATS, c.ratio_disc_non_disc_ats,d.num_dist_catg_purchased , e.Num_units_6mth,
e.Num_txn_6mth, e.Num_units_12mth, e.Num_txn_12mth ,f.ratio_rev_rewd_12mth ,g.ratio_rev_wo_rewd_12mth,
k. On_sales_item_rev_12mth, k.On_sales_rev_ratio_12mth, k.on_sale_item_qty_12mth
from  
sruidas.${hiveconf:BRAND}_disc_sens_model3_tmp a 
left outer join
(select customer_key, min(CARD_STATUS) AS CARD_STATUS FROM  sruidas.${hiveconf:BRAND}_${hiveconf:DATEINDEX}_CUST_CARD_STATUS GROUP BY CUSTOMER_KEY) b 
on a.customer_key=b.customer_key
left outer join
(select customer_key, disc_ATS,non_disc_ATS,ratio_disc_non_disc_ats from sruidas.ticket_size_ratio ) c
on a.customer_key=c.customer_key
left outer join
sruidas.Num_Dist_Catg_purchased d
on a.customer_key=d.customer_key
left outer join
sruidas.${hiveconf:BRAND}_order_txn_size e
on a.customer_key=e.customer_key
left outer join
sruidas.Avg_disc_percent_with_rewards f
on a.customer_key=f.customer_key
left outer join
sruidas.Avg_disc_percent_wo_rewards g
on a.customer_key=g.customer_key
left outer join
sruidas.On_sale_qty_12_mth_tmp k
on a.customer_key=k.customer_key  ;
 --5206883

 




 
 -- Data not available in hit_data_agg before one year
 
 
SET hive.exec.dynamic.partition=true;
SET hive.mapred.supports.subdirectories=true;
SET hive.stats.autogather=false;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions.pernode=20000;
SET hive.exec.max.dynamic.partitions=160000;
SET hive.exec.max.created.files=150000;

SET hive.cli.print.header=true;
set mapred.job.queue.name=default;
set mapred.job.priority=VERY_HIGH;
set BRAND = BF;

SET DATE_NOW=2016-03-01;
--SET DATE_NOW=2015-12-01;

SET DATEINDEX = 20160301;
--SET DATEINDEX = 20151201;

--Data pull quarterly as monthly isyearly volume pulling is not possible

drop table if exists sruidas.${hiveconf:BRAND}_browse_data_${hiveconf:DATEINDEX} ;
create table sruidas.${hiveconf:BRAND}_browse_data_${hiveconf:DATEINDEX} as
SELECT post_visid_high
, post_visid_low
, visit_num
, t_time_info
, visit_start_time_gmt
, unk_shop_id
, customer_key -- (Note: This is ecommerceid in customer resolution)
, email_key
, purchaseid
, purchaseid_ind
, event_array
, product_list
, pagename
, pagename_clean
, post_pagename
, post_pagename_clean
, pagename_clean_array
, mobile_ind
, searchdex_ind
-- Identify hit with one and only brand
, CASE
WHEN ARRAY_CONTAINS(ARRAY('br', 'gp', 'on', 'at'), pagename_clean_array[0])
THEN UPPER(pagename_clean_array[0])
WHEN pagename_clean_array[0] = 'brfs'
THEN 'BF'
WHEN pagename_clean_array[0] = 'gpfs'
THEN 'GF'
ELSE 'UNKNOWN'
END AS brand
-- Below are a large number of indicators for each page so we can quickly tally them up
, CASE
WHEN pagename_clean_array[0] = 'gp'
THEN 1
ELSE 0
END AS gp_hit_ind
, CASE
WHEN pagename_clean_array[0] = 'gp'
AND (  ARRAY_CONTAINS(pagename_clean_array, 'maternity')
OR ARRAY_CONTAINS(pagename_clean_array, 'gapmaternity') )
THEN 1
ELSE 0
END AS gp_div_maternity_hit_ind
, CASE
WHEN pagename_clean_array[0] = 'br'
THEN 1
ELSE 0
END AS br_hit_ind
, CASE WHEN pagename_clean_array[0] = 'on' THEN 1 ELSE 0 END AS on_hit_ind     
, CASE WHEN pagename_clean_array[0] = 'at' THEN 1 ELSE 0 END AS at_hit_ind
, CASE WHEN pagename_clean RLIKE 'factory' THEN 1 ELSE 0 END AS factory_hit_ind
, CASE WHEN pagename_clean RLIKE 'sale' THEN 1 ELSE 0 END AS sale_hit_ind
, CASE WHEN pagename_clean RLIKE 'markdown' THEN 1 ELSE 0 END AS markdown_hit_ind
, CASE WHEN pagename_clean RLIKE 'clearance' THEN 1 ELSE 0 END AS clearance_hit_ind
, CASE WHEN pagename_clean RLIKE '% off' THEN 1 ELSE 0 END AS pct_off_hit_ind
, CASE
WHEN SUBSTRING(pagename_clean_array[1], 0, 6) = 'browse'
OR SUBSTRING(pagename_clean_array[2], 0, 6) = 'browse'
THEN 1
ELSE 0
END AS browse_hit_ind
, CASE
WHEN SUBSTRING(pagename_clean_array[1], 0, 4) = 'home'
OR SUBSTRING(pagename_clean_array[2], 0, 4) = 'home'
THEN 1
ELSE 0
END AS home_hit_ind
, CASE WHEN prop33 = 'inlineBagAdd' THEN 1 ELSE 0 END AS bag_add_ind
, prop33 AS prop33
, prop1
, brand_from_product_list
, brand_from_pagename
, date_time
  FROM ( SELECT post_visid_high
, post_visid_low
, visit_start_time_gmt
, visit_num
, t_time_info
, date_time
, unk_shop_id
, customer_key
, email_key
, purchaseid
, purchaseid_ind
, event_array
, product_array
, product_list
, pagename
, pagename_clean
, post_pagename
, post_pagename_clean
, CASE
WHEN SUBSTR(pagename_clean, 1, INSTR(pagename_clean, ':') - 1) IN ( 'mobile', 'sd' )
THEN SPLIT(SUBSTR(pagename_clean, INSTR(pagename_clean, ':') + 1), ':')
ELSE SPLIT(pagename_clean, ':')
END AS pagename_clean_array
-- Note mobile identified visit
, CASE
WHEN SUBSTR(pagename_clean, 1, INSTR(pagename_clean, ':') - 1) = 'mobile'
THEN 1
ELSE 0
END AS mobile_ind
-- Note searchdex identified visit
, CASE
WHEN SUBSTR(pagename_clean, 1, INSTR(pagename_clean, ':') - 1) = 'sd'
OR pagename_clean RLIKE '.*:sd:.*'
THEN 1
ELSE 0
END AS searchdex_ind
, prop33
, prop1
, brand_from_product_list
, brand_from_pagename
FROM ( SELECT NVL(post_visid_high, -1) AS post_visid_high
, NVL(post_visid_low, -1) AS post_visid_low
, visit_start_time_gmt AS visit_start_time_gmt
, NVL(visit_num, -1) AS visit_num
, t_time_info
, TRIM(date_time) AS date_time
, CASE
WHEN LENGTH(TRIM(unk_shop_id)) > 0
AND unk_shop_id != 'NULL'
THEN TRIM(unk_shop_id)
ELSE NULL
END AS unk_shop_id
, CASE
WHEN LENGTH(TRIM(customer_key)) > 0
AND customer_key != 'NULL' AND customer_key != 'undefined'
THEN TRIM(customer_key)
ELSE NULL
END AS customer_key
, CASE
WHEN LENGTH(TRIM(post_evar43)) > 0
AND post_evar43 != 'NULL'
THEN TRIM(post_evar43)
WHEN LENGTH(TRIM(evar43)) > 0
AND evar43 != 'NULL'
THEN TRIM(evar43)
ELSE NULL
END AS email_key
, CASE
WHEN LENGTH(TRIM(purchaseid)) > 0
THEN TRIM(purchaseid)
ELSE NULL
END AS purchaseid
, CASE
WHEN purchaseid is null
OR purchaseid = ""
OR purchaseid = " "
OR purchaseid = 'NULL' 
THEN 0 
ELSE 1
END AS purchaseid_ind  
, SPLIT(event_list, ',') AS event_array
, SPLIT(product_list, ',') AS product_array
, product_list
, pagename AS pagename
-- Get rid of extra spaces and apostrophes and lower to normalize
, LOWER(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(pagename, '\\s*:\\s*', ':'), '\'|\&\#39\\;', ''))) AS pagename_clean
, post_pagename AS post_pagename
, LOWER(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(post_pagename, '\\s*:\\s*', ':'), '\'|\&\#39\\;', ''))) AS post_pagename_clean
, prop33
, prop1
, CASE WHEN SPLIT((SPLIT(product_list, ',')[0]),'\;')[0]='AT' OR SPLIT((SPLIT(product_list, ',')[0]),'\;')[0]='ATHLETA' THEN 'AT'
WHEN SPLIT((SPLIT(product_list, ',')[0]),'\;')[0]='BR' OR SPLIT((SPLIT(product_list, ',')[0]),'\;')[0]='BANANAREPUBLIC' THEN 'BR'
WHEN SPLIT((SPLIT(product_list, ',')[0]),'\;')[0]='GP' OR SPLIT((SPLIT(product_list, ',')[0]),'\;')[0]='GAP' THEN 'GAP'
WHEN SPLIT((SPLIT(product_list, ',')[0]),'\;')[0]='ON' OR SPLIT((SPLIT(product_list, ',')[0]),'\;')[0]='OLDNAVY' THEN 'ON'
ELSE 'NULL'
END AS brand_from_product_list
, case when ARRAY_CONTAINS(split(lower(pagename), ':'), 'gp') or ARRAY_CONTAINS(split(lower(pagename), ':'), 'gpfs') or ARRAY_CONTAINS(split(lower(pagename), ':'), 'gap') or ARRAY_CONTAINS(split(lower(pagename), ':'), 'gapfit') or ARRAY_CONTAINS(split(lower(pagename), ' '), 'gp') or ARRAY_CONTAINS(split(lower(pagename), ' '), 'gpfs') or  ARRAY_CONTAINS(split(lower(pagename), ' '), 'gap') or ARRAY_CONTAINS(split(lower(pagename), ' '), 'gapfit') or ARRAY_CONTAINS(split(lower(pagename), '/'), 'gp') or ARRAY_CONTAINS(split(lower(pagename), '/'), 'gpfs') or ARRAY_CONTAINS(split(lower(pagename), '/'), 'gap') or ARRAY_CONTAINS(split(lower(pagename), '/'), 'gapfit') or ARRAY_CONTAINS(split(lower(pagename), '_'), 'gp') or ARRAY_CONTAINS(split(lower(pagename), '_'), 'gpfs') or ARRAY_CONTAINS(split(lower(pagename), '_'), 'gapfit') or ARRAY_CONTAINS(split(lower(pagename), '_'), 'gap') then 'GAP'
when ARRAY_CONTAINS(split(lower(pagename), ':'), 'at') or ARRAY_CONTAINS(split(lower(pagename), ':'), 'athleta') or ARRAY_CONTAINS(split(lower(pagename), ' '), 'at') or ARRAY_CONTAINS(split(lower(pagename), ' '), 'athleta') or ARRAY_CONTAINS(split(lower(pagename), '/'), 'at') or ARRAY_CONTAINS(split(lower(pagename), '/'), 'athleta') or ARRAY_CONTAINS(split(lower(pagename), '_'), 'at') or ARRAY_CONTAINS(split(lower(pagename), '_'), 'atfs') or ARRAY_CONTAINS(split(lower(pagename), '_'), 'athleta') then 'AT'
when ARRAY_CONTAINS(split(lower(pagename), ':'), 'on') or ARRAY_CONTAINS(split(lower(pagename), ':'), 'oldnavy') or ARRAY_CONTAINS(split(lower(pagename), ' '), 'on') or ARRAY_CONTAINS(split(lower(pagename), ' '), 'oldnavy') or ARRAY_CONTAINS(split(lower(pagename), '/'), 'on') or ARRAY_CONTAINS(split(lower(pagename), '/'), 'oldnavy') or ARRAY_CONTAINS(split(lower(pagename), '_'), 'on') or  ARRAY_CONTAINS(split(lower(pagename), '_'), 'oldnavy') then 'ON'
when ARRAY_CONTAINS(split(lower(pagename), ':'), 'br') or ARRAY_CONTAINS(split(lower(pagename), ':'), 'brfs') or ARRAY_CONTAINS(split(lower(pagename), ':'), 'banarepublic') or ARRAY_CONTAINS(split(lower(pagename), ' '), 'br') or ARRAY_CONTAINS(split(lower(pagename), ' '), 'brfs') or  ARRAY_CONTAINS(split(lower(pagename), ' '), 'banarepublic') or ARRAY_CONTAINS(split(lower(pagename), '/'), 'br') or ARRAY_CONTAINS(split(lower(pagename), '/'), 'brfs') or ARRAY_CONTAINS(split(lower(pagename), '/'), 'bananarepublic') or ARRAY_CONTAINS(split(lower(pagename), '_'), 'br') or  ARRAY_CONTAINS(split(lower(pagename), '_'), 'brfs') or  ARRAY_CONTAINS(split(lower(pagename), '_'), 'bananarepublic')then 'BR'
else 'NULL'
end as brand_from_pagename
FROM omniture.hit_data_t_history
WHERE date_time between  DATE_ADD('${hiveconf:DATE_NOW}', - 182 ) and DATE_ADD('${hiveconf:DATE_NOW}', -1) 
-- Certain records should be ignored because they are considered duplicates
AND duplicate_purchase = 0
AND exclude_hit = 0 
AND hit_source NOT IN (5,7,8,9) ) a ) hd ;
 
 ));
 
drop table if exists sruidas.hit_data_agg_subset ;
 create table sruidas.hit_data_agg_subset as
 SELECT
unk_shop_id,
post_visid_high,
post_visid_low,
visit_num,
SUM(mobile_ind) as mobile_ind_tot,
SUM(searchdex_ind) as searchdex_ind_tot ,
SUM(gp_hit_ind) as gp_hit_ind_tot,
SUM(br_hit_ind) as br_hit_ind_tot,
SUM(on_hit_ind) as on_hit_ind_tot,
SUM(at_hit_ind) as at_hit_ind_tot,
SUM(factory_hit_ind) as factory_hit_ind_tot,
SUM(sale_hit_ind) as sale_hit_ind_tot,
SUM(markdown_hit_ind) as markdown_hit_ind_tot,
SUM(clearance_hit_ind) as clearance_hit_ind_tot,
SUM(pct_off_hit_ind) as pct_off_hit_ind_tot,
SUM(browse_hit_ind) as browse_hit_ind_tot,
SUM(home_hit_ind) as home_hit_ind_tot,
SUM(bag_add_ind) as bag_add_ind_tot,
SUM(purchaseid_ind) as purchaseid_ind,
customer_key,
date_time
from  sruidas.${hiveconf:BRAND}_browse_data_20160301 a 
WHERE 
date_time is not null AND 
unk_shop_id is not null AND
length(unk_shop_id) = 32 AND
unk_shop_id RLIKE '^[a-zA-Z0-9]*$' 
group by  date_time, unk_shop_id, customer_key, email_key, visit_num, post_visid_high, post_visid_low, visit_start_time_gmt, brand_from_pagename, prop33;

 


 drop table if exists sruidas.${hiveconf:BRAND}_disc_sens_model5_tmp ;
 create table sruidas.${hiveconf:BRAND}_disc_sens_model5_tmp as
 select  t1.*, t2.*, t3.Resp_Disc_Percent
 from sruidas.${hiveconf:BRAND}_disc_sens_model4_tmp  t1 
 left outer join
 (select customerkey,masterkey, SUM(mobile_ind_tot) as mobile_ind_tot,
SUM(searchdex_ind_tot) as searchdex_ind_tot ,
SUM(gp_hit_ind_tot) as gp_hit_ind_tot,
SUM(br_hit_ind_tot) as br_hit_ind_tot,
SUM(on_hit_ind_tot) as on_hit_ind_tot,
SUM(at_hit_ind_tot) as at_hit_ind_tot,
SUM(factory_hit_ind_tot) as factory_hit_ind_tot,
SUM(sale_hit_ind_tot) as sale_hit_ind_tot,
SUM(markdown_hit_ind_tot) as markdown_hit_ind_tot,
SUM(clearance_hit_ind_tot) as clearance_hit_ind_tot,
SUM(pct_off_hit_ind_tot) as pct_off_hit_ind_tot,
SUM(browse_hit_ind_tot) as browse_hit_ind_tot,
SUM(home_hit_ind_tot) as home_hit_ind_tot,
SUM(bag_add_ind_tot) as bag_add_ind_tot,
SUM(purchased) as purchased from
 (select distinct masterkey , customerkey , unknownshopperid from crmanalytics.customerresolution) b 
 left outer join 
 ( select unk_shop_id, SUM(mobile_ind_tot) as mobile_ind_tot,
SUM(searchdex_ind_tot) as searchdex_ind_tot ,
SUM(gp_hit_ind_tot) as gp_hit_ind_tot,
SUM(br_hit_ind_tot) as br_hit_ind_tot,
SUM(on_hit_ind_tot) as on_hit_ind_tot,
SUM(at_hit_ind_tot) as at_hit_ind_tot,
SUM(factory_hit_ind_tot) as factory_hit_ind_tot,
SUM(sale_hit_ind_tot) as sale_hit_ind_tot,
SUM(markdown_hit_ind_tot) as markdown_hit_ind_tot,
SUM(clearance_hit_ind_tot) as clearance_hit_ind_tot,
SUM(pct_off_hit_ind_tot) as pct_off_hit_ind_tot,
SUM(browse_hit_ind_tot) as browse_hit_ind_tot,
SUM(home_hit_ind_tot) as home_hit_ind_tot,
SUM(bag_add_ind_tot) as bag_add_ind_tot,
SUM(purchaseid_ind) as purchased from sruidas.hit_data_agg_subset
 where date_time between  DATE_ADD('${hiveconf:DATE_NOW}', -182) and DATE_ADD('${hiveconf:DATE_NOW}', -1) 
 group by unk_shop_id ) c 
 on b.unknownshopperid=c.unk_shop_id 
 group by customerkey,masterkey) t2
 on t1.customer_key=t2.customerkey
 left outer join
 (select customer_key, coalesce(sum(discount_amt),0) / coalesce(sum(gross_sales_amt) , 0 ) as Resp_Disc_Percent from sruidas.${hiveconf:BRAND}_customer_txn_resp_period 
 group by customer_key) t3
 on t1.customer_key=t3.customer_key ;

 --5206883
 

  drop table if exists sruidas.${hiveconf:BRAND}_disc_sens_model6_ext ;
 create external table sruidas.${hiveconf:BRAND}_disc_sens_model6_ext 
 (customer_key bigint ,
flag_employee string ,
offline_last_txn_date string ,
online_last_txn_date string ,
offline_disc_last_txn_date string ,
total_plcc_cards bigint ,
acquisition_date string ,
time_since_last_retail_purchase int ,
time_since_last_online_purchase int ,
time_since_last_disc_purchase int ,
num_days_on_books int ,
avg_order_amt_last_6_mth double ,
num_order_num_last_6_mth bigint ,
avg_order_amt_last_12_mth double ,
ratio_order_6_12_mth double ,
num_order_num_last_12_mth bigint ,
ratio_order_units_6_12_mth double ,
br_net_sales_amt_12_mth double ,
br_on_sales_ratio double ,
num_disc_comm_responded bigint ,
percent_disc_last_6_mth double ,
percent_disc_last_12_mth double ,
br_go_net_sales_ratio double ,
br_bf_net_sales_ratio double ,
br_gp_net_sales_ratio double ,
card_status int ,
disc_ats double ,
non_disc_ats double ,
ratio_disc_non_disc_ats double ,
num_dist_catg_purchased bigint ,
num_units_6mth bigint ,
num_txn_6mth bigint ,
num_units_12mth bigint ,
num_txn_12mth bigint ,
ratio_rev_rewd_12mth double ,
ratio_rev_wo_rewd_12mth double ,
on_sales_item_rev_12mth double ,
on_sales_rev_ratio_12mth double ,
on_sale_item_qty_12mth bigint ,
customerkey bigint ,
masterkey string ,
mobile_ind_tot bigint ,
searchdex_ind_tot bigint ,
gp_hit_ind_tot bigint ,
br_hit_ind_tot bigint ,
on_hit_ind_tot bigint ,
at_hit_ind_tot bigint ,
factory_hit_ind_tot bigint ,
sale_hit_ind_tot bigint ,
markdown_hit_ind_tot bigint ,
clearance_hit_ind_tot bigint ,
pct_off_hit_ind_tot bigint ,
browse_hit_ind_tot bigint ,
home_hit_ind_tot bigint ,
bag_add_ind_tot bigint ,
purchased bigint ,
resp_disc_percent double  )
row format delimited 
fields terminated by '|' 
lines terminated by '\n' 
location '/user/sruidas/brfs_discsen/${hiveconf:BRAND}_disc_sens_model6_ext'
TBLPROPERTIES ("serialization.null.format"=""); 


insert overwrite table sruidas.${hiveconf:BRAND}_disc_sens_model6_ext 
select distinct customer_key ,
flag_employee ,
offline_last_txn_date ,
online_last_txn_date ,
offline_disc_last_txn_date ,
total_plcc_cards ,
acquisition_date ,
time_since_last_retail_purchase ,
time_since_last_online_purchase ,
time_since_last_disc_purchase ,
num_days_on_books ,
avg_order_amt_last_6_mth ,
num_order_num_last_6_mth ,
avg_order_amt_last_12_mth ,
ratio_order_6_12_mth ,
num_order_num_last_12_mth ,
ratio_order_units_6_12_mth ,
bf_net_sales_amt_12_mth ,
bf_on_sales_ratio ,
num_disc_comm_responded ,
percent_disc_last_6_mth ,
percent_disc_last_12_mth ,
bf_go_net_sales_ratio ,
bf_br_net_sales_ratio ,
bf_gp_net_sales_ratio ,
card_status ,
disc_ats ,
non_disc_ats ,
ratio_disc_non_disc_ats ,
num_dist_catg_purchased ,
num_units_6mth ,
num_txn_6mth ,
num_units_12mth ,
num_txn_12mth ,
ratio_rev_rewd_12mth ,
ratio_rev_wo_rewd_12mth ,
on_sales_item_rev_12mth ,
on_sales_rev_ratio_12mth ,
on_sale_item_qty_12mth ,
customerkey ,
masterkey ,
mobile_ind_tot ,
searchdex_ind_tot ,
gp_hit_ind_tot ,
br_hit_ind_tot ,
on_hit_ind_tot ,
at_hit_ind_tot ,
factory_hit_ind_tot ,
sale_hit_ind_tot ,
markdown_hit_ind_tot ,
clearance_hit_ind_tot ,
pct_off_hit_ind_tot ,
browse_hit_ind_tot ,
home_hit_ind_tot ,
bag_add_ind_tot ,
purchased ,
resp_disc_percent   
from sruidas.${hiveconf:BRAND}_disc_sens_model5_tmp  ;
 
--hadoop fs -getmerge /user/sruidas/brfs_discsen/BF_disc_sens_model6_ext  /home/sruidas/brfs_discsen/bf_disc_sens_model6.txt



/user/sruidas/ brfs_discsen/BF_disc_sens_model6_ext


















