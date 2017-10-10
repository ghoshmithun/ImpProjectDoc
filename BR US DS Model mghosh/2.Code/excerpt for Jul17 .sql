set mapred.job.queue.name=aa;
SET hive.cli.print.header=true;

SET DATE_NOW=2017-07-08;
SET DATEINDEX = 20170708;
set BRAND = BR;
set database=mghosh;

4475930

-- Monetary	Average discount percent retail without rewards	12 months
drop table if exists ${hiveconf:database}.Avg_disc_percent_wo_rewards ;
create table ${hiveconf:database}.Avg_disc_percent_wo_rewards as  
select x.customer_key, (1.00-sum(x.net_sales_amt)/sum(x.gross_sales_amt)) as  ratio_rev_wo_rewd_12mth from
(select a.customer_key, a.gross_sales_amt,a.ship_date,a.transaction_date,a.transaction_num,b.discount_amt , (coalesce(a.gross_sales_amt,cast(0 as float)) + coalesce(b.discount_amt,cast(0 as float)))  as net_sales_amt
from sruidas.BR_customer_txn_last_year a left outer join (select transaction_num ,  sum(discount_amt) as discount_amt from mds_new.ods_orderline_discounts_t where brand='${hiveconf:BRAND}'  and country= 'US' and discount_reason_cd not in ('505','513','562','638','661')  group by transaction_num ) b
on a.transaction_num =b.transaction_num) x group by x.customer_key ;


-- 17	Monetary	Average discount percent retail with rewards 12 months
drop table if exists ${hiveconf:database}.Avg_disc_percent_with_rewards ;
create table ${hiveconf:database}.Avg_disc_percent_with_rewards as  
select x.customer_key, (1.00-sum(x.net_sales_amt)/sum(x.gross_sales_amt)) as  ratio_rev_rewd_12mth from
(select a.customer_key, a.gross_sales_amt,a.ship_date,a.transaction_date,a.transaction_num,b.discount_amt , (coalesce(a.gross_sales_amt,cast(0 as float)) + coalesce(b.discount_amt,cast(0 as float)))  as net_sales_amt
from sruidas.${hiveconf:BRAND}_customer_txn_last_year a left outer join (select transaction_num ,  sum(discount_amt) as discount_amt from mds_new.ods_orderline_discounts_t where brand='${hiveconf:BRAND}'  and country= 'US' and discount_reason_cd  in ('505','513','562','638','661')  group by transaction_num ) b
on a.transaction_num =b.transaction_num) x group by x.customer_key ;


-- 20	Ratio of communication sent through electronic medium	12 months and EM plus DM

  drop table if exists ${hiveconf:database}.Total_DM_EM_12mth;
   create table ${hiveconf:database}.Total_DM_EM_12mth as
   select t1.*,t2.Num_EM_Campaign  , t2.Num_EM_Campaign/(coalesce(t1.Num_DM_Campaign,0)+coalesce(t2.Num_EM_Campaign)) as Per_Elec_Comm
   from 
   (select a.customer_key, b.Num_DM_Campaign   
   from    sruidas.${hiveconf:BRAND}_customer_base  a left outer join (select  customer_key,count(1) as Num_DM_Campaign from mds_new.ods_mailingmailhouse_t group by customer_key ) b on a.customer_key=b.customer_key) t1 left outer join
   (select customer_key, count(1) as Num_EM_Campaign from mds_new.ods_mailingemail_t group by customer_key) t2
   on t1.customer_key=t2.customer_key;
   

drop table if exists ${hiveconf:database}.${hiveconf:BRAND}_order_txn_size ;
create table ${hiveconf:database}.${hiveconf:BRAND}_order_txn_size as 
select a.customer_key,a.Num_units_6mth,a.Num_txn_6mth,b.Num_units_12mth,b.Num_txn_12mth from
 (select x.customer_key, count(x.line_num) as Num_units_6mth, count(distinct x.transaction_num) as Num_txn_6mth from mds_new.Ods_orderline_t x where  x.transaction_date between DATE_ADD('${hiveconf:DATE_NOW}', - 183) AND DATE_ADD('${hiveconf:DATE_NOW}', - 1) group by x.customer_key ) a
 full outer  join 
 (select y.customer_key, count(y.line_num) as Num_units_12mth, count(distinct y.transaction_num) as Num_txn_12mth from mds_new.Ods_orderline_t y where y.transaction_date between DATE_ADD('${hiveconf:DATE_NOW}', - 366) AND DATE_ADD('${hiveconf:DATE_NOW}', - 1) group by y.customer_key ) b
on a.customer_key=b.customer_key;


-- 26	Other	# of distinct product category the customer is buying (baby/woman/man/boy/girl etc)	No
drop table if exists ${hiveconf:database}.Product_category_12mth ;
create table ${hiveconf:database}.Product_category_12mth as
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
(select a.customer_key,a.transaction_num,b.line_num, b.product_key from sruidas.${hiveconf:BRAND}_customer_txn_last_year a inner join mds_new.ods_orderline_t b on a.transaction_num=b.transaction_num where b.transaction_date between DATE_ADD('${hiveconf:DATE_NOW}', - 366) AND DATE_ADD('${hiveconf:DATE_NOW}', - 1)) t1 left outer join  mds_new.ods_product_t  t2
on t1.product_key=t2.product_key;


drop table if exists ${hiveconf:database}.Num_Dist_Catg_purchased ;
create table ${hiveconf:database}.Num_Dist_Catg_purchased as
select customer_key, count(distinct mdse_div_desc) as Num_Dist_Catg_purchased from ${hiveconf:database}.Product_category_12mth group by customer_key;

-- 28	Monetary	Avg ticket size Ratio of non discounted  items and discounted items 
drop table if exists ${hiveconf:database}.ticket_size_ratio ;
create table ${hiveconf:database}.ticket_size_ratio as  
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




DROP TABLE IF EXISTS ${hiveconf:database}.${hiveconf:BRAND}_${hiveconf:DATEINDEX}_CUST_CARD_STATUS_1;
CREATE TABLE ${hiveconf:database}.${hiveconf:BRAND}_${hiveconf:DATEINDEX}_CUST_CARD_STATUS_1 AS SELECT
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


DROP TABLE IF EXISTS ${hiveconf:database}.${hiveconf:BRAND}_${hiveconf:DATEINDEX}_CUST_CARD_STATUS_2;
CREATE TABLE ${hiveconf:database}.${hiveconf:BRAND}_${hiveconf:DATEINDEX}_CUST_CARD_STATUS_2 AS SELECT
T1.CUSTOMER_KEY, T1.START_DT, T1.END_DT, T1.BRAND, T1.PLATE_CODE, T1.CARD_TYP,
MAX(T1.OPEN_DATE) AS OPEN_DATE, MAX(T1.REISSUE_DATE) AS REISSUE_DATE, MAX(T1.EST_CLOSED_DT) AS EST_CLOSED_DT
FROM ${hiveconf:database}.${hiveconf:BRAND}_${hiveconf:DATEINDEX}_CUST_CARD_STATUS_1 T1 
GROUP BY T1.CUSTOMER_KEY, T1.START_DT, T1.END_DT, T1.BRAND, T1.PLATE_CODE, T1.CARD_TYP;

DROP TABLE IF EXISTS ${hiveconf:database}.${hiveconf:BRAND}_${hiveconf:DATEINDEX}_CUST_CARD_STATUS_1;



DROP TABLE IF EXISTS ${hiveconf:database}.${hiveconf:BRAND}_${hiveconf:DATEINDEX}_CUST_CARD_STATUS_3;
CREATE TABLE ${hiveconf:database}.${hiveconf:BRAND}_${hiveconf:DATEINDEX}_CUST_CARD_STATUS_3 AS SELECT
T1.CUSTOMER_KEY, T1.START_DT, T1.END_DT, T1.BRAND, T1.PLATE_CODE, T1.CARD_TYP,  
T1.OPEN_DATE, T1.REISSUE_DATE,  T1.EST_CLOSED_DT,
(CASE WHEN BRAND='${hiveconf:BRAND}' AND PLATE_CODE<4 THEN 1 ELSE 0 END) BASIC_FLAG,
(CASE WHEN BRAND='${hiveconf:BRAND}' AND PLATE_CODE=4 THEN 1 ELSE 0 END) SILVER_FLAG,
(CASE WHEN BRAND<>'${hiveconf:BRAND}' THEN 1 ELSE 0 END) SISTER_FLAG
FROM ${hiveconf:database}.${hiveconf:BRAND}_${hiveconf:DATEINDEX}_CUST_CARD_STATUS_2 T1;

DROP TABLE IF EXISTS ${hiveconf:database}.${hiveconf:BRAND}_${hiveconf:DATEINDEX}_CUST_CARD_STATUS_2;


DROP TABLE IF EXISTS ${hiveconf:database}.${hiveconf:BRAND}_${hiveconf:DATEINDEX}_CUST_CARD_STATUS_4;             
CREATE TABLE ${hiveconf:database}.${hiveconf:BRAND}_${hiveconf:DATEINDEX}_CUST_CARD_STATUS_4 AS SELECT
CUSTOMER_KEY, 
MAX(BASIC_FLAG) AS BASIC_FLAG,
MAX(SISTER_FLAG) AS SISTER_FLAG,
MAX(SILVER_FLAG) AS SILVER_FLAG
FROM ${hiveconf:database}.${hiveconf:BRAND}_${hiveconf:DATEINDEX}_CUST_CARD_STATUS_3
GROUP BY CUSTOMER_KEY;

DROP TABLE IF EXISTS ${hiveconf:database}.${hiveconf:BRAND}_${hiveconf:DATEINDEX}_CUST_CARD_STATUS_3; 


DROP TABLE IF EXISTS ${hiveconf:database}.${hiveconf:BRAND}_${hiveconf:DATEINDEX}_CUST_CARD_STATUS_5; 
CREATE TABLE ${hiveconf:database}.${hiveconf:BRAND}_${hiveconf:DATEINDEX}_CUST_CARD_STATUS_5 AS SELECT
T1.CUSTOMER_KEY, T2.BASIC_FLAG, T2.SILVER_FLAG, T2.SISTER_FLAG
FROM sruidas.${hiveconf:BRAND}_customer_base T1
LEFT OUTER JOIN ${hiveconf:database}.${hiveconf:BRAND}_${hiveconf:DATEINDEX}_CUST_CARD_STATUS_4 T2
ON T1.CUSTOMER_KEY=T2.CUSTOMER_KEY;

DROP TABLE IF EXISTS ${hiveconf:database}.${hiveconf:BRAND}_${hiveconf:DATEINDEX}_CUST_CARD_STATUS_4;                    


DROP TABLE IF EXISTS ${hiveconf:database}.${hiveconf:BRAND}_${hiveconf:DATEINDEX}_CUST_CARD_STATUS;  
CREATE TABLE ${hiveconf:database}.${hiveconf:BRAND}_${hiveconf:DATEINDEX}_CUST_CARD_STATUS AS SELECT
CUSTOMER_KEY, BASIC_FLAG, SILVER_FLAG, SISTER_FLAG,
(
CASE 
WHEN SILVER_FLAG = 1 THEN 1
WHEN BASIC_FLAG = 1 AND SILVER_FLAG <> 1 THEN 2
WHEN SISTER_FLAG = 1 AND SILVER_FLAG <> 1 AND BASIC_FLAG <> 1 THEN 3 
ELSE 4 
END
) AS CARD_STATUS
FROM ${hiveconf:database}.${hiveconf:BRAND}_${hiveconf:DATEINDEX}_CUST_CARD_STATUS_5;

DROP TABLE IF EXISTS ${hiveconf:database}.${hiveconf:BRAND}_${hiveconf:DATEINDEX}_CUST_CARD_STATUS_5; 



drop table if exists ${hiveconf:database}.${hiveconf:BRAND}_disc_sens_model4_tmp ;
create table ${hiveconf:database}.${hiveconf:BRAND}_disc_sens_model4_tmp as 
select a.*,b.card_status , c.disc_ATS, c.non_disc_ATS, c.ratio_disc_non_disc_ats,d.num_dist_catg_purchased , e.Num_units_6mth,
e.Num_txn_6mth, e.Num_units_12mth, e.Num_txn_12mth ,f.ratio_rev_rewd_12mth ,g.ratio_rev_wo_rewd_12mth, h.Num_EM_Campaign, h.Per_Elec_Comm,
k. On_sales_item_rev_12mth, k.On_sales_rev_ratio_12mth, k.on_sale_item_qty_12mth
from  sruidas.${hiveconf:BRAND}_disc_sens_model3_tmp a 
left outer join
(select customer_key, min(CARD_STATUS) AS CARD_STATUS FROM  ${hiveconf:database}.${hiveconf:BRAND}_${hiveconf:DATEINDEX}_CUST_CARD_STATUS GROUP BY CUSTOMER_KEY) b 
on a.customer_key=b.customer_key
left outer join
(select customer_key, disc_ATS,non_disc_ATS,ratio_disc_non_disc_ats from ${hiveconf:database}.ticket_size_ratio ) c
on a.customer_key=c.customer_key
left outer join
${hiveconf:database}.Num_Dist_Catg_purchased d
on a.customer_key=d.customer_key
left outer join
${hiveconf:database}.${hiveconf:BRAND}_order_txn_size e
on a.customer_key=e.customer_key
left outer join
${hiveconf:database}.Avg_disc_percent_with_rewards f
on a.customer_key=f.customer_key
left outer join
${hiveconf:database}.Avg_disc_percent_wo_rewards g
on a.customer_key=g.customer_key
left outer join
${hiveconf:database}.Total_DM_EM_12mth h
on a.customer_key=h.customer_key
left outer join
sruidas.On_sale_qty_12_mth_tmp k
on a.customer_key=k.customer_key  ;


 --4475930

set DATE_NOW =2017-07-01;

drop table if exists mghosh.BR_disc_sens_model5_tmp ;
create table mghosh.BR_disc_sens_model5_tmp as
select  t1.customer_key, t2.*
from sruidas.BR_disc_sens_model1_tmp  t1 
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
SUM(bag_add_ind) as bag_add_ind_tot,
SUM(purchaseid_ind) as purchased from crmanalytics.hit_data_agg
where date_time between  DATE_ADD('${hiveconf:DATE_NOW}', -366) and DATE_ADD('${hiveconf:DATE_NOW}', -1) 
 group by unk_shop_id ) c 
 on b.unknownshopperid=c.unk_shop_id 
 group by customerkey,masterkey) t2
on t1.customer_key=t2.customerkey;

 drop table if exists ${hiveconf:database}.${hiveconf:BRAND}_disc_sens_mode6_tmp ;
create table ${hiveconf:database}.${hiveconf:BRAND}_disc_sens_mode6_tmp as
select  t1.*, t2.customerkey ,
t2.masterkey ,
t2.mobile_ind_tot ,
t2.searchdex_ind_tot ,
t2.gp_hit_ind_tot ,
t2.br_hit_ind_tot ,
t2.on_hit_ind_tot ,
t2.at_hit_ind_tot ,
t2.factory_hit_ind_tot ,
t2.sale_hit_ind_tot ,
t2.markdown_hit_ind_tot ,
t2.clearance_hit_ind_tot ,
t2.pct_off_hit_ind_tot ,
t2.browse_hit_ind_tot ,
t2.home_hit_ind_tot ,
t2.bag_add_ind_tot ,
t2.purchased 
from ${hiveconf:database}.${hiveconf:BRAND}_disc_sens_model4_tmp  t1 
 left outer join  mghosh.BR_disc_sens_model5_tmp  t2
 on t1.customer_key=t2.customer_key;


 drop table if exists ${hiveconf:database}.${hiveconf:BRAND}_disc_sens_model7_ext ;
 create external table ${hiveconf:database}.${hiveconf:BRAND}_disc_sens_model7_ext 
 (customer_key  bigint ,
flag_employee  string ,
offline_last_txn_date  string ,
online_last_txn_date  string ,
offline_disc_last_txn_date  string ,
total_plcc_cards  bigint ,
acquisition_date  string ,
time_since_last_retail_purchase  int ,
time_since_last_online_purchase  int ,
time_since_last_disc_purchase  int ,
num_days_on_books  int ,
avg_order_amt_last_6_mth  double ,
num_order_num_last_6_mth  bigint ,
avg_order_amt_last_12_mth  double ,
ratio_order_6_12_mth  double ,
num_order_num_last_12_mth  bigint ,
ratio_order_units_6_12_mth  double ,
br_net_sales_amt_12_mth  double ,
br_on_sales_ratio  double ,
num_disc_comm_responded  bigint ,
percent_disc_last_6_mth  double ,
percent_disc_last_12_mth  double ,
br_go_net_sales_ratio  double ,
br_bf_net_sales_ratio  double ,
br_gp_net_sales_ratio  double ,
card_status  int ,
disc_ats  double ,
non_disc_ats  double ,
ratio_disc_non_disc_ats  double ,
num_dist_catg_purchased  bigint ,
num_units_6mth  bigint ,
num_txn_6mth  bigint ,
num_units_12mth  bigint ,
num_txn_12mth  bigint ,
ratio_rev_rewd_12mth  double ,
ratio_rev_wo_rewd_12mth  double ,
num_em_campaign  bigint ,
per_elec_comm  double ,
on_sales_item_rev_12mth  double ,
on_sales_rev_ratio_12mth  double ,
on_sale_item_qty_12mth  bigint ,
customerkey  bigint ,
masterkey  string ,
mobile_ind_tot  bigint ,
searchdex_ind_tot  bigint ,
gp_hit_ind_tot  bigint ,
br_hit_ind_tot  bigint ,
on_hit_ind_tot  bigint ,
at_hit_ind_tot  bigint ,
factory_hit_ind_tot  bigint ,
sale_hit_ind_tot  bigint ,
markdown_hit_ind_tot  bigint ,
clearance_hit_ind_tot  bigint ,
pct_off_hit_ind_tot  bigint ,
browse_hit_ind_tot  bigint ,
home_hit_ind_tot  bigint ,
bag_add_ind_tot  bigint ,
purchased  bigint  )
row format delimited 
fields terminated by '|' 
lines terminated by '\n' 
location '/user/mghosh/Disc_Sens_BR_Jul17'
TBLPROPERTIES ("serialization.null.format"=""); 


insert overwrite table ${hiveconf:database}.${hiveconf:BRAND}_disc_sens_model7_ext 
select distinct customer_key   ,
flag_employee   ,
offline_last_txn_date   ,
online_last_txn_date   ,
offline_disc_last_txn_date   ,
total_plcc_cards   ,
acquisition_date   ,
time_since_last_retail_purchase   ,
time_since_last_online_purchase   ,
time_since_last_disc_purchase   ,
num_days_on_books   ,
avg_order_amt_last_6_mth   ,
num_order_num_last_6_mth   ,
avg_order_amt_last_12_mth   ,
ratio_order_6_12_mth   ,
num_order_num_last_12_mth   ,
ratio_order_units_6_12_mth   ,
br_net_sales_amt_12_mth   ,
br_on_sales_ratio   ,
num_disc_comm_responded   ,
percent_disc_last_6_mth   ,
percent_disc_last_12_mth   ,
br_go_net_sales_ratio   ,
br_bf_net_sales_ratio   ,
br_gp_net_sales_ratio   ,
card_status   ,
disc_ats   ,
non_disc_ats   ,
ratio_disc_non_disc_ats   ,
num_dist_catg_purchased   ,
num_units_6mth   ,
num_txn_6mth   ,
num_units_12mth   ,
num_txn_12mth   ,
ratio_rev_rewd_12mth   ,
ratio_rev_wo_rewd_12mth   ,
num_em_campaign   ,
per_elec_comm   ,
on_sales_item_rev_12mth   ,
on_sales_rev_ratio_12mth   ,
on_sale_item_qty_12mth   ,
customerkey   ,
masterkey   ,
mobile_ind_tot   ,
searchdex_ind_tot   ,
gp_hit_ind_tot   ,
br_hit_ind_tot   ,
on_hit_ind_tot   ,
at_hit_ind_tot   ,
factory_hit_ind_tot   ,
sale_hit_ind_tot   ,
markdown_hit_ind_tot   ,
clearance_hit_ind_tot   ,
pct_off_hit_ind_tot   ,
browse_hit_ind_tot   ,
home_hit_ind_tot   ,
bag_add_ind_tot   ,
purchased      from ${hiveconf:database}.${hiveconf:BRAND}_disc_sens_mode6_tmp  ;

--4475930
 
hadoop fs -getmerge /user/mghosh/Disc_Sens_BR_Jul17/ /home/mghosh/Disc_Sens_fulldata/BR_disc_sens_Data_pull_Jul17.txt

