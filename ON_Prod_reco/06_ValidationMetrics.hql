set brw_brand=ON;
set  start_date_brw=2015-12-01;
set  end_date_brw=2016-03-31;
set  country=US;
set hive.root.logger=console;

USE sdube_on;

set mapred.job.queue.name=cem;


drop table if exists browse_data_Val;
create table browse_data_Val as 
select unk_shop_id, purchaseid, prop1, visid_high, visid_low, visit_num, pagename, date_time from omniture.hit_data_t where date_time between '${hiveconf:start_date_brw}'
and '${hiveconf:end_date_brw}' and upper(split(pagename,  '[:]')[0]) = '${hiveconf:brw_brand}' ;


drop table if exists browse_data_Val_v2;
create table browse_data_Val_v2 as 
SELECT *,  substr(prop1, 1, 6) AS style_cd
FROM browse_data_Val 
WHERE 
unk_shop_id!='' and prop1 !='' ;

DROP TABLE IF EXISTS browse_data_Val_v3;
CREATE TABLE browse_data_Val_v3 AS 
SELECT a.masterkey, a.emailkey, a.unknownshopperid, a.ecommerceid, a.customerkey, b.* 
FROM 
(SELECT * FROM all_keys) a
INNER JOIN
(SELECT * FROM browse_data_Val_v2) b
ON
a.unknownshopperid = b.unk_shop_id
;

DROP TABLE IF EXISTS browse_data_Val_v4;
CREATE TABLE browse_data_Val_v4 AS 
SELECT distinct a.masterkey, a.emailkey, a.ecommerceid, a.customerkey, a.style_cd,  b.mdse_div_desc, b.mdse_dept_desc, b.mdse_class_desc, b.season_cd, b.season_desc 
FROM  
(SELECT * FROM browse_data_Val_v3 where style_cd is not null ) a
INNER JOIN
(SELECT * FROM mds_new.ods_product_t) b
ON
a.style_cd=b.style_cd 
;


--Offline txn



SET brand_flag=6;
SET inv_year_month=201603;
SET start_date=2015-04-01;
SET end_date=2016-03-31;
SET trans_end_date=2016-03-31;
SET brand=ON;
SET last_trans_dt=2016-03-31;

USE ${hiveconf:database};
set mapred.job.queue.name=aa;
set country = US;

--BRING IN TRANS TYPE S OR M onLY. BOTH onLINE and RETAIL TRANS
DROP TABLE IF EXISTS cust_ord_val;
CREATE TABLE cust_ord_val AS 
SELECT customer_key, order_status, transaction_num, order_num, demand_date, ship_date, store_key, item_qty ,(CASE WHEN order_status='O'  THEN to_date(demand_date) ELSE to_date(ship_date) END) AS trnx_date
FROM 
mds_new.ods_orderheader_t 
WHERE 
brand ='${hiveconf:brand}' AND 
(
    (order_status='R' AND ship_date BETWEEN '${hiveconf:start_date}' AND '${hiveconf:end_date}' ) 
    OR 
    (order_status='O'  AND unix_timestamp(demand_date, 'yyyy-MM-dd_HH-mm-ss_Z') BETWEEN unix_timestamp('${hiveconf:start_date}','yyyy-MM-dd') and unix_timestamp('${hiveconf:end_date}','yyyy-MM-dd') ) 
)
AND country = '${hiveconf:country}' 
AND (transaction_type_cd = 'S'  OR transaction_type_cd = 'M') ;


--create trnx_date and then filter on the dates to be BETWEEN start date and transaction end date. We are padding the transaction end date, since that is the date the order is shipped. and INNER JOIN helps rectify the issue of getting more transactions FROM orderline


DROP TABLE IF EXISTS cust_ord_val_v2;
CREATE TABLE cust_ord_val_v2 AS 
SELECT * 
FROM  
cust_ord_val
WHERE trnx_date BETWEEN '${hiveconf:start_date}' AND '${hiveconf:trans_end_date}';


-- join the product key desc FROM orderline
DROP TABLE IF EXISTS cust_ord_val_v3;
CREATE TABLE cust_ord_val_v3 AS
SELECT 
a.*, b.product_key, b.item_qty AS ln_qty
FROM 
cust_ord_val_v2 a
INNER JOIN
mds_new.ods_orderline_t b
ON 
a.transaction_num = b.transaction_num AND
a.customer_key = b.customer_key
WHERE 
b.transaction_date BETWEEN '${hiveconf:start_date}' AND '${hiveconf:trans_end_date}' AND b.brand='${hiveconf:brand}' AND country='${hiveconf:country}';

-- get the div, dept, class, season_cd, season_desc and style information FROM product_t. Filter out the BRFS style codes. 
DROP TABLE IF EXISTS cust_ord_val_v4;
CREATE TABLE cust_ord_val_v4 AS
SELECT
a.*, b.mdse_div_desc, b.mdse_dept_desc, b.mdse_class_desc, b.style_color_cd, b.style_cd, b.season_cd, b.season_desc
FROM 
cust_ord_val_v3 a
INNER JOIN
(SELECT * FROM mds_new.ods_product_t where mdse_div_id != '29' ) b
ON
a.product_key = b.product_key
;

DROP TABLE IF EXISTS cust_ord_val_v5;
CREATE TABLE cust_ord_val_v5 AS 
SELECT a.masterkey,  a.customerkey, b.* 
FROM 
(SELECT * FROM all_keys) a
INNER JOIN
(SELECT * FROM cust_ord_val_v4) b
ON
a.customerkey = b.customer_key;
;

drop table if exists purchase_val ;
create table purchase_val as
select masterkey, style_cd , count(*)  as Num_txn from cust_ord_val_v5 group by masterkey, style_cd
order by masterkey,  Num_txn desc;

DROP TABLE IF EXISTS  purchase_val_v2;
create table purchase_val_v2 AS 
SELECT *, rank() over(partition by masterkey,style_cd) AS Rnk 
FROM 
purchase_val;



DROP TABLE IF EXISTS  masterkey_cust_recos_ib_v4;
create table masterkey_cust_recos_ib_v4 AS 
select a.* , coalesce(b.Purchase_tag,0) as Purchase_tag from 
(select * from masterkey_cust_recos_ib_v3) a
left outer join
(select masterkey ,style_cd ,1 as Purchase_tag from purchase_val_v2) b 
on a.masterkey=b.masterkey
and a.reco =b.style_cd;

DROP TABLE IF EXISTS  masterkey_cust_recos_ib_v5;
create table masterkey_cust_recos_ib_v5 AS 
select a.* , coalesce(b.Browse_tag,0) as Browse_tag from 
(select * from masterkey_cust_recos_ib_v4) a
left outer join
(select  distinct masterkey ,style_cd ,1 as Browse_tag from browse_data_Val_v3) b 
on a.masterkey=b.masterkey
and a.reco =b.style_cd;

--- Calculate MAP
select sum(a.response/a.Num_Style_Predicted) as MAP_total, sum(a.browse_tag/a.Num_Style_Predicted) as MAP_browse,sum(a.purchase_tag/a.Num_Style_Predicted) as MAP_purchase, count(distinct a.masterkey) as Num_Cust from
(select masterkey, count(distinct reco) as Num_Style_Predicted, sum(browse_tag) as browse_tag, sum(purchase_tag) as purchase_tag,
sum(case when browse_tag+purchase_tag > 0 then 1 else 0 end) as response from masterkey_cust_recos_ib_v5
group by masterkey) a;

--297713.66153295565      82044.40222489639       234633.8161024671       17662983





------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------
-- random MAP
drop table if exists distinct_style_cd;
create table distinct_style_cd  as
select distinct reco, row_number() over() as row_id from masterkey_cust_recos_ib_v5;


drop table if exists distinct_customer;
create table distinct_customer as
select distinct masterkey from masterkey_cust_recos_ib_v5;

drop table if exists random_reco ;
create table random_reco as
select reco  from distinct_style_cd order by rand() limit 28;

select count(*) from random_reco;

drop table if exists distinct_customer_v2;
create table distinct_customer_v2 as
select a.masterkey, b.reco 
from distinct_customer a inner join
(select reco from distinct_style_cd order by rand() limit 28 ) b  ;


DROP TABLE IF EXISTS  rnd_distinct_customer_v2;
create table rnd_distinct_customer_v2 AS 
select a.* , coalesce(b.Purchase_tag,0) as Purchase_tag from 
(select * from distinct_customer_v2) a
left outer join
(select masterkey ,style_cd ,1 as Purchase_tag from purchase_val_v2) b 
on a.masterkey=b.masterkey
and a.reco =b.style_cd;

DROP TABLE IF EXISTS  rnd_distinct_customer_v3;
create table rnd_distinct_customer_v3 AS 
select a.* , coalesce(b.Browse_tag,0) as Browse_tag from 
(select * from rnd_distinct_customer_v2) a
left outer join
(select  distinct masterkey ,style_cd ,1 as Browse_tag from browse_data_Val_v3) b 
on a.masterkey=b.masterkey
and a.reco =b.style_cd;

select sum(a.response/a.Num_Style_Predicted) as MAP_total, sum(a.browse_tag/a.Num_Style_Predicted) as MAP_browse,sum(a.purchase_tag/a.Num_Style_Predicted) as MAP_purchase, count(distinct a.masterkey) as Num_Cust from
(select masterkey, count(distinct reco) as Num_Style_Predicted, sum(browse_tag) as browse_tag, sum(purchase_tag) as purchase_tag,
sum(case when browse_tag+purchase_tag > 0 then 1 else 0 end) as response from rnd_distinct_customer_v3
group by masterkey) a;

--53218.75000106786       18085.39285708017       39321.17857161408       17662983


------------------------------------------------------------RANK CORRECTNESS CHECKING-----------------------------------------

-- Rank correctness checking
drop table if  exists masterkey_cust_recos_ib_v6 ;
create table masterkey_cust_recos_ib_v6 as
select *, (case when purchase_tag = 1 or browse_tag = 1 then 1 else 0 end) as Overall_tag,
(case when purchase_tag = 1 or browse_tag = 1 then 1.0/power(cast(2 as double),cast((rnk-1)/2 as double)) else 0.0 end) as Rankscore_p from masterkey_cust_recos_ib_v5
order by masterkey, rnk;

 
drop table if  exists masterkey_cust_recos_ib_v7 ;
create table masterkey_cust_recos_ib_v7 as
select masterkey , sum(Rankscore_p) as Rankscore_p, sum(Overall_tag) as No_Hits from masterkey_cust_recos_ib_v6
group by masterkey;


add file /home/sdube/rankscore2.py
 
drop table if  exists masterkey_cust_recos_ib_v8 ;
create table masterkey_cust_recos_ib_v8 as
select transform(masterkey, Rankscore_p, no_hits) using 'python rankscore2.py' as masterkey, Rankscore_p, no_hits, Rankscore_max from masterkey_cust_recos_ib_v7 ;
 
drop table if  exists masterkey_cust_recos_ib_v9 ;
create table masterkey_cust_recos_ib_v9 as
select *, Rankscore_p/Rankscore_max as Rankscore from masterkey_cust_recos_ib_v8;
 
 
select sum(Rankscore_p)/sum(Rankscore_max) as overall_Rankscore from masterkey_cust_recos_ib_v8;
 
0.21764150284763867

 
-- Random Rank
 
set database=MGHOSH_GP_TEST;


 
USE ${hiveconf:database};
set mapred.job.queue.name=aa;
 
 
drop table if exists customer_rank_shuffle;
create table customer_rank_shuffle as
select masterkey,reco,purchase_tag ,browse_tag , rand() as rand_num from masterkey_cust_recos_ib_v5;
 
drop table if exists customer_rank_shuffle_v2;
create table customer_rank_shuffle_v2 as
select *, rank() over(partition by masterkey order by rand_num) AS Rnk ,
(case when purchase_tag = 1 or browse_tag = 1 then 1 else 0 end) as Overall_tag from customer_rank_shuffle
order by masterkey, rnk;
 
 
drop table if exists customer_rank_shuffle_v3;
create table customer_rank_shuffle_v3 as
select *,(case when purchase_tag = 1 or browse_tag = 1 then 1.0/power(cast(2 as double),cast((rnk-1)/2 as double)) else 0.0 end) as Rankscore_p 
from customer_rank_shuffle_v2;
 
drop table if  exists customer_rank_shuffle_v4 ;
create table customer_rank_shuffle_v4 as
select masterkey , sum(Rankscore_p) as Rankscore_p, sum(Overall_tag) as No_Hits from customer_rank_shuffle_v3
group by masterkey;
 
drop table if  exists customer_rank_shuffle_v5 ;
create table customer_rank_shuffle_v5 as
select transform(masterkey, Rankscore_p, no_hits) using 'python rankscore2.py' as masterkey, Rankscore_p, no_hits, Rankscore_max 
from customer_rank_shuffle_v4 ;

 
select sum(Rankscore_p)/sum(Rankscore_max) as overall_Rankscore from customer_rank_shuffle_v5;
 
--0.21546546675128342



