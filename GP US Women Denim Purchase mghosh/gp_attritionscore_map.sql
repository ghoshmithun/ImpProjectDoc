set mapred.job.queue.name=aa;  
set hive.cli.print.header=true;
--set purch_window_start_date = '2016-07-22';

select max(processingdate)from evergreenattribute.gpattritionattribute; 
--2017-08-02


drop table if exists sruidas.evergreen_gpattrition;
create table sruidas.evergreen_gpattrition as
select customer_key,householdkey,medianscore
from evergreenattribute.gpattritionattribute
lateral view explode(customerkey) customerkey AS customer_key
where processingdate='2017-08-02' and brand='GP' and market='US';

select count(*) from sruidas.evergreen_gpattrition;
--8340478

drop table if exists  mghosh.gp_attrition_household;
create table mghosh.gp_attrition_household as
select a.*, b.household_key from sruidas.evergreen_gpattrition a 
left outer join (select distinct customer_key , household_key from mds_new.ods_corp_cust_t) b 
on a.customer_key=b.customer_key;

drop table if exists  mghosh.gp_attrition_household_zip;
create table mghosh.gp_attrition_household_zip as
select a.*, b.addr_zip_code
from mghosh.gp_attrition_household  a left outer join (select distinct household_key,addr_zip_code  from mds_new.ods_household_t where ctry_cd = 'US' ) b 
on a.household_key=b.household_key;

/* drop table if exists mghosh.map_cat_attr_collect ;
create table mghosh.map_cat_attr_collect as
select *, (case when ('WOMENS_DENIM' in explode(category)) then 1 when  ('DENIM' in explode(category) )then 2 else 0 end)  as purchase_categories,
(case when ('WOMENS_DENIM' in explode(attribute)) then 1 when  ('DENIM' in explode(attribute)) then 2 else 0 end)  as prospective_purchase from sruidas.map_cat_attr_collect ;

drop table if exists mghosh.map_cat_attr_collect ;
create table mghosh.map_cat_attr_collect as
select customer_key, category , attribute from sruidas.map_cat_attr_collect
LATERAL VIEW explode(category , attribute) ;

SELECT customer_key ,
e_category,
e_attribute
FROM   sruidas.map_cat_attr_collect
LATERAL VIEW posexplode(category) e_category  as seqp, e_category
LATERAL VIEW posexplode(attribute) e_attribute  as seqc, e_attribute
WHERE seqp = seqc; */

drop table if exists mghosh.map_cat_attr_collect ;
create table mghosh.map_cat_attr_collect as
SELECT * FROM sruidas.map_cat_attr_collect
LATERAL VIEW explode(category) myTable1 AS purchase_categories
LATERAL VIEW explode(attribute) myTable2 AS pros_categories;

drop table if exists mghosh.map_cat_attr_collect2 ;
create table mghosh.map_cat_attr_collect2 as
select distinct customer_key, (case when purchase_categories="womens_denim" then 1 else 0 end ) as womens_denim_shopper, 
(case when pros_categories="WOMENS_DENIM" then 1 else 0 end ) as womens_denim_prospective from mghosh.map_cat_attr_collect;

drop table if exists mghosh.gp_final_table ;
create external table mghosh.gp_final_table 
(customer_key bigint,
addr_zip_code string,
household_key bigint,
medianscore double,
womens_denim_shopper  int ,
womens_denim_prospective  int )
row format delimited 
fields terminated by '|' 
lines terminated by '\n' 
location '/user/mghosh/gp_analysis'
TBLPROPERTIES ("serialization.null.format"=""); 

insert overwrite table mghosh.gp_final_table
select distinct a.customer_key, a.addr_zip_code, a.household_key, a.medianscore , b.womens_denim_shopper , b.womens_denim_prospective
from mghosh.gp_attrition_household_zip a left outer  join mghosh.map_cat_attr_collect2 b 
on a.customer_key=b.customer_key;


hadoop fs -getmerge /user/mghosh/gp_analysis/ /home/mghosh/gp_analysis/gp_womens_denim_analysis.txt