
DROP DATABASE IF EXISTS sdube_on CASCADE;
CREATE DATABASE sdube_on
LOCATION '/apps/hive/warehouse/aa_cem/sdube/sdube_on';


USE sdube_on;

set mapred.job.queue.name=cem;
SET start_date_brw=2015-10-01;
SET end_date_brw=2015-12-31;
SET brw_brand=on;
SET country=US;

select split(pagename,  '[:]')[0] from omniture.hit_data_t limit 20;
--BRING IN BROWSE
drop table if exists browse_data;
create table browse_data as select unk_shop_id, purchaseid, prop1, visid_high, visid_low, visit_num, pagename, date_time from omniture.hit_data_t where date_time between '${hiveconf:start_date_brw}'
and '${hiveconf:end_date_brw}' and split(pagename,  '[:]')[0] = '${hiveconf:brw_brand}' ;
--462,112,580
--1,375,009,653
--1,328,754,917 2015-10-01      2015-12-31





--now parse the data to get a list of unique style color codes for each ink shop id
DROP TABLE IF EXISTS unk_shop_id_styl_cd_0;
CREATE TABLE unk_shop_id_styl_cd_0 As 
SELECT unk_shop_id, visid_high, visid_low, visit_num, date_time 
FROM browse_data 
WHERE 
unk_shop_id!='' 
GROUP BY 
visid_high, visid_low, visit_num, unk_shop_id, date_time ;




DROP TABLE IF EXISTS unk_shop_id_styl_cd_1;
CREATE TABLE unk_shop_id_styl_cd_1 AS 
SELECT DISTINCT a.unk_shop_id, substr(a.prop1, 1, 6) AS style_cd, a.date_time 
FROM
(SELECT * FROM browse_data) a
INNER JOIN
(SELECT * FROM unk_shop_id_styl_cd_0) b
ON
a.unk_shop_id = b.unk_shop_id
;

DROP TABLE IF EXISTS unk_shop_id_styl_cd;
CREATE TABLE unk_shop_id_styl_cd AS 
SELECT DISTINCT unk_shop_id, style_cd, date_time 
FROM 
unk_shop_id_styl_cd_1 
WHERE 
style_cd !='' ;



--get all the keys FROM customer resolution
DROP TABLE IF EXISTS all_keys;
CREATE TABLE all_keys AS 
SELECT masterkey, emailkey, unknownshopperid, ecommerceid, customerkey 
FROM 
crmanalytics.customerresolution;


--append all the keys
DROP TABLE IF EXISTS master_brw;
CREATE TABLE master_brw AS 
SELECT a.masterkey, a.emailkey, a.unknownshopperid, a.ecommerceid, a.customerkey, b.* 
FROM 
(SELECT * FROM all_keys) a
INNER JOIN
(SELECT * FROM unk_shop_id_styl_cd) b
ON
a.unknownshopperid = b.unk_shop_id
;



--COMPUTE ITEM PREF BASED ON ENGAGEMENT IN BROWSE
--now parse the data to get list of style color codes for each unk shop id and the count of style color codes corresponding to the ink shop id
DROP TABLE IF EXISTS unk_shop_id_prop1;
CREATE TABLE unk_shop_id_prop1 AS 
SELECT unk_shop_id, prop1, count(*) over(partition by unk_shop_id, prop1) as cnt_prop1 
FROM 
browse_data
WHERE 
prop1 !='' ;

--join the unk_shop_id to masterkey 
DROP TABLE IF EXISTS masterkey_brw_prop1;
CREATE TABLE masterkey_brw_prop1 AS 
SELECT distinct b.masterkey, b.emailkey, b.ecommerceid, b.customerkey, a.unk_shop_id, a.prop1, a.cnt_prop1
FROM
(SELECT * FROM unk_shop_id_prop1) a
INNER JOIN
(SELECT * FROM master_brw) b
ON
a.unk_shop_id = b.unknownshopperid
;


--aggregate the prop1 counts by masterkey
--convert prop1 to style cd
DROP TABLE IF EXISTS masterkey_brw_prop1_cnts_0;
CREATE TABLE masterkey_brw_prop1_cnts_0 AS 
SELECT masterkey, substring(prop1, 1, 6) AS style_cd, sum(cnt_prop1) AS masterkey_cnt_prop1 
FROM  
masterkey_brw_prop1 
WHERE 
prop1 !='' and prop1 is not null
GROUP BY 
masterkey, prop1
;

DROP TABLE IF EXISTS masterkey_brw_prop1_cnts_1;
CREATE TABLE masterkey_brw_prop1_cnts_1 AS 
SELECT a.masterkey, b.emailkey, b.ecommerceid, b.customerkey, a.style_cd, a.masterkey_cnt_prop1, rand(100) as split 
FROM
(SELECT * FROM masterkey_brw_prop1_cnts_0) a
INNER JOIN
(SELECT * FROM masterkey_brw_prop1) b
ON
a.masterkey = b.masterkey
;


--fails because of size (now fixed)
DROP TABLE IF EXISTS masterkey_brw_prop1_cnts_ftr;
CREATE TABLE masterkey_brw_prop1_cnts_ftr AS 
SELECT distinct a.masterkey, a.emailkey, a.ecommerceid, a.customerkey, a.style_cd, a.masterkey_cnt_prop1, b.mdse_div_desc, b.mdse_dept_desc, b.mdse_class_desc, b.season_cd, b.season_desc 
FROM  
(SELECT * FROM masterkey_brw_prop1_cnts_1 where style_cd is not null ) a
INNER JOIN
(SELECT * FROM mds.ods_product_t) b
ON
a.style_cd=b.style_cd 
;
















