SET CURRENT_BRAND = ON;  SET DATEINDEX = 20151201;  SET DATE_NOW = 2015-12-01; SET CURRENT_COUNTRY=US;
SET CURRENT_BRAND = BR;  SET DATEINDEX = 20151201;  SET DATE_NOW = 2015-12-01; SET CURRENT_COUNTRY=US;
SET CURRENT_BRAND = GP;  SET DATEINDEX = 20151201;  SET DATE_NOW = 2015-12-01; SET CURRENT_COUNTRY=US;

---creating training data and predictor variables
-- creating table for active customers

drop table if exists sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_US_ACTIVE_CUSTOMERS;
create table sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_US_ACTIVE_CUSTOMERS as
select CUST_KEY as CUSTOMER_KEY, min(ACQUISITION_DATE) as ACQUISITION_DATE
from MDS_New.ODS_BRAND_CUSTOMER_T
where 
BRAND='${hiveconf:CURRENT_BRAND}' and
COUNTRY='${hiveconf:CURRENT_COUNTRY}' and
ACQUISITION_DATE is not null and 
UNIX_TIMESTAMP(ACQUISITION_DATE, 'yyyy-MM-dd_HH-mm-ss_Z') < UNIX_TIMESTAMP('${hiveconf:DATE_NOW}','yyyy-MM-dd')
group by CUST_KEY;

--SELECT COUNT(*) FROM sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_US_ACTIVE_CUSTOMERS;
--49702701

--pulling customer keys based on order status
drop table if exists sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_US_ACTIVE_CUSTOMERS1;
create table sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_US_ACTIVE_CUSTOMERS1 as
select distinct t1.customer_key,t2.acquisition_date
from mds_new.ods_orderheader_t t1
inner join 
sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_US_ACTIVE_CUSTOMERS t2
on t1.customer_key=t2.customer_key
where
T1.COUNTRY='${hiveconf:CURRENT_COUNTRY}' AND
T1.BRAND='${hiveconf:CURRENT_BRAND}' AND
T1.TRANSACTION_TYPE_CD IN ('S','M','R') AND
(
(
 T1.ORDER_STATUS='R' AND
 UNIX_TIMESTAMP(T1.SHIP_DATE,'yyyy-MM-dd') BETWEEN
 UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}', - 365), 'yyyy-MM-dd') AND 
 UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}', - 1), 'yyyy-MM-dd')
 ) OR
(
 T1.ORDER_STATUS='O' AND
 UNIX_TIMESTAMP(T1.DEMAND_DATE, 'yyyy-MM-dd_HH-mm-ss_Z') BETWEEN
 UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}', - 365), 'yyyy-MM-dd') AND 
 UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}', - 1), 'yyyy-MM-dd')
)
);

--SELECT COUNT(*) FROM sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_US_ACTIVE_CUSTOMERS1;
--26500736



--drop table if exists sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_US_ACTIVE_CUSTOMERS2;
--create table sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_US_ACTIVE_CUSTOMERS as
--select t1.customer_key,t2.acquisition_date
--from sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_US_ACTIVE_CUSTOMERS1 t1
--inner join 
--sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_US_ACTIVE_CUSTOMERS t2
--on t1.customer_key=t2.customer_key;



---adding columns such as transaction #,order # ,order dates and seperating cases of online and retail purchases

DROP TABLE IF EXISTS sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_LTV1;
CREATE TABLE sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_LTV1 AS SELECT
T1.CUSTOMER_KEY,T2.ACQUISITION_DATE, 
T1.TRANSACTION_NUM, 
T1.ORDER_NUM, 
T1.TRANSACTION_TYPE_CD, 
T1.ORDER_STATUS, 
T1.DEMAND_DATE, 
T1.SHIP_DATE,
(
CASE 
WHEN T1.ORDER_STATUS = 'O' THEN T1.ORDER_NUM 
WHEN T1.ORDER_STATUS = 'R' THEN CAST(T1.TRANSACTION_NUM AS STRING) 
END
) AS UNIQUE_TRANSACTION_NUM,
(
CASE 
WHEN T1.ORDER_STATUS = 'O' THEN TO_DATE(T1.DEMAND_DATE)
WHEN T1.ORDER_STATUS = 'R' THEN T1.SHIP_DATE 
END
) AS PURCHASE_DATE,
(
CASE
WHEN T1.CUSTOMER_KEY > 0 THEN DATE_ADD('${hiveconf:DATE_NOW}', - 365)
ELSE NULL
END
) AS START_DT,
(
CASE
WHEN T1.CUSTOMER_KEY > 0 THEN DATE_ADD('${hiveconf:DATE_NOW}', - 1)
ELSE NULL
END
) AS END_DT
FROM
MDS_NEW.ODS_ORDERHEADER_T T1
INNER JOIN
sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_US_ACTIVE_CUSTOMERS1 T2
ON T1.CUSTOMER_KEY = T2.CUSTOMER_KEY
WHERE
T1.COUNTRY='${hiveconf:CURRENT_COUNTRY}' AND
T1.BRAND='${hiveconf:CURRENT_BRAND}' AND
T1.TRANSACTION_TYPE_CD IN ('S','M','R') AND 
(
(T1.ORDER_STATUS='R' AND
 UNIX_TIMESTAMP(T1.SHIP_DATE,'yyyy-MM-dd') BETWEEN
 UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}', - 365), 'yyyy-MM-dd') AND 
 UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}', - 1), 'yyyy-MM-dd')
 ) OR
(T1.ORDER_STATUS='O' AND
 UNIX_TIMESTAMP(T1.DEMAND_DATE, 'yyyy-MM-dd_HH-mm-ss_Z') BETWEEN
 UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}', - 365), 'yyyy-MM-dd') AND 
 UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}', - 1), 'yyyy-MM-dd')
)
);
--select count(*), count(distinct transaction_num) from sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_LTV1;
--95637971        95637971



----pulling sales amount,item_qty

DROP TABLE IF EXISTS sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_LTV2;
create table sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_LTV2 as select 
T1.*, 
T2.line_num, 
COALESCE(T2.SALES_AMT, 0) AS SALES_AMT, 
COALESCE(T2.ITEM_QTY, 0) AS ITEM_QTY
from sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_LTV1 T1
INNER JOIN MDS_NEW.ODS_ORDERLINE_T T2
ON T1.TRANSACTION_NUM = T2.TRANSACTION_NUM
AND T1.CUSTOMER_KEY = T2.CUSTOMER_KEY;


--calculating discount 
drop table if exists sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_LTV2_1;
create table sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_LTV2_1 as select 
T2.customer_key, T2.transaction_num, T2.line_num,sum(T1.Discount_Amt) as DISCOUNT_AMT
from  mds_new.ods_orderline_discounts_t T1
inner join sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_LTV2 T2
ON T1.transaction_num = T2.transaction_num
and T1.line_num=T2.line_num
and T1.customer_key=T2.customer_key
group by T2.customer_key, T2.transaction_num,T2.line_num;


--calculating net sales
drop table if exists sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_LTV3;
create table sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_LTV3 as select 
T2.*,COALESCE(T1.DISCOUNT_AMT, 0) as DISCOUNT_AMT, (T2.SALES_AMT+COALESCE(T1.DISCOUNT_AMT, 0)) as net_sales
from  sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_LTV2_1 T1
right outer join sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_LTV2 T2
ON T1.transaction_num = T2.transaction_num
and T1.line_num=T2.line_num
and T1.customer_key=T2.customer_key;


--subsetting customers who have net sales positive and have atleast purchased
DROP TABLE IF EXISTS sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_LTV5;
CREATE TABLE sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_LTV5 as
select * from sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_LTV3
where TRANSACTION_TYPE_CD in ('S','M') and net_sales >= 0;


--finding first purchase date
DROP TABLE IF EXISTS sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_LTV_CALIB_FPDT;
CREATE TABLE sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_LTV_CALIB_FPDT
AS SELECT CUSTOMER_KEY, MIN(PURCHASE_DATE) AS FIRST_PURCHASE_DATE
FROM sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_LTV5
group by customer_key;


--merging table LTV5 and LTV_CaLIb_FPDT to add first purchase date
DROP TABLE IF EXISTS sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_LTV_CALIB_V2;
CREATE TABLE sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_LTV_CALIB_V2
AS SELECT T1.*, T2.FIRST_PURCHASE_DATE
FROM sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_LTV5 T1
left outer join sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_LTV_CALIB_FPDT T2
on T1.CUSTOMER_KEY = T2.CUSTOMER_KEY;



--calculating sales ,discount, item qty but excluding first purchasen information
DROP TABLE IF EXISTS sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_LTV_CALIB_V3;
CREATE TABLE sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_LTV_CALIB_V3
AS SELECT
T1.*,
(
CASE 
WHEN T1.PURCHASE_DATE = T1.FIRST_PURCHASE_DATE THEN CAST(0 AS FLOAT)
ELSE T1.SALES_AMT
END
) AS LTV_SALES_AMT,
(
CASE 
WHEN T1.PURCHASE_DATE = T1.FIRST_PURCHASE_DATE THEN CAST(0 AS DOUBLE)
ELSE T1.DISCOUNT_AMT
END
) AS LTV_DISCOUNT_AMT,
(
CASE 
WHEN T1.PURCHASE_DATE = T1.FIRST_PURCHASE_DATE THEN 0L
ELSE T1.ITEM_QTY
END
) AS LTV_ITEM_QTY
FROM
sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_LTV_CALIB_V2 T1;


--creating required variables but excluding first purchase information
DROP TABLE IF EXISTS sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_LTV_CALIB_V4;
CREATE TABLE sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_LTV_CALIB_V4
AS SELECT 
T1.CUSTOMER_KEY,min(T1.acquisition_date) as acquisition_date,
MIN(T1.START_DT) AS START_DT,
MAX(T1.END_DT) AS END_DT,
MIN(T1.PURCHASE_DATE) AS MIN_PURCHASE_DATE, 
MAX(T1.PURCHASE_DATE) AS MAX_PURCHASE_DATE, 
COUNT(DISTINCT T1.PURCHASE_DATE) AS TRANSACTION_DAYS, 
COUNT(DISTINCT T1.PURCHASE_DATE) - 1 AS REPEAT_TRANSACTION_DAYS,
COUNT(DISTINCT UNIQUE_TRANSACTION_NUM) AS TRANSACTIONS,
COUNT(DISTINCT UNIQUE_TRANSACTION_NUM) - 1 AS REPEAT_TRANSACTIONS,
SUM(T1.SALES_AMT) + SUM(T1.DISCOUNT_AMT) AS SPEND,
SUM(T1.LTV_SALES_AMT) + SUM(T1.LTV_DISCOUNT_AMT) AS REPEAT_SPEND 
FROM 
sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_LTV_CALIB_V3 T1 
GROUP BY T1.CUSTOMER_KEY;


--creating recency and cohort
DROP TABLE IF EXISTS sruidas${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_LTV_CALIB_V5;
CREATE TABLE sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_LTV_CALIB_V5
AS SELECT 
T1.*,
DATEDIFF(T1.MAX_PURCHASE_DATE, T1.MIN_PURCHASE_DATE) / 7 AS CUSTOMER_DURATION,
DATEDIFF(T1.END_DT, T1.MIN_PURCHASE_DATE) / 7 AS STUDY_DURATION
FROM 
sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_LTV_CALIB_V4 T1;


--creating average spend
DROP TABLE IF EXISTS sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_LTV_CALIB_V6;
CREATE TABLE sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_LTV_CALIB_V6
AS SELECT 
T1.*,
(
CASE 
WHEN REPEAT_TRANSACTION_DAYS > 0 THEN REPEAT_SPEND / REPEAT_TRANSACTION_DAYS
ELSE CAST(0 AS DOUBLE)
END
) AS REPEAT_AVG_SPEND
FROM
sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_LTV_CALIB_V5 T1;



exporting training dataset
drop table if exists sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_LTV_CALIB_V6_txt;

create external table sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_LTV_CALIB_V6_txt
(customer_key bigint,
acquisition_date string,
start_dt string,
end_dt string,
min_purchase_date string,
max_purchase_date string,
transaction_days bigint,
repeat_transaction_days bigint,
transactions bigint,
repeat_transactions bigint,
spend double,
repeat_spend double,
customer_duration double,
study_duration double,
repeat_avg_spend double)

row format delimited 
fields terminated by '|' 
lines terminated by '\n' 
location '/user/sruidas/${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_LTV_CALIB_V6_txt'
TBLPROPERTIES('serialization.null.format'='');

insert overwrite table sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_LTV_CALIB_V6_txt
select * from sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_LTV_CALIB_V6;

hadoop fs -getmerge /user/sruidas/ON_20151201_LTV_CALIB_V6_txt /home/sruidas


--creating validation data
-----adding columns such as transaction #,order # ,order dates and seperating cases of online and retail purchases

DROP TABLE IF EXISTS sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_VALID_V1;
CREATE TABLE sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_VALID_V1 AS SELECT
T1.CUSTOMER_KEY, 
T1.TRANSACTION_NUM, 
T1.ORDER_NUM, 
T1.TRANSACTION_TYPE_CD, 
T1.ORDER_STATUS, 
T1.DEMAND_DATE, 
T1.SHIP_DATE

(
CASE 
WHEN T1.ORDER_STATUS = 'O' THEN T1.ORDER_NUM 
WHEN T1.ORDER_STATUS = 'R' THEN CAST(T1.TRANSACTION_NUM AS STRING) 
END
) AS UNIQUE_TRANSACTION_NUM,
(
CASE 
WHEN T1.ORDER_STATUS = 'O' THEN TO_DATE(T1.DEMAND_DATE)
WHEN T1.ORDER_STATUS = 'R' THEN T1.SHIP_DATE 
END
) AS PURCHASE_DATE
FROM
sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_LTV_CALIB_V6 T2
LEFT OUTER JOIN
MDS_NEW.ODS_ORDERHEADER_T T1
ON T1.CUSTOMER_KEY = T2.CUSTOMER_KEY
WHERE
T1.COUNTRY='${hiveconf:CURRENT_COUNTRY}' AND
T1.BRAND='${hiveconf:CURRENT_BRAND}' AND
T1.TRANSACTION_TYPE_CD IN ('S','M') AND
(
(
 T1.ORDER_STATUS='R' AND
 UNIX_TIMESTAMP(T1.SHIP_DATE,'yyyy-MM-dd') BETWEEN
 UNIX_TIMESTAMP('${hiveconf:DATE_NOW}', 'yyyy-MM-dd') and UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}', 366), 'yyyy-MM-dd') 
 ) OR
(
 T1.ORDER_STATUS='O' AND
 UNIX_TIMESTAMP(T1.DEMAND_DATE, 'yyyy-MM-dd_HH-mm-ss_Z') BETWEEN
 UNIX_TIMESTAMP('${hiveconf:DATE_NOW}', 'yyyy-MM-dd') and UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}', 366), 'yyyy-MM-dd') 
)
);


----pulling sales amount,item_qty

DROP TABLE IF EXISTS sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_VALID_V2;
create table sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_VALID_V2 as select 
T1.*, 
T2.line_num, 
COALESCE(T2.SALES_AMT, 0) AS SALES_AMT, 
COALESCE(T2.ITEM_QTY, 0) AS ITEM_QTY
from sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_VALID_V1 T1
INNER JOIN MDS_NEW.ODS_ORDERLINE_T T2
ON T1.TRANSACTION_NUM = T2.TRANSACTION_NUM
AND T1.CUSTOMER_KEY = T2.CUSTOMER_KEY;

---calculating discount 
drop table if exists sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_VALID_V2_1;
create table sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_VALID_V2_1 as select 
T2.customer_key, T2.transaction_num, T2.line_num,sum(T1.Discount_Amt) as DISCOUNT_AMT
from  mds_new.ods_orderline_discounts_t T1
inner join sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_VALID_V2 T2
ON T1.transaction_num = T2.transaction_num
and T1.line_num=T2.line_num
and T1.customer_key=T2.customer_key
group by T2.customer_key, T2.transaction_num,T2.line_num;

---calculating net sales
drop table if exists sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_VALID_V3;
create table sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_VALID_V3 as select 
T2.*,COALESCE(T1.DISCOUNT_AMT, 0) as DISCOUNT_AMT, (T2.SALES_AMT+COALESCE(T1.DISCOUNT_AMT, 0)) as net_sales
from  sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_VALID_V2_1 T1
right outer join sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_VALID_V2 T2
ON T1.transaction_num = T2.transaction_num
and T1.line_num=T2.line_num
and T1.customer_key=T2.customer_key;


--creating no of transaction days for keys with net sales positive
DROP TABLE IF EXISTS sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_VALID_V5;
CREATE TABLE sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_VALID_V5
AS SELECT 
T1.CUSTOMER_KEY
COUNT(DISTINCT(T1.PURCHASE_DATE)) AS TRANSACTION_DAYS,
SUM(T1.net_sales) AS SPEND
from sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_VALID_V3 T1 
WHERE T1.net_sales>=0
GROUP BY T1.CUSTOMER_KEY;


--creating average spend
DROP TABLE IF EXISTS sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_VALID_V6;
CREATE TABLE sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_VALID_V6
AS SELECT T1.*,(T1.SPEND/T1.TRANSACTION_DAYS) as AVG_SPEND
from sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_VALID_V5 T1;


--subsetting common keys between training and validation
DROP TABLE IF EXISTS sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_VALID;
CREATE TABLE sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_VALID
AS SELECT 
T2.*
FROM  sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_LTV_CALIB_V6 T1
INNER JOIN 
sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_VALID_V6 T2
ON T1.CUSTOMER_KEY=T2.CUSTOMER_KEY;



--exporting validation data 
drop table if exists sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_VALID_txt;

create external table sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_VALID_txt
(customer_key bigint,
transaction_days bigint,
spend double,
avg_spend double)

row format delimited 
fields terminated by '|' 
lines terminated by '\n' 
location '/user/sruidas/${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_VALID_txt'
TBLPROPERTIES('serialization.null.format'='');

insert overwrite table sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_VALID_txt
select * from sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_VALID;

hadoop fs -getmerge /user/sruidas/ON_20151201_VALID_txt /home/sruidas



--return calibration  tables--
--LTV3 for which net sales is negative and there is at least a return 
DROP TABLE IF EXISTS sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_Return1;
CREATE TABLE sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_Return1 as
select * from sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_LTV3
where TRANSACTION_TYPE_CD in ('R','M') and net_sales < 0;

--finding first purchase date
DROP TABLE IF EXISTS sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_Return2;
CREATE TABLE sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_Return2
AS SELECT
A.CUSTOMER_KEY, MIN(A.PURCHASE_DATE) AS FIRST_PURCHASE_DATE
FROM sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_Return1 A
group by A.customer_key;


--joining first purcahse date column to return2
DROP TABLE IF EXISTS sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_Return3;
CREATE TABLE sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_Return3
AS SELECT
T1.*, T2.FIRST_PURCHASE_DATE
FROM
sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_Return1 T1
LEFT OUTER JOIN
sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_Return2 T2
ON T1.CUSTOMER_KEY = T2.CUSTOMER_KEY;

--creating variables needed for modeling but excluding first purchase information
DROP TABLE IF EXISTS sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_Return4;
CREATE TABLE sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_Return4
AS SELECT
T1.*,
(
CASE 
WHEN T1.PURCHASE_DATE = T1.FIRST_PURCHASE_DATE THEN CAST(0 AS FLOAT)
ELSE T1.SALES_AMT
END
) AS LTV_SALES_AMT,
(
CASE 
WHEN T1.PURCHASE_DATE = T1.FIRST_PURCHASE_DATE THEN CAST(0 AS DOUBLE)
ELSE T1.DISCOUNT_AMT
END
) AS LTV_DISCOUNT_AMT,
(
CASE 
WHEN T1.PURCHASE_DATE = T1.FIRST_PURCHASE_DATE THEN 0L
ELSE T1.ITEM_QTY
END
) AS LTV_ITEM_QTY
FROM
sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_Return3 T1;


---creating variables needed for modeling but excluding first purchase information
DROP TABLE IF EXISTS sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_Return5;
CREATE TABLE sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_Return5
AS SELECT 
T1.CUSTOMER_KEY,min(acquisition_date) as acquisition_date,
MIN(T1.START_DT) AS START_DT,
MAX(T1.END_DT) AS END_DT,
MIN(T1.PURCHASE_DATE) AS MIN_PURCHASE_DATE, 
MAX(T1.PURCHASE_DATE) AS MAX_PURCHASE_DATE, 
COUNT(DISTINCT T1.PURCHASE_DATE) AS TRANSACTION_DAYS, 
COUNT(DISTINCT T1.PURCHASE_DATE) - 1 AS REPEAT_TRANSACTION_DAYS,
COUNT(DISTINCT UNIQUE_TRANSACTION_NUM) AS TRANSACTIONS,
COUNT(DISTINCT UNIQUE_TRANSACTION_NUM) - 1 AS REPEAT_TRANSACTIONS,
SUM(T1.SALES_AMT) + SUM(T1.DISCOUNT_AMT) AS SPEND,
SUM(T1.LTV_SALES_AMT) + SUM(T1.LTV_DISCOUNT_AMT) AS REPEAT_SPEND 
FROM 
sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_Return4 T1 
GROUP BY T1.CUSTOMER_KEY;

--creating recency cohort
DROP TABLE IF EXISTS sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_Return6;
CREATE TABLE sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_Return6
AS SELECT 
T1.*,
DATEDIFF(T1.MAX_PURCHASE_DATE, T1.MIN_PURCHASE_DATE) / 7 AS CUSTOMER_DURATION,
DATEDIFF(T1.END_DT, T1.MIN_PURCHASE_DATE) / 7 AS STUDY_DURATION
FROM 
sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_Return5 T1;

--creating avg spend
DROP TABLE IF EXISTS sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_Return7;
CREATE TABLE sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_Return7
AS SELECT 
T1.*,
(
CASE 
WHEN REPEAT_TRANSACTION_DAYS > 0 THEN REPEAT_SPEND / REPEAT_TRANSACTION_DAYS
ELSE CAST(0 AS DOUBLE)
END
) AS REPEAT_AVG_SPEND
FROM
sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_Return6 T1;


--exporting validation data
DROP TABLE IF EXISTS sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_Return8;
CREATE TABLE sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_Return8
AS SELECT
customer_key ,acquisition_date,
start_dt ,
end_dt ,
min_purchase_date ,
max_purchase_date ,
transaction_days ,
repeat_transaction_days ,
transactions ,
repeat_transactions ,
spend*-1 as spend,
repeat_spend*-1 as repeat_spend,
customer_duration ,
study_duration ,
repeat_avg_spend*-1 as repeat_avg_spend
from
sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_Return7;


drop table if exists sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_RETURN_calib;

create external table sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_RETURN_calib
(customer_key bigint,
acquisition_date string,
start_dt string,
end_dt string,
min_purchase_date string,
max_purchase_date string,
transaction_days bigint,
repeat_transaction_days bigint,
transactions bigint,
repeat_transactions bigint,
spend double,
repeat_spend double,
customer_duration double,
study_duration double,
repeat_avg_spend double)
row format delimited 
fields terminated by '|' 
lines terminated by '\n' 
location '/user/sruidas/${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_RETURN_calib'
TBLPROPERTIES('serialization.null.format'='');

insert overwrite table sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_RETURN_calib
select * from sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_Return8;

hadoop fs -getmerge /user/sruidas/ON_20151201_RETURN_calib /home/sruidas

--creating validation data for returns
--quite similar to earlier steps

DROP TABLE IF EXISTS sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_RETURN_VALID1;
CREATE TABLE sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_RETURN_VALID1 AS SELECT
T1.CUSTOMER_KEY, 
T1.TRANSACTION_NUM, 
T1.ORDER_NUM, 
T1.TRANSACTION_TYPE_CD, 
T1.ORDER_STATUS, 
T1.DEMAND_DATE, 
T1.SHIP_DATE,
(
CASE 
WHEN T1.ORDER_STATUS = 'O' THEN T1.ORDER_NUM 
WHEN T1.ORDER_STATUS = 'R' THEN CAST(T1.TRANSACTION_NUM AS STRING) 
END
) AS UNIQUE_TRANSACTION_NUM,
(
CASE 
WHEN T1.ORDER_STATUS = 'O' THEN TO_DATE(T1.DEMAND_DATE)
WHEN T1.ORDER_STATUS = 'R' THEN T1.SHIP_DATE 
END
) AS PURCHASE_DATE
FROM
sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_RETURN8 T2
LEFT OUTER JOIN
MDS_NEW.ODS_ORDERHEADER_T T1
ON T1.CUSTOMER_KEY = T2.CUSTOMER_KEY
WHERE
T1.COUNTRY='${hiveconf:CURRENT_COUNTRY}' AND
T1.BRAND='${hiveconf:CURRENT_BRAND}' AND
T1.TRANSACTION_TYPE_CD IN ('R','M') AND
(
(
 T1.ORDER_STATUS='R' AND
 UNIX_TIMESTAMP(T1.SHIP_DATE,'yyyy-MM-dd') BETWEEN
 UNIX_TIMESTAMP('${hiveconf:DATE_NOW}', 'yyyy-MM-dd') and UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}', 366), 'yyyy-MM-dd') 
 ) OR
(
 T1.ORDER_STATUS='O' AND
 UNIX_TIMESTAMP(T1.DEMAND_DATE, 'yyyy-MM-dd_HH-mm-ss_Z') BETWEEN
 UNIX_TIMESTAMP('${hiveconf:DATE_NOW}', 'yyyy-MM-dd') and UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}', 366), 'yyyy-MM-dd') 
)
);



DROP TABLE IF EXISTS sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_RETURN_VALID2;
create table sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_RETURN_VALID2 as select 
T1.*, 
T2.line_num, 
COALESCE(T2.SALES_AMT, 0) AS SALES_AMT, 
COALESCE(T2.ITEM_QTY, 0) AS ITEM_QTY
from sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_RETURN_VALID1 T1
INNER JOIN MDS_NEW.ODS_ORDERLINE_T T2
ON T1.TRANSACTION_NUM = T2.TRANSACTION_NUM
AND T1.CUSTOMER_KEY = T2.CUSTOMER_KEY;



drop table if exists sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_RETURN_VALID3;
create table sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_RETURN_VALID3 as select 
T2.customer_key, T2.transaction_num, T2.line_num,sum(T1.Discount_Amt) as DISCOUNT_AMT
from  mds_new.ods_orderline_discounts_t T1
inner join sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_RETURN_VALID2 T2
ON T1.transaction_num = T2.transaction_num
and t1.line_num=t2.line_num
and t1.customer_key=t2.customer_key
group by t2.customer_key, T2.transaction_num,t2.line_num;


DROP TABLE IF EXISTS sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_RETURN_VALID4;
create table sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_RETURN_VALID4 as select 
T2.*,COALESCE(T1.DISCOUNT_AMT, 0) as DISCOUNT_AMT, (T2.SALES_AMT+COALESCE(T1.DISCOUNT_AMT, 0)) as net_sales
from  sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_RETURN_VALID3 T1
right outer join sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_RETURN_VALID2 T2
ON T1.transaction_num = T2.transaction_num
and t1.line_num=t2.line_num
and t1.customer_key=t2.customer_key;


DROP TABLE IF EXISTS sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_RETURN_VALID5;
CREATE TABLE sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_RETURN_VALID5
AS SELECT 
T1.CUSTOMER_KEY,
COUNT(DISTINCT(T1.PURCHASE_DATE)) AS TRANSACTION_DAYS,
SUM(net_sales) AS SPEND,
SUM(net_sales)/COUNT(DISTINCT(T1.PURCHASE_DATE)) AS AVG_SPEND
from
sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_RETURN_VALID4 T1 
WHERE T1.net_sales<0
GROUP BY T1.CUSTOMER_KEY;



DROP TABLE IF EXISTS sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_RETURN_VALID6;
CREATE TABLE sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_RETURN_VALID6
AS SELECT 
T2.customer_key,
T2.transaction_days,
T2.spend*-1 as spend,
T2.avg_spend*-1 as avg_spend
FROM  sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_RETURN8 T1
INNER JOIN 
sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_RETURN_VALID5 T2
ON T1.CUSTOMER_KEY=T2.CUSTOMER_KEY;


drop table if exists sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_RETURN_VALID;

create external table sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_RETURN_VALID
(customer_key bigint,
transaction_days bigint,
spend double,
avg_spend double)

row format delimited 
fields terminated by '|' 
lines terminated by '\n' 
location '/user/sruidas/${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_RETURN_VALID'
TBLPROPERTIES('serialization.null.format'='');

insert overwrite table sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_RETURN_VALID
select * from sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_RETURN_VALID6;

--hadoop fs -getmerge /user/sruidas/ON_20151201_RETURN_VALID /home/sruidas


DROP TABLE IF EXISTS sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_Return_sales_calib;
CREATE TABLE sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_Return_sales_calib as select
T1.customer_key ,
T1.acquisition_date,
T1.transaction_days,
T1.repeat_transaction_days,
T1.spend,
T1.repeat_spend,
T1.customer_duration,
T1.study_duration,
T1.repeat_avg_spend,
(
case when T2.acquisition_date=T1.acquisition_date then T2.acquisition_date else T1.acquisition_date end) as acquisition_date_ret,
coalesce(T2.transaction_days,0) as transaction_days_ret,
coalesce(T2.repeat_transaction_days,0) as rep_transaction_days_ret,
coalesce(T2.spend,0) as spend_ret,
coalesce(T2.repeat_spend,0) as rep_spend_ret,
coalesce(T2.customer_duration ,0)as customer_duration_ret,
coalesce(T2.study_duration,0) as study_duration_ret,
coalesce(T2.repeat_avg_spend,0) as repeat_avg_spend_ret
from sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_LTV_CALIB_V6 T1
left outer join
sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_Return8 T2
on T1.customer_key=T2.customer_key;


drop table if exists sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_return_sales_calibfinal;

create external table sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_return_sales_calibfinal
(customer_key bigint,
acquisition_date string,
transaction_days bigint,
repeat_transaction_days bigint,
spend double,
repeat_spend double,
customer_duration double,
study_duration double,
repeat_avg_spend double,
acquisition_date_ret string,
transaction_days_ret bigint,
rep_transaction_days_ret bigint,
spend_ret double,
rep_spend_ret double,
customer_duration_ret double,
study_duration_ret double,
repeat_avg_spend_ret double
)

row format delimited 
fields terminated by '|' 
lines terminated by '\n' 
location '/user/sruidas/${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_return_sales_calibfinal'
TBLPROPERTIES('serialization.null.format'='');

insert overwrite table sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_return_sales_calibfinal
select * from sruidas.${hiveconf:CURRENT_BRAND}_${hiveconf:DATEINDEX}_Return_sales_calib;

--hadoop fs -getmerge /user/sruidas/ON_20151201_return_sales_calibfinal /home/sruidas