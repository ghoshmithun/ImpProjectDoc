USE sdube_on;

SET ib_input=/user/sdube/i;
SET good_styles=/user/sdube/g;
SET brand_flag=6;
SET inv_year_month=201512;
SET end_date_brw=2015-12-31;
SET end_date_inv=2015-12-31;
SET trans_end_date=2015-12-31;
SET brand=ON;
SET last_trans_dt=2015-12-31;

SET mapred.job.queue.name=cem;


--ib_input = location WHERE input data is sent
--good_styles = location WHERE good styles are sent
--master_brw_loc=location of master_brw_table 
--transactor_loc=location of transactor table


--COMBINE BROWSE & PURCHASE
DROP TABLE IF EXISTS  trans_brw_data_0;
CREATE TABLE trans_brw_data_0
(
masterkey STRING,
emailkey BIGINT,
ecommerceid BIGINT,
customerkey BIGINT,
style_cd STRING,
cnt INT,
mdse_div_desc STRING,
mdse_dept_desc STRING,
mdse_class_desc STRING,
season_cd STRING,
channel STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

INSERT OVERWRITE TABLE trans_brw_data_0 
SELECT masterkey, emailkey, ecommerceid, customer_key as customerkey, style_Cd, ln_qty as cnt, mdse_div_desc, mdse_dept_desc ,mdse_class_desc, season_cd,  CAST ('T' as STRING) AS channel 
FROM 
masterkey_purch_style_cd WHERE ln_qty > 0
;


INSERT INTO TABLE trans_brw_data_0 
SELECT masterkey, emailkey, ecommerceid, customerkey, style_Cd, masterkey_cnt_prop1 as cnt, mdse_div_desc, mdse_dept_desc ,mdse_class_desc, season_cd,  CAST ('B' as STRING) AS channel 
FROM 
masterkey_brw_prop1_cnts_ftr where style_Cd is not null and masterkey_cnt_prop1 > 0
;




DROP TABLE IF EXISTS  trans_brw_data;
CREATE TABLE trans_brw_data AS 
SELECT DISTINCT masterkey, emailkey, ecommerceid, customerkey, style_Cd, cnt, mdse_div_desc, mdse_dept_desc ,mdse_class_desc, season_cd 
FROM 
trans_brw_data_0;


--compute pref wts based on engagement
DROP TABLE IF EXISTS  style_cd_cnts;
CREATE TABLE style_cd_cnts AS 
SELECT style_cd, count(distinct masterkey) as stylecd_user_cnt 
FROM 
trans_brw_data 
GROUP BY 
style_Cd;


--compute the user based counts needed
DROP TABLE IF EXISTS  masterkey_brw_cnts;
CREATE TABLE masterkey_brw_cnts AS 
SELECT masterkey, style_cd, sum(cnt) as user_stylecd_cnt 
FROM 
trans_brw_data 
GROUP BY 
masterkey, style_cd;


--compute the engagement weights
DROP TABLE IF EXISTS  stylecd_wts_0;
CREATE TABLE stylecd_wts_0 AS 
SELECT a.masterkey, a.style_cd, a.user_stylecd_cnt, b.stylecd_user_cnt 
FROM 
(SELECT * FROM masterkey_brw_cnts) a
INNER JOIN
(SELECT * FROM style_cd_cnts) b
ON
a.style_Cd = b.style_cd
;


--distinct masterkeys
DROP TABLE IF EXISTS  masterkey_cnts_0;
create table masterkey_cnts_0 AS 
SELECT distinct masterkey 
FROM 
stylecd_wts_0;


--assign row number 1 to each row in the above table
DROP TABLE IF EXISTS  masterkey_cnts_1;
create table masterkey_cnts_1 AS 
SELECT *, rank() over(partition by masterkey) AS row_num_1 
FROM 
masterkey_cnts_0;


DROP TABLE IF EXISTS  masterkey_cnts_2;
create table masterkey_cnts_2 AS 
SELECT *, rank() over(partition by masterkey) AS row_num_1_1 
FROM 
masterkey_cnts_1;


DROP TABLE IF EXISTS  masterkey_cnts_3;
create table masterkey_cnts_3 AS 
SELECT masterkey, sum(row_num_1) over(partition by row_num_1_1) AS cnt_masterkey 
FROM 
masterkey_cnts_2;


--parametrize the total number of unique masterkeys (cnt_masterkey)
DROP TABLE IF EXISTS  stylecd_wts_1;
CREATE TABLE stylecd_wts_1 as 
SELECT b.masterkey, b.style_cd, b.user_stylecd_cnt, b.stylecd_user_cnt, a.cnt_masterkey, (b.user_stylecd_cnt* (log (a.cnt_masterkey)/b.stylecd_user_cnt)  )as wt FROM
(SELECT masterkey, cnt_masterkey FROM masterkey_cnts_3 ) a
INNER JOIN
(SELECT * FROM stylecd_wts_0 ) b
on
a.masterkey=b.masterkey
;


--RECENCY WT FOR TRANSACTION BEHAVIOR
--CREATE THE AVG_TRANS_DIFF VALUES FOR THE SAMPLE
DROP TABLE IF EXISTS  masterkey_ord_date;
CREATE TABLE masterkey_ord_date AS 
SELECT distinct masterkey, (CASE WHEN order_status=='O' THEN  order_num ELSE CAST(transaction_num as STRING) END) as my_trans, my_date 
FROM 
masterkey_purch_style_cd 
WHERE ln_qty>0
;

--compute the avg time betw trans 
--rank the transactions by masterkey and my_date
DROP TABLE IF EXISTS  masterkey_avg_trans_time_0;
CREATE TABLE masterkey_avg_trans_time_0 AS 
SELECT a.masterkey, a.my_trans, a.my_date, b.rank 
FROM
(SELECT * FROM masterkey_ord_date) a
INNER JOIN
(SELECT distinct masterkey, my_date, rank() over(partition by masterkey order by my_date) AS rank FROM masterkey_ord_date ) b
ON
(a.masterkey=b.masterkey AND a.my_date=b.my_date)
;


--change recency computation. Formula is exp^((-(max(x)-x))/(((max(x)-min(x))/2)*log(2))), where x is my_date per masterkey
DROP TABLE IF EXISTS  masterkey_avg_trans_time_1;
CREATE TABLE masterkey_avg_trans_time_1 AS
SELECT *, max(my_date) over(partition by masterkey) as max_my_date_mastkey,min(my_date) over(partition by masterkey) as min_my_date_mastkey
FROM
masterkey_avg_trans_time_0
;

DROP TABLE IF EXISTS masterkey_avg_trans_time_2;
CREATE TABLE masterkey_avg_trans_time_2 AS
SELECT *, exp(-(datediff(max_my_date_mastkey,my_date))/(((datediff(max_my_date_mastkey, min_my_date_mastkey))/2)*log(2)))as recency_wt
FROM
masterkey_avg_trans_time_1
;

DROP TABLE IF EXISTS masterkey_avg_trans_time_3;
CREATE TABLE masterkey_avg_trans_time_3 AS
SELECT masterkey, my_trans, my_date, rank, max_my_date_mastkey, min_my_date_mastkey, 
(CASE WHEN recency_wt=='NaN' THEN 0.0 ELSE recency_wt END) as recency_wt
FROM
masterkey_avg_trans_time_2
;


DROP TABLE IF EXISTS  recency_trans_0;
CREATE TABLE recency_trans_0 AS 
SELECT a.*, b.recency_wt 
FROM
(SELECT * FROM masterkey_purch_style_cd WHERE ln_qty>0) a
INNER JOIN
(SELECT * FROM masterkey_avg_trans_time_3) b
ON
(a.masterkey = b.masterkey and 
 a.my_date=b.my_date)
;


--aggregate the recency wt for each masterkry for each style code
DROP TABLE IF EXISTS  recency_wght_1;
CREATE TABLE recency_wght_1 AS 
SELECT masterkey, style_Cd, sum(recency_wt) over(partition by masterkey, style_cd) as recency_wt 
FROM
recency_trans_0
;


DROP TABLE IF EXISTS  recency_wght_2;
CREATE TABLE recency_wght_2 AS 
SELECT DISTINCT masterkey, style_Cd, COALESCE(recency_wt, 0) as recency_wt 
FROM
recency_wght_1
;

--now add this to the trans+browse engagement weight
DROP TABLE IF EXISTS  masterkey_recency_item_pref_0;
CREATE TABLE masterkey_recency_item_pref_0 AS 
SELECT a.masterkey, a.style_cd, a.wt as item_pref_wt,COALESCE(b.recency_wt, 0) as recency_wt 
FROM
(SELECT * FROM stylecd_wts_1) a
LEFT OUTER JOIN
(SELECT * FROM recency_wght_2) b
ON
(a.masterkey = b.masterkey AND a.style_cd=b.style_cd)
;


DROP TABLE IF EXISTS  masterkey_recency_item_pref_1;
CREATE TABLE masterkey_recency_item_pref_1 AS 
SELECT distinct masterkey, style_cd, item_pref_wt, recency_wt 
FROM
masterkey_recency_item_pref_0
;


--combine engagement based and recency based
DROP TABLE IF EXISTS  masterkey_recency_item_pref_2;
CREATE TABLE masterkey_recency_item_pref_2 AS 
SELECT masterkey, style_Cd, item_pref_wt, recency_wt, 
    (CASE WHEN recency_wt  !=0.00 THEN sqrt(item_pref_wt* recency_wt) 
        ELSE sqrt(item_pref_wt) 
    END) as final_wt 
FROM
masterkey_recency_item_pref_1
;

--CLEAN UP FINAL TABLE. AGGREGATE IT TO ONE KEY LEVEL

--clean up the array format in the above table
DROP TABLE IF EXISTS  input_data_SET_0;
CREATE TABLE input_data_SET_0 AS 
SELECT DISTINCT masterkey, CAST(style_cd as INT) as  style_cd,   final_wt as pref_value
FROM
masterkey_recency_item_pref_2 
;

--masterkey mapping
DROP TABLE IF EXISTS masterkey_map_0;
CREATE TABLE masterkey_map_0 AS
SELECT masterkey, rank() over(partition by row_num_1 order by masterkey) as masterkey_map
FROM
masterkey_cnts_1
WHERE masterkey IS NOT NULL
;



--join the mapping to the input data set


DROP TABLE IF EXISTS  input_data_SET_1;
CREATE TABLE input_data_SET_1 AS 
SELECT a.masterkey, a.style_cd, a.pref_value, b.masterkey_map
FROM
(SELECT * FROM input_data_SET_0) a
INNER JOIN
(SELECT * FROM masterkey_map_0 ) b
ON
a.masterkey=b.masterkey
;

DROP TABLE IF EXISTS input_data_SET_2;
CREATE TABLE input_data_SET_2 AS 
SELECT DISTINCT masterkey_map , style_cd, pref_value
FROM
input_data_SET_1
WHERE 
style_Cd IS NOT NULL
;



--CREATE INPUT FOR REC ALGORITHM
DROP TABLE IF EXISTS  entire_cust_csv;
CREATE TABLE entire_cust_csv 
(masterkey_map INT,
style_cd INT,
pref_value DOUBLE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
LOCATION '${hiveconf:ib_input}';

INSERT OVERWRITE TABLE entire_cust_csv 
SELECT masterkey_map , style_cd,  pref_value
FROM 
input_data_SET_2 
WHERE style_cd IS NOT NULL 
;


--CREATE INPUT FOR THE good style codes (not suppressed ones)
--aggregate the suppressed style codes
DROP TABLE IF EXISTS  style_cd_desc;
CREATE TABLE style_cd_desc AS 
SELECT DISTINCT mdse_div_desc, mdse_dept_desc, mdse_class_desc, style_cd 
FROM 
trans_brw_data;

--now drop the style codes which come under Underwear/socks/tights/bras
DROP TABLE IF EXISTS  good_style_cd_0;
CREATE TABLE good_style_cd_0 AS 
SELECT DISTINCT mdse_div_desc, mdse_dept_desc, mdse_class_desc, style_cd  
FROM 
style_cd_desc
WHERE 
( 
    !(mdse_div_desc RLIKE 'SOCKS') AND !(mdse_dept_desc RLIKE 'SOCKS') AND !(mdse_class_desc RLIKE 'SOCKS')  AND !(mdse_div_desc RLIKE 'TIGHTS') AND !(mdse_dept_desc RLIKE 'TIGHTS') AND !(mdse_class_Desc RLIKE 'TIGHTS') AND !(mdse_div_desc RLIKE 'UNDERWEAR') AND !(mdse_dept_desc RLIKE 'UNDERWEAR') AND !(mdse_class_Desc RLIKE 'UNDERWEAR')  AND !(mdse_div_desc RLIKE 'BOXER') AND !(mdse_dept_desc RLIKE 'BOXER') AND !(mdse_class_Desc RLIKE 'BOXER')
    AND !(mdse_dept_desc RLIKE 'BRAS' ) AND !(mdse_class_desc RLIKE 'BRAS') 
) 
;

DROP TABLE IF EXISTS  good_style_cd_1;
CREATE TABLE good_style_cd_1 AS 
SELECT distinct style_cd  
FROM 
good_style_cd_0
;

--incorporate the brand data FROM VENUS.sku_dim. Get the opr_sz_cd for the style codes in the above table.
DROP TABLE IF EXISTS  good_style_cd_2;
CREATE TABLE good_style_cd_2 AS 
SELECT a.style_cd, b.long_sz_desc, b.opr_sty_cd, b.opr_sty_clr_cd, b.sku_key, b.opr_sz_cd
FROM
(SELECT * FROM good_style_cd_1) a
INNER JOIN
(SELECT * FROM venus.sku_dim WHERE brd_key=${hiveconf:brand_flag}) b
ON
a.style_cd = b.opr_sty_cd
;


DROP TABLE IF EXISTS good_style_cd_3;
CREATE TABLE good_style_cd_3 AS
SELECT a.style_cd, a.opr_sty_cd, a.opr_sty_clr_cd,a.opr_sz_cd,  a.sku_key, b.sku_by_styl_cd, c.cnt_uniq_sz
from
(select * from good_style_cd_2) a
inner join
(select opr_sty_cd, count(distinct sku_key) as sku_by_styl_cd from good_style_cd_2 group by opr_sty_cd) b
on
a.opr_sty_cd=b.opr_sty_cd 
inner join
(select opr_sty_cd, count(distinct opr_sz_cd) as cnt_uniq_sz from good_style_cd_2 group by opr_sty_cd )c
on
a.opr_sty_cd=c.opr_sty_cd 
;


--get the product key for these products
DROP TABLE IF EXISTS good_style_cd_4;
CREATE TABLE good_style_cd_4 AS
SELECT a.*, b.product_key 
FROM
(SELECT * FROM good_style_cd_3) a
INNER JOIN
(SELECT * FROM mds_new.ods_product_t) b
ON
a.style_cd = b.style_cd and
a.opr_sty_clr_cd=b.style_color_cd
;


--get the sales amounts for these style codes using product key from oderline_t
DROP TABLE IF EXISTS good_style_cd_5;
CREATE TABLE good_style_cd_5 AS
SELECT a.*, b.sales_amt
FROM
(SELECT * FROM good_style_cd_4) a
INNER JOIN
(SELECT * FROM mds_new.ods_orderline_t where brand='${hiveconf:brand}' and transaction_date='${hiveconf:last_trans_dt}' and (!(sales_amt RLIKE '.99$') and !(sales_amt RLIKE '.97$') ) )b
ON
a.product_key=b.product_key
;

DROP TABLE IF EXISTS dt_key_value;
CREATE TABLE dt_key_value AS 
SELECT dt_key, this_cal_dt 
FROM 
venus.date_dim 
WHERE split(this_cal_dt, '[_]')[0]=='${hiveconf:end_date_inv}';

--get the style codes which have >0 units on the prev day. Do this using the year_month filter on sku_inv_dc_fct table. SKUs with >0 units is marked as gt_0_sku
DROP TABLE IF EXISTS  good_style_cd_6;
CREATE TABLE good_style_cd_6 AS 
SELECT a.*, b.sku_key as gt_0_sku, b.icm_inv_aval_qty
FROM
(SELECT * FROM good_style_cd_5) a
INNER JOIN
(SELECT * FROM venus.sku_inv_dc_fct WHERE brd_key=${hiveconf:brand_flag} AND year_month = ${hiveconf:inv_year_month}  AND icm_inv_aval_qty>0) b
ON
a.sku_key = b.sku_key
INNER JOIN
(SELECT * FROM dt_key_value) c
ON
b.inv_end_dt_key= c.dt_key
;

--example of year_month = 201409
--example of inv_end_dt_key=6810


DROP TABLE IF EXISTS good_style_cd_7;
CREATE TABLE good_style_cd_7 AS
SELECT DISTINCT opr_sty_cd, opr_sty_clr_cd, opr_sz_cd, sku_key,  gt_0_sku, icm_inv_aval_qty, sku_by_styl_cd, cnt_uniq_sz
FROM
good_style_cd_6
;


--count how may skus per style codes have >0 units available
DROP TABLE IF EXISTS good_style_cd_8;
CREATE TABLE good_style_cd_8 AS
SELECT DISTINCT  a.opr_sty_cd, a.sku_key, a.icm_inv_aval_qty, a.cnt_uniq_sz, a.sku_by_styl_cd,  b.cnt_in_stock_sz_cd 
FROM
(Select * from good_style_cd_7) a
inner join
(select opr_sty_cd, count(distinct opr_sz_cd) as cnt_in_stock_sz_cd  from good_style_cd_7 group by opr_sty_cd) b
on
a.opr_sty_cd = b.opr_sty_cd
;

--retain style codes which have > 60% of the sizes
DROP TABLE IF EXISTS good_style_cd_9;
CREATE TABLE good_style_cd_9 AS
SELECT *
FROM
good_style_cd_8
WHERE cnt_in_stock_sz_cd > 0.6*cnt_uniq_sz
;

DROP TABLE IF EXISTS  good_style_cd_10;
CREATE TABLE good_style_cd_10 AS 
SELECT DISTINCT opr_sty_cd as style_cd  
FROM 
good_style_cd_9
;


DROP TABLE IF EXISTS  good_style_cd;
CREATE TABLE good_style_cd 
(style_cd BIGINT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
LOCATION '${hiveconf:good_styles}'
;

INSERT OVERWRITE TABLE good_style_cd 
SELECT CAST(style_cd as INT) as style_cd 
FROM 
good_style_cd_10
WHERE (style_cd IS NOT NULL );

