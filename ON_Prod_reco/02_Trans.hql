
USE sdube_on;

SET start_date=2015-01-01;
SET end_date=2015-12-31;
SET trans_end_date=2015-12-31;
SET brand =ON;
SET country=US;
SET mapred.reduce.tasks=50;
SET mapred.job.queue.name=cem;

--BRING IN PURCHASE
--BRING IN TRANS TYPE S OR M onLY. BOTH onLINE and RETAIL TRANS
DROP TABLE IF EXISTS cust_ord_0;
CREATE TABLE cust_ord_0 AS 
SELECT customer_key, order_status, transaction_num, order_num, demand_date, ship_date, store_key, item_qty 
FROM 
mds_new.ods_orderheader_t 
WHERE 
brand ='${hiveconf:brand}' AND 
(
    (order_status='R' AND unix_timestamp(ship_date, 'yyyy-MM-dd_HH-mm-ss_Z') BETWEEN unix_timestamp('${hiveconf:start_date}','yyyy-MM-dd') and unix_timestamp('${hiveconf:end_date}','yyyy-MM-dd') ) 
    OR 
    (order_status='O'  AND unix_timestamp(demand_date, 'yyyy-MM-dd_HH-mm-ss_Z') BETWEEN unix_timestamp('${hiveconf:start_date}','yyyy-MM-dd') and unix_timestamp('${hiveconf:end_date}','yyyy-MM-dd') ) 
)
AND country = '${hiveconf:country}' 
AND (transaction_type_cd = 'S'  OR transaction_type_cd = 'M');



--create my_date and then filter on the dates to be BETWEEN start date and transaction end date. We are padding the transaction end date, since that is the date the order is shipped. and INNER JOIN helps rectify the issue of getting more transactions FROM orderline
DROP TABLE IF EXISTS cust_ord_1;
CREATE TABLE cust_ord_1 AS 
SELECT 
*, 
(CASE WHEN order_status='O'  THEN to_date(demand_date) ELSE to_date(ship_date) END) AS my_date 
FROM 
cust_ord_0;


DROP TABLE IF EXISTS cust_ord;
CREATE TABLE cust_ord AS 
SELECT * 
FROM  
cust_ord_1
WHERE my_date BETWEEN '${hiveconf:start_date}' AND '${hiveconf:trans_end_date}';


-- join the product key desc FROM orderline
DROP TABLE IF EXISTS cust_ord_ln;
CREATE TABLE cust_ord_ln AS
SELECT 
a.*, b.product_key, b.item_qty AS ln_qty
FROM 
cust_ord a
INNER JOIN
mds_new.ods_orderline_t b
ON 
a.transaction_num = b.transaction_num AND
a.customer_key = b.customer_key
WHERE 
b.transaction_date BETWEEN '${hiveconf:start_date}' AND '${hiveconf:trans_end_date}' AND b.brand='${hiveconf:brand}' AND country='${hiveconf:country}';


-- get the div, dept, class, season_cd, season_desc and style information FROM product_t.
DROP TABLE IF EXISTS cust_ord_ln_prod_0;
CREATE TABLE cust_ord_ln_prod_0 AS
SELECT
a.*, b.mdse_div_desc, b.mdse_dept_desc, b.mdse_class_desc, b.style_color_cd, b.style_cd, b.season_cd, b.season_desc
FROM 
cust_ord_ln a
INNER JOIN
(SELECT * FROM mds_new.ods_product_t ) b
ON
a.product_key = b.product_key
;

DROP TABLE IF EXISTS cust_ord_ln_prod;
CREATE TABLE cust_ord_ln_prod AS 
SELECT *, 
(CASE WHEN order_status='O'  THEN order_num ELSE CAST(transaction_num AS STRING) END) AS my_trans_num
FROM 
cust_ord_ln_prod_0; 


--join to customer resolution
DROP TABLE IF EXISTS masterkey_purch_style_cd;
CREATE TABLE masterkey_purch_style_cd AS 
SELECT DISTINCT b.masterkey, b.emailkey, b.ecommerceid, a.customer_key, a.order_status, a.transaction_num, a.order_num, a.demand_date, a.ship_date, a.store_key, a.my_date, a.product_key, a.ln_qty, a.mdse_div_desc, a.mdse_dept_desc, a.mdse_class_desc, a.style_color_cd, a.style_cd, a.season_cd, a.season_desc
FROM
(SELECT * from cust_ord_ln_prod) a
INNER JOIN
(SELECT * from all_keys) b
ON
a.customer_key = b.customerkey
;




