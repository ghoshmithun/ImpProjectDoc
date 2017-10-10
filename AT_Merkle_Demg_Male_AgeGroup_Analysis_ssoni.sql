------------------------- This code is for running an analysis on Athleta active and lapsed male customers in age group ranges of 5 years
------------------------ Requested by Ashley Beech
----------------------- First run date - 1st Aug 2017



-----------------Get the customer demographics data from Merkle database

create table ssoni.at_merkle_demg_test1 stored as orc as 
select (case when instr(cust_key,'E') = 0 then concat(substr(cust_key,0,instr(cust_key,'.')-1),'0') else concat(substr(cust_key,0,1),substr(cust_key,3,instr(cust_key,'E')-3)) end) as cust_key,
adj_net_wrth,mega_age,age,gndr 
from crm_reporting.cust_demg;
-- Mega_age has null values, age has 0 as value. There are more values in mega_age 52M vs 41M non-zero in age variable
-- There is no record where mega_age is null but age is non-zero, however there are many for whom mega_age is not null but age is zero 

----------------------- Get the first and last purchase date of all athleta customers

create table ssoni.AT_Custbase_Firstlast_Purchases stored as orc as 
select  customer_key,substr(max(txn_dt),1,10) as last_purchase_date, substr(min(txn_dt),1,10) as first_purchase_date
from mds_new.ods_orderheader_t
where brand = 'AT' and order_status <> 'D' and  item_qty > 0
group by customer_key;
-- 3,716,088

------------------------ JOin the customer_key woth ;
create table ssoni.AT_Custbase_Householdkey stored as orc as 
select a.customer_key,b.household_key
from ssoni.AT_Custbase_Firstlast_Purchases a 
inner join (select distinct customer_key,household_key from mds_new.ods_corp_cust_t where customer_key is not null and household_key is not null) b on a.customer_key = b.customer_key;
-- 3,583,394

-------------------------- Join the household_key to get other customers in the same household

create table ssoni.AT_Custbase_Householdkey_Allcustomers stored as orc as
select b.customer_key,a.household_key
from ssoni.AT_Custbase_Householdkey a 
inner join (select distinct customer_key,household_key from mds_new.ods_corp_cust_t where customer_key is not null and household_key is not null) b on a.household_key = b.household_key;
-- 4,876,403

------------------- Mark the customers that have no purchase in the past 12 months as lapsed. Change the dates here according to 12 month window

create table ssoni.AT_Custbase_Active_Lapsed_Customers stored as orc as 
select customer_key,(case when substr(last_purchase_date,1,10) < '2016-07-31' then 'lapsed' else 'active' end) as active_status
from ssoni.AT_Custbase_Firstlast_Purchases;
-- 3,716,088


---------------- Combine the demographics table with AT custbase

drop table if exists ssoni.AT_Custbase_Demographics;
create table ssoni.AT_Custbase_Demographics stored as orc as 
select distinct a.customer_key,mega_age as age,gndr as gender,net_worth,coalesce(active_status,'Non-AT-Customer') as active_status
from ssoni.AT_Custbase_Householdkey_Allcustomers a
left outer join ssoni.AT_Custbase_Active_Lapsed_Customers b on a.customer_key = b.customer_key
left outer join (select cust_key,adj_net_wrth as net_worth,mega_age,gndr from ssoni.at_merkle_demg_test1) c on cast(a.customer_key as string) = c.cust_key;

-------------------------- Get the data only for Male customers
drop table if exists ssoni.AT_Custbase_Demographics_Male;
create table ssoni.AT_Custbase_Demographics_Male stored as orc as 
select distinct  customer_key,age,gender,net_worth,active_status
from ssoni.AT_Custbase_Demographics
where age is not null and gender = 'M';
-- 658,141

----------------------- Create the external table to export
drop table if exists ssoni.AT_Custbase_Demographics_Male_Export;
create external table ssoni.AT_Custbase_Demographics_Male_Export
(customer_key string,
age int,
gender string,
net_worth float,
active_status string)
row format delimited 
fields terminated by '|' 
lines terminated by '\n' 
location '/user/ssoni/AT_Custbase_Demographics_Male_Export'
TBLPROPERTIES('serialization.null.format'='');

insert overwrite table ssoni.AT_Custbase_Demographics_Male_Export
select * from ssoni.AT_Custbase_Demographics_Male ;

exit;
hadoop fs -getmerge /user/ssoni/AT_Custbase_Demographics_Male_Export /home/ssoni